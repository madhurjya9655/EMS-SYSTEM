# FILE: apps/kam/views.py
from __future__ import annotations

from decimal import Decimal
from functools import wraps
from typing import Iterable, List, Dict, Optional, Tuple

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import redirect_to_login
from django.core.mail import EmailMessage
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.db import transaction
from django.db.models import Sum, Q
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseForbidden,
    StreamingHttpResponse,
    JsonResponse,
)
from django.shortcuts import render, redirect, get_object_or_404
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from apps.users.permissions import _user_permission_codes  # app-level codes

from .forms import (
    VisitPlanForm,
    VisitActualForm,
    CallForm,
    CollectionForm,
    TargetLineInlineForm,  # legacy, kept for compatibility only
    TargetSettingForm,  # SECTION F (kept; not used by redesigned manager targets)
    CollectionPlanForm,
    VisitBatchForm,
    MultiVisitPlanLineForm,
    ManagerTargetForm,  # NEW: manager targets UI/form (bulk + fixed 3 months)
)
from .models import (
    Customer,
    InvoiceFact,
    LeadFact,
    OverdueSnapshot,
    TargetHeader,  # legacy, kept for historical
    TargetLine,  # legacy, kept for historical
    TargetSetting,  # SECTION F
    VisitPlan,
    VisitActual,
    CallLog,
    CollectionTxn,
    VisitApprovalAudit,
    SyncIntent,
    CollectionPlan,
    VisitBatch,
    KamManagerMapping,
)
from . import sheets_adapter  # adapter with step_sync() and run_sync_now()


User = get_user_model()

# ---------------------------------------------------------------------
# Status constants (must align with models.py choices)
# ---------------------------------------------------------------------
STATUS_DRAFT = VisitBatch.DRAFT
STATUS_PENDING_APPROVAL = VisitBatch.PENDING_APPROVAL
STATUS_PENDING_LEGACY = VisitBatch.PENDING
STATUS_APPROVED = VisitBatch.APPROVED
STATUS_REJECTED = VisitBatch.REJECTED

# ---------------------------------------------------------------------
# Form prefixes (IMPORTANT: prevents duplicate HTML ids)
# ---------------------------------------------------------------------
SINGLE_PREFIX = "single"
BATCH_PREFIX = "batch"

# ---------------------------------------------------------------------
# Secure token signer for email approval links
# ---------------------------------------------------------------------
_BATCH_SIGNER = TimestampSigner(salt="kam.visitbatch.approval.v1")
BATCH_TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7  # 7 days


# ---------------------------------------------------------------------
# Group/role helpers (DO NOT rely on templates only)
# ---------------------------------------------------------------------
def _in_group(user, names: Tuple[str, ...]) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    try:
        return user.groups.filter(name__in=names).exists()
    except Exception:
        return False


def _is_admin(user) -> bool:
    return bool(getattr(user, "is_superuser", False) or _in_group(user, ("Admin",)))


def _is_manager(user) -> bool:
    # Keep your original group logic intact while tightening data scope via mapping
    return bool(_in_group(user, ("Manager", "Admin", "Finance")))


def _is_kam(user) -> bool:
    # If they can access the KAM module but not manager group, treat as KAM
    return bool(getattr(user, "is_authenticated", False) and not _is_manager(user))


def require_kam_code(code: str):
    """
    Decorator: require a specific KAM app-level permission code.
    Superusers bypass. Anonymous users are redirected to login.
    """
    required = (code or "").strip().lower()

    def _decorator(view_func):
        @wraps(view_func)
        def _wrapped(request: HttpRequest, *args, **kwargs):
            user = getattr(request, "user", None)
            if not getattr(user, "is_authenticated", False):
                return redirect_to_login(request.get_full_path())

            if getattr(user, "is_superuser", False):
                return view_func(request, *args, **kwargs)

            try:
                user_codes = _user_permission_codes(user)
            except Exception:
                user_codes = set()

            if {"*", "all"} & user_codes or required in user_codes:
                return view_func(request, *args, **kwargs)

            return HttpResponseForbidden("403 Forbidden: KAM permission required.")

        return _wrapped

    return _decorator


def require_any_kam_code(*codes: str):
    """
    Decorator: require ANY of the given KAM app-level permission codes.
    Superusers bypass. Anonymous users redirected to login.
    """
    required = {((c or "").strip().lower()) for c in codes if (c or "").strip()}

    def _decorator(view_func):
        @wraps(view_func)
        def _wrapped(request: HttpRequest, *args, **kwargs):
            user = getattr(request, "user", None)
            if not getattr(user, "is_authenticated", False):
                return redirect_to_login(request.get_full_path())

            if getattr(user, "is_superuser", False):
                return view_func(request, *args, **kwargs)

            try:
                user_codes = _user_permission_codes(user)
            except Exception:
                user_codes = set()

            if {"*", "all"} & user_codes:
                return view_func(request, *args, **kwargs)

            if required and (user_codes & required):
                return view_func(request, *args, **kwargs)

            return HttpResponseForbidden("403 Forbidden: KAM permission required.")

        return _wrapped

    return _decorator


# ---------------------------------------------------------------------
# IP helper
# ---------------------------------------------------------------------
def _get_ip(request: HttpRequest) -> Optional[str]:
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


# ---------------------------------------------------------------------
# Mail helper (best-effort; non-blocking)
# ---------------------------------------------------------------------
def _send_safe_mail(subject: str, body: str, to_users: List[User], cc_users: List[User] | None = None):
    try:
        to_emails = [u.email for u in to_users if getattr(u, "email", None)]
        cc_emails = [u.email for u in (cc_users or []) if getattr(u, "email", None)]
        if not to_emails and not cc_emails:
            return
        email = EmailMessage(subject=subject, body=body, to=to_emails, cc=cc_emails)
        email.content_subtype = "html" if "<html" in (body or "").lower() else "plain"
        email.send(fail_silently=True)
    except Exception:
        pass


# ---------------------------------------------------------------------
# Mapping helpers (CRITICAL: manager routing and manager RBAC)
# ---------------------------------------------------------------------
def _active_manager_for_kam(kam_user: User) -> Optional[User]:
    if not kam_user or not getattr(kam_user, "id", None):
        return None
    m = (
        KamManagerMapping.objects.select_related("manager")
        .filter(kam=kam_user, active=True)
        .order_by("-assigned_at", "-created_at")
        .first()
    )
    return m.manager if m else None


def _kams_managed_by_manager(manager_user: User) -> List[int]:
    if not manager_user or not getattr(manager_user, "id", None):
        return []
    if _is_admin(manager_user):
        # Admin can see all KAMs
        return list(User.objects.filter(is_active=True).values_list("id", flat=True))
    return list(
        KamManagerMapping.objects.filter(manager=manager_user, active=True).values_list("kam_id", flat=True).distinct()
    )


# ---------------------------------------------------------------------
# Queryset scope helpers (STRICT at queryset level)
# ---------------------------------------------------------------------
def _customer_qs_for_user(user: User):
    """
    KAM: only their customers (ownership by kam or primary_kam).
    Manager: only customers of KAMs assigned to them.
    Admin: all customers.
    """
    qs = Customer.objects.all()

    if _is_admin(user):
        return qs

    if _is_manager(user):
        kam_ids = _kams_managed_by_manager(user)
        return qs.filter(Q(kam_id__in=kam_ids) | Q(primary_kam_id__in=kam_ids))

    # KAM scope
    return qs.filter(Q(kam=user) | Q(primary_kam=user))


def _visitbatch_qs_for_user(user: User):
    qs = VisitBatch.objects.select_related("kam")
    if _is_admin(user):
        return qs
    if _is_manager(user):
        kam_ids = _kams_managed_by_manager(user)
        return qs.filter(kam_id__in=kam_ids)
    return qs.filter(kam=user)


def _visitplan_qs_for_user(user: User):
    qs = VisitPlan.objects.select_related("customer", "kam", "batch")
    if _is_admin(user):
        return qs
    if _is_manager(user):
        kam_ids = _kams_managed_by_manager(user)
        return qs.filter(kam_id__in=kam_ids)
    return qs.filter(kam=user)


# ---------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------
def _parse_iso_date(s: str) -> Optional[timezone.datetime.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return timezone.datetime.fromisoformat(s).date()
    except Exception:
        return None


def _safe_decimal(val) -> Decimal:
    try:
        return Decimal(val or 0)
    except Exception:
        return Decimal(0)


def _parse_decimal_or_none(s: str) -> Optional[Decimal]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


# ---------------------------------------------------------------------
# Period helpers (existing)
# ---------------------------------------------------------------------
def _iso_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=7)
    iso_year, iso_week, _ = start.isocalendar()
    period_id = f"{iso_year}-W{iso_week:02d}"
    return start, end, period_id


def _ms_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())  # Monday
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=6)  # Sunday 00:00 (end-exclusive)
    iso_year, iso_week, _ = start.isocalendar()
    period_id = f"{iso_year}-W{iso_week:02d}"
    return start, end, period_id


def _last_completed_ms_week_end(dt: timezone.datetime) -> timezone.datetime:
    start, end, _ = _ms_week_bounds(dt)
    now_local = timezone.localtime(dt)
    if now_local < end:
        return start - timezone.timedelta(days=1)
    return end


def _month_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    period_id = f"{start.year}-{start.month:02d}"
    return start, end, period_id


def _quarter_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    q = (local.month - 1) // 3 + 1
    start_month = 3 * (q - 1) + 1
    start = local.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = start_month + 3
    if end_month > 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=end_month)
    pid = f"{start.year}-Q{q}"
    return start, end, pid


def _year_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1)
    pid = f"{start.year}"
    return start, end, pid


def _parse_ymd_part(s: str) -> Optional[timezone.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if len(s) == 4:
            y, m, d = int(s), 1, 1
        elif len(s) == 7 and s[4] == "-":
            y, m, d = int(s[0:4]), int(s[5:7]), 1
        elif len(s) == 10 and s[4] == "-" and s[7] == "-":
            y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        else:
            return None
        naive = timezone.datetime(y, m, d, 0, 0, 0)
        return timezone.make_aware(naive)
    except Exception:
        return None


def _get_period(request: HttpRequest) -> Tuple[str, timezone.datetime, timezone.datetime, str]:
    from_q = request.GET.get("from")
    to_q = request.GET.get("to")
    if from_q and to_q:
        s = _parse_ymd_part(from_q)
        e = _parse_ymd_part(to_q)
        if s and e and s <= e:
            e = e + timezone.timedelta(days=1)
            return "CUSTOM", s, e, f"{s.date()}..{(e - timezone.timedelta(days=1)).date()}"

    p = (request.GET.get("period") or "").lower()
    ref = _parse_ymd_part(request.GET.get("asof")) or timezone.now()

    if p == "month":
        return ("MONTH", *_month_bounds(ref))
    if p == "quarter":
        return ("QUARTER", *_quarter_bounds(ref))
    if p == "year":
        return ("YEAR", *_year_bounds(ref))
    if p == "all":
        s = timezone.make_aware(timezone.datetime(2000, 1, 1))
        e = timezone.make_aware(timezone.datetime(2100, 1, 1))
        return "ALL", s, e, "ALL"

    return ("WEEK", *_iso_week_bounds(ref))


def _get_dashboard_range(request: HttpRequest) -> Tuple[timezone.datetime, timezone.datetime, str]:
    from_s = (request.GET.get("from") or "").strip()
    to_s = (request.GET.get("to") or "").strip()

    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)
    if from_d and to_d and from_d <= to_d:
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        end = (
            timezone.make_aware(timezone.datetime(to_d.year, to_d.month, to_d.day, 0, 0, 0))
            + timezone.timedelta(days=1)
        )
        label = f"{from_d} → {to_d}"
        return start, end, label

    ws, we, _ = _iso_week_bounds(timezone.now())
    label = f"{ws.date()} → {(we - timezone.timedelta(days=1)).date()}"
    return ws, we, label


def _resolve_scope(request: HttpRequest, actor: User) -> Tuple[Optional[int], str]:
    """
    Scope rules:
      - KAM: always self (ignore ?user)
      - Manager: only KAMs assigned to them (optional ?user=username)
      - Admin: may choose any user
    """
    if not _is_manager(actor):
        return actor.id, actor.username

    uname = (request.GET.get("user") or "").strip()
    if uname:
        u = User.objects.filter(username=uname, is_active=True).first()
        if not u:
            return None, "ALL"

        if _is_admin(actor):
            return u.id, u.username

        allowed = set(_kams_managed_by_manager(actor))
        if u.id in allowed:
            return u.id, u.username
        return None, "ALL"

    return None, "ALL"


# ---------------------------------------------------------------------
# Target setting helper (existing)
# ---------------------------------------------------------------------
def _target_setting_for_kam_window(kam_id: int, start_date, end_date_inclusive) -> Optional[TargetSetting]:
    if not kam_id:
        return None
    return (
        TargetSetting.objects.filter(kam_id=kam_id, from_date__lte=start_date, to_date__gte=end_date_inclusive)
        .order_by("-created_at", "from_date")
        .first()
    )


# ---------------------------------------------------------------------
# Customer 360 strict filter (existing)
# ---------------------------------------------------------------------
def _get_customer360_range(request: HttpRequest) -> Tuple[str, timezone.datetime.date, timezone.datetime.date, str]:
    from_s = (request.GET.get("from") or "").strip()
    to_s = (request.GET.get("to") or "").strip()
    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)

    if from_d and to_d and from_d <= to_d:
        return "CUSTOM", from_d, to_d, f"{from_d}..{to_d}"

    p = (request.GET.get("period") or "week").strip().lower()
    now = timezone.now()

    if p == "month":
        sdt, edt, pid = _month_bounds(now)
        return "MONTH", sdt.date(), (edt - timezone.timedelta(days=1)).date(), pid
    if p == "quarter":
        sdt, edt, pid = _quarter_bounds(now)
        return "QUARTER", sdt.date(), (edt - timezone.timedelta(days=1)).date(), pid
    if p == "year":
        sdt, edt, pid = _year_bounds(now)
        return "YEAR", sdt.date(), (edt - timezone.timedelta(days=1)).date(), pid
    if p == "all":
        return "ALL", timezone.datetime(2000, 1, 1).date(), timezone.datetime(2100, 1, 1).date(), "ALL"

    sdt, edt, pid = _iso_week_bounds(now)
    return "WEEK", sdt.date(), (edt - timezone.timedelta(days=1)).date(), pid


def _add_months(d: timezone.datetime.date, months: int) -> timezone.datetime.date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    if m == 12:
        next_month_first = timezone.datetime(y + 1, 1, 1).date()
    else:
        next_month_first = timezone.datetime(y, m + 1, 1).date()
    last_day = next_month_first - timezone.timedelta(days=1)
    day = min(d.day, last_day.day)
    return timezone.datetime(y, m, day).date()


# ---------------------------------------------------------------------
# Approval token helpers
# ---------------------------------------------------------------------
def _make_batch_token(batch_id: int, action: str) -> str:
    raw = f"{batch_id}:{(action or '').strip().upper()}"
    return _BATCH_SIGNER.sign(raw)


def _parse_batch_token(token: str) -> Tuple[int, str]:
    value = _BATCH_SIGNER.unsign(token, max_age=BATCH_TOKEN_MAX_AGE_SECONDS)
    parts = (value or "").split(":", 1)
    if len(parts) != 2:
        raise BadSignature("invalid token payload")
    bid = int(parts[0])
    action = (parts[1] or "").strip().upper()
    if action not in {"APPROVE", "REJECT"}:
        raise BadSignature("invalid token action")
    return bid, action


# ---------------------------------------------------------------------
# Admin UI: KAM → Manager Mapping (Admin Only)
# ---------------------------------------------------------------------
@login_required
@require_any_kam_code("kam_manager", "kam_dashboard", "kam_plan")
def admin_kam_manager_mapping(request: HttpRequest) -> HttpResponse:
    if not _is_admin(request.user):
        return HttpResponseForbidden("403 Forbidden: Admin access required.")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip().lower()

        if action in {"assign", "update"}:
            kam_id = (request.POST.get("kam_id") or "").strip()
            manager_id = (request.POST.get("manager_id") or "").strip()
            if not (kam_id.isdigit() and manager_id.isdigit()):
                messages.error(request, "Invalid KAM/Manager selection.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))

            if kam_id == manager_id:
                messages.error(request, "KAM and Manager cannot be the same user.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))

            kam_user = User.objects.filter(id=int(kam_id), is_active=True).first()
            mgr_user = User.objects.filter(id=int(manager_id), is_active=True).first()
            if not kam_user or not mgr_user:
                messages.error(request, "Invalid KAM/Manager user.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))

            with transaction.atomic():
                KamManagerMapping.objects.filter(kam=kam_user, active=True).update(active=False, updated_at=timezone.now())
                KamManagerMapping.objects.create(
                    kam=kam_user,
                    manager=mgr_user,
                    assigned_by=request.user,
                    active=True,
                )
            messages.success(request, f"Assigned manager for {kam_user.username} → {mgr_user.username}.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

        if action in {"remove", "deactivate"}:
            map_id = (request.POST.get("mapping_id") or "").strip()
            if not map_id.isdigit():
                messages.error(request, "Invalid mapping id.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))

            with transaction.atomic():
                m = KamManagerMapping.objects.select_for_update().filter(id=int(map_id)).first()
                if not m:
                    messages.error(request, "Mapping not found.")
                    return redirect(reverse("kam:admin_kam_manager_mapping"))
                m.active = False
                m.save(update_fields=["active", "updated_at"])
            messages.success(request, "Mapping deactivated.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

        messages.error(request, "Unknown action.")
        return redirect(reverse("kam:admin_kam_manager_mapping"))

    active_only = (request.GET.get("active") or "1").strip() != "0"

    mappings = KamManagerMapping.objects.select_related("kam", "manager", "assigned_by").order_by("-active", "-assigned_at")
    if active_only:
        mappings = mappings.filter(active=True)

    all_users = User.objects.filter(is_active=True).order_by("username")
    kam_users = [u for u in all_users if not _is_manager(u)]
    manager_users = [u for u in all_users if _is_manager(u)]

    ctx = {
        "page_title": "KAM → Manager Mapping",
        "rows": list(mappings[:500]),
        "kam_users": kam_users,
        "manager_users": manager_users,
        "active_only": active_only,
    }
    return render(request, "kam/admin_kam_manager_mapping.html", ctx)


# ---------------------------------------------------------------------
# Dashboard (existing, only tightened scope to mapping for managers)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_dashboard")
def dashboard(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    sales_target_mt = Decimal(0)
    calls_target = 0
    leads_target_mt = Decimal(0)
    collections_plan_amount = Decimal(0)
    visits_target = 6

    if scope_kam_id:
        start_date = start_dt.date()
        end_date_inclusive = (end_dt - timezone.timedelta(days=1)).date()
        ts = _target_setting_for_kam_window(scope_kam_id, start_date, end_date_inclusive)
        if ts:
            sales_target_mt = _safe_decimal(ts.sales_target_mt)
            calls_target = int(ts.calls_target or 0)
            leads_target_mt = _safe_decimal(ts.leads_target_mt)
            collections_plan_amount = _safe_decimal(ts.collections_target_amount)

    inv_qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
    visit_plan_qs = VisitPlan.objects.filter(visit_date__gte=start_dt.date(), visit_date__lt=end_dt.date())
    visit_act_qs = VisitActual.objects.filter(plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date())
    call_qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
    lead_qs = LeadFact.objects.filter(doe__gte=start_dt.date(), doe__lt=end_dt.date())
    coll_qs = CollectionTxn.objects.filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)

    if scope_kam_id is not None:
        inv_qs = inv_qs.filter(kam_id=scope_kam_id)
        visit_plan_qs = visit_plan_qs.filter(kam_id=scope_kam_id)
        visit_act_qs = visit_act_qs.filter(plan__kam_id=scope_kam_id)
        call_qs = call_qs.filter(kam_id=scope_kam_id)
        lead_qs = lead_qs.filter(kam_id=scope_kam_id)
        coll_qs = coll_qs.filter(kam_id=scope_kam_id)

    sales_mt = _safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt")).get("mt"))
    visits_planned = visit_plan_qs.count()
    visits_actual = visit_act_qs.count()
    visits_successful = visit_act_qs.filter(successful=True).count()
    calls = call_qs.count()

    leads_agg = lead_qs.aggregate(total_mt=Sum("qty_mt"), won_mt=Sum("qty_mt", filter=Q(status="WON")))
    leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
    leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))

    collections_actual = _safe_decimal(coll_qs.aggregate(total_amt=Sum("amount")).get("total_amt"))

    overdue_snapshot_date = None
    prev_overdue_snapshot_date = None
    credit_limit_sum = Decimal(0)
    exposure_sum = Decimal(0)
    overdue_sum = Decimal(0)
    prev_overdue_sum = Decimal(0)

    if scope_kam_id is not None:
        customer_ids = list(
            Customer.objects.filter(Q(kam_id=scope_kam_id) | Q(primary_kam_id=scope_kam_id)).values_list("id", flat=True)
        )
    else:
        customer_ids = list(_customer_qs_for_user(request.user).values_list("id", flat=True))

    if customer_ids:
        credit_limit_sum = _safe_decimal(
            Customer.objects.filter(id__in=customer_ids).aggregate(total_cl=Sum("credit_limit")).get("total_cl")
        )

        end_date_inclusive = (end_dt - timezone.timedelta(days=1)).date()
        start_date_inclusive = start_dt.date()

        overdue_snapshot_date = (
            OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date__lte=end_date_inclusive)
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )
        if overdue_snapshot_date:
            agg = (
                OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date=overdue_snapshot_date).aggregate(
                    total_exposure=Sum("exposure"),
                    total_overdue=Sum("overdue"),
                    a0=Sum("ageing_0_30"),
                    a1=Sum("ageing_31_60"),
                    a2=Sum("ageing_61_90"),
                    a3=Sum("ageing_90_plus"),
                )
            )
            exposure_sum = _safe_decimal(agg.get("total_exposure"))
            overdue_sum = _safe_decimal(agg.get("total_overdue"))
            if not exposure_sum:
                ageing_sum = _safe_decimal(agg.get("a0")) + _safe_decimal(agg.get("a1")) + _safe_decimal(agg.get("a2")) + _safe_decimal(agg.get("a3"))
                if ageing_sum:
                    exposure_sum = ageing_sum
            if not exposure_sum and overdue_sum:
                exposure_sum = overdue_sum

        prev_overdue_snapshot_date = (
            OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date__lt=start_date_inclusive)
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )
        if prev_overdue_snapshot_date:
            agg2 = (
                OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date=prev_overdue_snapshot_date).aggregate(
                    total_overdue=Sum("overdue")
                )
            )
            prev_overdue_sum = _safe_decimal(agg2.get("total_overdue"))

    def _pct(n: Decimal, d: Decimal) -> Optional[Decimal]:
        if d and d != 0:
            return (n / d) * Decimal("100")
        return None

    sales_ach_pct = _pct(sales_mt, sales_target_mt) if sales_target_mt else None
    visit_ach_pct = _pct(Decimal(visits_actual), Decimal(visits_target)) if visits_target else None
    call_ach_pct = _pct(Decimal(calls), Decimal(calls_target)) if calls_target else None
    lead_conv_pct = _pct(leads_won_mt, leads_total_mt) if leads_total_mt else None
    coll_eff_pct = _pct(collections_actual, collections_plan_amount) if collections_plan_amount else None
    overdue_reduction_pct = _pct(prev_overdue_sum - overdue_sum, prev_overdue_sum) if prev_overdue_sum else None
    overdue_risk_ratio = (exposure_sum / credit_limit_sum) if credit_limit_sum else None
    visit_success_pct = _pct(Decimal(visits_successful), Decimal(visits_actual)) if visits_actual else None

    prod_by_grade = list(inv_qs.values("grade").annotate(mt=Sum("qty_mt")).order_by("-mt"))
    prod_by_size = list(inv_qs.values("size").annotate(mt=Sum("qty_mt")).order_by("-mt"))

    trend_rows: List[Dict] = []
    anchor_end = _last_completed_ms_week_end(timezone.now())

    for k in (3, 2, 1, 0):
        end_i = anchor_end - timezone.timedelta(days=7 * k)
        start_i = end_i - timezone.timedelta(days=6)

        _, __, pid_i = _ms_week_bounds(start_i)

        inv_i = InvoiceFact.objects.filter(invoice_date__gte=start_i.date(), invoice_date__lt=end_i.date())
        vis_i = VisitActual.objects.filter(plan__visit_date__gte=start_i.date(), plan__visit_date__lt=end_i.date())
        calls_i = CallLog.objects.filter(call_datetime__gte=start_i, call_datetime__lt=end_i)
        coll_i = CollectionTxn.objects.filter(txn_datetime__gte=start_i, txn_datetime__lt=end_i)

        if scope_kam_id is not None:
            inv_i = inv_i.filter(kam_id=scope_kam_id)
            vis_i = vis_i.filter(plan__kam_id=scope_kam_id)
            calls_i = calls_i.filter(kam_id=scope_kam_id)
            coll_i = coll_i.filter(kam_id=scope_kam_id)

        trend_rows.append(
            {
                "week": pid_i,
                "sales_mt": _safe_decimal(inv_i.aggregate(mt=Sum("qty_mt")).get("mt")),
                "visits": vis_i.count(),
                "calls": calls_i.count(),
                "collections": _safe_decimal(coll_i.aggregate(a=Sum("amount")).get("a")),
            }
        )

    kam_options: List[str] = []
    if _is_manager(request.user):
        if _is_admin(request.user):
            kam_options = list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
        else:
            allowed_ids = _kams_managed_by_manager(request.user)
            kam_options = list(
                User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True)
            )

    ctx = {
        "page_title": "KAM Dashboard",
        "range_label": range_label,
        "can_choose_kam": _is_manager(request.user),
        "scope_label": scope_label,
        "kam_options": kam_options,
        "kpi": {
            "sales_mt": sales_mt,
            "sales_target_mt": sales_target_mt,
            "sales_ach_pct": sales_ach_pct,
            "visits_planned": visits_planned,
            "visits_actual": visits_actual,
            "visits_target": visits_target,
            "visit_ach_pct": visit_ach_pct,
            "visit_success_pct": visit_success_pct,
            "calls": calls,
            "calls_target": calls_target,
            "call_ach_pct": call_ach_pct,
            "leads_total_mt": leads_total_mt,
            "leads_won_mt": leads_won_mt,
            "lead_conv_pct": lead_conv_pct,
            "collections_actual": collections_actual,
            "collections_eff_pct": coll_eff_pct,
            "overdue_sum": overdue_sum,
            "prev_overdue_sum": prev_overdue_sum,
            "overdue_reduction_pct": overdue_reduction_pct,
            "credit_limit_sum": credit_limit_sum,
            "exposure_sum": exposure_sum,
            "overdue_risk_ratio": overdue_risk_ratio,
            "overdue_snapshot_date": overdue_snapshot_date,
            "prev_overdue_snapshot_date": prev_overdue_snapshot_date,
        },
        "prod_by_grade": prod_by_grade,
        "prod_by_size": prod_by_size,
        "trend_rows": trend_rows,
    }
    return render(request, "kam/kam_dashboard.html", ctx)


# ---------------------------------------------------------------------
# Manager summary (existing)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_manager")
def manager_dashboard(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    today_start = timezone.localtime(timezone.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today_start + timezone.timedelta(days=1)
    calls_today = CallLog.objects.filter(call_datetime__gte=today_start, call_datetime__lt=tomorrow).count()
    visits_today = VisitActual.objects.filter(plan__visit_date__gte=today_start.date(), plan__visit_date__lt=tomorrow.date()).count()
    collections_today = _safe_decimal(
        CollectionTxn.objects.filter(txn_datetime__gte=today_start, txn_datetime__lt=tomorrow).aggregate(a=Sum("amount"))["a"]
    )
    ctx = {
        "page_title": "Manager Dashboard",
        "kpi": {
            "calls_today": calls_today,
            "visits_today": visits_today,
            "collections_today": collections_today,
        },
    }
    return render(request, "kam/manager_dashboard.html", ctx)


@login_required
@require_kam_code("kam_manager_kpis")
def manager_kpis(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    period_type, start_dt, end_dt, period_id = _get_period(request)

    if _is_admin(request.user):
        kams = User.objects.filter(is_active=True).order_by("username")
    else:
        allowed_ids = _kams_managed_by_manager(request.user)
        kams = User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username")

    def _pct(n: Decimal, d: Decimal) -> Optional[Decimal]:
        if d and d != 0:
            return (n / d) * Decimal("100")
        return None

    latest_snap_date = OverdueSnapshot.objects.order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()

    rows: List[Dict] = []
    for kam in kams:
        inv_qs = InvoiceFact.objects.filter(kam=kam, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        sales_mt = _safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt")).get("mt"))

        visits_qs = VisitActual.objects.filter(plan__kam=kam, plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date())
        visits_actual = visits_qs.count()
        visits_successful = visits_qs.filter(successful=True).count()
        visit_success_pct = _pct(Decimal(visits_successful), Decimal(visits_actual)) if visits_actual else None

        calls = CallLog.objects.filter(kam=kam, call_datetime__gte=start_dt, call_datetime__lt=end_dt).count()

        collections_actual = _safe_decimal(
            CollectionTxn.objects.filter(kam=kam, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt).aggregate(a=Sum("amount")).get("a")
        )

        leads_agg = LeadFact.objects.filter(kam=kam, doe__gte=start_dt.date(), doe__lt=end_dt.date()).aggregate(
            total_mt=Sum("qty_mt"), won_mt=Sum("qty_mt", filter=Q(status="WON"))
        )
        leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
        leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))
        lead_conv_pct = _pct(leads_won_mt, leads_total_mt) if leads_total_mt else None

        credit_limit_sum = _safe_decimal(Customer.objects.filter(Q(kam=kam) | Q(primary_kam=kam)).aggregate(s=Sum("credit_limit")).get("s"))
        exposure_sum = overdue_sum = Decimal(0)

        if latest_snap_date:
            cust_ids = list(Customer.objects.filter(Q(kam=kam) | Q(primary_kam=kam)).values_list("id", flat=True))
            if cust_ids:
                agg = OverdueSnapshot.objects.filter(customer_id__in=cust_ids, snapshot_date=latest_snap_date).aggregate(
                    exposure=Sum("exposure"),
                    overdue=Sum("overdue"),
                    a0=Sum("ageing_0_30"),
                    a31=Sum("ageing_31_60"),
                    a61=Sum("ageing_61_90"),
                    a90=Sum("ageing_90_plus"),
                )
                exposure_sum = _safe_decimal(agg.get("exposure"))
                overdue_sum = _safe_decimal(agg.get("overdue"))
                if not exposure_sum:
                    ageing_sum = _safe_decimal(agg.get("a0")) + _safe_decimal(agg.get("a31")) + _safe_decimal(agg.get("a61")) + _safe_decimal(agg.get("a90"))
                    if ageing_sum:
                        exposure_sum = ageing_sum
                if not exposure_sum and overdue_sum:
                    exposure_sum = overdue_sum

        risk_ratio = (exposure_sum / credit_limit_sum) if credit_limit_sum else None

        rows.append(
            {
                "kam": kam,
                "sales_mt": sales_mt,
                "visits_actual": visits_actual,
                "visit_success_pct": visit_success_pct,
                "calls": calls,
                "collections_actual": collections_actual,
                "lead_conv_pct": lead_conv_pct,
                "risk_ratio": risk_ratio,
            }
        )

    ctx = {
        "page_title": "Manager KPIs",
        "period_type": period_type,
        "period_id": period_id,
        "rows": rows,
        "risky": [],
    }
    return render(request, "kam/manager_kpis.html", ctx)


# ---------------------------------------------------------------------
# Plan Visit: redesigned page (Single Draft + Batch primary)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def weekly_plan(request: HttpRequest) -> HttpResponse:
    user = request.user
    customer_qs = _customer_qs_for_user(user).order_by("name")

    single_form = VisitPlanForm(prefix=SINGLE_PREFIX)
    batch_form = VisitBatchForm(prefix=BATCH_PREFIX)

    if "customer" in single_form.fields:
        single_form.fields["customer"].queryset = customer_qs
    if "customers" in batch_form.fields:
        batch_form.fields["customers"].queryset = customer_qs

    # -------------------------
    # POST: Single (Draft only)
    # -------------------------
    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "single":
        single_form = VisitPlanForm(request.POST, prefix=SINGLE_PREFIX)
        if "customer" in single_form.fields:
            single_form.fields["customer"].queryset = customer_qs

        if single_form.is_valid():
            plan: VisitPlan = single_form.save(commit=False)
            plan.kam = user

            if plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if plan.customer_id and not customer_qs.filter(id=plan.customer_id).exists():
                    messages.error(request, "Invalid customer selection (out of your scope).")
                    return redirect(reverse("kam:plan"))
            else:
                # keep customer empty for non-customer categories
                plan.customer = None

            if not (plan.location or "").strip():
                if plan.visit_category == VisitPlan.CAT_CUSTOMER and plan.customer and plan.customer.address:
                    plan.location = plan.customer.address

            plan.approval_status = STATUS_DRAFT
            plan.save()

            messages.success(request, "Single visit saved as Draft.")
            return redirect(reverse("kam:plan"))

        messages.error(request, "Single visit has errors. Please correct and save again.")

    # -------------------------
    # POST: Batch (Primary)
    # -------------------------
    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "batch":
        batch_form = VisitBatchForm(request.POST, prefix=BATCH_PREFIX)
        if "customers" in batch_form.fields:
            batch_form.fields["customers"].queryset = customer_qs

        action = (request.POST.get("action") or request.POST.get("submit_action") or "").strip().lower()
        proceed_flag = action in {"proceed", "proceed_to_manager", "proceed-manager", "manager", "proceed_to_manager_btn"}

        if not batch_form.is_valid():
            messages.error(request, "Batch submission has errors. Please correct and re-submit.")
        else:
            visit_category = batch_form.cleaned_data.get("visit_category")
            from_date = batch_form.cleaned_data.get("from_date")
            to_date = batch_form.cleaned_data.get("to_date")
            remarks = (batch_form.cleaned_data.get("purpose") or "").strip()

            # UI provides customers_selected[] else fallback to legacy multi-select
            selected_ids: List[int] = []
            if request.POST.getlist("customers_selected[]"):
                for s in request.POST.getlist("customers_selected[]"):
                    try:
                        selected_ids.append(int(s))
                    except Exception:
                        continue
            else:
                customers_selected = batch_form.cleaned_data.get("customers") or []
                selected_ids = [c.id for c in customers_selected]

            # Non-customer multi-lines
            non_customer_lines: List[MultiVisitPlanLineForm] = []
            if visit_category in (VisitPlan.CAT_SUPPLIER, VisitPlan.CAT_WAREHOUSE, VisitPlan.CAT_VENDOR):
                names = request.POST.getlist("counterparty_name[]")
                locs = request.POST.getlist("counterparty_location[]")
                purs = request.POST.getlist("counterparty_purpose[]")
                max_n = max(len(names), len(locs), len(purs))
                for i in range(max_n):
                    f = MultiVisitPlanLineForm(
                        {
                            "counterparty_name": (names[i] if i < len(names) else "").strip(),
                            "counterparty_location": (locs[i] if i < len(locs) else "").strip(),
                            "counterparty_purpose": (purs[i] if i < len(purs) else "").strip(),
                        }
                    )
                    if f.is_valid() and (f.cleaned_data.get("counterparty_name") or "").strip():
                        non_customer_lines.append(f)

            # -------------------------
            # Proceed to Manager (Customer-only)
            # -------------------------
            if proceed_flag:
                if visit_category != VisitPlan.CAT_CUSTOMER:
                    messages.error(request, "Proceed to Manager is allowed only for Customer Visit batches.")
                    return redirect(reverse("kam:plan"))

                if not (from_date and to_date):
                    messages.error(request, "From/To dates are required.")
                    return redirect(reverse("kam:plan"))

                if not selected_ids:
                    messages.error(request, "Select at least one customer to proceed.")
                    return redirect(reverse("kam:plan"))

                if not remarks:
                    messages.error(request, "Remarks are required to proceed to Manager.")
                    return redirect(reverse("kam:plan"))

                mgr_user = _active_manager_for_kam(user)
                if not mgr_user or not getattr(mgr_user, "email", None):
                    messages.error(request, "No manager is assigned for you. Contact admin to set KAM → Manager mapping.")
                    return redirect(reverse("kam:plan"))

                allowed_ids = set(customer_qs.values_list("id", flat=True))
                if not set(selected_ids).issubset(allowed_ids):
                    messages.error(request, "Invalid customer selection (out of your scope).")
                    return redirect(reverse("kam:plan"))

                customers_selected = list(Customer.objects.filter(id__in=selected_ids).order_by("name"))

                line_rows: List[Dict] = []
                parse_errors = False

                for cust in customers_selected:
                    vd = _parse_iso_date(request.POST.get(f"visit_date_{cust.id}") or "") or from_date
                    vdt = _parse_iso_date(request.POST.get(f"visit_date_to_{cust.id}") or "") or to_date
                    if vd and vdt and vdt < vd:
                        messages.error(request, f"To date cannot be earlier than From date for customer: {cust.name}")
                        parse_errors = True

                    lp = (request.POST.get(f"purpose_{cust.id}") or "").strip()
                    loc = (request.POST.get(f"location_{cust.id}") or "").strip() or (cust.address or "").strip()

                    es_raw = request.POST.get(f"expected_sales_mt_{cust.id}") or ""
                    ec_raw = request.POST.get(f"expected_collection_{cust.id}") or ""

                    expected_sales = _parse_decimal_or_none(es_raw)
                    expected_coll = _parse_decimal_or_none(ec_raw)

                    if es_raw.strip() != "" and expected_sales is None:
                        messages.error(request, f"Expected Sales (MT) is invalid for customer: {cust.name}")
                        parse_errors = True
                    if ec_raw.strip() != "" and expected_coll is None:
                        messages.error(request, f"Expected Collection (₹) is invalid for customer: {cust.name}")
                        parse_errors = True

                    line_rows.append(
                        {
                            "customer": cust,
                            "visit_date": vd,
                            "visit_date_to": vdt,
                            "purpose": lp or None,
                            "location": loc or "",
                            "expected_sales_mt": expected_sales,
                            "expected_collection": expected_coll,
                        }
                    )

                if parse_errors:
                    return redirect(reverse("kam:plan"))

                with transaction.atomic():
                    # Duplicate prevention (same KAM + window + remarks + same customer set)
                    existing_qs = (
                        VisitBatch.objects.select_for_update()
                        .filter(
                            kam=user,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            from_date=from_date,
                            to_date=to_date,
                            purpose=remarks,
                            approval_status__in=[STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY],
                        )
                        .order_by("-created_at")
                    )
                    for b in existing_qs[:10]:
                        existing_ids = list(
                            VisitPlan.objects.filter(batch=b, customer_id__isnull=False).values_list("customer_id", flat=True)
                        )
                        if sorted(existing_ids) == sorted(selected_ids):
                            messages.error(
                                request,
                                f"Duplicate submission blocked: Batch #{b.id} is already pending approval with same customers.",
                            )
                            return redirect(reverse("kam:plan"))

                    # Create consolidated batch + N plans
                    batch: VisitBatch = batch_form.save(commit=False)
                    batch.kam = user
                    batch.visit_category = VisitPlan.CAT_CUSTOMER
                    batch.from_date = from_date
                    batch.to_date = to_date
                    batch.purpose = remarks
                    batch.approval_status = STATUS_PENDING_APPROVAL
                    batch.save()

                    for r in line_rows:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=r["customer"],
                            kam=user,
                            visit_date=r["visit_date"],
                            visit_date_to=r["visit_date_to"],
                            visit_type=VisitPlan.PLANNED,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            purpose=r["purpose"],
                            expected_sales_mt=r["expected_sales_mt"],
                            expected_collection=r["expected_collection"],
                            location=r["location"],
                            approval_status=STATUS_PENDING_APPROVAL,
                        )

                    approve_token = _make_batch_token(batch.id, "APPROVE")
                    reject_token = _make_batch_token(batch.id, "REJECT")

                    approve_url = request.build_absolute_uri(reverse("kam:visit_batch_approve_link", args=[approve_token]))
                    reject_url = request.build_absolute_uri(reverse("kam:visit_batch_reject_link", args=[reject_token]))

                    html_body = ""
                    try:
                        html_body = render_to_string(
                            "kam/emails/visit_batch_approval.html",
                            {
                                "batch": batch,
                                "kam_user": user,
                                "visit_category_label": "Customer Visit",
                                "date_range": f"{batch.from_date} → {batch.to_date}",
                                "remarks": remarks,
                                "customers": [r["customer"] for r in line_rows],
                                "approve_url": approve_url,
                                "reject_url": reject_url,
                            },
                        )
                    except Exception:
                        html_body = ""

                    subject = f"[KAM] Approval Required: Batch #{batch.id} ({batch.from_date}..{batch.to_date}) - {user.username}"

                    if html_body:
                        body = html_body
                    else:
                        lines_txt = [f"{i}. {r['customer'].name}" for i, r in enumerate(line_rows, start=1)]
                        body = (
                            f"Batch ID: {batch.id}\n"
                            f"KAM: {user.get_full_name() or user.username}\n"
                            f"Category: Customer Visit\n"
                            f"Date Range: {batch.from_date} to {batch.to_date}\n"
                            f"Remarks:\n{remarks}\n\n"
                            f"Customers:\n" + "\n".join(lines_txt) + "\n\n"
                            f"Approve: {approve_url}\n"
                            f"Reject:  {reject_url}\n"
                        )

                    _send_safe_mail(subject, body, [mgr_user], [])

                messages.success(request, f"Submitted for manager approval: {len(line_rows)} customers (Batch #{batch.id}).")
                return redirect(reverse("kam:plan"))

            # -------------------------
            # Save Draft batch (DRAFT)
            # -------------------------
            with transaction.atomic():
                batch: VisitBatch = batch_form.save(commit=False)
                batch.kam = user
                batch.approval_status = STATUS_DRAFT
                batch.save()

                created_lines = 0
                if visit_category == VisitPlan.CAT_CUSTOMER:
                    if not selected_ids:
                        messages.error(request, "Select at least one customer to save a customer batch draft.")
                        transaction.set_rollback(True)
                        return redirect(reverse("kam:plan"))

                    allowed_ids = set(customer_qs.values_list("id", flat=True))
                    if not set(selected_ids).issubset(allowed_ids):
                        messages.error(request, "Invalid customer selection (out of your scope).")
                        transaction.set_rollback(True)
                        return redirect(reverse("kam:plan"))

                    customers_selected = list(Customer.objects.filter(id__in=selected_ids).order_by("name"))
                    for cust in customers_selected:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=cust,
                            kam=user,
                            visit_date=from_date,
                            visit_date_to=to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            purpose=remarks or None,
                            location=(cust.address or "").strip(),
                            approval_status=STATUS_DRAFT,
                        )
                        created_lines += 1
                else:
                    if not non_customer_lines:
                        messages.error(request, "Add at least one line to save a non-customer batch draft.")
                        transaction.set_rollback(True)
                        return redirect(reverse("kam:plan"))

                    for f in non_customer_lines:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=None,
                            counterparty_name=f.cleaned_data["counterparty_name"],
                            kam=user,
                            visit_date=from_date,
                            visit_date_to=to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=visit_category,
                            purpose=(f.cleaned_data.get("counterparty_purpose") or remarks or None),
                            location=(f.cleaned_data.get("counterparty_location") or "").strip(),
                            approval_status=STATUS_DRAFT,
                        )
                        created_lines += 1

                messages.success(request, f"Batch saved as Draft: {created_lines} lines (Batch #{batch.id}).")
                return redirect(reverse("kam:plan"))

    # This-week plans (scoped)
    week_start, week_end, _ = _iso_week_bounds(timezone.now())
    my_plans = (
        _visitplan_qs_for_user(user)
        .filter(visit_date__gte=week_start.date(), visit_date__lt=week_end.date())
        .order_by("visit_date", "customer__name")
    )

    ctx = {
        "page_title": "Plan Visit",
        "form": single_form,  # legacy key
        "single_form": single_form,
        "batch_form": batch_form,
        "plans": my_plans,
        "customers": list(customer_qs),
        "SINGLE_PREFIX": SINGLE_PREFIX,
        "BATCH_PREFIX": BATCH_PREFIX,
        "status_constants": {
            "DRAFT": STATUS_DRAFT,
            "PENDING_APPROVAL": STATUS_PENDING_APPROVAL,
            "APPROVED": STATUS_APPROVED,
            "REJECTED": STATUS_REJECTED,
        },
    }
    return render(request, "kam/plan_visit.html", ctx)


# ---------------------------------------------------------------------
# Customer list API for redesigned checkbox table
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def customers_api(request: HttpRequest) -> JsonResponse:
    user = request.user
    qs = _customer_qs_for_user(user).order_by("name")

    # Optional filtering (manager/admin only): ?kam=username
    if _is_manager(user):
        kam_u = (request.GET.get("kam") or "").strip()
        if kam_u:
            u = User.objects.filter(username=kam_u, is_active=True).first()
            if u:
                if _is_admin(user) or u.id in set(_kams_managed_by_manager(user)):
                    qs = qs.filter(Q(kam=u) | Q(primary_kam=u))
                else:
                    qs = qs.none()
            else:
                qs = qs.none()

    q = (request.GET.get("q") or "").strip()
    if q:
        qs = qs.filter(Q(name__icontains=q) | Q(code__icontains=q) | Q(mobile__icontains=q))

    source = (request.GET.get("source") or "").strip().upper()
    if source:
        qs = qs.filter(source=source)

    rows = []
    for c in qs[:500]:
        rows.append(
            {
                "id": c.id,
                "name": c.name,
                "code": getattr(c, "code", "") or "",
                "mobile": getattr(c, "mobile", "") or "",
                "address": getattr(c, "address", "") or "",
                "source": (getattr(c, "source", "") or "").upper(),
            }
        )
    return JsonResponse({"ok": True, "count": len(rows), "customers": rows})


# ---------------------------------------------------------------------
# Manual customer CRUD for Plan Visit
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def customer_create_manual(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)

    actor = request.user

    target_kam = actor
    if _is_admin(actor) and (request.POST.get("primary_kam") or "").strip():
        uname = (request.POST.get("primary_kam") or "").strip()
        u = User.objects.filter(username=uname, is_active=True).first()
        if not u:
            return JsonResponse({"ok": False, "error": "Invalid primary_kam"}, status=400)
        target_kam = u

    name = (request.POST.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "name required"}, status=400)

    addr = (request.POST.get("address") or "").strip() or None
    mobile = (request.POST.get("mobile") or "").strip() or None
    email = (request.POST.get("email") or "").strip() or None
    gst = (request.POST.get("gst_number") or "").strip() or None
    pincode = (request.POST.get("pincode") or "").strip() or None

    dup_qs = Customer.objects.filter(Q(kam=target_kam) | Q(primary_kam=target_kam)).filter(name__iexact=name)
    if dup_qs.exists():
        return JsonResponse({"ok": False, "error": "Customer already exists in your scope"}, status=409)

    with transaction.atomic():
        c = Customer.objects.create(
            name=name,
            address=addr,
            mobile=mobile,
            email=email,
            gst_number=gst,
            pincode=pincode,
            kam=target_kam,
            primary_kam=target_kam,
            source=Customer.SOURCE_MANUAL,
            created_by=actor,
            synced_identifier=None,
        )

    return JsonResponse({"ok": True, "customer": {"id": c.id, "name": c.name}})


@login_required
@require_kam_code("kam_plan")
def customer_update_manual(request: HttpRequest, customer_id: int) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)

    user = request.user
    qs = _customer_qs_for_user(user)
    c = get_object_or_404(qs, id=customer_id)

    if not _is_admin(user):
        if (getattr(c, "source", "") or "").upper() != Customer.SOURCE_MANUAL:
            return JsonResponse({"ok": False, "error": "Sheet customer is read-only"}, status=403)

    for field in ["name", "address", "mobile", "email", "gst_number", "pincode"]:
        if field in request.POST:
            val = (request.POST.get(field) or "").strip()
            setattr(c, field, val or None)

    c.save()
    return JsonResponse({"ok": True})


@login_required
@require_kam_code("kam_plan")
def customer_delete_manual(request: HttpRequest, customer_id: int) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)

    user = request.user
    qs = _customer_qs_for_user(user)
    c = get_object_or_404(qs, id=customer_id)

    if not _is_admin(user):
        if (getattr(c, "source", "") or "").upper() != Customer.SOURCE_MANUAL:
            return JsonResponse({"ok": False, "error": "Sheet customer cannot be deleted"}, status=403)

    blocking = VisitPlan.objects.filter(customer=c).exclude(approval_status__in=[STATUS_DRAFT, STATUS_REJECTED]).exists()
    if blocking:
        return JsonResponse({"ok": False, "error": "Customer is used in submitted/approved plans"}, status=409)

    c.delete()
    return JsonResponse({"ok": True})


# ---------------------------------------------------------------------
# Visit batches: HTML page + API
# ---------------------------------------------------------------------
def _wants_json(request: HttpRequest) -> bool:
    fmt = (request.GET.get("format") or "").strip().lower()
    if fmt in {"json", "api"}:
        return True
    accept = (request.headers.get("Accept") or "").lower()
    if "application/json" in accept:
        return True
    return False


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches(request: HttpRequest) -> HttpResponse:
    if _wants_json(request):
        return visit_batches_api(request)
    return visit_batches_page(request)


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_page(request: HttpRequest) -> HttpResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")

    status = (request.GET.get("status") or "").strip().upper()
    if status:
        qs = qs.filter(approval_status=status)

    batches = list(qs[:300])
    ctx = {
        "page_title": "Visit Batches",
        "rows": batches,
        "can_view_all": _is_manager(user),
    }
    return render(request, "kam/visit_batches.html", ctx)


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_api(request: HttpRequest) -> JsonResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")

    rows = []
    for b in qs[:300]:
        rows.append(
            {
                "id": b.id,
                "kam": b.kam.username if b.kam_id else None,
                "from_date": str(b.from_date),
                "to_date": str(b.to_date),
                "visit_category": b.visit_category,
                "visit_category_label": b.get_visit_category_display(),
                "approval_status": b.approval_status,
                "remarks": b.purpose or "",
                "created_at": timezone.localtime(b.created_at).isoformat() if b.created_at else None,
            }
        )
    return JsonResponse({"ok": True, "count": len(rows), "batches": rows})


# ---------------------------------------------------------------------
# Batch detail page (used by UI "View Details" if you add it)
# ---------------------------------------------------------------------
@login_required
@require_any_kam_code("kam_manager", "kam_plan")
def visit_batch_detail(request: HttpRequest, batch_id: int) -> HttpResponse:
    b = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    lines = list(VisitPlan.objects.select_related("customer").filter(batch=b).order_by("customer__name"))
    can_approve = _is_manager(request.user)
    ctx = {
        "page_title": f"Batch #{b.id}",
        "batch": b,
        "lines": lines,
        "can_approve": can_approve,
        "can_edit": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT, STATUS_REJECTED}),
        "can_delete": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT}),
    }
    return render(request, "kam/visit_batch_detail.html", ctx)


# ---------------------------------------------------------------------
# Batch delete (KAM only; strict)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def visit_batch_delete(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")

    user = request.user
    batch = get_object_or_404(VisitBatch, id=batch_id, kam=user)

    if batch.approval_status != STATUS_DRAFT:
        messages.error(request, "Only DRAFT batches can be deleted.")
        return redirect(reverse("kam:visit_batches"))

    with transaction.atomic():
        batch = VisitBatch.objects.select_for_update().get(id=batch_id)
        if batch.approval_status != STATUS_DRAFT:
            messages.error(request, "Only DRAFT batches can be deleted.")
            return redirect(reverse("kam:visit_batches"))

        VisitPlan.objects.filter(batch=batch).delete()
        VisitApprovalAudit.objects.create(
            batch=batch,
            actor=user,
            action=VisitApprovalAudit.ACTION_DELETE,
            note="Deleted draft batch",
            actor_ip=_get_ip(request),
        )
        batch.delete()

    messages.success(request, f"Batch #{batch_id} deleted.")
    return redirect(reverse("kam:visit_batches"))


# ---------------------------------------------------------------------
# Batch approve/reject (manager dashboard buttons -> POST)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_manager")
def visit_batch_approve(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    qs = _visitbatch_qs_for_user(request.user)
    batch = get_object_or_404(qs, id=batch_id)

    with transaction.atomic():
        batch = VisitBatch.objects.select_for_update().get(id=batch_id)

        if not _is_admin(request.user):
            allowed = set(_kams_managed_by_manager(request.user))
            if batch.kam_id not in allowed:
                return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

        if batch.approval_status == STATUS_APPROVED:
            messages.info(request, f"Batch #{batch.id} is already approved.")
            return redirect(reverse("kam:visit_batches"))

        if batch.approval_status not in {STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY}:
            messages.error(request, f"Batch #{batch.id} is not pending approval.")
            return redirect(reverse("kam:visit_batches"))

        now_ts = timezone.now()
        batch.approval_status = STATUS_APPROVED
        batch.approved_by = request.user
        batch.approved_at = now_ts
        batch.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])

        VisitPlan.objects.filter(batch=batch).update(
            approval_status=STATUS_APPROVED,
            approved_by=request.user,
            approved_at=now_ts,
            updated_at=now_ts,
        )

        VisitApprovalAudit.objects.create(
            batch=batch,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_APPROVE,
            note="Approved batch",
            actor_ip=_get_ip(request),
        )

    messages.success(request, f"Batch #{batch.id} approved.")
    return redirect(reverse("kam:visit_batches"))


@login_required
@require_kam_code("kam_manager")
def visit_batch_reject(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    qs = _visitbatch_qs_for_user(request.user)
    batch = get_object_or_404(qs, id=batch_id)

    reason = (request.POST.get("reason") or "").strip() or "Rejected"

    with transaction.atomic():
        batch = VisitBatch.objects.select_for_update().get(id=batch_id)

        if not _is_admin(request.user):
            allowed = set(_kams_managed_by_manager(request.user))
            if batch.kam_id not in allowed:
                return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

        if batch.approval_status == STATUS_REJECTED:
            messages.info(request, f"Batch #{batch.id} is already rejected.")
            return redirect(reverse("kam:visit_batches"))

        if batch.approval_status not in {STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY}:
            messages.error(request, f"Batch #{batch.id} is not pending approval.")
            return redirect(reverse("kam:visit_batches"))

        now_ts = timezone.now()
        batch.approval_status = STATUS_REJECTED
        batch.approved_by = request.user
        batch.approved_at = now_ts
        batch.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])

        VisitPlan.objects.filter(batch=batch).update(
            approval_status=STATUS_REJECTED,
            approved_by=request.user,
            approved_at=now_ts,
            updated_at=now_ts,
        )

        VisitApprovalAudit.objects.create(
            batch=batch,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_REJECT,
            note=reason[:255],
            actor_ip=_get_ip(request),
        )

    messages.info(request, f"Batch #{batch.id} rejected.")
    return redirect(reverse("kam:visit_batches"))


# ---------------------------------------------------------------------
# Secure email links (GET -> login -> execute action)
# NOTE: We do NOT mutate request.method (unsafe/undefined). We call the
# approval logic inline with the same validations.
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_manager")
def visit_batch_approve_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    try:
        batch_id, action = _parse_batch_token(token)
    except SignatureExpired:
        messages.error(request, "Approval link expired.")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid approval link.")
        return redirect(reverse("kam:visit_batches"))

    if action != "APPROVE":
        messages.error(request, "Invalid action for this link.")
        return redirect(reverse("kam:visit_batches"))

    # Perform approve
    with transaction.atomic():
        batch = get_object_or_404(VisitBatch.objects.select_for_update(), id=batch_id)

        if not _is_admin(request.user):
            allowed = set(_kams_managed_by_manager(request.user))
            if batch.kam_id not in allowed:
                return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

        if batch.approval_status == STATUS_APPROVED:
            messages.info(request, f"Batch #{batch.id} is already approved.")
            return redirect(reverse("kam:visit_batches"))

        if batch.approval_status not in {STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY}:
            messages.error(request, f"Batch #{batch.id} is not pending approval.")
            return redirect(reverse("kam:visit_batches"))

        now_ts = timezone.now()
        batch.approval_status = STATUS_APPROVED
        batch.approved_by = request.user
        batch.approved_at = now_ts
        batch.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])

        VisitPlan.objects.filter(batch=batch).update(
            approval_status=STATUS_APPROVED,
            approved_by=request.user,
            approved_at=now_ts,
            updated_at=now_ts,
        )

        VisitApprovalAudit.objects.create(
            batch=batch,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_APPROVE,
            note="Approved via email link",
            actor_ip=_get_ip(request),
        )

    messages.success(request, f"Batch #{batch_id} approved.")
    return redirect(reverse("kam:visit_batches"))


@login_required
@require_kam_code("kam_manager")
def visit_batch_reject_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    try:
        batch_id, action = _parse_batch_token(token)
    except SignatureExpired:
        messages.error(request, "Reject link expired.")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid reject link.")
        return redirect(reverse("kam:visit_batches"))

    if action != "REJECT":
        messages.error(request, "Invalid action for this link.")
        return redirect(reverse("kam:visit_batches"))

    reason = "Rejected via email link"

    with transaction.atomic():
        batch = get_object_or_404(VisitBatch.objects.select_for_update(), id=batch_id)

        if not _is_admin(request.user):
            allowed = set(_kams_managed_by_manager(request.user))
            if batch.kam_id not in allowed:
                return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

        if batch.approval_status == STATUS_REJECTED:
            messages.info(request, f"Batch #{batch.id} is already rejected.")
            return redirect(reverse("kam:visit_batches"))

        if batch.approval_status not in {STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY}:
            messages.error(request, f"Batch #{batch.id} is not pending approval.")
            return redirect(reverse("kam:visit_batches"))

        now_ts = timezone.now()
        batch.approval_status = STATUS_REJECTED
        batch.approved_by = request.user
        batch.approved_at = now_ts
        batch.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])

        VisitPlan.objects.filter(batch=batch).update(
            approval_status=STATUS_REJECTED,
            approved_by=request.user,
            approved_at=now_ts,
            updated_at=now_ts,
        )

        VisitApprovalAudit.objects.create(
            batch=batch,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_REJECT,
            note=reason[:255],
            actor_ip=_get_ip(request),
        )

    messages.info(request, f"Batch #{batch_id} rejected.")
    return redirect(reverse("kam:visit_batches"))


# ---------------------------------------------------------------------
# Visits & Calls (aligned to updated VisitActualForm + models)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_visits")
def visits(request: HttpRequest) -> HttpResponse:
    user = request.user
    plan_id = request.GET.get("plan_id")
    error_expected = False

    if request.method == "POST":
        plan_id_post = request.POST.get("plan_id") or plan_id
        plan = get_object_or_404(VisitPlan, id=plan_id_post, kam=user)
        instance = getattr(plan, "actual", None)
        form = VisitActualForm(request.POST, instance=instance)

        exp_sales_raw = (request.POST.get("expected_sales_mt") or "").strip()
        exp_coll_raw = (request.POST.get("expected_collection") or "").strip()

        if exp_sales_raw == "":
            messages.error(request, "Expected Sales (MT) is required.")
            error_expected = True
        if exp_coll_raw == "":
            messages.error(request, "Expected Collection (₹) is required.")
            error_expected = True

        try:
            exp_sales = Decimal(exp_sales_raw) if exp_sales_raw != "" else None
        except Exception:
            messages.error(request, "Expected Sales (MT) must be a number.")
            error_expected = True
            exp_sales = None

        try:
            exp_coll = Decimal(exp_coll_raw) if exp_coll_raw != "" else None
        except Exception:
            messages.error(request, "Expected Collection (₹) must be a number.")
            error_expected = True
            exp_coll = None

        if form.is_valid() and not error_expected:
            with transaction.atomic():
                actual: VisitActual = form.save(commit=False)
                actual.plan = plan
                actual.save()

                if exp_sales is not None:
                    plan.expected_sales_mt = exp_sales
                if exp_coll is not None:
                    plan.expected_collection = exp_coll

                if not (plan.location or "").strip():
                    plan.location = actual.confirmed_location

                plan.save(update_fields=["expected_sales_mt", "expected_collection", "location", "updated_at"])

            messages.success(request, "Visit actual saved.")
            return redirect(f"{reverse('kam:visits')}?plan_id={plan.id}")

    selected_plan = None
    form = None
    if plan_id:
        selected_plan = get_object_or_404(VisitPlan, id=plan_id, kam=user)
        instance = getattr(selected_plan, "actual", None)
        form = VisitActualForm(instance=instance)

    days = (request.GET.get("days") or "").strip()
    from_date = _parse_iso_date(request.GET.get("from_date") or "")
    to_date = _parse_iso_date(request.GET.get("to_date") or "")

    end = timezone.localtime(timezone.now()).date() + timezone.timedelta(days=1)
    start = end - timezone.timedelta(days=14)

    if days.isdigit():
        start = end - timezone.timedelta(days=int(days))
    elif from_date and to_date and from_date <= to_date:
        start = from_date
        end = to_date + timezone.timedelta(days=1)

    recent_plans = (
        VisitPlan.objects.select_related("customer")
        .filter(kam=user, visit_date__gte=start, visit_date__lt=end)
        .order_by("-visit_date")
    )

    ctx = {
        "page_title": "Visits & Calls",
        "form": form,
        "selected_plan": selected_plan,
        "recent_plans": recent_plans,
    }
    return render(request, "kam/visit_actual.html", ctx)


# ---------------------------------------------------------------------
# Legacy single-plan approve/reject (kept)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_visit_approve")
def visit_approve(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    plan = get_object_or_404(VisitPlan, id=plan_id)
    if not _is_admin(request.user):
        allowed = set(_kams_managed_by_manager(request.user))
        if plan.kam_id not in allowed:
            return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

    plan.approval_status = STATUS_APPROVED
    plan.approved_by = request.user
    plan.approved_at = timezone.now()
    plan.save(update_fields=["approval_status", "approved_by", "approved_at"])
    VisitApprovalAudit.objects.create(
        plan=plan,
        actor=request.user,
        action=VisitApprovalAudit.ACTION_APPROVE,
        note="Approved",
        actor_ip=_get_ip(request),
    )
    messages.success(request, "Visit approved.")
    return redirect(reverse("kam:manager_kpis"))


@login_required
@require_kam_code("kam_visit_reject")
def visit_reject(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    plan = get_object_or_404(VisitPlan, id=plan_id)
    if not _is_admin(request.user):
        allowed = set(_kams_managed_by_manager(request.user))
        if plan.kam_id not in allowed:
            return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")

    plan.approval_status = STATUS_REJECTED
    plan.approved_by = request.user
    plan.approved_at = timezone.now()
    plan.save(update_fields=["approval_status", "approved_by", "approved_at"])
    VisitApprovalAudit.objects.create(
        plan=plan,
        actor=request.user,
        action=VisitApprovalAudit.ACTION_REJECT,
        note="Rejected",
        actor_ip=_get_ip(request),
    )
    messages.info(request, "Visit rejected.")
    return redirect(reverse("kam:manager_kpis"))


# ---------------------------------------------------------------------
# Quick entry: Call / Collection (customer qs is scoped)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_call_new")
def call_new(request: HttpRequest) -> HttpResponse:
    qs = _customer_qs_for_user(request.user).order_by("name")

    if request.method == "POST":
        form = CallForm(request.POST)
        if "customer" in form.fields:
            form.fields["customer"].queryset = qs
        if form.is_valid():
            obj: CallLog = form.save(commit=False)
            obj.kam = request.user
            obj.save()
            messages.success(request, "Call saved.")
            return redirect(reverse("kam:dashboard"))
    else:
        form = CallForm()
        if "customer" in form.fields:
            form.fields["customer"].queryset = qs

    return render(request, "kam/call_new.html", {"page_title": "Log Call", "form": form})


@login_required
@require_kam_code("kam_collection_new")
def collection_new(request: HttpRequest) -> HttpResponse:
    qs = _customer_qs_for_user(request.user).order_by("name")

    if request.method == "POST":
        form = CollectionForm(request.POST)
        if "customer" in form.fields:
            form.fields["customer"].queryset = qs
        if form.is_valid():
            obj: CollectionTxn = form.save(commit=False)
            obj.kam = request.user
            obj.save()
            messages.success(request, "Collection saved.")
            return redirect(reverse("kam:dashboard"))
    else:
        form = CollectionForm()
        if "customer" in form.fields:
            form.fields["customer"].queryset = qs

    return render(request, "kam/collection_new.html", {"page_title": "Collection Entry", "form": form})


# ---------------------------------------------------------------------
# Customer 360 (existing; tighten base_qs using mapping)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_customers")
def customers(request: HttpRequest) -> HttpResponse:
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    customer_id = request.GET.get("id")

    if _is_admin(request.user):
        base_qs = Customer.objects.all()
        if scope_kam_id is not None:
            base_qs = base_qs.filter(Q(kam_id=scope_kam_id) | Q(primary_kam_id=scope_kam_id))
    elif _is_manager(request.user):
        allowed = set(_kams_managed_by_manager(request.user))
        base_qs = Customer.objects.filter(Q(kam_id__in=allowed) | Q(primary_kam_id__in=allowed))
        if scope_kam_id is not None:
            base_qs = base_qs.filter(Q(kam_id=scope_kam_id) | Q(primary_kam_id=scope_kam_id))
    else:
        base_qs = Customer.objects.filter(Q(kam=request.user) | Q(primary_kam=request.user))

    customer_list = list(base_qs.order_by("name")[:300])
    customer = get_object_or_404(base_qs, id=customer_id) if customer_id else (customer_list[0] if customer_list else None)

    period_type, start_date, end_date, period_id = _get_customer360_range(request)

    exposure = overdue = credit_limit = Decimal(0)
    ageing = {"a0_30": Decimal(0), "a31_60": Decimal(0), "a61_90": Decimal(0), "a90_plus": Decimal(0)}
    sales_last12 = []
    collections_last12 = []
    risk_ratio = None

    recent_visits = []
    recent_calls = []
    followups = []

    if customer:
        latest_dt = (
            OverdueSnapshot.objects.filter(customer=customer)
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )
        if latest_dt:
            snap = OverdueSnapshot.objects.filter(customer=customer, snapshot_date=latest_dt).first()
            if snap:
                exposure = _safe_decimal(snap.exposure)
                overdue = _safe_decimal(snap.overdue)
                ageing = {
                    "a0_30": _safe_decimal(snap.ageing_0_30),
                    "a31_60": _safe_decimal(snap.ageing_31_60),
                    "a61_90": _safe_decimal(snap.ageing_61_90),
                    "a90_plus": _safe_decimal(snap.ageing_90_plus),
                }

        credit_limit = _safe_decimal(customer.credit_limit)
        if not exposure:
            age_sum = ageing["a0_30"] + ageing["a31_60"] + ageing["a61_90"] + ageing["a90_plus"]
            if age_sum:
                exposure = age_sum
            elif overdue:
                exposure = overdue

        if credit_limit:
            try:
                risk_ratio = exposure / credit_limit if credit_limit else None
            except Exception:
                risk_ratio = None

        sales = (
            InvoiceFact.objects.filter(customer=customer, invoice_date__gte=start_date, invoice_date__lte=end_date)
            .values("invoice_date__year", "invoice_date__month")
            .annotate(mt=Sum("qty_mt"))
            .order_by("invoice_date__year", "invoice_date__month")
        )
        sales_last12 = [{"year": r["invoice_date__year"], "month": r["invoice_date__month"], "mt": _safe_decimal(r["mt"])} for r in sales]

        colls = (
            CollectionTxn.objects.filter(customer=customer, txn_datetime__date__gte=start_date, txn_datetime__date__lte=end_date)
            .values("txn_datetime__year", "txn_datetime__month")
            .annotate(amount=Sum("amount"))
            .order_by("txn_datetime__year", "txn_datetime__month")
        )
        collections_last12 = [{"year": r["txn_datetime__year"], "month": r["txn_datetime__month"], "amount": _safe_decimal(r["amount"])} for r in colls]

        recent_visits = list(
            VisitPlan.objects.filter(customer=customer, visit_date__gte=start_date, visit_date__lte=end_date)
            .order_by("-visit_date")[:10]
        )

        recent_calls = list(
            CallLog.objects.filter(customer=customer, call_datetime__date__gte=start_date, call_datetime__date__lte=end_date)
            .order_by("-call_datetime")[:10]
        )

        today = timezone.localdate()
        followups = list(
            VisitActual.objects.filter(
                plan__customer=customer,
                next_action__isnull=False,
                next_action__gt="",
                next_action_date__isnull=False,
                next_action_date__gte=today,
            )
            .order_by("next_action_date")[:10]
        )

    kam_options: List[str] = []
    if _is_manager(request.user):
        if _is_admin(request.user):
            kam_options = list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
        else:
            allowed_ids = _kams_managed_by_manager(request.user)
            kam_options = list(
                User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True)
            )

    ctx = {
        "page_title": "Customer 360",
        "period_type": period_type,
        "period_id": period_id,
        "scope_label": scope_label,
        "kam_options": kam_options,
        "customer_list": customer_list,
        "customer": customer,
        "exposure": exposure,
        "overdue": overdue,
        "credit_limit": credit_limit,
        "risk_ratio": risk_ratio,
        "ageing": ageing,
        "sales_last12": sales_last12,
        "collections_last12": collections_last12,
        "recent_visits": recent_visits,
        "recent_calls": recent_calls,
        "followups": followups,
    }
    return render(request, "kam/customer_360.html", ctx)


# ---------------------------------------------------------------------
# SECTION F: Targets (manager UI using ManagerTargetForm, persisted into TargetSetting)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_targets")
def targets(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    if _is_admin(request.user):
        kam_options = list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
    else:
        allowed_ids = _kams_managed_by_manager(request.user)
        kam_options = list(User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True))

    uname = (request.GET.get("user") or "").strip()
    f = _parse_iso_date(request.GET.get("from") or "")
    t = _parse_iso_date(request.GET.get("to") or "")

    selected_kam = User.objects.filter(username=uname, is_active=True).first() if uname else None
    if selected_kam and (not _is_admin(request.user)):
        if selected_kam.id not in set(_kams_managed_by_manager(request.user)):
            selected_kam = None

    edit_id = (request.GET.get("id") or "").strip()
    edit_obj = TargetSetting.objects.filter(id=edit_id).first() if edit_id.isdigit() else None

    overdue_sum = Decimal(0)
    suggested_collections = Decimal(0)

    if request.method == "POST":
        form = ManagerTargetForm(request.POST, kam_options=kam_options)
        if not form.is_valid():
            messages.error(request, "Please correct the errors and save again.")
        else:
            cd = form.cleaned_data

            from_date = cd["from_date"]
            to_date = cd.get("to_date")
            fixed_3m = bool(cd.get("fixed_for_next_3_months"))

            if not to_date:
                if fixed_3m:
                    to_date = _add_months(from_date, 3) - timezone.timedelta(days=1)
                else:
                    to_date = from_date + timezone.timedelta(days=6)

            bulk_all = bool(cd.get("bulk_all_kams"))
            kam_username = (cd.get("kam_username") or "").strip()

            if bulk_all or not kam_username:
                kam_users = list(User.objects.filter(is_active=True, username__in=kam_options).order_by("username"))
                if not kam_users:
                    messages.error(request, "No KAM users found for bulk apply.")
                    return redirect(reverse("kam:targets"))
            else:
                kam_u = User.objects.filter(is_active=True, username=kam_username).first()
                if not kam_u:
                    messages.error(request, "Selected KAM is invalid.")
                    return redirect(reverse("kam:targets"))
                kam_users = [kam_u]

            sales_target_mt = _safe_decimal(cd.get("sales_target_mt"))
            leads_target_mt = _safe_decimal(cd.get("leads_target_mt"))
            calls_target = int(cd.get("calls_target") or 0)

            coll_input = cd.get("collections_target_amount")
            auto_coll = bool(cd.get("auto_collections_30pct_overdue"))

            created = 0
            updated = 0

            with transaction.atomic():
                for kuser in kam_users:
                    collections_target_amount = coll_input
                    if auto_coll or collections_target_amount is None:
                        collections_target_amount = Decimal("0.00")

                    overlap_qs = TargetSetting.objects.filter(kam=kuser, from_date__lte=to_date, to_date__gte=from_date)

                    post_id = (request.POST.get("id") or "").strip()
                    inst = None
                    if (not bulk_all) and post_id.isdigit():
                        inst = TargetSetting.objects.filter(id=int(post_id)).first()
                        if inst:
                            overlap_qs = overlap_qs.exclude(id=inst.id)

                    if overlap_qs.exists():
                        messages.error(
                            request,
                            f"Overlapping target window exists for KAM: {kuser.username} ({from_date} → {to_date}).",
                        )
                        raise transaction.TransactionManagementError("Overlap detected")

                    if inst and (len(kam_users) == 1):
                        obj = inst
                        obj.kam = kuser
                        updated += 1
                    else:
                        obj = TargetSetting(kam=kuser)
                        created += 1

                    obj.from_date = from_date
                    obj.to_date = to_date
                    obj.manager = request.user

                    obj.sales_target_mt = sales_target_mt
                    obj.leads_target_mt = leads_target_mt
                    obj.calls_target = calls_target
                    obj.collections_target_amount = _safe_decimal(collections_target_amount)

                    if fixed_3m:
                        obj.fixed_sales_mt = sales_target_mt
                        obj.fixed_leads_mt = leads_target_mt
                        obj.fixed_calls = calls_target
                        obj.fixed_collections_amount = _safe_decimal(collections_target_amount)

                    obj.save()

            if bulk_all or len(kam_users) > 1:
                messages.success(request, f"Targets saved in bulk. Created: {created}, Updated: {updated}.")
                return redirect(reverse("kam:targets"))

            return redirect(f"{reverse('kam:targets')}?from={from_date}&to={to_date}&user={kam_users[0].username}")
    else:
        initial = {}
        if edit_obj:
            initial = {
                "id": str(edit_obj.id),
                "from_date": edit_obj.from_date,
                "to_date": edit_obj.to_date,
                "kam_username": edit_obj.kam.username if edit_obj.kam_id else "",
                "sales_target_mt": edit_obj.sales_target_mt,
                "leads_target_mt": edit_obj.leads_target_mt,
                "calls_target": edit_obj.calls_target,
                "collections_target_amount": edit_obj.collections_target_amount,
            }
        else:
            if f:
                initial["from_date"] = f
            if t:
                initial["to_date"] = t
            if selected_kam:
                initial["kam_username"] = selected_kam.username
            initial["auto_collections_30pct_overdue"] = True

        form = ManagerTargetForm(initial=initial, kam_options=kam_options)

    qs = TargetSetting.objects.select_related("kam", "manager").order_by("-created_at")
    if selected_kam:
        qs = qs.filter(kam=selected_kam)
    if f and t and f <= t:
        qs = qs.filter(from_date__lte=t, to_date__gte=f)
    rows = list(qs[:200])

    ctx = {
        "page_title": "Manager Target Setting",
        "kam_options": kam_options,
        "selected_user": uname,
        "filter_from": f,
        "filter_to": t,
        "rows": rows,
        "form": form,
        "edit_obj": edit_obj,
        "overdue_sum": overdue_sum,
        "suggested_collections": suggested_collections,
    }
    return render(request, "kam/targets.html", ctx)


@login_required
@require_kam_code("kam_targets_lines")
def targets_lines(request: HttpRequest) -> HttpResponse:
    return redirect(reverse("kam:targets"))


# ---------------------------------------------------------------------
# Reports (scope tightened by mapping via _resolve_scope)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    metric = (request.GET.get("metric") or "").strip().lower() or "sales"
    rows = []

    if metric == "sales":
        qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(qs.values("customer__name", "kam__username").annotate(mt=Sum("qty_mt")).order_by("-mt")[:300])

    elif metric == "calls":
        qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(qs.values("id", "call_datetime", "kam__username", "customer_id", "customer__name").order_by("-call_datetime")[:500])

    elif metric == "visits":
        qs = VisitActual.objects.filter(plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(plan__kam_id=scope_kam_id)
        rows = list(
            qs.values(
                "id", "successful", "plan__visit_date", "plan__kam__username", "plan__customer_id", "plan__customer__name"
            ).order_by("-plan__visit_date")[:500]
        )

    else:
        metric = "sales"
        qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(qs.values("customer__name", "kam__username").annotate(mt=Sum("qty_mt")).order_by("-mt")[:300])

    kam_options: List[str] = []
    if _is_manager(request.user):
        if _is_admin(request.user):
            kam_options = list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
        else:
            allowed_ids = _kams_managed_by_manager(request.user)
            kam_options = list(User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True))

    ctx = {
        "page_title": "KAM Reports",
        "metric": metric,
        "range_label": range_label,
        "scope_label": scope_label,
        "can_choose_kam": _is_manager(request.user),
        "kam_options": kam_options,
        "rows": rows,
    }
    return render(request, "kam/reports.html", ctx)


# ---------------------------------------------------------------------
# CSV export (scope tightened)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_export_kpi_csv")
def export_kpi_csv(request: HttpRequest) -> StreamingHttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)

    if _is_manager(request.user):
        user_q = (request.GET.get("user") or "").strip()
        if user_q:
            u = User.objects.filter(username=user_q, is_active=True).first()
            if not u:
                kam_user_ids = []
            else:
                if _is_admin(request.user) or u.id in set(_kams_managed_by_manager(request.user)):
                    kam_user_ids = [u.id]
                else:
                    kam_user_ids = []
        else:
            if _is_admin(request.user):
                kam_user_ids = list(
                    InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
                    .values_list("kam_id", flat=True)
                    .distinct()
                )
            else:
                kam_user_ids = _kams_managed_by_manager(request.user)
    else:
        kam_user_ids = [request.user.id]

    rows = [["period_type", "period_id", "kam_id", "sales_mt", "calls", "visits_actual", "collections_amount"]]
    for kam_id in kam_user_ids:
        sales_mt = _safe_decimal(
            InvoiceFact.objects.filter(kam_id=kam_id, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date()).aggregate(mt=Sum("qty_mt"))["mt"]
        )
        calls = CallLog.objects.filter(kam_id=kam_id, call_datetime__gte=start_dt, call_datetime__lt=end_dt).count()
        visits_actual = VisitActual.objects.filter(plan__kam_id=kam_id, plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date()).count()
        collections_amount = _safe_decimal(
            CollectionTxn.objects.filter(kam_id=kam_id, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt).aggregate(a=Sum("amount"))["a"]
        )
        rows.append([period_type, period_id, kam_id, f"{sales_mt}", f"{calls}", f"{visits_actual}", f"{collections_amount}"])

    def _iter_csv() -> Iterable[bytes]:
        import io
        import csv

        buf = io.StringIO()
        writer = csv.writer(buf)
        for r in rows:
            buf.seek(0)
            buf.truncate(0)
            writer.writerow(r)
            yield buf.getvalue().encode("utf-8")

    resp = StreamingHttpResponse(_iter_csv(), content_type="text/csv")
    resp["Content-Disposition"] = f'attachment; filename="kam_kpis_{period_id}.csv"'
    return resp


# ---------------------------------------------------------------------
# Collections Plan (unchanged)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_collections_plan")
def collections_plan(request: HttpRequest) -> HttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)

    if request.method == "POST":
        form = CollectionPlanForm(request.POST)
        if form.is_valid():
            cp: CollectionPlan = form.save(commit=False)
            # primary owner fallback; keep safe
            cp.kam = cp.customer.kam or cp.customer.primary_kam or request.user
            cp.save()
            messages.success(request, "Collection plan saved.")
            return redirect(f"{reverse('kam:collections_plan')}?period={request.GET.get('period','month')}&asof={request.GET.get('asof','')}")
    else:
        form = CollectionPlanForm(initial={"period_type": period_type, "period_id": period_id})

    plan_qs = CollectionPlan.objects.select_related("customer", "kam")

    period_rows = plan_qs.filter(period_type=period_type, period_id=period_id)
    range_rows = plan_qs.filter(from_date__isnull=False, to_date__isnull=False, from_date__lte=end_dt.date(), to_date__gte=start_dt.date())
    plan_qs = (period_rows | range_rows).distinct()

    plan_customer_ids = list(plan_qs.values_list("customer_id", flat=True))

    overdue_map: Dict[int, Decimal] = {}
    if plan_customer_ids:
        for cust_id in plan_customer_ids:
            latest = OverdueSnapshot.objects.filter(customer_id=cust_id).order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()
            if latest:
                val = OverdueSnapshot.objects.filter(customer_id=cust_id, snapshot_date=latest).values_list("overdue", flat=True).first() or 0
                overdue_map[cust_id] = _safe_decimal(val)

    actual_map: Dict[int, Decimal] = {}
    if plan_customer_ids:
        coll_qs = (
            CollectionTxn.objects.filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt, customer_id__in=plan_customer_ids)
            .values("customer_id")
            .annotate(actual=Sum("amount"))
        )
        actual_map = {r["customer_id"]: _safe_decimal(r["actual"]) for r in coll_qs}

    rows = []
    for p in plan_qs.order_by("customer__name"):
        rows.append(
            {
                "customer": p.customer,
                "kam": p.kam,
                "overdue": overdue_map.get(p.customer_id, Decimal(0)),
                "planned": _safe_decimal(p.planned_amount),
                "actual": actual_map.get(p.customer_id, Decimal(0)),
                "from_date": getattr(p, "from_date", None),
                "to_date": getattr(p, "to_date", None),
                "period_type": getattr(p, "period_type", None),
                "period_id": getattr(p, "period_id", None),
            }
        )

    ctx = {
        "page_title": "Collections Plan",
        "period_type": period_type,
        "period_id": period_id,
        "rows": rows,
        "form": form,
    }
    return render(request, "kam/collections_plan.html", ctx)


# ---------------------------------------------------------------------
# Sync endpoints (UNCHANGED; DO NOT MODIFY IMPORTER LOGIC)
# ---------------------------------------------------------------------
@login_required
@require_kam_code("kam_sync_now")
def sync_now(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    try:
        stats = sheets_adapter.run_sync_now()
        messages.success(request, f"Sync complete. {stats.as_message()}")
    except Exception as e:
        messages.error(request, f"Sync failed: {e}")
    return redirect(reverse("kam:dashboard"))


@login_required
@require_kam_code("kam_sync_trigger")
def sync_trigger(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    token = timezone.now().strftime("%Y%m%d%H%M%S") + f"_{request.user.id}"
    intent = SyncIntent.objects.create(token=token, created_by=request.user, scope=SyncIntent.SCOPE_TEAM)
    messages.success(request, f"Sync triggered (token={intent.token}). Now run /kam/sync/step/?token=TOKEN repeatedly until done.")
    return redirect(reverse("kam:dashboard"))


@login_required
@require_kam_code("kam_sync_step")
def sync_step(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    token = (request.GET.get("token") or request.POST.get("token") or "").strip()

    if request.method == "POST" and not token:
        try:
            stats = sheets_adapter.run_sync_now()
            messages.success(request, f"Sync complete. {stats.as_message()}")
        except Exception as e:
            messages.error(request, f"Sync failed: {e}")
        return redirect(reverse("kam:manager"))

    if not token:
        return JsonResponse({"ok": False, "error": "token missing"}, status=400)

    intent = get_object_or_404(SyncIntent, token=token)
    try:
        intent.status = SyncIntent.STATUS_RUNNING
        intent.step_count += 1
        intent.save(update_fields=["status", "step_count", "updated_at"])

        result = sheets_adapter.step_sync(intent)

        intent.status = SyncIntent.STATUS_SUCCESS if result.get("done") else SyncIntent.STATUS_PENDING
        intent.save(update_fields=["status", "updated_at"])

        return JsonResponse({"ok": True, "result": result})
    except Exception as e:
        intent.status = SyncIntent.STATUS_ERROR
        intent.last_error = str(e)
        intent.save(update_fields=["status", "last_error", "updated_at"])
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
