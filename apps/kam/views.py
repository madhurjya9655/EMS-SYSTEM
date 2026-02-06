# apps/kam/views.py
from __future__ import annotations
from decimal import Decimal
from functools import wraps
from typing import Iterable, List, Dict, Optional, Tuple

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import redirect_to_login
from django.core.mail import send_mail, EmailMessage
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
from django.urls import reverse
from django.utils import timezone

from apps.users.permissions import _user_permission_codes  # app-level codes

from .forms import (
    VisitPlanForm,
    VisitActualForm,
    CallForm,
    CollectionForm,
    TargetLineInlineForm,
    CollectionPlanForm,
    VisitBatchForm,
    MultiVisitPlanLineForm,
)
from .models import (
    Customer,
    InvoiceFact,
    LeadFact,
    OverdueSnapshot,
    TargetHeader,
    TargetLine,
    VisitPlan,
    VisitActual,
    CallLog,
    CollectionTxn,
    VisitApprovalAudit,
    SyncIntent,
    CollectionPlan,
    VisitBatch,
)
from . import sheets_adapter  # adapter with step_sync() and run_sync_now()

User = get_user_model()

# -----------------------------------------------------------------------------
# Constants for approvals / mail routing
# -----------------------------------------------------------------------------
APPROVAL_PRIMARY_MANAGER_USERNAME = "chandan"
APPROVAL_CC_USERNAMES = ["ganesh", "vilas", "amreen", "smriti"]
APPROVAL_BLOCKED_USERNAMES = {"amreen", "smriti"}  # cannot approve

# -----------------------------------------------------------------------------
# Access helpers (use app-level permission codes like other modules)
# -----------------------------------------------------------------------------
def _in_group(user, names: Tuple[str, ...]) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    try:
        return user.groups.filter(name__in=names).exists()
    except Exception:
        return False


def _is_manager(user) -> bool:
    return _in_group(user, ("Manager", "Admin", "Finance"))


def _cannot_approve(user) -> bool:
    uname = (getattr(user, "username", "") or "").strip().lower()
    return uname in APPROVAL_BLOCKED_USERNAMES and not getattr(user, "is_superuser", False)


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

            # Superuser bypass
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


# -----------------------------------------------------------------------------
# Mail helper (best-effort; non-blocking on failures)
# -----------------------------------------------------------------------------
def _send_safe_mail(subject: str, body: str, to_users: List[User], cc_users: List[User] | None = None):
    try:
        to_emails = [u.email for u in to_users if getattr(u, "email", None)]
        cc_emails = [u.email for u in (cc_users or []) if getattr(u, "email", None)]
        if not to_emails and not cc_emails:
            return
        email = EmailMessage(subject=subject, body=body, to=to_emails, cc=cc_emails)
        email.send(fail_silently=True)
    except Exception:
        # Never crash business flow on email issues
        pass


def _get_named_users(usernames: List[str]) -> List[User]:
    wanted = [u.strip().lower() for u in usernames if u and str(u).strip()]
    if not wanted:
        return []
    return list(User.objects.filter(username__iexact=wanted[0])) + list(
        User.objects.filter(username__in=wanted[1:])
    )


# -----------------------------------------------------------------------------
# Period helpers
# -----------------------------------------------------------------------------
def _iso_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=7)
    iso_year, iso_week, _ = start.isocalendar()
    period_id = f"{iso_year}-W{iso_week:02d}"
    return start, end, period_id


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
    # From–To overrides Period
    from_q = request.GET.get("from")
    to_q = request.GET.get("to")
    if from_q and to_q:
        s = _parse_ymd_part(from_q)
        e = _parse_ymd_part(to_q)
        if s and e and s <= e:
            e = e + timezone.timedelta(days=1)  # make end exclusive
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


def _resolve_scope(request: HttpRequest, actor: User) -> Tuple[Optional[int], str]:
    if not _is_manager(actor):
        return actor.id, actor.username
    uname = (request.GET.get("user") or "").strip()
    if uname:
        try:
            u = User.objects.get(username=uname)
            return u.id, u.username
        except User.DoesNotExist:
            pass
    return None, "ALL"


def _safe_decimal(val) -> Decimal:
    return Decimal(val or 0)


# -----------------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_dashboard")
def dashboard(request: HttpRequest) -> HttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    # Targets (if available)
    tline = None
    if scope_kam_id:
        header = TargetHeader.objects.filter(period_type=period_type, period_id=period_id).order_by("-id").first()
        if header:
            tline = TargetLine.objects.filter(header=header, kam_id=scope_kam_id).first()

    sales_target_mt = _safe_decimal(getattr(tline, "sales_target_mt", None)) if tline else Decimal(0)
    visits_target = getattr(tline, "visits_target", 6 if period_type == "WEEK" else 0) if tline else 6 if period_type == "WEEK" else 0
    calls_target = getattr(tline, "calls_target", 24 if period_type == "WEEK" else 24) if tline else 24
    leads_target_mt = _safe_decimal(getattr(tline, "leads_target_mt", None)) if tline else Decimal(0)
    collections_plan_amount = _safe_decimal(getattr(tline, "collections_plan_amount", None)) if tline else Decimal(0)

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

    # Overdues / exposure / credit limits
    if scope_kam_id is not None:
        kam_ids = [scope_kam_id]
    else:
        kam_ids = list(
            Customer.objects.exclude(primary_kam__isnull=True).values_list("primary_kam_id", flat=True).distinct()
        ) or list(InvoiceFact.objects.values_list("kam_id", flat=True).distinct())

    credit_limit_sum = exposure_sum = overdue_sum = Decimal(0)
    prev_overdue_sum = Decimal(0)

    def _overdue_rollup_for_kam_ids(kam_ids: List[int]) -> Tuple[Decimal, Decimal, Decimal]:
        cust_ids = Customer.objects.filter(primary_kam_id__in=kam_ids).values_list("id", flat=True)
        if not cust_ids:
            return Decimal(0), Decimal(0), Decimal(0)
        latest = (
            OverdueSnapshot.objects.filter(customer_id__in=cust_ids)
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )
        if not latest:
            return Decimal(0), Decimal(0), Decimal(0)
        agg = (
            OverdueSnapshot.objects.filter(customer_id__in=cust_ids, snapshot_date=latest)
            .aggregate(
                total_exposure=Sum("exposure"),
                total_overdue=Sum("overdue"),
                a0=Sum("ageing_0_30"),
                a1=Sum("ageing_31_60"),
                a2=Sum("ageing_61_90"),
                a3=Sum("ageing_90_plus"),
            )
        )
        exposure = _safe_decimal(agg.get("total_exposure"))
        overdue = _safe_decimal(agg.get("total_overdue"))
        if not exposure:
            ageing_sum = (
                _safe_decimal(agg.get("a0"))
                + _safe_decimal(agg.get("a1"))
                + _safe_decimal(agg.get("a2"))
                + _safe_decimal(agg.get("a3"))
            )
            if ageing_sum:
                exposure = ageing_sum
        if not exposure and overdue:
            exposure = overdue
        cl_agg = Customer.objects.filter(id__in=cust_ids).aggregate(total_cl=Sum("credit_limit"))
        credit_limit_sum = _safe_decimal(cl_agg.get("total_cl"))
        return credit_limit_sum, exposure, overdue

    def _previous_overdue_sum_for_kam_ids(kam_ids: List[int], before_dt: timezone.datetime) -> Decimal:
        cust_ids = Customer.objects.filter(primary_kam_id__in=kam_ids).values_list("id", flat=True)
        if not cust_ids:
            return Decimal(0)
        prev_date = (
            OverdueSnapshot.objects.filter(customer_id__in=cust_ids, snapshot_date__lt=before_dt.date())
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )
        if not prev_date:
            return Decimal(0)
        agg = OverdueSnapshot.objects.filter(customer_id__in=cust_ids, snapshot_date=prev_date).aggregate(
            total_overdue=Sum("overdue")
        )
        return _safe_decimal(agg.get("total_overdue"))

    if kam_ids:
        credit_limit_sum, exposure_sum, overdue_sum = _overdue_rollup_for_kam_ids(kam_ids)
        prev_overdue_sum = _previous_overdue_sum_for_kam_ids(kam_ids, start_dt)

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
    anchor = timezone.now()
    for i in range(4):
        end_i = _iso_week_bounds(anchor)[1] - timezone.timedelta(days=7 * i)
        start_i = end_i - timezone.timedelta(days=7)
        pid_i = _iso_week_bounds(start_i)[2]
        inv_i = InvoiceFact.objects.filter(invoice_date__gte=start_i.date(), invoice_date__lt=end_i.date())
        vis_i = VisitActual.objects.filter(plan__visit_date__gte=start_i.date(), plan__visit_date__lt=end_i.date())
        calls_i = CallLog.objects.filter(call_datetime__gte=start_i, call_datetime__lt=end_i)
        coll_i = CollectionTxn.objects.filter(txn_datetime__gte=start_i, txn_datetime__lt=end_i)
        if scope_kam_id is not None:
            inv_i = inv_i.filter(kam_id=scope_kam_id)
            vis_i = vis_i.filter(plan__kam_id=scope_kam_id)
            calls_i = calls_i.filter(kam_id=scope_kam_id)
            coll_i = coll_i.filter(kam_id=scope_kam_id)
        trend_rows.insert(
            0,
            {
                "week": pid_i,
                "sales_mt": _safe_decimal(inv_i.aggregate(mt=Sum("qty_mt")).get("mt")),
                "visits": vis_i.count(),
                "calls": calls_i.count(),
                "collections": _safe_decimal(coll_i.aggregate(a=Sum("amount")).get("a")),
            },
        )

    # For manager dropdown (template will render a select)
    kam_options = list(
        User.objects.filter(is_active=True, invoicefact__isnull=False)
        .distinct()
        .order_by("username")
        .values_list("username", flat=True)
    )

    ctx = {
        "page_title": "KAM Dashboard",
        "period_type": period_type,
        "period_id": period_id,
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
        },
        "prod_by_grade": prod_by_grade,
        "prod_by_size": prod_by_size,
        "trend_rows": trend_rows,
    }
    return render(request, "kam/kam_dashboard.html", ctx)


# -----------------------------------------------------------------------------
# Manager summary
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_manager")
def manager_dashboard(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    today_start = timezone.localtime(timezone.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today_start + timezone.timedelta(days=1)
    calls_today = CallLog.objects.filter(call_datetime__gte=today_start, call_datetime__lt=tomorrow).count()
    visits_today = VisitActual.objects.filter(
        plan__visit_date__gte=today_start.date(), plan__visit_date__lt=tomorrow.date()
    ).count()
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
    # Reuse the same simple KPI presentation
    return manager_dashboard(request)


# -----------------------------------------------------------------------------
# Visit planning & posting
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def weekly_plan(request: HttpRequest) -> HttpResponse:
    """
    Renamed behaviorally to "Visit Plan":
      - Single-visit (legacy) still supported
      - NEW: Batch submission with multiple customers and one approval header
      - Approval is non-blocking for execution (emails only)
    """
    user = request.user

    # ----- Batch submission path -----
    if request.method == "POST" and request.POST.get("mode") == "batch":
        batch_form = VisitBatchForm(request.POST)
        non_customer_lines: List[MultiVisitPlanLineForm] = []
        category = request.POST.get("visit_category") or ""

        # Build dynamic non-customer line forms (supplier/warehouse)
        if category in (VisitPlan.CAT_SUPPLIER, VisitPlan.CAT_WAREHOUSE):
            # Accept up to 20 lines from POST arrays (counterparty_name[], location[], purpose[])
            names = request.POST.getlist("counterparty_name[]")
            locs = request.POST.getlist("counterparty_location[]")
            purs = request.POST.getlist("counterparty_purpose[]")
            max_n = max(len(names), len(locs), len(purs))
            for i in range(max_n):
                f = MultiVisitPlanLineForm(
                    {
                        "counterparty_name": (names[i] if i < len(names) else "").strip(),
                        "location": (locs[i] if i < len(locs) else "").strip(),
                        "purpose": (purs[i] if i < len(purs) else "").strip(),
                    }
                )
                if f.is_valid():
                    non_customer_lines.append(f)

        if batch_form.is_valid():
            with transaction.atomic():
                batch: VisitBatch = batch_form.save(commit=False)
                batch.kam = user
                batch.approval_status = VisitBatch.PENDING
                batch.save()

                created_lines = 0

                # Customer category — create one line per selected customer
                if batch.visit_category == VisitPlan.CAT_CUSTOMER:
                    customers = batch_form.cleaned_data["customers"]
                    for cust in customers:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=cust,
                            kam=user,
                            visit_date=batch.from_date,
                            visit_date_to=batch.to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            purpose=batch.purpose,
                            location=cust.address or "",
                            approval_status=VisitPlan.PENDING,
                        )
                        created_lines += 1

                # Supplier / Warehouse category — create one line per free-text counterparty
                else:
                    for f in non_customer_lines:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=None,
                            counterparty_name=f.cleaned_data["counterparty_name"],
                            kam=user,
                            visit_date=batch.from_date,
                            visit_date_to=batch.to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=batch.visit_category,
                            purpose=f.cleaned_data.get("purpose") or batch.purpose,
                            location=f.cleaned_data["location"],
                            approval_status=VisitPlan.PENDING,
                        )
                        created_lines += 1

                # Fire approval email (non-blocking)
                mgr = list(User.objects.filter(username__iexact=APPROVAL_PRIMARY_MANAGER_USERNAME)) or []
                cc = list(User.objects.filter(username__in=APPROVAL_CC_USERNAMES))
                subject = f"[KAM] Visit batch submitted by {user.username} ({batch.from_date}..{batch.to_date})"
                body = (
                    f"KAM: {user.get_full_name() or user.username}\n"
                    f"Category: {batch.get_visit_category_display()}\n"
                    f"Window: {batch.from_date} to {batch.to_date}\n"
                    f"Lines: {created_lines}\n"
                    f"Purpose: {batch.purpose or '-'}\n"
                    f"Batch Id: {batch.id}\n"
                    f"Approval is informational and does not block execution."
                )
                _send_safe_mail(subject, body, mgr, cc)

                messages.success(request, f"Visit batch saved: {created_lines} lines created (Batch #{batch.id}).")
                return redirect(reverse("kam:plan"))
        else:
            # Show errors for batch form same screen
            messages.error(request, "Batch submission has errors. Please correct and re-submit.")
    # ----- Single-visit path (legacy; still allowed) -----
    elif request.method == "POST":
        form = VisitPlanForm(request.POST)
        if form.is_valid():
            plan: VisitPlan = form.save(commit=False)
            plan.kam = user
            if not plan.location:
                # Auto-fill from customer if category=customer and address present
                if plan.visit_category == VisitPlan.CAT_CUSTOMER and plan.customer and plan.customer.address:
                    plan.location = plan.customer.address
            # Weekly cap applies only to PLANNED and category=customer (as per old logic)
            if plan.visit_type == VisitPlan.PLANNED and plan.visit_category == VisitPlan.CAT_CUSTOMER:
                dt_anchor = timezone.make_aware(
                    timezone.datetime.combine(plan.visit_date, timezone.datetime.min.time())
                )
                week_start, week_end, _ = _iso_week_bounds(dt_anchor)
                planned_count = (
                    VisitPlan.objects.filter(
                        kam=user,
                        visit_date__gte=week_start.date(),
                        visit_date__lt=week_end.date(),
                        visit_type=VisitPlan.PLANNED,
                        visit_category=VisitPlan.CAT_CUSTOMER,
                    ).count()
                )
                if planned_count >= 6:
                    messages.error(request, "Weekly planned visit limit (6) reached.")
                    return redirect(reverse("kam:plan"))
            plan.approval_status = VisitPlan.PENDING  # approval is informational
            plan.save()
            # fire informational mail for a single planned customer visit
            try:
                if plan.visit_type == VisitPlan.PLANNED:
                    mgr = list(User.objects.filter(username__iexact=APPROVAL_PRIMARY_MANAGER_USERNAME)) or []
                    cc = list(User.objects.filter(username__in=APPROVAL_CC_USERNAMES))
                    subject = f"[KAM] Visit planned by {user.username} on {plan.visit_date}"
                    base = plan.customer.name if plan.customer_id else (plan.counterparty_name or "-")
                    body = (
                        f"Plan Id: {plan.id}\n"
                        f"KAM: {user.get_full_name() or user.username}\n"
                        f"Entity: {base}\n"
                        f"Category: {plan.get_visit_category_display()}\n"
                        f"Date: {plan.visit_date}\n"
                        f"Purpose: {plan.purpose or '-'}\n"
                        f"Approval is informational and does not block execution."
                    )
                    _send_safe_mail(subject, body, mgr, cc)
            except Exception:
                pass

            messages.success(request, "Visit plan saved.")
            return redirect(reverse("kam:plan"))
    # GET: render forms & lists
    else:
        form = VisitPlanForm()
        batch_form = VisitBatchForm()

    # Scope: this week plans for the logged-in user (legacy list)
    _, week_end, _ = _iso_week_bounds(timezone.now())
    week_start = week_end - timezone.timedelta(days=7)
    my_plans = (
        VisitPlan.objects.select_related("customer")
        .filter(kam=user, visit_date__gte=week_start.date(), visit_date__lt=week_end.date())
        .order_by("visit_date", "customer__name")
    )
    ctx = {
        "page_title": "Plan Visit",
        "form": locals().get("form", VisitPlanForm()),
        "plans": my_plans,
        "batch_form": locals().get("batch_form", VisitBatchForm()),
        "non_customer_template_line": MultiVisitPlanLineForm(),  # template uses this to render empty row
    }
    return render(request, "kam/visit_plan.html", ctx)


@login_required
@require_kam_code("kam_visits")
def visits(request: HttpRequest) -> HttpResponse:
    """
    Post-visit update:
      - Mandatory: expected_sales_mt & expected_collection (written back to plan),
                   remarks (summary), confirmed location.
      - We ALSO allow capturing actual_sales_mt and actual_collection.
    """
    user = request.user
    plan_id = request.GET.get("plan_id")
    error_expected = False

    if request.method == "POST":
        plan_id_post = request.POST.get("plan_id") or plan_id
        plan = get_object_or_404(VisitPlan, id=plan_id_post, kam=user)
        instance = getattr(plan, "actual", None)
        form = VisitActualForm(request.POST, instance=instance)

        # Enforce expected_* at post-visit time (write to plan)
        exp_sales_raw = (request.POST.get("expected_sales_mt") or "").strip()
        exp_coll_raw = (request.POST.get("expected_collection") or "").strip()
        if not exp_sales_raw:
            messages.error(request, "Expected Sales (MT) is required.")
            error_expected = True
        if not exp_coll_raw:
            messages.error(request, "Expected Collection (₹) is required.")
            error_expected = True

        # Attempt parse (allow zero)
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
                # Save/update actual row
                actual: VisitActual = form.save(commit=False)
                actual.plan = plan
                actual.save()
                # Persist expecteds back on the plan (mandatory capture at post-visit)
                if exp_sales is not None:
                    plan.expected_sales_mt = exp_sales
                if exp_coll is not None:
                    plan.expected_collection = exp_coll
                # ensure location is present on plan if missing
                if not (plan.location or "").strip():
                    plan.location = actual.confirmed_location
                plan.save(update_fields=["expected_sales_mt", "expected_collection", "location", "updated_at"])
            messages.success(request, "Visit actual saved.")
            return redirect(f"{reverse('kam:visits')}?plan_id={plan.id}")
    else:
        form = None
        plan = None
        if plan_id:
            plan = get_object_or_404(VisitPlan, id=plan_id, kam=user)
            instance = getattr(plan, "actual", None)
            form = VisitActualForm(instance=instance)

    end = timezone.localtime(timezone.now()).date() + timezone.timedelta(days=1)
    start = end - timezone.timedelta(days=14)
    my_plans = (
        VisitPlan.objects.select_related("customer")
        .filter(kam=user, visit_date__gte=start, visit_date__lt=end)
        .order_by("-visit_date")
    )
    ctx = {
        "page_title": "Visits & Calls",
        "form": form,
        "selected_plan": locals().get("plan"),
        "recent_plans": my_plans,
    }
    return render(request, "kam/visit_actual.html", ctx)


@login_required
@require_kam_code("kam_visit_approve")
def visit_approve(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    if _cannot_approve(request.user):
        return HttpResponseForbidden("403 Forbidden: You are not allowed to approve visits.")

    plan = get_object_or_404(VisitPlan, id=plan_id)
    plan.approval_status = VisitPlan.APPROVED
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
    if _cannot_approve(request.user):
        return HttpResponseForbidden("403 Forbidden: You are not allowed to reject visits.")

    plan = get_object_or_404(VisitPlan, id=plan_id)
    plan.approval_status = VisitPlan.REJECTED
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


def _get_ip(request: HttpRequest) -> Optional[str]:
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


# -----------------------------------------------------------------------------
# Quick entry: Call / Collection
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_call_new")
def call_new(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = CallForm(request.POST)
        if form.is_valid():
            obj: CallLog = form.save(commit=False)
            obj.kam = request.user
            obj.save()
            messages.success(request, "Call saved.")
            return redirect(reverse("kam:dashboard"))
    else:
        form = CallForm()
    return render(
        request,
        "kam/quick_entry.html",
        {"page_title": "Log Call", "form": form, "submit_label": "Save Call"},
    )


@login_required
@require_kam_code("kam_collection_new")
def collection_new(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = CollectionForm(request.POST)
        if form.is_valid():
            obj: CollectionTxn = form.save(commit=False)
            obj.kam = request.user
            obj.save()
            messages.success(request, "Collection saved.")
            return redirect(reverse("kam:dashboard"))
    else:
        form = CollectionForm()
    return render(
        request,
        "kam/quick_entry.html",
        {"page_title": "Add Collection", "form": form, "submit_label": "Save Collection"},
    )


# -----------------------------------------------------------------------------
# Customer 360
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_customers")
def customers(request: HttpRequest) -> HttpResponse:
    # Data visibility: KAM sees self; Manager/Admin can filter by KAM; no cross-leakage
    scope_kam_id, _ = _resolve_scope(request, request.user)
    customer_id = request.GET.get("id")

    if _is_manager(request.user):
        base_qs = Customer.objects.all()
        if scope_kam_id is not None:
            base_qs = base_qs.filter(primary_kam_id=scope_kam_id)
    else:
        base_qs = Customer.objects.filter(primary_kam=request.user)

    customer = get_object_or_404(base_qs, id=customer_id) if customer_id else base_qs.order_by("name").first()

    period_type, start_dt, end_dt, _ = _get_period(request)
    start_date = start_dt.date()
    end_date = (end_dt - timezone.timedelta(days=1)).date()

    exposure = overdue = credit_limit = Decimal(0)
    # Keep ageing in backend for potential analytics, but template will no longer show ageing table
    ageing = {"a0_30": Decimal(0), "a31_60": Decimal(0), "a61_90": Decimal(0), "a90_plus": Decimal(0)}
    pending_next_actions = []
    sales_last12 = []
    collections_last12 = []
    risk_ratio = None

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
        sales_last12 = [
            {"year": r["invoice_date__year"], "month": r["invoice_date__month"], "mt": _safe_decimal(r["mt"])}
            for r in sales
        ]

        colls = (
            CollectionTxn.objects.filter(
                customer=customer, txn_datetime__date__gte=start_date, txn_datetime__date__lte=end_date
            )
            .values("txn_datetime__year", "txn_datetime__month")
            .annotate(amount=Sum("amount"))
            .order_by("txn_datetime__year", "txn_datetime__month")
        )
        collections_last12 = [
            {"year": r["txn_datetime__year"], "month": r["txn_datetime__month"], "amount": _safe_decimal(r["amount"])}
            for r in colls
        ]

        pending_next_actions = list(
            VisitActual.objects.filter(
                plan__customer=customer,
                next_action__isnull=False,
                next_action__gt="",
                next_action_date__isnull=False,
            )
            .order_by("next_action_date")
            .values("next_action", "next_action_date", "plan__kam__username")
        )

    ctx = {
        "page_title": "Customers",
        "customer": customer,
        "exposure": exposure,
        "overdue": overdue,
        "credit_limit": credit_limit,
        "risk_ratio": risk_ratio,
        "ageing": ageing,  # kept for backend, template no longer displays ageing table per requirement
        "sales_last12": sales_last12,
        "collections_last12": collections_last12,
        "pending_next_actions": pending_next_actions,
        # Convenience lists for template (recent)
        "recent_visits": list(
            VisitPlan.objects.filter(customer=customer).order_by("-visit_date")[:10]
        ) if customer else [],
        "recent_calls": list(
            CallLog.objects.filter(customer=customer).order_by("-call_datetime")[:10]
        ) if customer else [],
        "followups": list(
            VisitActual.objects.filter(plan__customer=customer, next_action__isnull=False, next_action__gt="")
            .order_by("next_action_date")[:10]
        ) if customer else [],
    }
    return render(request, "kam/customer_360.html", ctx)


# -----------------------------------------------------------------------------
# Targets
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_targets")
def targets(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    if request.method == "POST":
        period_type = request.POST.get("period_type") or TargetHeader.PERIOD_WEEK
        period_id = request.POST.get("period_id") or ""
        if not period_id:
            messages.error(request, "Period Id is required (e.g., 2026-W05 / 2026-01 / 2026-Q1 / 2026).")
            return redirect(reverse("kam:targets"))
        header, _created = TargetHeader.objects.get_or_create(
            period_type=period_type, period_id=period_id, defaults={"manager": request.user}
        )
        if request.POST.get("lock") == "1":
            if header.locked_at:
                messages.info(request, "Target already locked.")
            else:
                header.locked_at = timezone.now()
                header.save(update_fields=["locked_at"])
                # Optional: send summary email to KAMs (can duplicate with other mails; flag if needed)
                # Requirement mentions potential duplication risk; we keep it silent here.
                messages.success(request, "Targets locked & published.")
        else:
            if not header.manager_id:
                header.manager = request.user
                header.save(update_fields=["manager"])
            messages.success(request, "Target header saved.")
        return redirect(reverse("kam:targets"))

    headers = TargetHeader.objects.select_related("manager").order_by("-created_at")[:20]
    ctx = {"page_title": "Targets", "headers": headers}
    return render(request, "kam/targets.html", ctx)


@login_required
@require_kam_code("kam_targets_lines")
def targets_lines(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    header_id = request.GET.get("header_id")
    header = get_object_or_404(TargetHeader, id=header_id) if header_id else None

    if request.method == "POST":
        if not header:
            messages.error(request, "Select a target header first.")
            return redirect(reverse("kam:targets_lines"))
        if header.locked_at:
            messages.error(request, "Header is locked; cannot edit lines.")
            return redirect(f"{reverse('kam:targets_lines')}?header_id={header.id}")

        form = TargetLineInlineForm(request.POST)
        if form.is_valid():
            tl: TargetLine = form.save(commit=False)
            tl.header = header

            existing = TargetLine.objects.filter(header=header, kam=tl.kam).first()
            if existing:
                existing.sales_target_mt = tl.sales_target_mt
                existing.visits_target = tl.visits_target
                existing.calls_target = tl.calls_target
                existing.leads_target_mt = tl.leads_target_mt
                existing.nbd_target_monthly = tl.nbd_target_monthly
                existing.collections_plan_amount = tl.collections_plan_amount
                existing.save()
                # Email summary to the KAM after save — OPTIONAL; disabled by default to avoid duplication
                messages.success(request, "Target updated for KAM.")
            else:
                tl.save()
                messages.success(request, "Target created for KAM.")
            return redirect(f"{reverse('kam:targets_lines')}?header_id={header.id}")
    else:
        form = TargetLineInlineForm()

    headers = TargetHeader.objects.select_related("manager").order_by("-created_at")[:20]
    lines = TargetLine.objects.select_related("kam").filter(header=header).order_by("kam__username") if header else []
    ctx = {"page_title": "Target Lines", "headers": headers, "selected_header": header, "form": form, "lines": lines}
    return render(request, "kam/targets_lines.html", ctx)


# -----------------------------------------------------------------------------
# Reports stub
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    ctx = {"page_title": "KAM Reports"}
    return render(request, "kam/reports.html", ctx)


# -----------------------------------------------------------------------------
# CSV export
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_export_kpi_csv")
def export_kpi_csv(request: HttpRequest) -> StreamingHttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)
    if _is_manager(request.user):
        user_q = (request.GET.get("user") or "").strip()
        if user_q:
            try:
                kam_user_ids = [User.objects.get(username=user_q).id]
            except User.DoesNotExist:
                kam_user_ids = []
        else:
            kam_user_ids = (
                InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
                .values_list("kam_id", flat=True)
                .distinct()
            )
    else:
        kam_user_ids = [request.user.id]

    rows = [["period_type", "period_id", "kam_id", "sales_mt", "calls", "visits_actual", "collections_amount"]]
    for kam_id in kam_user_ids:
        sales_mt = _safe_decimal(
            InvoiceFact.objects.filter(
                kam_id=kam_id, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date()
            ).aggregate(mt=Sum("qty_mt"))["mt"]
        )
        calls = CallLog.objects.filter(kam_id=kam_id, call_datetime__gte=start_dt, call_datetime__lt=end_dt).count()
        visits_actual = VisitActual.objects.filter(
            plan__kam_id=kam_id, plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date()
        ).count()
        collections_amount = _safe_decimal(
            CollectionTxn.objects.filter(kam_id=kam_id, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt).aggregate(
                a=Sum("amount")
            )["a"]
        )
        rows.append(
            [period_type, period_id, kam_id, f"{sales_mt}", f"{calls}", f"{visits_actual}", f"{collections_amount}"]
        )

    def _iter_csv() -> Iterable[bytes]:
        import io, csv

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


# -----------------------------------------------------------------------------
# Collections Plan (period or range)
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_collections_plan")
def collections_plan(request: HttpRequest) -> HttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)

    if request.method == "POST":
        form = CollectionPlanForm(request.POST)
        if form.is_valid():
            cp: CollectionPlan = form.save(commit=False)
            # Safe fallback: if customer has no primary_kam, attribute to current user
            cp.kam = cp.customer.primary_kam or request.user
            cp.save()
            messages.success(request, "Collection plan saved.")
            return redirect(
                f"{reverse('kam:collections_plan')}?period={request.GET.get('period','month')}&asof={request.GET.get('asof','')}"
            )
    else:
        form = CollectionPlanForm(initial={"period_type": period_type, "period_id": period_id})

    # Fetch planned rows (both period-mode and range-mode) for the selected window
    plan_qs = CollectionPlan.objects.select_related("customer", "kam")

    # If the UI currently shows a period (month/...), restrict rows to that period id; otherwise show recent range-mode too
    # We will prefer period-mode matching the computed period_id; range-mode shown if its window overlaps start_dt..end_dt
    period_rows = plan_qs.filter(period_type=period_type, period_id=period_id)
    range_rows = plan_qs.filter(
        from_date__isnull=False, to_date__isnull=False, from_date__lte=end_dt.date(), to_date__gte=start_dt.date()
    )
    plan_qs = (period_rows | range_rows).distinct()

    # Compute latest overdue for only customers present in the displayed plans
    plan_customer_ids = list(plan_qs.values_list("customer_id", flat=True))

    overdue_map: Dict[int, Decimal] = {}
    if plan_customer_ids:
        for cust_id in plan_customer_ids:
            latest = (
                OverdueSnapshot.objects.filter(customer_id=cust_id)
                .order_by("-snapshot_date")
                .values_list("snapshot_date", flat=True)
                .first()
            )
            if latest:
                val = (
                    OverdueSnapshot.objects.filter(customer_id=cust_id, snapshot_date=latest)
                    .values_list("overdue", flat=True)
                    .first()
                    or 0
                )
                overdue_map[cust_id] = _safe_decimal(val)

    # Actual collections in window, restricted to planned customers
    actual_map: Dict[int, Decimal] = {}
    if plan_customer_ids:
        coll_qs = (
            CollectionTxn.objects.filter(
                txn_datetime__gte=start_dt, txn_datetime__lt=end_dt, customer_id__in=plan_customer_ids
            )
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


# -----------------------------------------------------------------------------
# Sync endpoints
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_sync_now")
def sync_now(request: HttpRequest) -> HttpResponse:
    """Run a single full import from the Google Sheet immediately (one-off)."""
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
    """Create (or reuse) a pending SyncIntent token—useful if you later call sync_step via JS/cron."""
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    token = timezone.now().strftime("%Y%m%d%H%M%S") + f"_{request.user.id}"
    intent = SyncIntent.objects.create(token=token, created_by=request.user, scope=SyncIntent.SCOPE_TEAM)
    messages.success(
        request,
        f"Sync triggered (token={intent.token}). Now run /kam/sync/step/?token=TOKEN repeatedly until done.",
    )
    return redirect(reverse("kam:dashboard"))


@login_required
@require_kam_code("kam_sync_step")
def sync_step(request: HttpRequest) -> HttpResponse:
    """Advance one sync step (idempotent page-wise). Returns JSON; you can loop until done=True."""
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    token = (request.GET.get("token") or "").strip()
    if not token and request.method == "POST":
        # convenience: allow POST from button without token by minting one-off SELF scope
        token = timezone.now().strftime("%Y%m%d%H%M%S") + f"_{request.user.id}"
        SyncIntent.objects.create(token=token, created_by=request.user, scope=SyncIntent.SCOPE_TEAM)

    if not token:
        return JsonResponse({"ok": False, "error": "token missing"}, status=400)
    intent = get_object_or_404(SyncIntent, token=token)
    try:
        intent.status = SyncIntent.STATUS_RUNNING
        intent.step_count += 1
        intent.save(update_fields=["status", "step_count", "updated_at"])
        result = sheets_adapter.step_sync(intent)
        if result.get("done"):
            intent.status = SyncIntent.STATUS_SUCCESS
        else:
            intent.status = SyncIntent.STATUS_PENDING
        intent.save(update_fields=["status", "updated_at"])
        return JsonResponse({"ok": True, "result": result})
    except Exception as e:
        intent.status = SyncIntent.STATUS_ERROR
        intent.last_error = str(e)
        intent.save(update_fields=["status", "last_error", "updated_at"])
        return JsonResponse({"ok": False, "error": str(e)}, status=500)
