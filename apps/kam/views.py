# File: E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\kam\views.py
from __future__ import annotations

from decimal import Decimal
from functools import wraps
from typing import Iterable, List, Dict, Optional, Tuple

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import redirect_to_login
from django.core.mail import EmailMessage
from django.db import transaction
from django.db.models import Sum, Q, Max, Value, DateTimeField
from django.db.models.functions import Cast, Coalesce, Greatest
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
# Form prefixes (IMPORTANT: prevents duplicate HTML ids between single & batch)
# -----------------------------------------------------------------------------
SINGLE_PREFIX = "single"
BATCH_PREFIX = "batch"


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
    # Keep your original group logic intact
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
        pass


# -----------------------------------------------------------------------------
# Scope helpers (IMPORTANT: prevent cross-KAM leakage in dropdowns)
# -----------------------------------------------------------------------------
def _customer_qs_for_user(user: User):
    """
    Non-manager users must only see their own customers in any dropdown.
    Managers can see all customers.
    """
    if _is_manager(user):
        return Customer.objects.all()
    return Customer.objects.filter(primary_kam=user)


# -----------------------------------------------------------------------------
# Period helpers (kept for other pages, dashboard will use strict From-To)
# -----------------------------------------------------------------------------
def _iso_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=7)
    iso_year, iso_week, _ = start.isocalendar()
    period_id = f"{iso_year}-W{iso_week:02d}"
    return start, end, period_id


def _ms_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    """
    Week definition for 4-week trend: Monday to Saturday.
    Represented as [Mon 00:00, Sun 00:00) (end-exclusive) so Sunday is excluded.
    """
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())  # Monday
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=6)  # Sunday 00:00 (end-exclusive)
    iso_year, iso_week, _ = start.isocalendar()
    period_id = f"{iso_year}-W{iso_week:02d}"
    return start, end, period_id


def _last_completed_ms_week_end(dt: timezone.datetime) -> timezone.datetime:
    """
    Returns the end (exclusive) timestamp of the most recent COMPLETED Mon–Sat week.
    - If now is Mon..Sat (before Sun 00:00): last completed is previous week => returns previous Sunday 00:00
    - If now is Sun or later: current week completed => returns this Sunday 00:00
    """
    start, end, _ = _ms_week_bounds(dt)
    now_local = timezone.localtime(dt)
    if now_local < end:
        # previous week's end is previous Sunday 00:00, i.e., start - 1 day
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


def _parse_iso_date(s: str) -> Optional[timezone.datetime.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return timezone.datetime.fromisoformat(s).date()
    except Exception:
        return None


def _get_period(request: HttpRequest) -> Tuple[str, timezone.datetime, timezone.datetime, str]:
    """
    Kept for backward compatibility with other screens.
    Dashboard will NOT rely on period/asof anymore, but From-To still works here too.
    """
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


# -----------------------------------------------------------------------------
# STRICT dashboard date-range helper (calendar From/To)
# -----------------------------------------------------------------------------
def _get_dashboard_range(request: HttpRequest) -> Tuple[timezone.datetime, timezone.datetime, str]:
    """
    Dashboard date filter:
      - If from & to are present and valid => use inclusive range, end-exclusive (+1 day)
      - Else default to current ISO week
    Inputs accept YYYY-MM-DD (from HTML <input type="date">).
    """
    from_s = (request.GET.get("from") or "").strip()
    to_s = (request.GET.get("to") or "").strip()

    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)
    if from_d and to_d and from_d <= to_d:
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        end = timezone.make_aware(timezone.datetime(to_d.year, to_d.month, to_d.day, 0, 0, 0)) + timezone.timedelta(days=1)
        label = f"{from_d} → {to_d}"
        return start, end, label

    # Default = this week
    ws, we, _ = _iso_week_bounds(timezone.now())
    label = f"{ws.date()} → {(we - timezone.timedelta(days=1)).date()}"
    return ws, we, label


def _resolve_scope(request: HttpRequest, actor: User) -> Tuple[Optional[int], str]:
    """
    Scope rules:
      - Non-manager: always self (FORCE LOCK, ignore ?user)
      - Manager: may choose ?user=username else ALL
    """
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


def _parse_decimal_or_none(s: str) -> Optional[Decimal]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# KAM dropdown: recent activity first (Manager/Admin only)
# -----------------------------------------------------------------------------
def _manager_kam_options_recent_first() -> List[str]:
    """
    Show recently active KAMs first, then others.
    Activity based on max of:
      - calllog.call_datetime
      - collectiontxn.txn_datetime
      - invoicefact.invoice_date (cast to datetime)
      - visitplan.visit_date (cast to datetime)
      - leadfact.doe (cast to datetime)
    """
    baseline = timezone.make_aware(timezone.datetime(2000, 1, 1, 0, 0, 0))

    qs = (
        User.objects.filter(is_active=True)
        .annotate(
            last_call=Max("calllog__call_datetime"),
            last_coll=Max("collectiontxn__txn_datetime"),
            last_inv=Cast(Max("invoicefact__invoice_date"), output_field=DateTimeField()),
            last_visit=Cast(Max("visitplan__visit_date"), output_field=DateTimeField()),
            last_lead=Cast(Max("leadfact__doe"), output_field=DateTimeField()),
        )
        .annotate(
            last_activity=Greatest(
                Coalesce("last_call", Value(baseline)),
                Coalesce("last_coll", Value(baseline)),
                Coalesce("last_inv", Value(baseline)),
                Coalesce("last_visit", Value(baseline)),
                Coalesce("last_lead", Value(baseline)),
            )
        )
        .order_by("-last_activity", "username")
        .values_list("username", flat=True)
    )
    return list(qs)


# -----------------------------------------------------------------------------
# Overdue helpers (range-aware snapshots)
# -----------------------------------------------------------------------------
def _latest_snapshot_date_for_customers_upto(customer_ids: List[int], upto_date) -> Optional[timezone.datetime.date]:
    """
    Latest snapshot_date <= upto_date
    """
    if not customer_ids:
        return None
    return (
        OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date__lte=upto_date)
        .order_by("-snapshot_date")
        .values_list("snapshot_date", flat=True)
        .first()
    )


def _latest_snapshot_date_for_customers_before(customer_ids: List[int], before_date) -> Optional[timezone.datetime.date]:
    """
    Latest snapshot_date < before_date
    """
    if not customer_ids:
        return None
    return (
        OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date__lt=before_date)
        .order_by("-snapshot_date")
        .values_list("snapshot_date", flat=True)
        .first()
    )


def _rollup_overdue_for_customers(customer_ids: List[int], snapshot_date: timezone.datetime.date) -> Tuple[Decimal, Decimal]:
    """
    Returns (exposure_sum, overdue_sum) for the given snapshot date.
    Exposure fallback: if exposure sum is 0 but ageing buckets exist, use ageing sum; else fallback to overdue.
    """
    if not customer_ids or not snapshot_date:
        return Decimal(0), Decimal(0)

    agg = (
        OverdueSnapshot.objects.filter(customer_id__in=customer_ids, snapshot_date=snapshot_date)
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

    return exposure, overdue


# -----------------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_dashboard")
def dashboard(request: HttpRequest) -> HttpResponse:
    # STRICT range for dashboard
    start_dt, end_dt, range_label = _get_dashboard_range(request)

    # STRICT scope lock
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    # Targets remain available only when period header exists.
    # For custom date ranges, we do not attempt to guess targets; keep defaults.
    # For default week view, targets work as earlier.
    period_type, p_start, p_end, period_id = _get_period(request)
    is_default_week = (request.GET.get("from") or "").strip() == "" and (request.GET.get("to") or "").strip() == ""

    tline = None
    if scope_kam_id and is_default_week:
        header = TargetHeader.objects.filter(period_type=period_type, period_id=period_id).order_by("-id").first()
        if header:
            tline = TargetLine.objects.filter(header=header, kam_id=scope_kam_id).first()

    sales_target_mt = _safe_decimal(getattr(tline, "sales_target_mt", None)) if tline else Decimal(0)
    visits_target = getattr(tline, "visits_target", 6) if tline else 6
    calls_target = getattr(tline, "calls_target", 24) if tline else 24
    leads_target_mt = _safe_decimal(getattr(tline, "leads_target_mt", None)) if tline else Decimal(0)
    collections_plan_amount = _safe_decimal(getattr(tline, "collections_plan_amount", None)) if tline else Decimal(0)

    # STRICT date-range querysets
    inv_qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
    visit_plan_qs = VisitPlan.objects.filter(visit_date__gte=start_dt.date(), visit_date__lt=end_dt.date())
    visit_act_qs = VisitActual.objects.filter(plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date())
    call_qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
    lead_qs = LeadFact.objects.filter(doe__gte=start_dt.date(), doe__lt=end_dt.date())
    coll_qs = CollectionTxn.objects.filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)

    # STRICT scoping (NO leakage)
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

    # -----------------------------
    # Overdues / exposure / credit limits
    # -----------------------------
    overdue_snapshot_date = None
    prev_overdue_snapshot_date = None
    credit_limit_sum = Decimal(0)
    exposure_sum = Decimal(0)
    overdue_sum = Decimal(0)
    prev_overdue_sum = Decimal(0)

    if scope_kam_id is not None:
        customer_ids = list(Customer.objects.filter(primary_kam_id=scope_kam_id).values_list("id", flat=True))
    else:
        # Manager/Admin ALL
        customer_ids = list(Customer.objects.exclude(primary_kam__isnull=True).values_list("id", flat=True))

    if customer_ids:
        credit_limit_sum = _safe_decimal(
            Customer.objects.filter(id__in=customer_ids).aggregate(total_cl=Sum("credit_limit")).get("total_cl")
        )

        end_date_inclusive = (end_dt - timezone.timedelta(days=1)).date()
        start_date_inclusive = start_dt.date()

        overdue_snapshot_date = _latest_snapshot_date_for_customers_upto(customer_ids, end_date_inclusive)
        if overdue_snapshot_date:
            exposure_sum, overdue_sum = _rollup_overdue_for_customers(customer_ids, overdue_snapshot_date)

        prev_overdue_snapshot_date = _latest_snapshot_date_for_customers_before(customer_ids, start_date_inclusive)
        if prev_overdue_snapshot_date:
            _ex_prev, prev_overdue_sum = _rollup_overdue_for_customers(customer_ids, prev_overdue_snapshot_date)

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

    # ---------------------------------------------------------------------
    # 4 WEEK TREND (Mon–Sat), last 4 COMPLETED weeks only (from current date)
    # ---------------------------------------------------------------------
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

    kam_options = _manager_kam_options_recent_first() if _is_manager(request.user) else []

    ctx = {
        "page_title": "KAM Dashboard",
        "range_label": range_label,
        "can_choose_kam": _is_manager(request.user),

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

            "overdue_snapshot_date": overdue_snapshot_date,
            "prev_overdue_snapshot_date": prev_overdue_snapshot_date,
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
    """
    Manager KPIs page:
      - KAM-wise KPIs for selected period
      - Top risky customers (latest overdue snapshot), max 50
    """
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    period_type, start_dt, end_dt, period_id = _get_period(request)

    kam_ids = set(
        Customer.objects.exclude(primary_kam__isnull=True).values_list("primary_kam_id", flat=True).distinct()
    )
    kam_ids |= set(
        InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kams = User.objects.filter(is_active=True, id__in=list(kam_ids)).order_by("username")

    def _pct(n: Decimal, d: Decimal) -> Optional[Decimal]:
        if d and d != 0:
            return (n / d) * Decimal("100")
        return None

    latest_snap_date = OverdueSnapshot.objects.order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()

    rows: List[Dict] = []
    for kam in kams:
        inv_qs = InvoiceFact.objects.filter(
            kam=kam, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date()
        )
        sales_mt = _safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt")).get("mt"))

        visits_qs = VisitActual.objects.filter(
            plan__kam=kam, plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date()
        )
        visits_actual = visits_qs.count()
        visits_successful = visits_qs.filter(successful=True).count()
        visit_success_pct = _pct(Decimal(visits_successful), Decimal(visits_actual)) if visits_actual else None

        calls = CallLog.objects.filter(kam=kam, call_datetime__gte=start_dt, call_datetime__lt=end_dt).count()

        collections_actual = _safe_decimal(
            CollectionTxn.objects.filter(kam=kam, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)
            .aggregate(a=Sum("amount"))
            .get("a")
        )

        leads_agg = LeadFact.objects.filter(kam=kam, doe__gte=start_dt.date(), doe__lt=end_dt.date()).aggregate(
            total_mt=Sum("qty_mt"),
            won_mt=Sum("qty_mt", filter=Q(status="WON")),
        )
        leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
        leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))
        lead_conv_pct = _pct(leads_won_mt, leads_total_mt) if leads_total_mt else None

        credit_limit_sum = _safe_decimal(
            Customer.objects.filter(primary_kam=kam).aggregate(s=Sum("credit_limit")).get("s")
        )
        exposure_sum = overdue_sum = Decimal(0)

        if latest_snap_date:
            cust_ids = list(Customer.objects.filter(primary_kam=kam).values_list("id", flat=True))
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
                    ageing_sum = (
                        _safe_decimal(agg.get("a0")) + _safe_decimal(agg.get("a31")) +
                        _safe_decimal(agg.get("a61")) + _safe_decimal(agg.get("a90"))
                    )
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

    # -----------------------------
    # FIXED: Risky customers (avoid materializing Customer model; no select_related)
    # This prevents SQLite from selecting missing kam_customer.code column.
    # -----------------------------
    risky: List[Dict] = []
    if latest_snap_date:
        snaps = (
            OverdueSnapshot.objects
            .filter(snapshot_date=latest_snap_date)
            .values(
                "customer_id",
                "customer__name",
                "customer__credit_limit",
                "exposure",
                "overdue",
                "ageing_0_30",
                "ageing_31_60",
                "ageing_61_90",
                "ageing_90_plus",
            )
        )

        tmp = []
        for s in snaps:
            credit_limit = _safe_decimal(s.get("customer__credit_limit"))
            if not credit_limit:
                continue

            exposure = _safe_decimal(s.get("exposure"))
            overdue = _safe_decimal(s.get("overdue"))

            if not exposure:
                ageing_sum = (
                    _safe_decimal(s.get("ageing_0_30"))
                    + _safe_decimal(s.get("ageing_31_60"))
                    + _safe_decimal(s.get("ageing_61_90"))
                    + _safe_decimal(s.get("ageing_90_plus"))
                )
                if ageing_sum:
                    exposure = ageing_sum
            if not exposure and overdue:
                exposure = overdue

            rr = (exposure / credit_limit) if credit_limit else None
            if rr is None:
                continue

            tmp.append(
                {
                    "name": s.get("customer__name") or "-",
                    "credit_limit": credit_limit,
                    "exposure": exposure,
                    "overdue": overdue,
                    "risk_ratio": rr,
                }
            )

        tmp.sort(key=lambda x: x["risk_ratio"], reverse=True)
        risky = tmp[:50]

    ctx = {
        "page_title": "Manager KPIs",
        "period_type": period_type,
        "period_id": period_id,
        "rows": rows,
        "risky": risky,
    }
    return render(request, "kam/manager_kpis.html", ctx)


# -----------------------------------------------------------------------------
# Visit planning & posting
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_plan")
def weekly_plan(request: HttpRequest) -> HttpResponse:
    """
    Plan Visit:
      - Single-visit (legacy) supported
      - Batch submission supported
      - NEW (Section B4): Proceed-to-Manager consolidated batch for multiple customers with per-customer details

    IMPORTANT:
      - Uses prefixes (single/batch) to prevent duplicate HTML ids and ensure correct POST binding.
      - Templates must render: {{ form }} with prefix SINGLE_PREFIX and {{ batch_form }} with prefix BATCH_PREFIX.
    """
    user = request.user

    # B1: strict scoped customers for KAM; manager/admin can see all
    customer_qs = _customer_qs_for_user(user).order_by("name")
    customers = customer_qs

    # Default (GET) forms with prefixes
    form = VisitPlanForm(prefix=SINGLE_PREFIX)
    batch_form = VisitBatchForm(prefix=BATCH_PREFIX)

    if "customer" in form.fields:
        form.fields["customer"].queryset = customer_qs
    if "customers" in batch_form.fields:
        batch_form.fields["customers"].queryset = customer_qs

    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "batch":
        # -----------------------------
        # BATCH POST
        # -----------------------------
        batch_form = VisitBatchForm(request.POST, prefix=BATCH_PREFIX)
        if "customers" in batch_form.fields:
            batch_form.fields["customers"].queryset = customer_qs

        action = (request.POST.get("action") or request.POST.get("submit_action") or "").strip().lower()
        proceed_flag = action in {"proceed", "proceed_to_manager", "proceed-manager", "manager"} or (
            (request.POST.get("proceed_to_manager") or "").strip() == "1"
        )

        # Non-customer (Supplier/Warehouse/Vendor) line validation helper
        non_customer_lines: List[MultiVisitPlanLineForm] = []
        raw_category = (request.POST.get(f"{BATCH_PREFIX}-visit_category") or request.POST.get("visit_category") or "").strip()

        if raw_category in (VisitPlan.CAT_SUPPLIER, VisitPlan.CAT_WAREHOUSE, VisitPlan.CAT_VENDOR):
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

        if not batch_form.is_valid():
            messages.error(request, "Batch submission has errors. Please correct and re-submit.")
        else:
            # --------------------------------------------
            # B4: Proceed-to-Manager (Customer multi-select)
            # --------------------------------------------
            if proceed_flag:
                visit_category = batch_form.cleaned_data.get("visit_category")
                if visit_category != VisitPlan.CAT_CUSTOMER:
                    messages.error(request, "Proceed to Manager is allowed only for Customer Visit batches.")
                else:
                    # Remarks mandatory (using existing purpose field to avoid schema changes/migrations)
                    remarks = (batch_form.cleaned_data.get("purpose") or "").strip()
                    if not remarks:
                        messages.error(request, "Remarks are required to proceed to Manager.")
                    else:
                        customers_selected = batch_form.cleaned_data.get("customers") or []
                        if not customers_selected or len(customers_selected) == 0:
                            messages.error(request, "Select at least one customer to proceed.")
                        else:
                            # Server-side safety: ensure all customers are within scoped queryset (no POST tampering)
                            allowed_ids = set(customer_qs.values_list("id", flat=True))
                            selected_ids = {c.id for c in customers_selected}
                            if not selected_ids.issubset(allowed_ids):
                                messages.error(request, "Invalid customer selection (out of your scope).")
                                return redirect(reverse("kam:plan"))

                            line_rows: List[Dict] = []
                            parse_errors = False

                            for cust in customers_selected:
                                # Accept per-customer fields by stable naming: visit_date_<id>, visit_date_to_<id>, etc.
                                vd = _parse_iso_date(request.POST.get(f"visit_date_{cust.id}") or "")
                                vdt = _parse_iso_date(request.POST.get(f"visit_date_to_{cust.id}") or "")
                                lp = (request.POST.get(f"purpose_{cust.id}") or "").strip()
                                loc = (request.POST.get(f"location_{cust.id}") or "").strip()
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

                                # If per-customer dates not provided, fall back to batch window
                                if not vd:
                                    vd = batch_form.cleaned_data.get("from_date")
                                if not vdt:
                                    vdt = batch_form.cleaned_data.get("to_date")

                                # Minimal sanity: end date >= start date
                                if vd and vdt and vdt < vd:
                                    messages.error(request, f"To date cannot be earlier than From date for customer: {cust.name}")
                                    parse_errors = True

                                if not loc:
                                    loc = (cust.address or "").strip()

                                line_rows.append(
                                    {
                                        "customer": cust,
                                        "visit_date": vd,
                                        "visit_date_to": vdt,
                                        "purpose": lp,
                                        "location": loc,
                                        "expected_sales_mt": expected_sales,
                                        "expected_collection": expected_coll,
                                    }
                                )

                            if not parse_errors:
                                with transaction.atomic():
                                    batch: VisitBatch = batch_form.save(commit=False)
                                    batch.kam = user
                                    batch.approval_status = VisitBatch.PENDING
                                    batch.save()

                                    created_lines = 0
                                    for r in line_rows:
                                        VisitPlan.objects.create(
                                            batch=batch,
                                            customer=r["customer"],
                                            kam=user,
                                            visit_date=r["visit_date"],
                                            visit_date_to=r["visit_date_to"],
                                            visit_type=VisitPlan.PLANNED,
                                            visit_category=VisitPlan.CAT_CUSTOMER,
                                            purpose=r["purpose"] or None,
                                            expected_sales_mt=r["expected_sales_mt"],
                                            expected_collection=r["expected_collection"],
                                            location=r["location"] or "",
                                            approval_status=VisitPlan.PENDING,
                                        )
                                        created_lines += 1

                                    mgr = list(User.objects.filter(username__iexact=APPROVAL_PRIMARY_MANAGER_USERNAME)) or []
                                    cc = list(User.objects.filter(username__in=APPROVAL_CC_USERNAMES))

                                    subject = f"[KAM] Proceed to Manager: Visit batch by {user.username} ({batch.from_date}..{batch.to_date})"

                                    # Consolidated body (one mail only)
                                    lines_txt = []
                                    for i, r in enumerate(line_rows, start=1):
                                        c = r["customer"]
                                        es = r["expected_sales_mt"]
                                        ec = r["expected_collection"]
                                        lines_txt.append(
                                            "\n".join(
                                                [
                                                    f"{i}. {c.name}",
                                                    f"   Window: {r['visit_date']} to {r['visit_date_to']}",
                                                    f"   Purpose: {r['purpose'] or '-'}",
                                                    f"   Location: {r['location'] or '-'}",
                                                    f"   Expected Sales (MT): {es if es is not None else '-'}",
                                                    f"   Expected Collection (₹): {ec if ec is not None else '-'}",
                                                ]
                                            )
                                        )

                                    body = (
                                        f"KAM: {user.get_full_name() or user.username}\n"
                                        f"Category: Customer Visit\n"
                                        f"Batch Window: {batch.from_date} to {batch.to_date}\n"
                                        f"Batch Id: {batch.id}\n"
                                        f"Remarks (mandatory):\n{remarks}\n\n"
                                        f"Customers & Details:\n"
                                        + "\n\n".join(lines_txt)
                                        + "\n\nApproval is informational and does not block execution."
                                    )

                                    _send_safe_mail(subject, body, mgr, cc)

                                messages.success(
                                    request,
                                    f"Proceed to Manager successful: {len(line_rows)} customers submitted (Batch #{batch.id}).",
                                )
                                return redirect(reverse("kam:plan"))

            # --------------------------------------------
            # Legacy batch submission (kept for compatibility)
            # --------------------------------------------
            with transaction.atomic():
                batch: VisitBatch = batch_form.save(commit=False)
                batch.kam = user
                batch.approval_status = VisitBatch.PENDING
                batch.save()

                created_lines = 0

                if batch.visit_category == VisitPlan.CAT_CUSTOMER:
                    customers_selected = batch_form.cleaned_data.get("customers")
                    # Safety: customers_selected already validated against scoped queryset
                    for cust in customers_selected:
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
                            purpose=(f.cleaned_data.get("counterparty_purpose") or batch.purpose),
                            location=(f.cleaned_data.get("counterparty_location") or ""),
                            approval_status=VisitPlan.PENDING,
                        )
                        created_lines += 1

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

    elif request.method == "POST":
        # -----------------------------
        # SINGLE POST
        # -----------------------------
        form = VisitPlanForm(request.POST, prefix=SINGLE_PREFIX)
        if "customer" in form.fields:
            form.fields["customer"].queryset = customer_qs

        if form.is_valid():
            plan: VisitPlan = form.save(commit=False)
            plan.kam = user

            # B1: enforce posted customer is within scope (extra guard)
            if plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if plan.customer_id:
                    if not customer_qs.filter(id=plan.customer_id).exists():
                        messages.error(request, "Invalid customer selection (out of your scope).")
                        return redirect(reverse("kam:plan"))

            if not plan.location:
                if plan.visit_category == VisitPlan.CAT_CUSTOMER and plan.customer and plan.customer.address:
                    plan.location = plan.customer.address

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

            plan.approval_status = VisitPlan.PENDING
            plan.save()

            try:
                if plan.visit_type == VisitPlan.PLANNED:
                    mgr = list(User.objects.filter(username__iexact=APPROVAL_PRIMARY_MANAGER_USERNAME)) or []
                    cc = list(User.objects.filter(username__in=APPROVAL_CC_USERNAMES))
                    base = plan.customer.name if plan.customer_id else (plan.counterparty_name or "-")
                    subject = f"[KAM] Visit planned by {user.username} on {plan.visit_date}"
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

    week_start, week_end, _ = _iso_week_bounds(timezone.now())
    my_plans = (
        VisitPlan.objects.select_related("customer")
        .filter(kam=user, visit_date__gte=week_start.date(), visit_date__lt=week_end.date())
        .order_by("visit_date", "customer__name")
    )

    ctx = {
        "page_title": "Plan Visit",
        "form": form,
        "plans": my_plans,
        "customers": customers,
        "batch_form": batch_form,
        "non_customer_template_line": MultiVisitPlanLineForm(),
        "SINGLE_PREFIX": SINGLE_PREFIX,
        "BATCH_PREFIX": BATCH_PREFIX,
    }
    return render(request, "kam/visit_plan.html", ctx)


@login_required
@require_kam_code("kam_manager")
def visit_batches(request: HttpRequest) -> HttpResponse:
    """
    B4: Manager/Admin can view all VisitBatches.
    Until template is delivered, returns JSON by default.
    """
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    qs = (
        VisitBatch.objects.select_related("kam")
        .order_by("-created_at")
    )

    # Lightweight JSON response (no template dependency)
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
                "purpose": b.purpose or "",
                "created_at": timezone.localtime(b.created_at).isoformat() if b.created_at else None,
            }
        )
    return JsonResponse({"ok": True, "count": len(rows), "batches": rows})


@login_required
@require_kam_code("kam_visits")
def visits(request: HttpRequest) -> HttpResponse:
    """
    Post-visit update:
      - Mandatory: expected_sales_mt & expected_collection (written back to plan),
                   remarks (summary), confirmed location.
      - Allows capturing actual_sales_mt and actual_collection.
    """
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


# -----------------------------------------------------------------------------
# Customer 360 (unchanged)
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_customers")
def customers(request: HttpRequest) -> HttpResponse:
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    customer_id = request.GET.get("id")

    if _is_manager(request.user):
        base_qs = Customer.objects.all()
        if scope_kam_id is not None:
            base_qs = base_qs.filter(primary_kam_id=scope_kam_id)
    else:
        base_qs = Customer.objects.filter(primary_kam=request.user)

    customer_list = list(base_qs.order_by("name")[:300])
    customer = get_object_or_404(base_qs, id=customer_id) if customer_id else (customer_list[0] if customer_list else None)

    period_type, start_dt, end_dt, period_id = _get_period(request)
    start_date = start_dt.date()
    end_date = (end_dt - timezone.timedelta(days=1)).date()

    exposure = overdue = credit_limit = Decimal(0)
    ageing = {"a0_30": Decimal(0), "a31_60": Decimal(0), "a61_90": Decimal(0), "a90_plus": Decimal(0)}
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

    kam_options = _manager_kam_options_recent_first() if _is_manager(request.user) else []

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
# Targets (unchanged)
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
# Reports (Deep Analysis)
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    """
    Deep Analysis support:
      - ?metric=sales|calls|visits
      - Uses same strict scope rules as dashboard
      - Uses From/To date range like dashboard (defaults to current week)
    """
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    metric = (request.GET.get("metric") or "").strip().lower() or "sales"

    rows = []

    if metric == "sales":
        qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(
            qs.values("customer__name", "kam__username")
            .annotate(mt=Sum("qty_mt"))
            .order_by("-mt")[:300]
        )

    elif metric == "calls":
        # FIXED: do NOT select_related(customer) because Customer table is missing column 'code' in DB.
        qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "call_datetime",
                "kam__username",
                "customer_id",
                "customer__name",
            ).order_by("-call_datetime")[:500]
        )

    elif metric == "visits":
        # FIXED: avoid select_related(plan__customer) for the same reason.
        qs = VisitActual.objects.filter(
            plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date()
        )
        if scope_kam_id is not None:
            qs = qs.filter(plan__kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "successful",
                "plan__visit_date",
                "plan__kam__username",
                "plan__customer_id",
                "plan__customer__name",
            ).order_by("-plan__visit_date")[:500]
        )

    else:
        metric = "sales"
        qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(
            qs.values("customer__name", "kam__username")
            .annotate(mt=Sum("qty_mt"))
            .order_by("-mt")[:300]
        )

    kam_options = _manager_kam_options_recent_first() if _is_manager(request.user) else []

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
# Collections Plan (unchanged)
# -----------------------------------------------------------------------------
@login_required
@require_kam_code("kam_collections_plan")
def collections_plan(request: HttpRequest) -> HttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)

    if request.method == "POST":
        form = CollectionPlanForm(request.POST)
        if form.is_valid():
            cp: CollectionPlan = form.save(commit=False)
            cp.kam = cp.customer.primary_kam or request.user
            cp.save()
            messages.success(request, "Collection plan saved.")
            return redirect(
                f"{reverse('kam:collections_plan')}?period={request.GET.get('period','month')}&asof={request.GET.get('asof','')}"
            )
    else:
        form = CollectionPlanForm(initial={"period_type": period_type, "period_id": period_id})

    plan_qs = CollectionPlan.objects.select_related("customer", "kam")

    period_rows = plan_qs.filter(period_type=period_type, period_id=period_id)
    range_rows = plan_qs.filter(
        from_date__isnull=False, to_date__isnull=False, from_date__lte=end_dt.date(), to_date__gte=start_dt.date()
    )
    plan_qs = (period_rows | range_rows).distinct()

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
# Sync endpoints (unchanged)
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
    """Create (or reuse) a pending SyncIntent token."""
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
    """
    Advance one sync step (JSON).
    IMPORTANT: If called via HTML form POST without token, run full sync and redirect
    (so Manager "Sync now" button doesn't land on a JSON page).
    """
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
