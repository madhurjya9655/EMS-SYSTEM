# apps/reimbursement/views_analytics.py
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import (
    Sum,
    Count,
    Q,
    F,
    DecimalField,
    Value as V,
)
from django.db.models.functions import (
    Coalesce,
    TruncWeek,
    TruncMonth,
    TruncQuarter,
    TruncYear,
)
from django.http import JsonResponse, HttpResponseForbidden
from django.utils import timezone
from django.views.generic import TemplateView, View

from .models import (
    ReimbursementRequest,
    ReimbursementLine,
    ExpenseItem,
    REIMBURSEMENT_CATEGORY_CHOICES,
)
from .views import _user_is_admin, _user_is_finance, _user_is_manager  # reuse existing helpers

# ----------------------------
# Decimal helpers
# ----------------------------

# A reusable "decimal zero" with explicit output_field so Coalesce/Sum remain DecimalField
DEC0 = V(Decimal("0"), output_field=DecimalField(max_digits=18, decimal_places=2))


def d(val: str | int | float) -> Decimal:
    """Safe decimal ctor (e.g., d(0), d('100.50'))."""
    return Decimal(str(val))


# ----------------------------
# Access control (role-aware)
# ----------------------------

def _user_can_view_analytics(user) -> bool:
    """
    Allow only Admin / Finance / Management.
    Explicitly deny normal employees.
    """
    return bool(_user_is_admin(user) or _user_is_finance(user) or _user_is_manager(user))


# ----------------------------
# Filter parsing helpers
# ----------------------------

@dataclass
class Filters:
    employee_ids: List[int]
    categories: List[str]
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    granularity: str           # 'weekly' | 'monthly' | 'quarterly' | 'yearly'
    status_mode: str           # 'approved_and_paid' | 'approved_only' | 'paid_only'


_CATEGORY_KEYS = {key for key, _ in REIMBURSEMENT_CATEGORY_CHOICES}
_CATEGORY_LABELS = dict(REIMBURSEMENT_CATEGORY_CHOICES)

_GRANULARITIES = {"weekly", "monthly", "quarterly", "yearly"}
_STATUS_MODES = {"approved_and_paid", "approved_only", "paid_only"}


def _ist_now() -> datetime:
    tz = timezone.get_current_timezone()
    return timezone.localtime(timezone.now(), tz)


def _month_bounds(dt: date) -> Tuple[datetime, datetime]:
    start_day = date(dt.year, dt.month, 1)
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    end_day = date(dt.year, dt.month, last_day)
    tz = timezone.get_current_timezone()
    start_dt = datetime.combine(start_day, datetime.min.time()).replace(tzinfo=tz)
    end_dt = datetime.combine(end_day, datetime.max.time()).replace(tzinfo=tz)
    return start_dt, end_dt


def _quarter_bounds(dt: date) -> Tuple[datetime, datetime]:
    q = (dt.month - 1) // 3 + 1
    start_month = 3 * (q - 1) + 1
    end_month = start_month + 2
    tz = timezone.get_current_timezone()
    start_dt = datetime(dt.year, start_month, 1, 0, 0, 0, tzinfo=tz)
    last_day = calendar.monthrange(dt.year, end_month)[1]
    end_dt = datetime(dt.year, end_month, last_day, 23, 59, 59, tzinfo=tz)
    return start_dt, end_dt


def _year_bounds(dt: date) -> Tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    start_dt = datetime(dt.year, 1, 1, 0, 0, 0, tzinfo=tz)
    end_dt = datetime(dt.year, 12, 31, 23, 59, 59, tzinfo=tz)
    return start_dt, end_dt


def _parse_filters(request) -> Filters:
    """
    Query params:
      - employees: CSV of user IDs
      - categories: CSV of expense category keys (travel, meal, yard, office, other)
      - from, to: ISO dates (YYYY-MM-DD) inclusive
      - preset: weekly | monthly | quarterly | yearly  (sets granularity + default range)
      - granularity: weekly | monthly | quarterly | yearly
      - status: approved_and_paid (default) | approved_only | paid_only
    """
    # employees
    emp_csv = (request.GET.get("employees") or "").strip()
    employee_ids: List[int] = []
    for piece in [p.strip() for p in emp_csv.split(",") if p.strip()]:
        if piece.isdigit():
            employee_ids.append(int(piece))

    # categories
    cat_csv = (request.GET.get("categories") or "").strip().lower()
    categories = [c for c in [p.strip() for p in cat_csv.split(",") if p.strip()] if c in _CATEGORY_KEYS]

    # status
    status_mode = (request.GET.get("status") or "approved_and_paid").strip().lower()
    if status_mode not in _STATUS_MODES:
        status_mode = "approved_and_paid"

    # granularity & presets
    preset = (request.GET.get("preset") or "").strip().lower()
    granularity = (request.GET.get("granularity") or "").strip().lower()
    if not granularity:
        granularity = preset or "monthly"
    if granularity not in _GRANULARITIES:
        granularity = "monthly"

    # date range (defaults)
    tz_now = _ist_now()
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None

    if preset in {"weekly", "monthly", "quarterly", "yearly"}:
        today = tz_now.date()
        if preset == "weekly":
            # Last 7 days window ending today
            tz = timezone.get_current_timezone()
            end_dt = datetime.combine(today, datetime.max.time()).replace(tzinfo=tz)
            start_dt = end_dt - timedelta(days=6)
        elif preset == "monthly":
            start_dt, end_dt = _month_bounds(today)
        elif preset == "quarterly":
            start_dt, end_dt = _quarter_bounds(today)
        else:
            start_dt, end_dt = _year_bounds(today)

    # explicit from/to override preset
    raw_from = (request.GET.get("from") or "").strip()
    raw_to = (request.GET.get("to") or "").strip()
    tz = timezone.get_current_timezone()
    if raw_from:
        y, m, d_ = [int(x) for x in raw_from.split("-")]
        start_dt = datetime(y, m, d_, 0, 0, 0, tzinfo=tz)
    if raw_to:
        y, m, d_ = [int(x) for x in raw_to.split("-")]
        end_dt = datetime(y, m, d_, 23, 59, 59, tzinfo=tz)

    return Filters(
        employee_ids=employee_ids,
        categories=categories,
        date_from=start_dt,
        date_to=end_dt,
        granularity=granularity,
        status_mode=status_mode,
    )


# ----------------------------
# Base queryset (server-side)
# ----------------------------

def _status_q(status_mode: str) -> Q:
    if status_mode == "approved_only":
        return Q(request__status=ReimbursementRequest.Status.APPROVED)
    if status_mode == "paid_only":
        return Q(request__status=ReimbursementRequest.Status.PAID)
    # approved_and_paid (default)
    return Q(request__status__in=[ReimbursementRequest.Status.APPROVED, ReimbursementRequest.Status.PAID])


def _base_lines_qs(f: Filters):
    """
    Start from ReimbursementLine to keep category fidelity.
    Only INCLUDED lines, joined to request & expense item.
    """
    qs = (
        ReimbursementLine.objects.filter(status=ReimbursementLine.Status.INCLUDED)
        .filter(_status_q(f.status_mode))
        .select_related("request", "expense_item", "request__created_by")
    )

    if f.employee_ids:
        qs = qs.filter(request__created_by_id__in=f.employee_ids)

    if f.categories:
        qs = qs.filter(expense_item__category__in=f.categories)

    if f.date_from:
        qs = qs.filter(request__submitted_at__gte=f.date_from)
    if f.date_to:
        qs = qs.filter(request__submitted_at__lte=f.date_to)

    return qs


# ----------------------------
# Views
# ----------------------------

class AnalyticsDashboardView(LoginRequiredMixin, TemplateView):
    """
    HTML dashboard shell. All data loads via fetch() from the JSON endpoints below.
    """
    template_name = "reimbursement/analytics_dashboard.html"

    def dispatch(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return HttpResponseForbidden("You are not allowed to access Reimbursement Analytics.")
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        # Provide labels to JS
        ctx["categoryLabels"] = _CATEGORY_LABELS
        return ctx


class AnalyticsSummaryAPI(LoginRequiredMixin, View):
    """
    Top KPI cards (dynamic with filters).
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        # Total (current filter window)
        total_spend = qs.aggregate(total=Coalesce(Sum("amount"), DEC0))["total"] or d(0)

        # Current Month / Quarter / Year (always IST "now")
        now = _ist_now().date()
        m_start, m_end = _month_bounds(now)
        q_start, q_end = _quarter_bounds(now)
        y_start, y_end = _year_bounds(now)

        def _sum_between(start: datetime, end: datetime) -> Decimal:
            qs_w = _base_lines_qs(f).filter(
                request__submitted_at__gte=start,
                request__submitted_at__lte=end,
            )
            return qs_w.aggregate(total=Coalesce(Sum("amount"), DEC0))["total"] or d(0)

        month_spend = _sum_between(m_start, m_end)
        quarter_spend = _sum_between(q_start, q_end)
        year_spend = _sum_between(y_start, y_end)

        # Average per employee (unique employees in window)
        emp_qs = qs.values("request__created_by_id").annotate(s=Coalesce(Sum("amount"), DEC0))
        emp_count = emp_qs.count()
        avg_per_employee = (total_spend / d(emp_count)) if emp_count else d(0)

        # Highest spender (within filtered data)
        top_emp_row = (
            qs.values(
                "request__created_by_id",
                "request__created_by__first_name",
                "request__created_by__last_name",
                "request__created_by__username",
            )
            .annotate(total=Coalesce(Sum("amount"), DEC0))
            .order_by("-total")
            .first()
        )
        highest_spender = None
        if top_emp_row:
            fn = (top_emp_row.get("request__created_by__first_name") or "").strip()
            ln = (top_emp_row.get("request__created_by__last_name") or "").strip()
            uname = (top_emp_row.get("request__created_by__username") or "").strip()
            display = (f"{fn} {ln}".strip() or uname or f"User #{top_emp_row['request__created_by_id']}")
            highest_spender = {
                "employee_id": top_emp_row["request__created_by_id"],
                "employee_name": display,
                "total": float(top_emp_row["total"] or d(0)),
            }

        data = {
            "total_spend": float(total_spend),
            "current_month_spend": float(month_spend),
            "current_quarter_spend": float(quarter_spend),
            "current_year_spend": float(year_spend),
            "average_spend_per_employee": float(avg_per_employee),
            "highest_spender": highest_spender,
        }
        return JsonResponse(data, safe=False)


class AnalyticsTimeSeriesAPI(LoginRequiredMixin, View):
    """
    Time-based spend: weekly / monthly / quarterly / yearly series.
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        if f.granularity == "weekly":
            trunc = TruncWeek("request__submitted_at")
        elif f.granularity == "quarterly":
            trunc = TruncQuarter("request__submitted_at")
        elif f.granularity == "yearly":
            trunc = TruncYear("request__submitted_at")
        else:
            trunc = TruncMonth("request__submitted_at")

        rows = (
            qs.annotate(bucket=trunc)
              .values("bucket")
              .annotate(total=Coalesce(Sum("amount"), DEC0))
              .order_by("bucket")
        )

        out = [{"period": r["bucket"], "total": float(r["total"] or d(0))} for r in rows if r["bucket"]]
        return JsonResponse(out, safe=False)


class AnalyticsCategoryAPI(LoginRequiredMixin, View):
    """
    Category breakdown: pie/donut (percentage) & bars (absolute).
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        rows = (
            qs.values("expense_item__category")
              .annotate(total=Coalesce(Sum("amount"), DEC0))
              .order_by("-total")
        )
        total_all = sum([float(r["total"] or d(0)) for r in rows]) or 1.0
        out = []
        for r in rows:
            key = r["expense_item__category"]
            label = _CATEGORY_LABELS.get(key, key.title())
            value = float(r["total"] or d(0))
            out.append({
                "category": key,
                "label": label,
                "total": value,
                "percent": (value / total_all) * 100.0,
            })
        return JsonResponse(out, safe=False)


class AnalyticsEmployeeAPI(LoginRequiredMixin, View):
    """
    Employee spend table + 'Top 5' list.
    - total amount per employee
    - number of reimbursement requests (distinct request IDs)
    - most used expense category
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        # Base aggregates per employee
        base = (
            qs.values(
                "request__created_by_id",
                "request__created_by__first_name",
                "request__created_by__last_name",
                "request__created_by__username",
            )
            .annotate(
                total=Coalesce(Sum("amount"), DEC0),
                request_count=Count("request_id", distinct=True),
            )
        )

        rows_map: Dict[int, dict] = {}
        for r in base:
            uid = r["request__created_by_id"]
            fn = (r.get("request__created_by__first_name") or "").strip()
            ln = (r.get("request__created_by__last_name") or "").strip()
            uname = (r.get("request__created_by__username") or "").strip()
            display = (f"{fn} {ln}".strip() or uname or f"User #{uid}")
            rows_map[uid] = {
                "employee_id": uid,
                "employee_name": display,
                "total": float(r["total"] or d(0)),
                "request_count": int(r["request_count"] or 0),
                "top_category": "-",  # fill below
            }

        # Most used category per employee (by amount, break ties by count)
        cat_rows = (
            qs.values("request__created_by_id", "expense_item__category")
              .annotate(
                  amt=Coalesce(Sum("amount"), DEC0),
                  cnt=Count("id"),
              )
        )
        best: Dict[int, Tuple[str, float, int]] = {}  # uid -> (key, amt, cnt)
        for r in cat_rows:
            uid = r["request__created_by_id"]
            key = r["expense_item__category"]
            amt = float(r["amt"] or d(0))
            cnt = int(r["cnt"] or 0)
            cur = best.get(uid)
            if cur is None or amt > cur[1] or (amt == cur[1] and cnt > cur[2]):
                best[uid] = (key, amt, cnt)

        for uid, info in best.items():
            if uid in rows_map:
                key = info[0]
                rows_map[uid]["top_category"] = _CATEGORY_LABELS.get(key, key.title())

        rows = sorted(rows_map.values(), key=lambda x: (-x["total"], x["employee_name"]))
        top5 = rows[:5]

        return JsonResponse({"rows": rows, "top5": top5}, safe=False)


class AnalyticsHighRiskAPI(LoginRequiredMixin, View):
    """
    High-spend detection:
      - employees whose spend exceeds company average by X% (threshold param)
    Query param: threshold (default 50 for 50%)
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        threshold_pct = 50.0
        raw = (request.GET.get("threshold") or "").strip()
        try:
            if raw:
                threshold_pct = max(0.0, float(raw))
        except Exception:
            pass

        by_emp = (
            qs.values(
                "request__created_by_id",
                "request__created_by__first_name",
                "request__created_by__last_name",
                "request__created_by__username",
            )
            .annotate(total=Coalesce(Sum("amount"), DEC0))
        )
        totals = [float(r["total"] or d(0)) for r in by_emp]
        if not totals:
            return JsonResponse({"threshold": threshold_pct, "flags": []}, safe=False)

        avg = sum(totals) / len(totals)
        cutoff = avg * (1.0 + threshold_pct / 100.0)

        flags = []
        for r in by_emp:
            val = float(r["total"] or d(0))
            if val > cutoff:
                uid = r["request__created_by_id"]
                fn = (r.get("request__created_by__first_name") or "").strip()
                ln = (r.get("request__created_by__last_name") or "").strip()
                uname = (r.get("request__created_by__username") or "").strip()
                display = (f"{fn} {ln}".strip() or uname or f"User #{uid}")
                flags.append({"employee_id": uid, "employee_name": display, "total": val})

        flags.sort(key=lambda x: -x["total"])
        return JsonResponse({"threshold": threshold_pct, "average": avg, "cutoff": cutoff, "flags": flags}, safe=False)
