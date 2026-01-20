from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from django.contrib.auth import get_user_model
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import (
    Sum,
    Count,
    Q,
    DecimalField,
    Value as V,
    Max,
    Min,
)
from django.db.models.functions import Coalesce, TruncDay, TruncMonth
from django.http import JsonResponse, HttpResponseForbidden, HttpResponseNotFound
from django.utils import timezone
from django.views.generic import TemplateView, View

from .models import (
    ReimbursementRequest,
    ReimbursementLine,
    REIMBURSEMENT_CATEGORY_CHOICES,
)
from .views import _user_is_admin, _user_is_finance, _user_is_manager  # reuse existing helpers

User = get_user_model()

# ---------------------------------------------------------------------
# Decimal helpers
# ---------------------------------------------------------------------

# A reusable "decimal zero" with explicit output_field so Coalesce/Sum remain DecimalField
DEC0 = V(Decimal("0"), output_field=DecimalField(max_digits=18, decimal_places=2))


def d(val: str | int | float | Decimal | None) -> Decimal:
    """Safe decimal ctor (e.g., d(0), d('100.50'))."""
    try:
        return Decimal(str(val if val is not None else 0))
    except Exception:
        return Decimal("0")


# ---------------------------------------------------------------------
# Access control (role-aware)
# ---------------------------------------------------------------------

def _user_can_view_analytics(user) -> bool:
    """
    Allow only Admin / Finance / Management.
    Explicitly deny normal employees.
    """
    return bool(_user_is_admin(user) or _user_is_finance(user) or _user_is_manager(user))


# ---------------------------------------------------------------------
# Filters (date-range + bill-wise core)
# ---------------------------------------------------------------------

@dataclass
class Filters:
    """
    Filter set:
      - employees: CSV of user IDs (used to scope analytics)
      - status: approved_and_paid | approved_only | paid_only   ← applied to BILL STATUS
      - line_ids: CSV of ReimbursementLine PKs (drives bill-wise scoping)
      - from_date / to_date: YYYY-MM-DD (inclusive range on expense date)
      - preset: this_month | last_month | last_90_days | ytd | fytd (ignored if from/to supplied)
      - granularity: day | month (affects timeseries endpoint only)
      - categories: CSV of category keys (travel, meal, yard, office, other)
    """
    employee_ids: List[int]
    status_mode: str
    line_ids: List[int]
    from_date: Optional[date]
    to_date: Optional[date]
    preset: Optional[str]
    granularity: str
    categories: List[str]


_STATUS_MODES = {"approved_and_paid", "approved_only", "paid_only"}
_VALID_PRESETS = {"this_month", "last_month", "last_90_days", "ytd", "fytd"}
_VALID_GRANULARITY = {"day", "month"}
_CATEGORY_LABELS = dict(REIMBURSEMENT_CATEGORY_CHOICES)
_VALID_CATEGORY_KEYS = set(_CATEGORY_LABELS.keys())


def _parse_csv_ints(raw: str) -> List[int]:
    out: List[int] = []
    for piece in [p.strip() for p in (raw or "").split(",") if p.strip()]:
        try:
            out.append(int(piece))
        except Exception:
            continue
    return out


def _parse_csv_strs(raw: str) -> List[str]:
    return [p.strip().lower() for p in (raw or "").split(",") if p.strip()]


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _month_bounds(dt: date) -> Tuple[date, date]:
    start = dt.replace(day=1)
    if dt.month == 12:
        end = date(dt.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(dt.year, dt.month + 1, 1) - timedelta(days=1)
    return start, end


def _fy_start(dt: date) -> date:
    # Indian FY default (Apr 1)
    return date(dt.year if dt.month >= 4 else dt.year - 1, 4, 1)


def _preset_range(preset: str) -> Tuple[date, date]:
    today = timezone.localdate()
    if preset == "this_month":
        return _month_bounds(today)
    if preset == "last_month":
        if today.month == 1:
            prev = date(today.year - 1, 12, 1)
        else:
            prev = date(today.year, today.month - 1, 1)
        return _month_bounds(prev)
    if preset == "last_90_days":
        return today - timedelta(days=89), today
    if preset == "ytd":
        return date(today.year, 1, 1), today
    if preset == "fytd":
        return _fy_start(today), today
    # Fallback: this_month
    return _month_bounds(today)


def _parse_filters(request) -> Filters:
    """
    Accepted query params:
      - employees: CSV of user IDs to include (optional)
      - status: approved_and_paid (default) | approved_only | paid_only
      - line_ids: CSV of line PKs to include (optional; bill-wise filter)
      - from: YYYY-MM-DD (expense date)
      - to:   YYYY-MM-DD (expense date)
      - preset: this_month | last_month | last_90_days | ytd | fytd
      - granularity: day | month
      - categories: CSV of category keys (travel, meal, yard, office, other)
    """
    # employees (IDs come from dropdown behind the scenes; UI shows names)
    employee_ids = _parse_csv_ints((request.GET.get("employees") or "").strip())

    # status
    status_mode = (request.GET.get("status") or "approved_and_paid").strip().lower()
    if status_mode not in _STATUS_MODES:
        status_mode = "approved_and_paid"

    # bill-wise
    line_ids = _parse_csv_ints((request.GET.get("line_ids") or "").strip())

    # time windows
    from_date = _parse_date(request.GET.get("from"))
    to_date = _parse_date(request.GET.get("to"))
    preset = (request.GET.get("preset") or "").strip().lower() or None
    if (from_date is None or to_date is None) and preset in _VALID_PRESETS:
        start, end = _preset_range(preset)
        from_date, to_date = start, end

    # granularity (for timeseries)
    granularity = (request.GET.get("granularity") or "").strip().lower() or "day"
    if granularity not in _VALID_GRANULARITY:
        # Auto default: if range > 90 days -> month; else day
        if from_date and to_date and (to_date - from_date).days > 90:
            granularity = "month"
        else:
            granularity = "day"

    # categories (optional)
    categories = [c for c in _parse_csv_strs(request.GET.get("categories")) if c in _VALID_CATEGORY_KEYS]

    return Filters(
        employee_ids=employee_ids,
        status_mode=status_mode,
        line_ids=line_ids,
        from_date=from_date,
        to_date=to_date,
        preset=preset,
        granularity=granularity,
        categories=categories,
    )


# ---------------------------------------------------------------------
# Base queryset (server-side) — BILL-WISE
# ---------------------------------------------------------------------

def _status_q(status_mode: str) -> Q:
    """
    Status filter applied to *bill* status, per client requirement.
    - approved_only      -> FINANCE_APPROVED
    - paid_only          -> PAID
    - approved_and_paid  -> FINANCE_APPROVED or PAID
    """
    BS = ReimbursementLine.BillStatus
    if status_mode == "approved_only":
        return Q(bill_status=BS.FINANCE_APPROVED)
    if status_mode == "paid_only":
        return Q(bill_status=BS.PAID)
    # approved_and_paid (default)
    return Q(bill_status__in=[BS.FINANCE_APPROVED, BS.PAID])


def _base_lines_qs(f: Filters):
    """
    Start from ReimbursementLine to keep category fidelity and BILL-wise analytics.
    Only INCLUDED lines, joined to request & expense item.
    """
    qs = (
        ReimbursementLine.objects.filter(status=ReimbursementLine.Status.INCLUDED)
        .filter(_status_q(f.status_mode))
        .select_related("request", "expense_item", "request__created_by")
    )

    if f.employee_ids:
        qs = qs.filter(request__created_by_id__in=f.employee_ids)

    # Bill-wise filter must affect analytics
    if f.line_ids:
        qs = qs.filter(id__in=f.line_ids)

    # Time window on expense date (date-range)
    if f.from_date:
        qs = qs.filter(expense_item__date__gte=f.from_date)
    if f.to_date:
        qs = qs.filter(expense_item__date__lte=f.to_date)

    # Optional category filter
    if f.categories:
        qs = qs.filter(expense_item__category__in=f.categories)

    return qs


def _request_qs_scoped_by_lines(f: Filters):
    """
    Derive request queryset *scoped by the filtered lines*:
    - Take distinct request IDs from the current line scope
    - Then operate request-level counts on that subset
    """
    line_qs = _base_lines_qs(f)
    req_ids = line_qs.values_list("request_id", flat=True).distinct()
    return ReimbursementRequest.objects.filter(id__in=req_ids)


# ---------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------

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
        # Provide labels for UI legends
        ctx = super().get_context_data(**kwargs)
        ctx["categoryLabels"] = dict(REIMBURSEMENT_CATEGORY_CHOICES)
        return ctx


class EmployeeOptionsAPI(LoginRequiredMixin, View):
    """
    Populate the 'Employee' dropdown by *name*.
    Returns: [{id, name}] for active users who have at least one reimbursement line.
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        users_qs = (
            User.objects.filter(is_active=True, reimbursement_requests__lines__isnull=False)
            .distinct()
            .order_by("first_name", "last_name", "username")
        )

        def _display(u: User) -> str:
            full = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip()
            return full or (u.username or f"User #{u.id}")

        data = [{"id": u.id, "name": _display(u)} for u in users_qs]
        return JsonResponse(data, safe=False)


class BillwiseTableAPI(LoginRequiredMixin, View):
    """
    Raw bill-wise rows for the table (read-only).
    Filters respected:
      - employees (IDs)
      - status (mode)  ← bill_status
      - line_ids (bill-wise filter)
      - from/to/preset
      - categories
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        rows: List[Dict] = []
        for ln in qs.values(
            "id",
            "request_id",
            "amount",
            "bill_status",
            "request__status",
            "request__submitted_at",
            "request__updated_at",
            "request__created_by_id",
            "request__created_by__first_name",
            "request__created_by__last_name",
            "request__created_by__username",
            "expense_item__date",
            "expense_item__category",
            "expense_item__gst_type",
            "expense_item__vendor",
            "description",
        ):
            fn = (ln.get("request__created_by__first_name") or "").strip()
            ln_ = (ln.get("request__created_by__last_name") or "").strip()
            un = (ln.get("request__created_by__username") or "").strip()
            emp_name = (f"{fn} {ln_}".strip() or un or f"User #{ln['request__created_by_id']}")
            rows.append(
                {
                    "line_id": ln["id"],
                    "reimb_id": ln["request_id"],
                    "employee_id": ln["request__created_by_id"],
                    "employee_name": emp_name,
                    "amount": float(d(ln["amount"])) if ln["amount"] is not None else 0.0,
                    "category": ln["expense_item__category"] or "",
                    "gst_type": ln["expense_item__gst_type"] or "",
                    "expense_date": (ln["expense_item__date"].isoformat() if ln["expense_item__date"] else None),
                    "vendor": ln["expense_item__vendor"] or "",
                    "description": ln["description"] or "",
                    "request_status": ln["request__status"] or "",
                    "bill_status": ln["bill_status"] or "",
                    "submitted_at": (ln["request__submitted_at"].isoformat() if ln["request__submitted_at"] else None),
                    "updated_at": (ln["request__updated_at"].isoformat() if ln["request__updated_at"] else None),
                }
            )
        return JsonResponse({"rows": rows})


class AnalyticsSummaryAPI(LoginRequiredMixin, View):
    """
    KPI cards (BILL-wise; read-only):
      - total spend
      - highest bill amount
      - lowest bill amount
      - employee-wise spend list (respects all filters)
      - highest/lowest spender (by employee totals)
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        # Total spend across filtered lines
        total_spend = qs.aggregate(total=Coalesce(Sum("amount"), DEC0))["total"] or d(0)

        # Highest/Lowest *single bill*
        agg_hi = qs.aggregate(hi=Max("amount"))
        agg_lo = qs.aggregate(lo=Min("amount"))
        highest_bill = float(d(agg_hi.get("hi"))) if agg_hi.get("hi") is not None else 0.0
        lowest_bill = float(d(agg_lo.get("lo"))) if agg_lo.get("lo") is not None else 0.0

        # Employee aggregates (within current filter) — bill-wise sum and bill counts
        emp_rows = (
            qs.values(
                "request__created_by_id",
                "request__created_by__first_name",
                "request__created_by__last_name",
                "request__created_by__username",
            )
            .annotate(
                total=Coalesce(Sum("amount"), DEC0),
                bill_count=Count("id"),
            )
            .order_by("-total")
        )

        employee_spend: List[Dict] = []
        for r in emp_rows:
            uid = r["request__created_by_id"]
            fn = (r.get("request__created_by__first_name") or "").strip()
            ln = (r.get("request__created_by__last_name") or "").strip()
            uname = (r.get("request__created_by__username") or "").strip()
            display = (f"{fn} {ln}".strip() or uname or f"User #{uid}")
            employee_spend.append(
                {
                    "employee_id": uid,
                    "employee_name": display,
                    "total": float(d(r["total"])),
                    "bill_count": int(r["bill_count"] or 0),
                }
            )

        # Highest/lowest spender based on employee totals
        highest_spender = None
        lowest_spender = None
        if employee_spend:
            highest_spender = employee_spend[0]  # already ordered desc
            lowest_spender = sorted(employee_spend, key=lambda x: (x["total"], x["employee_name"]))[0]

        data = {
            "total_spend": float(d(total_spend)),
            "highest_spend_bill": highest_bill,
            "lowest_spend_bill": lowest_bill,
            "employee_wise_spend": employee_spend,
            "highest_spender": highest_spender,  # {"employee_id","employee_name","total","bill_count"} or None
            "lowest_spender": lowest_spender,    # {"employee_id","employee_name","total","bill_count"} or None
            "filters_applied": {
                "employee_ids": f.employee_ids,
                "status_mode": f.status_mode,
                "line_ids_count": len(f.line_ids),
                "from": f.from_date.isoformat() if f.from_date else None,
                "to": f.to_date.isoformat() if f.to_date else None,
                "preset": f.preset,
                "categories": f.categories,
            },
            "notes": "All computations are bill-wise. Date/status/category filters applied when provided.",
        }
        return JsonResponse(data)


class AnalyticsEmployeeAPI(LoginRequiredMixin, View):
    """
    Employee-wise spend table (BILL-wise):
      - total amount per employee
      - number of bills (bill_count)
      - number of reimbursement requests (distinct request IDs within scoped bills)
      - most used expense category (by amount; ties -> higher count)
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
                bill_count=Count("id"),
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
                "total": float(d(r["total"])),

                # expose both; UI should show bill_count to keep bill-wise semantics visible
                "bill_count": int(r["bill_count"] or 0),
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
            amt = float(d(r["amt"]))
            cnt = int(r["cnt"] or 0)
            cur = best.get(uid)
            if cur is None or amt > cur[1] or (amt == cur[1] and cnt > cur[2]):
                best[uid] = (key, amt, cnt)

        for uid, info in best.items():
            if uid in rows_map:
                key = info[0]
                rows_map[uid]["top_category"] = _CATEGORY_LABELS.get(key, (key or "").title())

        rows = sorted(rows_map.values(), key=lambda x: (-x["total"], x["employee_name"]))
        return JsonResponse({"rows": rows})


# ---------------------------------------------------------------------
# Time-series analytics (bill-wise)
# ---------------------------------------------------------------------

class AnalyticsTimeSeriesAPI(LoginRequiredMixin, View):
    """
    Time-series totals over expense date, bill-wise.
    Query params respected: employees, status, line_ids, from, to, preset, granularity, categories
    Returns:
      {
        "granularity": "day" | "month",
        "buckets": [{"period": "YYYY-MM-DD" or "YYYY-MM-01", "total": float}, ...],
        "from": "...", "to": "...",
      }
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        # Choose the truncate function by granularity
        if f.granularity == "month":
            trunc = TruncMonth("expense_item__date")
            fmt = lambda dt: dt.strftime("%Y-%m-01") if dt else None
        else:
            trunc = TruncDay("expense_item__date")
            fmt = lambda dt: dt.strftime("%Y-%m-%d") if dt else None

        rows = (
            qs.annotate(period=trunc)
              .values("period")
              .annotate(total=Coalesce(Sum("amount"), DEC0))
              .order_by("period")
        )

        buckets = []
        for r in rows:
            buckets.append({
                "period": fmt(r["period"]),
                "total": float(d(r["total"])),
            })

        data = {
            "granularity": f.granularity,
            "buckets": buckets,
            "from": f.from_date.isoformat() if f.from_date else None,
            "to": f.to_date.isoformat() if f.to_date else None,
            "preset": f.preset,
        }
        return JsonResponse(data)


# ---------------------------------------------------------------------
# Category totals (bill-wise)
# ---------------------------------------------------------------------

class AnalyticsCategoryAPI(LoginRequiredMixin, View):
    """
    Category totals (bill-wise) with labels.
    Query params respected: employees, status, line_ids, from, to, preset, categories (to subset)
    Returns:
      {
        "rows": [{"key":"travel","label":"Travel Expenses","total":1234.56,"count":17}, ...],
        "total": 9999.99
      }
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)
        qs = _base_lines_qs(f)

        aggs = (
            qs.values("expense_item__category")
              .annotate(total=Coalesce(Sum("amount"), DEC0), count=Count("id"))
              .order_by("-total", "expense_item__category")
        )

        rows = []
        grand = d(0)
        for r in aggs:
            key = r["expense_item__category"]
            total = d(r["total"])
            rows.append({
                "key": key or "",
                "label": _CATEGORY_LABELS.get(key, (key or "").title()),
                "total": float(total),
                "count": int(r["count"] or 0),
            })
            grand += total

        return JsonResponse({"rows": rows, "total": float(grand)})


# ---------------------------------------------------------------------
# Real-time numbers (scoped to current filtered lines)
# ---------------------------------------------------------------------

class AnalyticsRealtimeNumbersAPI(LoginRequiredMixin, View):
    """
    Live counters for dashboards, respecting the same filters as other endpoints.
    Returns numbers scoped to the set of requests that contain *filtered lines*.

    Example payload:
    {
      "scope": {"request_ids": 12, "line_count": 58},
      "counts": {
        "finance_pending_requests": 4,
        "manager_pending_requests": 3,
        "management_pending_requests": 1,
        "approved_requests": 2,
        "paid_requests": 7,
        "resubmitted_bills": 5,
        "finance_approved_bills": 41,
        "submitted_today": 2
      }
    }
    """
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)

        f = _parse_filters(request)

        # Base, filtered by all bill-wise constraints
        line_qs = _base_lines_qs(f)
        req_qs = _request_qs_scoped_by_lines(f)

        # Scope sizes
        scope_request_count = req_qs.values_list("id", flat=True).count()
        scope_line_count = line_qs.count()

        # Request-level counts within scope (kept as-is; these are parent states)
        counts = {
            "finance_pending_requests": req_qs.filter(status=ReimbursementRequest.Status.PENDING_FINANCE_VERIFY).count(),
            "manager_pending_requests": req_qs.filter(status=ReimbursementRequest.Status.PENDING_MANAGER).count(),
            "management_pending_requests": req_qs.filter(status=ReimbursementRequest.Status.PENDING_MANAGEMENT).count(),
            "approved_requests": req_qs.filter(status=ReimbursementRequest.Status.APPROVED).count(),
            "paid_requests": req_qs.filter(status=ReimbursementRequest.Status.PAID).count(),
        }

        # Bill-level counts within scope
        counts.update({
            "resubmitted_bills": line_qs.filter(
                bill_status=ReimbursementLine.BillStatus.EMPLOYEE_RESUBMITTED
            ).count(),
            "finance_approved_bills": line_qs.filter(
                bill_status=ReimbursementLine.BillStatus.FINANCE_APPROVED
            ).count(),
        })

        # "Submitted today" (by request submitted_at) inside scope
        today = timezone.localdate()
        counts["submitted_today"] = req_qs.filter(
            submitted_at__date=today
        ).count()

        data = {
            "scope": {
                "request_ids": scope_request_count,
                "line_count": scope_line_count,
            },
            "counts": counts,
            "filters_applied": {
                "employee_ids": f.employee_ids,
                "status_mode": f.status_mode,
                "line_ids_count": len(f.line_ids),
                "from": f.from_date.isoformat() if f.from_date else None,
                "to": f.to_date.isoformat() if f.to_date else None,
                "preset": f.preset,
                "categories": f.categories,
            },
        }
        return JsonResponse(data)


# ---------------------------------------------------------------------
# Deprecated/removed endpoint per requirements (kept disabled intentionally)
# ---------------------------------------------------------------------

class AnalyticsHighRiskAPI(LoginRequiredMixin, View):
    """Removed high-risk/high-spend flags."""
    def get(self, request, *args, **kwargs):
        if not _user_can_view_analytics(request.user):
            return JsonResponse({"detail": "forbidden"}, status=403)
        return HttpResponseNotFound("High-risk flags disabled.")
