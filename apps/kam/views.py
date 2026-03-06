# FILE: apps/kam/views.py
# PURPOSE: KAM module views — all functional improvements per spec
# UPDATED: 2026-03-05
#
# FIXES APPLIED IN THIS VERSION:
#   FIX-1  Import CollectionPlanActualForm
#   FIX-2  dashboard() — adds leads_total_count/converted count metrics,
#           collection_planned/actual/ach_pct from CollectionPlan fields,
#           lead_analysis_data + collection_analysis_data for Chart.js
#   FIX-3  collections_plan() POST — use correct field names (actual_amount /
#           collection_date / collection_reference) not wrong ones
#   FIX-4  _build_collections_plan_ctx() — pass totals, plans QuerySet,
#           cp_chart_data, scope_label, can_choose_kam etc. for new template
#   FIX-5  _build_collections_rows() — correct field refs throughout
#   FIX-6  collection_plan_record_actual() — uses CollectionPlanActualForm
#
# NON-NEGOTIABLE BUSINESS RULES
# - KAM sees only own data
# - Manager sees only mapped KAM data
# - Admin sees all
# - No cross-data leakage
# - No approval logic changes
# - No leave logic changes
# - No reimbursement logic changes
# - No mail logic changes

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal
from functools import wraps
from typing import Iterable, List, Dict, Optional, Tuple

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group
from django.contrib.auth.views import redirect_to_login
from django.core.mail import EmailMessage
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.db import transaction, models
from django.db.models import Sum, Q, F
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

from apps.users.permissions import _user_permission_codes

# FIX-1: CollectionPlanActualForm added
from .forms import (
    VisitPlanForm,
    VisitActualForm,
    CallForm,
    CollectionForm,
    TargetLineInlineForm,
    TargetSettingForm,
    CollectionPlanForm,
    CollectionPlanActualForm,
    VisitBatchForm,
    MultiVisitPlanLineForm,
    ManagerTargetForm,
)
from .models import (
    Customer,
    InvoiceFact,
    LeadFact,
    OverdueSnapshot,
    TargetHeader,
    TargetLine,
    TargetSetting,
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

from . import sheets

User = get_user_model()

# ---------------------------------------------------------------------
# Status constants (must align with models.py choices)
# ---------------------------------------------------------------------
STATUS_DRAFT = VisitBatch.DRAFT
STATUS_PENDING_APPROVAL = VisitBatch.PENDING_APPROVAL
STATUS_PENDING_LEGACY = VisitBatch.PENDING
STATUS_APPROVED = VisitBatch.APPROVED
STATUS_REJECTED = VisitBatch.REJECTED

SINGLE_PREFIX = "single"
BATCH_PREFIX = "batch"

_BATCH_SIGNER = TimestampSigner(salt="kam.visitbatch.approval.v1")
BATCH_TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7  # 7 days


# ---------------------------------------------------------------------
# Group/role helpers
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
    return bool(_in_group(user, ("Manager", "Admin", "Finance")))


def _is_kam(user) -> bool:
    return bool(getattr(user, "is_authenticated", False) and not _is_manager(user))


def require_kam_code(code: str):
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


def _get_ip(request: HttpRequest) -> Optional[str]:
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


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
        return list(User.objects.filter(is_active=True).values_list("id", flat=True))
    return list(
        KamManagerMapping.objects.filter(manager=manager_user, active=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )


def _safe_user_codes(u: User) -> set[str]:
    try:
        return set(_user_permission_codes(u) or set())
    except Exception:
        return set()


def _is_manager_candidate(u: User, codes: Optional[set[str]] = None) -> bool:
    if not u or not getattr(u, "is_active", False):
        return False
    if getattr(u, "is_superuser", False):
        return True
    codes = codes if codes is not None else _safe_user_codes(u)
    if _in_group(u, ("Manager", "Admin", "Finance")):
        return True
    if "kam_manager" in codes:
        return True
    return False


def _is_kam_candidate(u: User, codes: Optional[set[str]] = None) -> bool:
    if not u or not getattr(u, "is_active", False):
        return False
    if getattr(u, "is_superuser", False):
        return False
    codes = codes if codes is not None else _safe_user_codes(u)
    has_kam_any = any((c or "").startswith("kam_") for c in codes) or ("access_kam_module" in codes)
    return has_kam_any and (not _is_manager_candidate(u, codes=codes))


def _customer_qs_for_user(user: User):
    qs = Customer.objects.all()
    if _is_admin(user):
        return qs
    if _is_manager(user):
        kam_ids = _kams_managed_by_manager(user)
        return qs.filter(Q(kam_id__in=kam_ids) | Q(primary_kam_id__in=kam_ids))
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


def _first_query_value(request: HttpRequest, *names: str) -> str:
    for name in names:
        if not name:
            continue
        val = (request.GET.get(name) or "").strip()
        if val:
            return val
    return ""


def _scoped_kam_ids(actor: User, scope_kam_id: Optional[int]) -> Optional[List[int]]:
    if scope_kam_id is not None:
        return [scope_kam_id]
    if _is_admin(actor):
        return None
    if _is_manager(actor):
        return _kams_managed_by_manager(actor)
    return [actor.id]


def _filter_qs_by_kam_scope(qs, actor: User, scope_kam_id: Optional[int], field_name: str):
    kam_ids = _scoped_kam_ids(actor, scope_kam_id)
    if kam_ids is None:
        return qs
    if not kam_ids:
        return qs.none()
    return qs.filter(**{f"{field_name}__in": kam_ids})


def _parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return timezone.datetime.fromisoformat(s).date()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%m-%d-%Y",
                "%d/%m/%y", "%d-%m-%y", "%Y/%m/%d"):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(s, fmt).date()
        except Exception:
            pass
    sep = "/" if "/" in s else ("-" if "-" in s else None)
    if not sep:
        return None
    parts = s.split(sep)
    if len(parts) != 3:
        return None
    p0, p1, p2 = [p.strip() for p in parts]
    if not (p0.isdigit() and p1.isdigit() and p2.isdigit()):
        return None
    try:
        a, b, y = int(p0), int(p1), int(p2)
        if y < 100:
            y = 2000 + y
        if a > 12 and 1 <= b <= 12:
            d, m = a, b
        elif b > 12 and 1 <= a <= 12:
            m, d = a, b
        else:
            m, d = a, b
        return date(y, m, d)
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


def _iso_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=7)
    iso_year, iso_week, _ = start.isocalendar()
    return start, end, f"{iso_year}-W{iso_week:02d}"


def _ms_week_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local - timezone.timedelta(days=local.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timezone.timedelta(days=7)
    iso_year, iso_week, _ = start.isocalendar()
    return start, end, f"{iso_year}-W{iso_week:02d}"


def _last_completed_ms_week_end(dt: timezone.datetime) -> timezone.datetime:
    start, _end, _ = _ms_week_bounds(dt)
    return start


def _month_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end, f"{start.year}-{start.month:02d}"


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
    return start, end, f"{start.year}-Q{q}"


def _year_bounds(dt: timezone.datetime) -> Tuple[timezone.datetime, timezone.datetime, str]:
    local = timezone.localtime(dt)
    start = local.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1)
    return start, end, f"{start.year}"


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
        return timezone.make_aware(timezone.datetime(y, m, d, 0, 0, 0))
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
    now = timezone.now()
    range_shortcut = (request.GET.get("range") or "").strip().lower()
    if range_shortcut:
        today_local = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today_local + timezone.timedelta(days=1)
        if range_shortcut in ("last7", "7d", "7days"):
            start = today_local - timezone.timedelta(days=7)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"
        if range_shortcut in ("last30", "30d", "30days"):
            start = today_local - timezone.timedelta(days=30)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"
        if range_shortcut in ("last60", "60d"):
            start = today_local - timezone.timedelta(days=60)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"
        if range_shortcut in ("last90", "90d", "90days", "3m"):
            start = today_local - timezone.timedelta(days=90)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"
        if range_shortcut in ("thismonth", "this_month", "month"):
            ws, we, _ = _month_bounds(now)
            return ws, we, f"{ws.date()} → {(we - timezone.timedelta(days=1)).date()}"
        if range_shortcut in ("thisquarter", "this_quarter", "quarter"):
            ws, we, _ = _quarter_bounds(now)
            return ws, we, f"{ws.date()} → {(we - timezone.timedelta(days=1)).date()}"
        if range_shortcut in ("thisyear", "this_year", "year"):
            ws, we, _ = _year_bounds(now)
            return ws, we, f"{ws.date()} → {(we - timezone.timedelta(days=1)).date()}"
        if range_shortcut in ("all", "*"):
            s = timezone.make_aware(timezone.datetime(2000, 1, 1))
            e = timezone.make_aware(timezone.datetime(2100, 1, 1))
            return s, e, "ALL"

    from_s = _first_query_value(request, "from", "from_date", "start_date", "date_from", "fromDate", "startDate", "dateFrom")
    to_s = _first_query_value(request, "to", "to_date", "end_date", "date_to", "toDate", "endDate", "dateTo")
    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)

    if from_d and to_d and from_d <= to_d:
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        end = timezone.make_aware(timezone.datetime(to_d.year, to_d.month, to_d.day, 0, 0, 0)) + timezone.timedelta(days=1)
        return start, end, f"{from_d} → {to_d}"
    if from_d and not to_d:
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        return start, start + timezone.timedelta(days=1), f"{from_d} → {from_d}"

    today_local = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
    m, y = today_local.month, today_local.year
    fy_start_year = y if m >= 4 else y - 1
    fy_start = timezone.make_aware(timezone.datetime(fy_start_year, 4, 1, 0, 0, 0))
    fy_end = today_local + timezone.timedelta(days=1)
    return fy_start, fy_end, f"{fy_start.date()} → {today_local.date()} (Fiscal YTD)"


def _resolve_scope(request: HttpRequest, actor: User) -> Tuple[Optional[int], str]:
    if not _is_manager(actor):
        return actor.id, actor.username

    raw_scope = _first_query_value(request, "user", "kam", "KAM", "username", "user_name", "kam_username")
    raw_scope_id = _first_query_value(request, "kam_id", "user_id", "id")

    u = None
    if raw_scope_id and raw_scope_id.isdigit():
        u = User.objects.filter(id=int(raw_scope_id), is_active=True).first()
    elif raw_scope:
        if raw_scope.upper() in {"ALL", "*"}:
            return None, "ALL"
        u = User.objects.filter(Q(username__iexact=raw_scope) | Q(email__iexact=raw_scope), is_active=True).first()
        if not u and " " in raw_scope.strip():
            parts = [p for p in raw_scope.strip().split() if p]
            if len(parts) >= 2:
                u = User.objects.filter(first_name__iexact=parts[0], last_name__iexact=" ".join(parts[1:]), is_active=True).first()

    if not u:
        return None, "ALL"
    if _is_admin(actor):
        return u.id, u.username
    allowed = set(_kams_managed_by_manager(actor))
    if u.id in allowed:
        return u.id, u.username
    return None, "ALL"


def _target_setting_for_kam_window(kam_id: int, start_date, end_date_inclusive) -> Optional[TargetSetting]:
    if not kam_id:
        return None
    return (
        TargetSetting.objects.filter(kam_id=kam_id, from_date__lte=start_date, to_date__gte=end_date_inclusive)
        .order_by("-created_at", "from_date")
        .first()
    )


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


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    if m == 12:
        next_month_first = timezone.datetime(y + 1, 1, 1).date()
    else:
        next_month_first = timezone.datetime(y, m + 1, 1).date()
    last_day = next_month_first - timezone.timedelta(days=1)
    return timezone.datetime(y, m, min(d.day, last_day.day)).date()


def _make_batch_token(batch_id: int, action: str) -> str:
    return _BATCH_SIGNER.sign(f"{batch_id}:{(action or '').strip().upper()}")


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


def _calc_visits_target(start_dt: timezone.datetime, end_dt: timezone.datetime) -> int:
    delta_days = max(0, (end_dt - start_dt).days)
    if delta_days == 0:
        return 6
    return math.ceil(delta_days / 7) * 6


def _kam_options_for_user(user: User) -> List[str]:
    if not _is_manager(user):
        return []
    if _is_admin(user):
        return list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
    allowed_ids = _kams_managed_by_manager(user)
    return list(
        User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True)
    )


def _split_full_name(full_name: str) -> Tuple[str, str]:
    full_name = (full_name or "").strip()
    if not full_name:
        return "", ""
    parts = [p for p in full_name.split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

# =====================================================================
# ADMIN: KAM → Manager Mapping
# =====================================================================
@login_required
@require_any_kam_code("kam_manager", "kam_dashboard", "kam_plan")
def admin_kam_manager_mapping(request: HttpRequest) -> HttpResponse:
    if not _is_admin(request.user):
        return HttpResponseForbidden("403 Forbidden: Admin access required.")

    manager_group = Group.objects.filter(name="Manager").first()

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip().lower()

        if action == "manager_create":
            username = (request.POST.get("username") or "").strip()
            full_name = (request.POST.get("full_name") or "").strip()
            email = (request.POST.get("email") or "").strip()
            password = (request.POST.get("password") or "").strip()
            if not username:
                messages.error(request, "Manager username is required.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            if not full_name:
                messages.error(request, "Manager full name is required.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            if not email:
                messages.error(request, "Manager email is required.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            if User.objects.filter(username__iexact=username).exists():
                messages.error(request, "Username already exists.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            if User.objects.filter(email__iexact=email).exists():
                messages.error(request, "Email already exists.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            first_name, last_name = _split_full_name(full_name)
            with transaction.atomic():
                u = User(username=username, email=email, is_active=True)
                if hasattr(u, "first_name"):
                    u.first_name = first_name
                if hasattr(u, "last_name"):
                    u.last_name = last_name
                if password:
                    u.set_password(password)
                else:
                    u.set_password(User.objects.make_random_password())
                u.save()
                if manager_group:
                    u.groups.add(manager_group)
            messages.success(request, f"Manager created: {u.username}.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

        if action == "manager_update":
            manager_id = (request.POST.get("manager_id") or "").strip()
            full_name = (request.POST.get("full_name") or "").strip()
            email = (request.POST.get("email") or "").strip()
            password = (request.POST.get("password") or "").strip()
            is_active_flag = (request.POST.get("is_active") or "").strip().lower()
            if not manager_id.isdigit():
                messages.error(request, "Invalid manager id.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            u = User.objects.filter(id=int(manager_id)).first()
            if not u:
                messages.error(request, "Manager not found.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            if full_name:
                first_name, last_name = _split_full_name(full_name)
                if hasattr(u, "first_name"):
                    u.first_name = first_name
                if hasattr(u, "last_name"):
                    u.last_name = last_name
            if email:
                if User.objects.filter(email__iexact=email).exclude(id=u.id).exists():
                    messages.error(request, "Email already used by another user.")
                    return redirect(reverse("kam:admin_kam_manager_mapping"))
                u.email = email
            if is_active_flag in {"0", "false", "no", "off"}:
                u.is_active = False
            elif is_active_flag in {"1", "true", "yes", "on"}:
                u.is_active = True
            if password:
                u.set_password(password)
            with transaction.atomic():
                u.save()
                if manager_group and not u.groups.filter(id=manager_group.id).exists():
                    u.groups.add(manager_group)
                if not u.is_active:
                    KamManagerMapping.objects.filter(manager=u, active=True).update(active=False, updated_at=timezone.now())
            messages.success(request, f"Manager updated: {u.username}.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

        if action == "manager_delete":
            manager_id = (request.POST.get("manager_id") or "").strip()
            if not manager_id.isdigit():
                messages.error(request, "Invalid manager id.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            u = User.objects.filter(id=int(manager_id)).first()
            if not u:
                messages.error(request, "Manager not found.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            with transaction.atomic():
                u.is_active = False
                u.save(update_fields=["is_active"])
                KamManagerMapping.objects.filter(manager=u, active=True).update(active=False, updated_at=timezone.now())
            messages.success(request, f"Manager deactivated: {u.username}.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

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
                KamManagerMapping.objects.create(kam=kam_user, manager=mgr_user, assigned_by=request.user, active=True)
            messages.success(request, f"Assigned manager for {kam_user.username} → {mgr_user.username}.")
            return redirect(reverse("kam:admin_kam_manager_mapping"))

        if action in {"edit_mapping", "mapping_edit"}:
            mapping_id = (request.POST.get("mapping_id") or "").strip()
            manager_id = (request.POST.get("manager_id") or "").strip()
            if not (mapping_id.isdigit() and manager_id.isdigit()):
                messages.error(request, "Invalid mapping/manager selection.")
                return redirect(reverse("kam:admin_kam_manager_mapping"))
            with transaction.atomic():
                old = KamManagerMapping.objects.select_for_update().select_related("kam").filter(id=int(mapping_id)).first()
                if not old:
                    messages.error(request, "Mapping not found.")
                    return redirect(reverse("kam:admin_kam_manager_mapping"))
                kam_user = User.objects.filter(id=old.kam_id, is_active=True).first()
                mgr_user = User.objects.filter(id=int(manager_id), is_active=True).first()
                if not kam_user or not mgr_user:
                    messages.error(request, "Invalid KAM/Manager user.")
                    return redirect(reverse("kam:admin_kam_manager_mapping"))
                if kam_user.id == mgr_user.id:
                    messages.error(request, "KAM and Manager cannot be the same user.")
                    return redirect(reverse("kam:admin_kam_manager_mapping"))
                KamManagerMapping.objects.filter(kam=kam_user, active=True).update(active=False, updated_at=timezone.now())
                KamManagerMapping.objects.create(kam=kam_user, manager=mgr_user, assigned_by=request.user, active=True)
            messages.success(request, f"Updated mapping for {kam_user.username} → {mgr_user.username}.")
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

    all_users = list(User.objects.filter(is_active=True).order_by("username"))
    codes_map = {u.id: _safe_user_codes(u) for u in all_users}
    manager_users = [u for u in all_users if _is_manager_candidate(u, codes=codes_map.get(u.id, set()))]
    kam_users = [u for u in all_users if _is_kam_candidate(u, codes=codes_map.get(u.id, set()))]
    if not kam_users:
        kam_users = [u for u in all_users if (u not in manager_users) and (not getattr(u, "is_superuser", False))]

    manager_group_users_qs = User.objects.all().order_by("username")
    if manager_group:
        manager_group_users_qs = manager_group_users_qs.filter(groups__name=manager_group.name)

    ctx = {
        "page_title": "KAM → Manager Mapping",
        "rows": list(mappings[:500]),
        "kam_users": kam_users,
        "manager_users": manager_users,
        "active_only": active_only,
        "manager_group_users": list(manager_group_users_qs),
    }
    return render(request, "kam/admin_kam_manager_mapping.html", ctx)


# =====================================================================
# DASHBOARD — FIX-2: added lead count metrics + CollectionPlan-based
#              collection metrics + Chart.js data blobs
# =====================================================================
@login_required
@require_kam_code("kam_dashboard")
def dashboard(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    selected_user = _first_query_value(request, "user", "kam", "KAM", "username", "user_name", "kam_username")

    sales_target_mt = Decimal(0)
    calls_target = 0
    leads_target_mt = Decimal(0)
    collections_plan_amount = Decimal(0)

    if scope_kam_id:
        start_date_ts = start_dt.date()
        end_date_ts_inc = (end_dt - timezone.timedelta(days=1)).date()
        ts = _target_setting_for_kam_window(scope_kam_id, start_date_ts, end_date_ts_inc)
        if ts:
            sales_target_mt = _safe_decimal(ts.sales_target_mt)
            calls_target = int(ts.calls_target or 0)
            leads_target_mt = _safe_decimal(ts.leads_target_mt)
            collections_plan_amount = _safe_decimal(ts.collections_target_amount)

    visits_target = _calc_visits_target(start_dt, end_dt)
    start_date = start_dt.date()
    end_date = end_dt.date()

    inv_qs = InvoiceFact.objects.filter(invoice_date__gte=start_date, invoice_date__lt=end_date)
    visit_plan_qs = VisitPlan.objects.filter(visit_date__gte=start_date, visit_date__lt=end_date)
    visit_act_qs = VisitActual.objects.filter(plan__visit_date__gte=start_date, plan__visit_date__lt=end_date)
    call_qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
    lead_qs = LeadFact.objects.filter(doe__gte=start_date, doe__lt=end_date)
    coll_qs = CollectionTxn.objects.filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)

    inv_qs = _filter_qs_by_kam_scope(inv_qs, request.user, scope_kam_id, "kam_id")
    visit_plan_qs = _filter_qs_by_kam_scope(visit_plan_qs, request.user, scope_kam_id, "kam_id")
    visit_act_qs = _filter_qs_by_kam_scope(visit_act_qs, request.user, scope_kam_id, "plan__kam_id")
    call_qs = _filter_qs_by_kam_scope(call_qs, request.user, scope_kam_id, "kam_id")
    lead_qs = _filter_qs_by_kam_scope(lead_qs, request.user, scope_kam_id, "kam_id")
    coll_qs = _filter_qs_by_kam_scope(coll_qs, request.user, scope_kam_id, "kam_id")

    sales_mt = _safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt")).get("mt"))
    visits_planned = visit_plan_qs.count()
    visits_actual = visit_act_qs.count()
    visits_successful = visit_act_qs.filter(successful=True).count()
    calls_total = call_qs.count()
    calls_successful = call_qs.filter(outcome__isnull=False).exclude(outcome="").count()

    leads_agg = lead_qs.aggregate(
        total_mt=Sum("qty_mt"),
        won_mt=Sum("qty_mt", filter=Q(status="WON")),
    )
    leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
    leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))

    # FIX-2: count-based lead metrics
    leads_total_count = lead_qs.count()
    leads_converted_count = lead_qs.filter(status="WON").count()
    leads_converted_value = _safe_decimal(
        lead_qs.filter(status="WON").aggregate(v=Sum("qty_mt")).get("v")
    )

    collections_actual = _safe_decimal(coll_qs.aggregate(total_amt=Sum("amount")).get("total_amt"))

    # Customer scope for overdue + old collection plan logic
    if scope_kam_id is not None:
        customer_ids_for_scope = list(
            Customer.objects.filter(Q(kam_id=scope_kam_id) | Q(primary_kam_id=scope_kam_id)).values_list("id", flat=True)
        )
    else:
        customer_ids_for_scope = list(_customer_qs_for_user(request.user).values_list("id", flat=True))

    # FIX-2: CollectionPlan-based planned/actual using correct field names
    cp_qs = CollectionPlan.objects.filter(
        Q(from_date__isnull=False, from_date__lte=end_date, to_date__gte=start_date)
        | Q(period_type__isnull=False)
    )
    cp_qs = _filter_qs_by_kam_scope(cp_qs, request.user, scope_kam_id, "kam_id")
    cp_agg = cp_qs.aggregate(
        total_planned=Sum("planned_amount"),
        total_actual=Sum("actual_amount"),   # FIX-2: correct field
    )
    collection_planned = _safe_decimal(cp_agg.get("total_planned")) or collections_plan_amount
    collection_actual_plan = _safe_decimal(cp_agg.get("total_actual"))

    # Legacy customer-scoped planned (fallback)
    coll_plan_legacy_agg = (
        CollectionPlan.objects.filter(customer_id__in=customer_ids_for_scope)
        .filter(
            Q(from_date__isnull=False, from_date__lte=end_date, to_date__gte=start_date)
            | Q(period_type__isnull=False)
        )
        .aggregate(total_planned=Sum("planned_amount"))
    )
    collections_planned = _safe_decimal(coll_plan_legacy_agg.get("total_planned")) or collections_plan_amount

    overdue_snapshot_date = None
    prev_overdue_snapshot_date = None
    credit_limit_sum = Decimal(0)
    exposure_sum = Decimal(0)
    overdue_sum = Decimal(0)
    prev_overdue_sum = Decimal(0)

    if customer_ids_for_scope:
        credit_limit_sum = _safe_decimal(
            Customer.objects.filter(id__in=customer_ids_for_scope).aggregate(total_cl=Sum("credit_limit")).get("total_cl")
        )
        end_date_inclusive = (end_dt - timezone.timedelta(days=1)).date()
        start_date_inclusive = start_dt.date()

        overdue_snapshot_date = (
            OverdueSnapshot.objects.filter(customer_id__in=customer_ids_for_scope, snapshot_date__lte=end_date_inclusive)
            .order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()
        )
        if overdue_snapshot_date:
            agg = OverdueSnapshot.objects.filter(
                customer_id__in=customer_ids_for_scope, snapshot_date=overdue_snapshot_date
            ).aggregate(
                total_exposure=Sum("exposure"), total_overdue=Sum("overdue"),
                a0=Sum("ageing_0_30"), a1=Sum("ageing_31_60"), a2=Sum("ageing_61_90"), a3=Sum("ageing_90_plus"),
            )
            exposure_sum = _safe_decimal(agg.get("total_exposure"))
            overdue_sum = _safe_decimal(agg.get("total_overdue"))
            if not exposure_sum:
                ageing_sum = sum(_safe_decimal(agg.get(k)) for k in ("a0", "a1", "a2", "a3"))
                if ageing_sum:
                    exposure_sum = ageing_sum
            if not exposure_sum and overdue_sum:
                exposure_sum = overdue_sum

        prev_overdue_snapshot_date = (
            OverdueSnapshot.objects.filter(customer_id__in=customer_ids_for_scope, snapshot_date__lt=start_date_inclusive)
            .order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()
        )
        if prev_overdue_snapshot_date:
            agg2 = OverdueSnapshot.objects.filter(
                customer_id__in=customer_ids_for_scope, snapshot_date=prev_overdue_snapshot_date
            ).aggregate(total_overdue=Sum("overdue"))
            prev_overdue_sum = _safe_decimal(agg2.get("total_overdue"))

    def _pct(n: Decimal, d: Decimal) -> Optional[Decimal]:
        if d and d != 0:
            return (n / d) * Decimal("100")
        return None

    sales_ach_pct = _pct(sales_mt, sales_target_mt) if sales_target_mt else None
    visit_ach_pct = _pct(Decimal(visits_actual), Decimal(visits_target)) if visits_target else None
    call_ach_pct = _pct(Decimal(calls_total), Decimal(calls_target)) if calls_target else None
    calls_conversion_pct = _pct(Decimal(calls_successful), Decimal(calls_total)) if calls_total else None
    lead_conv_pct = _pct(leads_won_mt, leads_total_mt) if leads_total_mt else None
    lead_count_conv_pct = _pct(Decimal(leads_converted_count), Decimal(leads_total_count)) if leads_total_count else None
    coll_eff_pct = _pct(collections_actual, collections_planned) if collections_planned else None
    collection_ach_pct = _pct(collection_actual_plan, collection_planned) if collection_planned else None
    overdue_reduction_pct = _pct(prev_overdue_sum - overdue_sum, prev_overdue_sum) if prev_overdue_sum else None
    overdue_risk_ratio = (exposure_sum / credit_limit_sum) if credit_limit_sum else None
    visit_success_pct = _pct(Decimal(visits_successful), Decimal(visits_actual)) if visits_actual else None

    prod_by_grade = list(inv_qs.values("grade").annotate(mt=Sum("qty_mt")).order_by("-mt"))
    prod_by_size = list(inv_qs.values("size").annotate(mt=Sum("qty_mt")).order_by("-mt"))

    trend_rows: List[Dict] = []
    anchor_end = _last_completed_ms_week_end(timezone.now())
    for k in (3, 2, 1, 0):
        end_i = anchor_end - timezone.timedelta(days=7 * k)
        start_i = end_i - timezone.timedelta(days=7)
        _, __, pid_i = _ms_week_bounds(start_i)
        inv_i = _filter_qs_by_kam_scope(InvoiceFact.objects.filter(invoice_date__gte=start_i.date(), invoice_date__lt=end_i.date()), request.user, scope_kam_id, "kam_id")
        vis_i = _filter_qs_by_kam_scope(VisitActual.objects.filter(plan__visit_date__gte=start_i.date(), plan__visit_date__lt=end_i.date()), request.user, scope_kam_id, "plan__kam_id")
        calls_i = _filter_qs_by_kam_scope(CallLog.objects.filter(call_datetime__gte=start_i, call_datetime__lt=end_i), request.user, scope_kam_id, "kam_id")
        coll_i = _filter_qs_by_kam_scope(CollectionTxn.objects.filter(txn_datetime__gte=start_i, txn_datetime__lt=end_i), request.user, scope_kam_id, "kam_id")
        trend_rows.append({
            "week": pid_i,
            "sales_mt": _safe_decimal(inv_i.aggregate(mt=Sum("qty_mt")).get("mt")),
            "visits": vis_i.count(),
            "calls": calls_i.count(),
            "collections": _safe_decimal(coll_i.aggregate(a=Sum("amount")).get("a")),
        })

    ctx = {
        "page_title": "KAM Dashboard",
        "range_label": range_label,
        "can_choose_kam": _is_manager(request.user),
        "scope_label": scope_label,
        "kam_options": _kam_options_for_user(request.user),
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
        "selected_user": selected_user,
        "kpi": {
            "sales_mt": sales_mt, "sales_target_mt": sales_target_mt, "sales_ach_pct": sales_ach_pct,
            "visits_target": visits_target, "visits_planned": visits_planned, "visits_actual": visits_actual,
            "visit_ach_pct": visit_ach_pct, "visit_success_pct": visit_success_pct,
            "calls": calls_total, "calls_successful": calls_successful, "calls_target": calls_target,
            "call_ach_pct": call_ach_pct, "calls_conversion_pct": calls_conversion_pct,
            "leads_total_mt": leads_total_mt, "leads_won_mt": leads_won_mt,
            "leads_target_mt": leads_target_mt, "lead_conv_pct": lead_conv_pct,
            # FIX-2: count-based lead metrics
            "leads_total_count": leads_total_count,
            "leads_converted_count": leads_converted_count,
            "lead_count_conv_pct": lead_count_conv_pct,
            "leads_converted_value": leads_converted_value,
            # Legacy CollectionTxn-based
            "collections_actual": collections_actual,
            "collections_planned": collections_planned,
            "collections_eff_pct": coll_eff_pct,
            # FIX-2: CollectionPlan-based (new)
            "collection_planned": collection_planned,
            "collection_actual": collection_actual_plan,
            "collection_ach_pct": collection_ach_pct,
            "collection_overdue_amt": overdue_sum,
            # Overdues
            "overdue_sum": overdue_sum, "prev_overdue_sum": prev_overdue_sum,
            "overdue_reduction_pct": overdue_reduction_pct, "credit_limit_sum": credit_limit_sum,
            "exposure_sum": exposure_sum, "overdue_risk_ratio": overdue_risk_ratio,
            "overdue_snapshot_date": overdue_snapshot_date,
            "prev_overdue_snapshot_date": prev_overdue_snapshot_date,
        },
        "prod_by_grade": prod_by_grade,
        "prod_by_size": prod_by_size,
        "trend_rows": trend_rows,
        # FIX-2: Chart.js data blobs
        "lead_analysis_data": {
            "total": leads_total_count,
            "converted": leads_converted_count,
        },
        "collection_analysis_data": {
            "planned": float(collection_planned),
            "actual": float(collection_actual_plan),
            "overdue": float(overdue_sum),
        },
    }
    return render(request, "kam/kam_dashboard.html", ctx)


# =====================================================================
# TODAY'S DETAILS
# =====================================================================
@login_required
@require_kam_code("kam_manager")
def manager_dashboard(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    today_start = timezone.localtime(timezone.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today_start + timezone.timedelta(days=1)
    kam_ids = _kams_managed_by_manager(request.user)

    def _scope(qs, field):
        if _is_admin(request.user):
            return qs
        return qs.filter(**{f"{field}__in": kam_ids})

    visits_today_qs = _scope(VisitActual.objects.select_related("plan__customer", "plan__kam").filter(plan__visit_date__gte=today_start.date(), plan__visit_date__lt=tomorrow.date()), "plan__kam_id")
    calls_today_qs = _scope(CallLog.objects.select_related("customer", "kam").filter(call_datetime__gte=today_start, call_datetime__lt=tomorrow), "kam_id")
    leads_today_qs = _scope(LeadFact.objects.filter(doe=today_start.date()), "kam_id")
    collections_today_qs = _scope(CollectionTxn.objects.select_related("customer", "kam").filter(txn_datetime__gte=today_start, txn_datetime__lt=tomorrow), "kam_id")
    collections_today_total = _safe_decimal(collections_today_qs.aggregate(a=Sum("amount"))["a"])

    kam_rows = []
    kams = (User.objects.filter(is_active=True, id__in=kam_ids).order_by("username") if kam_ids else User.objects.none())
    if _is_admin(request.user):
        kams = User.objects.filter(is_active=True).order_by("username")
    for k in kams:
        v_count = VisitActual.objects.filter(plan__kam=k, plan__visit_date__gte=today_start.date(), plan__visit_date__lt=tomorrow.date()).count()
        c_count = CallLog.objects.filter(kam=k, call_datetime__gte=today_start, call_datetime__lt=tomorrow).count()
        l_count = LeadFact.objects.filter(kam=k, doe=today_start.date()).count()
        coll_amt = _safe_decimal(CollectionTxn.objects.filter(kam=k, txn_datetime__gte=today_start, txn_datetime__lt=tomorrow).aggregate(a=Sum("amount"))["a"])
        if v_count or c_count or l_count or coll_amt:
            kam_rows.append({"kam": k, "visits": v_count, "calls": c_count, "leads": l_count, "collections": coll_amt})

    ctx = {
        "page_title": "Today's Details",
        "today": today_start.date(),
        "today_visits_count": visits_today_qs.count(),
        "today_visits_success": visits_today_qs.filter(successful=True).count(),
        "today_calls_count": calls_today_qs.count(),
        "today_leads_count": leads_today_qs.count(),
        "today_collections_amount": collections_today_total,
        "today_collections_count": collections_today_qs.count(),
        "today_visits": list(visits_today_qs[:50]),
        "today_calls": list(calls_today_qs[:50]),
        "today_leads": list(leads_today_qs[:50]),
        "today_collections": list(collections_today_qs[:50]),
        "kam_rows": kam_rows,
    }
    return render(request, "kam/manager_dashboard.html", ctx)


# =====================================================================
# MANAGER KPIs
# =====================================================================
@login_required
@require_kam_code("kam_manager_kpis")
def manager_kpis(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    period_type, start_dt, end_dt, period_id = _get_period(request)
    if _is_admin(request.user):
        kams = User.objects.filter(is_active=True).order_by("username")
    else:
        kams = User.objects.filter(is_active=True, id__in=_kams_managed_by_manager(request.user)).order_by("username")

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
        collections_actual = _safe_decimal(CollectionTxn.objects.filter(kam=kam, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt).aggregate(a=Sum("amount")).get("a"))
        leads_agg = LeadFact.objects.filter(kam=kam, doe__gte=start_dt.date(), doe__lt=end_dt.date()).aggregate(total_mt=Sum("qty_mt"), won_mt=Sum("qty_mt", filter=Q(status="WON")))
        leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
        leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))
        lead_conv_pct = _pct(leads_won_mt, leads_total_mt) if leads_total_mt else None
        credit_limit_sum = _safe_decimal(Customer.objects.filter(Q(kam=kam) | Q(primary_kam=kam)).aggregate(s=Sum("credit_limit")).get("s"))
        exposure_sum = overdue_sum = Decimal(0)
        if latest_snap_date:
            cust_ids = list(Customer.objects.filter(Q(kam=kam) | Q(primary_kam=kam)).values_list("id", flat=True))
            if cust_ids:
                agg = OverdueSnapshot.objects.filter(customer_id__in=cust_ids, snapshot_date=latest_snap_date).aggregate(
                    exposure=Sum("exposure"), overdue=Sum("overdue"),
                    a0=Sum("ageing_0_30"), a31=Sum("ageing_31_60"), a61=Sum("ageing_61_90"), a90=Sum("ageing_90_plus"),
                )
                exposure_sum = _safe_decimal(agg.get("exposure"))
                overdue_sum = _safe_decimal(agg.get("overdue"))
                if not exposure_sum:
                    ageing_sum = sum(_safe_decimal(agg.get(k)) for k in ("a0", "a31", "a61", "a90"))
                    if ageing_sum:
                        exposure_sum = ageing_sum
                if not exposure_sum and overdue_sum:
                    exposure_sum = overdue_sum
        rows.append({
            "kam": kam, "sales_mt": sales_mt, "visits_actual": visits_actual,
            "visit_success_pct": visit_success_pct, "calls": calls,
            "collections_actual": collections_actual, "lead_conv_pct": lead_conv_pct,
            "risk_ratio": (exposure_sum / credit_limit_sum) if credit_limit_sum else None,
        })

    ctx = {"page_title": "Manager KPIs", "period_type": period_type, "period_id": period_id, "rows": rows, "risky": []}
    return render(request, "kam/manager_kpis.html", ctx)

# =====================================================================
# PLAN VISIT
# =====================================================================
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
    if "remarks" in single_form.fields:
        single_form.fields["remarks"].required = False
    if "purpose" in batch_form.fields:
        batch_form.fields["purpose"].required = False

    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "single":
        single_form = VisitPlanForm(request.POST, prefix=SINGLE_PREFIX)
        if "customer" in single_form.fields:
            single_form.fields["customer"].queryset = customer_qs
        if "remarks" in single_form.fields:
            single_form.fields["remarks"].required = False
        if single_form.is_valid():
            plan: VisitPlan = single_form.save(commit=False)
            plan.kam = user
            if plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if plan.customer_id and not customer_qs.filter(id=plan.customer_id).exists():
                    messages.error(request, "Invalid customer selection (out of your scope).")
                    return redirect(reverse("kam:plan"))
            else:
                plan.customer = None
            if not (plan.location or "").strip():
                if plan.visit_category == VisitPlan.CAT_CUSTOMER and plan.customer and plan.customer.address:
                    plan.location = plan.customer.address
            plan.approval_status = STATUS_DRAFT
            plan.save()
            messages.success(request, "Single visit saved as Draft.")
            return redirect(reverse("kam:plan"))
        messages.error(request, "Single visit has errors. Please correct and save again.")

    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "batch":
        batch_form = VisitBatchForm(request.POST, prefix=BATCH_PREFIX)
        if "customers" in batch_form.fields:
            batch_form.fields["customers"].queryset = customer_qs
        if "purpose" in batch_form.fields:
            batch_form.fields["purpose"].required = False

        action = (request.POST.get("action") or request.POST.get("submit_action") or "").strip().lower()
        proceed_flag = action in {"proceed", "proceed_to_manager", "proceed-manager", "manager", "proceed_to_manager_btn"}

        if not batch_form.is_valid():
            messages.error(request, "Batch submission has errors. Please correct and re-submit.")
        else:
            visit_category = batch_form.cleaned_data.get("visit_category")
            from_date = batch_form.cleaned_data.get("from_date")
            to_date = batch_form.cleaned_data.get("to_date")
            remarks = (batch_form.cleaned_data.get("purpose") or "").strip()

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

            non_customer_lines: List[MultiVisitPlanLineForm] = []
            if visit_category in (VisitPlan.CAT_SUPPLIER, VisitPlan.CAT_WAREHOUSE, VisitPlan.CAT_VENDOR):
                names = request.POST.getlist("counterparty_name[]")
                locs = request.POST.getlist("counterparty_location[]")
                purs = request.POST.getlist("counterparty_purpose[]")
                max_n = max(len(names), len(locs), len(purs))
                for i in range(max_n):
                    f = MultiVisitPlanLineForm({
                        "counterparty_name": (names[i] if i < len(names) else "").strip(),
                        "counterparty_location": (locs[i] if i < len(locs) else "").strip(),
                        "counterparty_purpose": (purs[i] if i < len(purs) else "").strip(),
                    })
                    if f.is_valid() and (f.cleaned_data.get("counterparty_name") or "").strip():
                        non_customer_lines.append(f)

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
                    lp = (request.POST.get(f"purpose_{cust.id}") or "").strip()
                    loc = (cust.address or "").strip()
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
                    line_rows.append({"customer": cust, "visit_date": from_date, "visit_date_to": to_date, "purpose": lp or None, "location": loc, "expected_sales_mt": expected_sales, "expected_collection": expected_coll})
                if parse_errors:
                    return redirect(reverse("kam:plan"))

                with transaction.atomic():
                    existing_qs = VisitBatch.objects.select_for_update().filter(kam=user, visit_category=VisitPlan.CAT_CUSTOMER, from_date=from_date, to_date=to_date, approval_status__in=[STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY]).order_by("-created_at")
                    for b in existing_qs[:10]:
                        existing_ids = list(VisitPlan.objects.filter(batch=b, customer_id__isnull=False).values_list("customer_id", flat=True))
                        if sorted(existing_ids) == sorted(selected_ids):
                            messages.error(request, f"Duplicate submission blocked: Batch #{b.id} is already pending approval.")
                            return redirect(reverse("kam:plan"))

                    batch: VisitBatch = batch_form.save(commit=False)
                    batch.kam = user
                    batch.visit_category = VisitPlan.CAT_CUSTOMER
                    batch.from_date = from_date
                    batch.to_date = to_date
                    batch.purpose = remarks or ""
                    batch.approval_status = STATUS_PENDING_APPROVAL
                    batch.save()
                    for r in line_rows:
                        VisitPlan.objects.create(batch=batch, customer=r["customer"], kam=user, visit_date=r["visit_date"], visit_date_to=r["visit_date_to"], visit_type=VisitPlan.PLANNED, visit_category=VisitPlan.CAT_CUSTOMER, purpose=r["purpose"], expected_sales_mt=r["expected_sales_mt"], expected_collection=r["expected_collection"], location=r["location"], approval_status=STATUS_PENDING_APPROVAL)

                    approve_token = _make_batch_token(batch.id, "APPROVE")
                    reject_token = _make_batch_token(batch.id, "REJECT")
                    approve_url = request.build_absolute_uri(reverse("kam:visit_batch_approve_link", args=[approve_token]))
                    reject_url = request.build_absolute_uri(reverse("kam:visit_batch_reject_link", args=[reject_token]))
                    html_body = ""
                    try:
                        html_body = render_to_string("kam/emails/visit_batch_approval.html", {"batch": batch, "kam_user": user, "visit_category_label": "Customer Visit", "date_range": f"{batch.from_date} → {batch.to_date}", "remarks": remarks, "customers": [r["customer"] for r in line_rows], "approve_url": approve_url, "reject_url": reject_url})
                    except Exception:
                        html_body = ""
                    subject = f"[KAM] Approval Required: Batch #{batch.id} ({batch.from_date}..{batch.to_date}) - {user.username}"
                    if not html_body:
                        lines_txt = [f"{i}. {r['customer'].name}" for i, r in enumerate(line_rows, start=1)]
                        html_body = f"Batch ID: {batch.id}\nKAM: {user.get_full_name() or user.username}\nCategory: Customer Visit\nDate Range: {batch.from_date} to {batch.to_date}\nRemarks:\n{remarks}\n\nCustomers:\n" + "\n".join(lines_txt) + f"\n\nApprove: {approve_url}\nReject:  {reject_url}\n"
                    _send_safe_mail(subject, html_body, [mgr_user], [])

                messages.success(request, f"Submitted for manager approval: {len(line_rows)} customers (Batch #{batch.id}).")
                return redirect(reverse("kam:plan"))

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
                    for cust in Customer.objects.filter(id__in=selected_ids).order_by("name"):
                        loc = (cust.address or "").strip()
                        lp = (request.POST.get(f"purpose_{cust.id}") or "").strip()
                        es_raw = request.POST.get(f"expected_sales_mt_{cust.id}") or ""
                        ec_raw = request.POST.get(f"expected_collection") or request.POST.get(f"expected_collection_{cust.id}") or ""
                        expected_sales = _parse_decimal_or_none(es_raw)
                        expected_coll = _parse_decimal_or_none(ec_raw)
                        if es_raw.strip() != "" and expected_sales is None:
                            messages.error(request, f"Expected Sales (MT) is invalid for customer: {cust.name}")
                            transaction.set_rollback(True)
                            return redirect(reverse("kam:plan"))
                        if ec_raw.strip() != "" and expected_coll is None:
                            messages.error(request, f"Expected Collection (₹) is invalid for customer: {cust.name}")
                            transaction.set_rollback(True)
                            return redirect(reverse("kam:plan"))
                        VisitPlan.objects.create(batch=batch, customer=cust, kam=user, visit_date=from_date, visit_date_to=to_date, visit_type=VisitPlan.PLANNED, visit_category=VisitPlan.CAT_CUSTOMER, purpose=lp or (remarks or None), expected_sales_mt=expected_sales, expected_collection=expected_coll, location=loc, approval_status=STATUS_DRAFT)
                        created_lines += 1
                else:
                    if not non_customer_lines:
                        messages.error(request, "Add at least one line to save a non-customer batch draft.")
                        transaction.set_rollback(True)
                        return redirect(reverse("kam:plan"))
                    for f in non_customer_lines:
                        VisitPlan.objects.create(batch=batch, customer=None, counterparty_name=f.cleaned_data["counterparty_name"], kam=user, visit_date=from_date, visit_date_to=to_date, visit_type=VisitPlan.PLANNED, visit_category=visit_category, purpose=(f.cleaned_data.get("counterparty_purpose") or remarks or None), location=(f.cleaned_data.get("counterparty_location") or "").strip(), approval_status=STATUS_DRAFT)
                        created_lines += 1
                messages.success(request, f"Batch saved as Draft: {created_lines} lines (Batch #{batch.id}).")
                return redirect(reverse("kam:plan"))

    week_start, week_end, _ = _iso_week_bounds(timezone.now())
    my_plans = _visitplan_qs_for_user(user).filter(visit_date__gte=week_start.date(), visit_date__lt=week_end.date()).order_by("visit_date", "customer__name")
    ctx = {
        "page_title": "Plan Visit",
        "form": single_form, "single_form": single_form, "batch_form": batch_form,
        "plans": my_plans, "customers": list(customer_qs),
        "SINGLE_PREFIX": SINGLE_PREFIX, "BATCH_PREFIX": BATCH_PREFIX,
        "status_constants": {"DRAFT": STATUS_DRAFT, "PENDING_APPROVAL": STATUS_PENDING_APPROVAL, "APPROVED": STATUS_APPROVED, "REJECTED": STATUS_REJECTED},
    }
    return render(request, "kam/plan_visit.html", ctx)


# =====================================================================
# Customer APIs + CRUD
# =====================================================================
@login_required
@require_kam_code("kam_plan")
def customers_api(request: HttpRequest) -> JsonResponse:
    user = request.user
    qs = _customer_qs_for_user(user).order_by("name")
    if _is_manager(user):
        kam_u = (request.GET.get("kam") or "").strip()
        if kam_u:
            u = User.objects.filter(username=kam_u, is_active=True).first()
            if u and (_is_admin(user) or u.id in set(_kams_managed_by_manager(user))):
                qs = qs.filter(Q(kam=u) | Q(primary_kam=u))
            else:
                qs = qs.none()
    q = (request.GET.get("q") or "").strip()
    if q:
        qs = qs.filter(Q(name__icontains=q) | Q(code__icontains=q) | Q(mobile__icontains=q))
    source = (request.GET.get("source") or "").strip().upper()
    if source:
        qs = qs.filter(source=source)
    rows = [{"id": c.id, "name": c.name, "code": getattr(c, "code", "") or "", "mobile": getattr(c, "mobile", "") or "", "address": getattr(c, "address", "") or "", "source": (getattr(c, "source", "") or "").upper()} for c in qs[:500]]
    return JsonResponse({"ok": True, "count": len(rows), "customers": rows})


@login_required
@require_kam_code("kam_plan")
def customer_create_manual(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)
    actor = request.user
    target_kam = actor
    if _is_admin(actor) and (request.POST.get("primary_kam") or "").strip():
        u = User.objects.filter(username=(request.POST.get("primary_kam") or "").strip(), is_active=True).first()
        if not u:
            return JsonResponse({"ok": False, "error": "Invalid primary_kam"}, status=400)
        target_kam = u
    name = (request.POST.get("name") or "").strip()
    if not name:
        return JsonResponse({"ok": False, "error": "name required"}, status=400)
    if Customer.objects.filter(Q(kam=target_kam) | Q(primary_kam=target_kam)).filter(name__iexact=name).exists():
        return JsonResponse({"ok": False, "error": "Customer already exists in your scope"}, status=409)
    with transaction.atomic():
        c = Customer.objects.create(name=name, address=(request.POST.get("address") or "").strip() or None, mobile=(request.POST.get("mobile") or "").strip() or None, email=(request.POST.get("email") or "").strip() or None, gst_number=(request.POST.get("gst_number") or "").strip() or None, pincode=(request.POST.get("pincode") or "").strip() or None, kam=target_kam, primary_kam=target_kam, source=Customer.SOURCE_MANUAL, created_by=actor, synced_identifier=None)
    return JsonResponse({"ok": True, "customer": {"id": c.id, "name": c.name}})


@login_required
@require_kam_code("kam_plan")
def customer_update_manual(request: HttpRequest, customer_id: int) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)
    user = request.user
    c = get_object_or_404(_customer_qs_for_user(user), id=customer_id)
    if not _is_admin(user):
        if (getattr(c, "source", "") or "").upper() != Customer.SOURCE_MANUAL:
            return JsonResponse({"ok": False, "error": "Sheet customer is read-only"}, status=403)
    for field in ["name", "address", "mobile", "email", "gst_number", "pincode"]:
        if field in request.POST:
            setattr(c, field, (request.POST.get(field) or "").strip() or None)
    c.save()
    return JsonResponse({"ok": True})


@login_required
@require_kam_code("kam_plan")
def customer_delete_manual(request: HttpRequest, customer_id: int) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)
    user = request.user
    c = get_object_or_404(_customer_qs_for_user(user), id=customer_id)
    if not _is_admin(user):
        if (getattr(c, "source", "") or "").upper() != Customer.SOURCE_MANUAL:
            return JsonResponse({"ok": False, "error": "Sheet customer cannot be deleted"}, status=403)
    if VisitPlan.objects.filter(customer=c).exclude(approval_status__in=[STATUS_DRAFT, STATUS_REJECTED]).exists():
        return JsonResponse({"ok": False, "error": "Customer is used in submitted/approved plans"}, status=409)
    c.delete()
    return JsonResponse({"ok": True})


# =====================================================================
# Visit batches / Visit History
# =====================================================================
def _wants_json(request: HttpRequest) -> bool:
    fmt = (request.GET.get("format") or "").strip().lower()
    if fmt in {"json", "api"}:
        return True
    return "application/json" in (request.headers.get("Accept") or "").lower()


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches(request: HttpRequest) -> HttpResponse:
    return visit_batches_api(request) if _wants_json(request) else visit_batches_page(request)


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_page(request: HttpRequest) -> HttpResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")
    status = (request.GET.get("status") or "").strip().upper()
    if status:
        qs = qs.filter(approval_status=status)
    return render(request, "kam/visit_batches.html", {"page_title": "Visit History", "rows": list(qs[:300]), "can_view_all": _is_manager(user)})


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_api(request: HttpRequest) -> JsonResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")
    rows = [{"id": b.id, "kam": b.kam.username if b.kam_id else None, "from_date": str(b.from_date), "to_date": str(b.to_date), "visit_category": b.visit_category, "visit_category_label": b.get_visit_category_display(), "approval_status": b.approval_status, "remarks": b.purpose or "", "created_at": timezone.localtime(b.created_at).isoformat() if b.created_at else None} for b in qs[:300]]
    return JsonResponse({"ok": True, "count": len(rows), "batches": rows})


@login_required
@require_any_kam_code("kam_plan", "kam_manager")
def visit_history_edit(request: HttpRequest, plan_id: int) -> HttpResponse:
    user = request.user
    if _is_admin(user):
        plan = get_object_or_404(VisitPlan.objects.select_related("customer", "kam", "batch"), id=plan_id)
    elif _is_manager(user):
        plan = get_object_or_404(VisitPlan.objects.select_related("customer", "kam", "batch"), id=plan_id, kam_id__in=_kams_managed_by_manager(user))
    else:
        plan = get_object_or_404(VisitPlan.objects.select_related("customer", "kam", "batch"), id=plan_id, kam=user)
    if not _is_manager(user) and plan.approval_status not in {STATUS_APPROVED, STATUS_DRAFT}:
        messages.error(request, "Only approved or draft visits can be edited.")
        return redirect(reverse("kam:visit_batches"))

    existing_actual = getattr(plan, "actual", None)
    if request.method == "POST":
        new_visit_date = _parse_iso_date((request.POST.get("visit_date") or "").strip())
        new_visit_date_to = _parse_iso_date((request.POST.get("visit_date_to") or "").strip())
        new_purpose = (request.POST.get("purpose") or "").strip() or None
        new_location = (request.POST.get("location") or "").strip() or None
        if new_visit_date:
            plan.visit_date = new_visit_date
        if new_visit_date_to:
            plan.visit_date_to = new_visit_date_to
        if new_purpose is not None:
            plan.purpose = new_purpose
        if new_location is not None:
            plan.location = new_location
        plan.save(update_fields=["visit_date", "visit_date_to", "purpose", "location", "updated_at"])
        actual_form = VisitActualForm(request.POST, instance=existing_actual)
        if actual_form.is_valid():
            actual: VisitActual = actual_form.save(commit=False)
            actual.plan = plan
            actual.save()
            messages.success(request, f"Visit #{plan.id} updated successfully.")
            return redirect(reverse("kam:visit_batches"))
        messages.error(request, "Please correct the errors below.")
    else:
        actual_form = VisitActualForm(instance=existing_actual)

    return render(request, "kam/visit_history_edit.html", {"page_title": "Edit Visit", "plan": plan, "actual_form": actual_form, "existing_actual": existing_actual, "can_edit_plan_fields": True})


@login_required
@require_any_kam_code("kam_manager", "kam_plan")
def visit_batch_detail(request: HttpRequest, batch_id: int) -> HttpResponse:
    b = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    lines = list(VisitPlan.objects.select_related("customer").filter(batch=b).order_by("customer__name"))
    can_approve = _is_manager(request.user)
    return render(request, "kam/visit_batch_detail.html", {"page_title": f"Visit History — Batch #{b.id}", "batch": b, "lines": lines, "can_approve": can_approve, "can_edit": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT, STATUS_REJECTED}), "can_delete": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT})})


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
        VisitApprovalAudit.objects.create(batch=batch, actor=user, action=VisitApprovalAudit.ACTION_DELETE, note="Deleted draft batch", actor_ip=_get_ip(request))
        batch.delete()
    messages.success(request, f"Batch #{batch_id} deleted.")
    return redirect(reverse("kam:visit_batches"))


@login_required
@require_kam_code("kam_manager")
def visit_batch_approve(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    batch = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    with transaction.atomic():
        batch = VisitBatch.objects.select_for_update().get(id=batch_id)
        if not _is_admin(request.user) and batch.kam_id not in set(_kams_managed_by_manager(request.user)):
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
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_APPROVED, approved_by=request.user, approved_at=now_ts, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved batch", actor_ip=_get_ip(request))
    messages.success(request, f"Batch #{batch.id} approved.")
    return redirect(reverse("kam:visit_batches"))


@login_required
@require_kam_code("kam_manager")
def visit_batch_reject(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    batch = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    reason = (request.POST.get("reason") or "").strip() or "Rejected"
    with transaction.atomic():
        batch = VisitBatch.objects.select_for_update().get(id=batch_id)
        if not _is_admin(request.user) and batch.kam_id not in set(_kams_managed_by_manager(request.user)):
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
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_REJECTED, approved_by=request.user, approved_at=now_ts, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note=reason[:255], actor_ip=_get_ip(request))
    messages.info(request, f"Batch #{batch.id} rejected.")
    return redirect(reverse("kam:visit_batches"))


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
    with transaction.atomic():
        batch = get_object_or_404(VisitBatch.objects.select_for_update(), id=batch_id)
        if not _is_admin(request.user) and batch.kam_id not in set(_kams_managed_by_manager(request.user)):
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
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_APPROVED, approved_by=request.user, approved_at=now_ts, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved via email link", actor_ip=_get_ip(request))
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
    with transaction.atomic():
        batch = get_object_or_404(VisitBatch.objects.select_for_update(), id=batch_id)
        if not _is_admin(request.user) and batch.kam_id not in set(_kams_managed_by_manager(request.user)):
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
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_REJECTED, approved_by=request.user, approved_at=now_ts, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note="Rejected via email link"[:255], actor_ip=_get_ip(request))
    messages.info(request, f"Batch #{batch_id} rejected.")
    return redirect(reverse("kam:visit_batches"))

# =====================================================================
# VISITS & CALLS
# =====================================================================
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
        if _is_admin(user):
            selected_plan = get_object_or_404(VisitPlan, id=plan_id)
        elif _is_manager(user):
            selected_plan = get_object_or_404(VisitPlan, id=plan_id, kam_id__in=_kams_managed_by_manager(user))
        else:
            selected_plan = get_object_or_404(VisitPlan, id=plan_id, kam=user)
        form = VisitActualForm(instance=getattr(selected_plan, "actual", None))

    days_raw = (request.GET.get("days") or "").strip()
    from_date_raw = (request.GET.get("from_date") or "").strip()
    to_date_raw = (request.GET.get("to_date") or "").strip()
    from_date = _parse_iso_date(from_date_raw)
    to_date = _parse_iso_date(to_date_raw)
    today = timezone.localtime(timezone.now()).date()
    start = today - timezone.timedelta(days=29)
    end = today + timezone.timedelta(days=1)
    if days_raw.isdigit():
        start = today - timezone.timedelta(days=int(days_raw) - 1)
        end = today + timezone.timedelta(days=1)
    elif from_date and to_date and from_date <= to_date:
        start = from_date
        end = to_date + timezone.timedelta(days=1)
    elif from_date:
        start = from_date
        end = from_date + timezone.timedelta(days=1)

    if _is_admin(user):
        visit_qs = VisitPlan.objects.select_related("customer", "kam")
        call_qs = CallLog.objects.select_related("customer", "kam")
    elif _is_manager(user):
        kam_ids = _kams_managed_by_manager(user)
        visit_qs = VisitPlan.objects.select_related("customer", "kam").filter(kam_id__in=kam_ids)
        call_qs = CallLog.objects.select_related("customer", "kam").filter(kam_id__in=kam_ids)
    else:
        visit_qs = VisitPlan.objects.select_related("customer", "kam").filter(kam=user)
        call_qs = CallLog.objects.select_related("customer", "kam").filter(kam=user)

    ctx = {
        "page_title": "Visits & Calls",
        "form": form, "selected_plan": selected_plan,
        "recent_plans": visit_qs.filter(visit_date__gte=start, visit_date__lt=end).order_by("-visit_date"),
        "recent_calls": call_qs.filter(call_datetime__date__gte=start, call_datetime__date__lt=end).order_by("-call_datetime"),
        "filter_from": from_date.isoformat() if from_date else start.isoformat(),
        "filter_to": to_date.isoformat() if to_date else (end - timezone.timedelta(days=1)).isoformat(),
        "filter_days": days_raw, "is_manager": _is_manager(user),
    }
    return render(request, "kam/visit_actual.html", ctx)


# =====================================================================
# MANAGER VIEW
# =====================================================================
@login_required
@require_kam_code("kam_manager")
def manager_view(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    active_tab = (request.GET.get("tab") or "visits").strip().lower()
    if active_tab not in {"visits", "calls", "sales", "leads", "collections"}:
        active_tab = "visits"

    start_date = start_dt.date()
    end_date = end_dt.date()

    def _pct(n: Decimal, d: Decimal) -> Optional[Decimal]:
        if d and d != 0:
            return (n / d) * Decimal("100")
        return None

    visits_data, visits_summary = [], {}
    calls_data, calls_summary = [], {}
    sales_data, sales_summary = [], {}
    leads_data, leads_summary = [], {}
    collections_data, collections_summary = [], {}

    if active_tab == "visits":
        qs = _filter_qs_by_kam_scope(VisitPlan.objects.select_related("customer", "kam").filter(visit_date__gte=start_date, visit_date__lt=end_date), request.user, scope_kam_id, "kam_id")
        visits_data = list(qs.order_by("-visit_date")[:500])
        total_actuals = VisitActual.objects.filter(plan__in=qs).count()
        successful = VisitActual.objects.filter(plan__in=qs, successful=True).count()
        visits_summary = {"total_planned": qs.count(), "total_actual": total_actuals, "successful": successful, "success_pct": _pct(Decimal(successful), Decimal(total_actuals)) if total_actuals else None}

    if active_tab == "calls":
        qs = _filter_qs_by_kam_scope(CallLog.objects.select_related("customer", "kam").filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt), request.user, scope_kam_id, "kam_id")
        calls_data = list(qs.order_by("-call_datetime")[:500])
        total_calls = qs.count()
        successful_calls = qs.exclude(outcome="").exclude(outcome__isnull=True).count()
        calls_summary = {"total": total_calls, "successful": successful_calls, "conversion_pct": _pct(Decimal(successful_calls), Decimal(total_calls)) if total_calls else None}

    if active_tab == "sales":
        qs = _filter_qs_by_kam_scope(InvoiceFact.objects.filter(invoice_date__gte=start_date, invoice_date__lt=end_date), request.user, scope_kam_id, "kam_id")
        sales_data = list(qs.values(customer_name=F("customer__name"), kam_username=F("kam__username")).annotate(mt=Sum("qty_mt")).order_by("-mt")[:300])
        sales_summary = {"total_mt": _safe_decimal(qs.aggregate(mt=Sum("qty_mt")).get("mt")), "customer_count": len(sales_data)}

    if active_tab == "leads":
        qs = _filter_qs_by_kam_scope(LeadFact.objects.select_related("customer", "kam").filter(doe__gte=start_date, doe__lt=end_date), request.user, scope_kam_id, "kam_id")
        leads_data = list(qs.order_by("-doe")[:500])
        agg = qs.aggregate(total_mt=Sum("qty_mt"), won_mt=Sum("qty_mt", filter=Q(status="WON")))
        total_mt = _safe_decimal(agg.get("total_mt"))
        won_mt = _safe_decimal(agg.get("won_mt"))
        leads_summary = {"total_mt": total_mt, "won_mt": won_mt, "conversion_pct": _pct(won_mt, total_mt) if total_mt else None, "total_count": qs.count()}

    if active_tab == "collections":
        qs = _filter_qs_by_kam_scope(CollectionTxn.objects.select_related("customer", "kam").filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt), request.user, scope_kam_id, "kam_id")
        collections_data = list(qs.order_by("-txn_datetime")[:500])
        collections_summary = {"total_amount": _safe_decimal(qs.aggregate(a=Sum("amount")).get("a")), "transaction_count": qs.count()}

    ctx = {
        "page_title": "Manager View", "range_label": range_label, "scope_label": scope_label,
        "active_tab": active_tab,
        "tabs": [("visits", "Visits"), ("calls", "Calls"), ("sales", "Sales"), ("leads", "Leads"), ("collections", "Collections")],
        "kam_options": _kam_options_for_user(request.user),
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
        "selected_user": scope_label if scope_label != "ALL" else "",
        "visits_data": visits_data, "visits_summary": visits_summary,
        "calls_data": calls_data, "calls_summary": calls_summary,
        "sales_data": sales_data, "sales_summary": sales_summary,
        "leads_data": leads_data, "leads_summary": leads_summary,
        "collections_data": collections_data, "collections_summary": collections_summary,
    }
    return render(request, "kam/manager_view.html", ctx)


# =====================================================================
# Legacy approve/reject
# =====================================================================
@login_required
@require_kam_code("kam_visit_approve")
def visit_approve(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    plan = get_object_or_404(VisitPlan, id=plan_id)
    if not _is_admin(request.user) and plan.kam_id not in set(_kams_managed_by_manager(request.user)):
        return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")
    plan.approval_status = STATUS_APPROVED
    plan.approved_by = request.user
    plan.approved_at = timezone.now()
    plan.save(update_fields=["approval_status", "approved_by", "approved_at"])
    VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved", actor_ip=_get_ip(request))
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
    if not _is_admin(request.user) and plan.kam_id not in set(_kams_managed_by_manager(request.user)):
        return HttpResponseForbidden("403 Forbidden: Not in your approval scope.")
    plan.approval_status = STATUS_REJECTED
    plan.approved_by = request.user
    plan.approved_at = timezone.now()
    plan.save(update_fields=["approval_status", "approved_by", "approved_at"])
    VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note="Rejected", actor_ip=_get_ip(request))
    messages.info(request, "Visit rejected.")
    return redirect(reverse("kam:manager_kpis"))


# =====================================================================
# Quick entry: Call / Collection
# =====================================================================
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


# =====================================================================
# CUSTOMER 360
# =====================================================================
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
    sales_history = collections_history = visit_history = call_history = lead_history = overdue_history = followups = []
    risk_ratio = None

    if customer:
        latest_dt = OverdueSnapshot.objects.filter(customer=customer).order_by("-snapshot_date").values_list("snapshot_date", flat=True).first()
        if latest_dt:
            snap = OverdueSnapshot.objects.filter(customer=customer, snapshot_date=latest_dt).first()
            if snap:
                exposure = _safe_decimal(snap.exposure)
                overdue = _safe_decimal(snap.overdue)
                ageing = {"a0_30": _safe_decimal(snap.ageing_0_30), "a31_60": _safe_decimal(snap.ageing_31_60), "a61_90": _safe_decimal(snap.ageing_61_90), "a90_plus": _safe_decimal(snap.ageing_90_plus)}

        credit_limit = _safe_decimal(customer.credit_limit)
        if not exposure:
            age_sum = ageing["a0_30"] + ageing["a31_60"] + ageing["a61_90"] + ageing["a90_plus"]
            if age_sum:
                exposure = age_sum
            elif overdue:
                exposure = overdue
        if credit_limit:
            try:
                risk_ratio = exposure / credit_limit
            except Exception:
                risk_ratio = None

        sales = InvoiceFact.objects.filter(customer=customer, invoice_date__gte=start_date, invoice_date__lte=end_date).values("invoice_date__year", "invoice_date__month").annotate(mt=Sum("qty_mt")).order_by("invoice_date__year", "invoice_date__month")
        sales_history = [{"year": r["invoice_date__year"], "month": r["invoice_date__month"], "mt": _safe_decimal(r["mt"])} for r in sales]
        colls = CollectionTxn.objects.filter(customer=customer, txn_datetime__date__gte=start_date, txn_datetime__date__lte=end_date).values("txn_datetime__year", "txn_datetime__month").annotate(amount=Sum("amount")).order_by("txn_datetime__year", "txn_datetime__month")
        collections_history = [{"year": r["txn_datetime__year"], "month": r["txn_datetime__month"], "amount": _safe_decimal(r["amount"])} for r in colls]
        visit_history = list(VisitPlan.objects.select_related("actual", "kam").filter(customer=customer, visit_date__gte=start_date, visit_date__lte=end_date).order_by("-visit_date")[:20])
        call_history = list(CallLog.objects.select_related("kam").filter(customer=customer, call_datetime__date__gte=start_date, call_datetime__date__lte=end_date).order_by("-call_datetime")[:20])
        lead_history = list(LeadFact.objects.filter(customer=customer, doe__gte=start_date, doe__lte=end_date).order_by("-doe")[:20])
        overdue_history = list(OverdueSnapshot.objects.filter(customer=customer).order_by("-snapshot_date")[:12])
        today = timezone.localdate()
        followups = list(VisitActual.objects.filter(plan__customer=customer, next_action__isnull=False, next_action__gt="", next_action_date__isnull=False, next_action_date__gte=today).order_by("next_action_date")[:10])

    ctx = {
        "page_title": "Customer 360", "period_type": period_type, "period_id": period_id,
        "scope_label": scope_label, "kam_options": _kam_options_for_user(request.user),
        "customer_list": customer_list, "customer": customer,
        "exposure": exposure, "overdue": overdue, "credit_limit": credit_limit,
        "risk_ratio": risk_ratio, "ageing": ageing,
        "sales_history": sales_history, "collections_history": collections_history,
        "visit_history": visit_history, "call_history": call_history,
        "lead_history": lead_history, "overdue_history": overdue_history, "followups": followups,
        "sales_last12": sales_history, "collections_last12": collections_history,
        "recent_visits": visit_history, "recent_calls": call_history,
    }
    return render(request, "kam/customer_360.html", ctx)

# =====================================================================
# TARGETS
# =====================================================================
@login_required
@require_kam_code("kam_targets")
def targets(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    kam_options = _kam_options_for_user(request.user)
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
                to_date = _add_months(from_date, 3) - timezone.timedelta(days=1) if fixed_3m else from_date + timezone.timedelta(days=6)

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
            try:
                with transaction.atomic():
                    for kuser in kam_users:
                        collections_target_amount = coll_input if (coll_input is not None and not auto_coll) else Decimal("0.00")
                        overlap_qs = TargetSetting.objects.filter(kam=kuser, from_date__lte=to_date, to_date__gte=from_date)
                        post_id = (request.POST.get("id") or "").strip()
                        inst = None
                        if (not bulk_all) and post_id.isdigit():
                            inst = TargetSetting.objects.filter(id=int(post_id)).first()
                            if inst:
                                overlap_qs = overlap_qs.exclude(id=inst.id)
                        if overlap_qs.exists():
                            messages.error(request, f"Overlapping target window exists for KAM: {kuser.username} ({from_date} → {to_date}).")
                            raise transaction.TransactionManagementError("Overlap detected")
                        obj = (inst if (inst and len(kam_users) == 1) else TargetSetting(kam=kuser))
                        if inst and len(kam_users) == 1:
                            updated += 1
                        else:
                            created += 1
                        obj.kam = kuser
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
            except transaction.TransactionManagementError:
                return redirect(reverse("kam:targets"))

            if bulk_all or len(kam_users) > 1:
                messages.success(request, f"Targets saved in bulk. Created: {created}, Updated: {updated}.")
                return redirect(reverse("kam:targets"))
            return redirect(f"{reverse('kam:targets')}?from={from_date}&to={to_date}&user={kam_users[0].username}")
    else:
        initial = {}
        if edit_obj:
            initial = {"id": str(edit_obj.id), "from_date": edit_obj.from_date, "to_date": edit_obj.to_date, "kam_username": edit_obj.kam.username if edit_obj.kam_id else "", "sales_target_mt": edit_obj.sales_target_mt, "leads_target_mt": edit_obj.leads_target_mt, "calls_target": edit_obj.calls_target, "collections_target_amount": edit_obj.collections_target_amount}
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

    ctx = {
        "page_title": "Manager Target Setting", "kam_options": kam_options,
        "selected_user": uname, "filter_from": f, "filter_to": t,
        "rows": list(qs[:200]), "form": form, "edit_obj": edit_obj,
        "overdue_sum": overdue_sum, "suggested_collections": suggested_collections,
    }
    return render(request, "kam/targets.html", ctx)


@login_required
@require_kam_code("kam_targets_lines")
def targets_lines(request: HttpRequest) -> HttpResponse:
    return redirect(reverse("kam:targets"))


# =====================================================================
# REPORTS
# =====================================================================
@login_required
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    anchor_end = _last_completed_ms_week_end(timezone.now())
    weeks_trend = []
    for k in (3, 2, 1, 0):
        end_i = anchor_end - timezone.timedelta(days=7 * k)
        start_i = end_i - timezone.timedelta(days=7)
        _, __, pid_i = _ms_week_bounds(start_i)
        inv_i = _filter_qs_by_kam_scope(InvoiceFact.objects.filter(invoice_date__gte=start_i.date(), invoice_date__lt=end_i.date()), request.user, scope_kam_id, "kam_id")
        vis_i = _filter_qs_by_kam_scope(VisitActual.objects.filter(plan__visit_date__gte=start_i.date(), plan__visit_date__lt=end_i.date()), request.user, scope_kam_id, "plan__kam_id")
        calls_i = _filter_qs_by_kam_scope(CallLog.objects.filter(call_datetime__gte=start_i, call_datetime__lt=end_i), request.user, scope_kam_id, "kam_id")
        coll_i = _filter_qs_by_kam_scope(CollectionTxn.objects.filter(txn_datetime__gte=start_i, txn_datetime__lt=end_i), request.user, scope_kam_id, "kam_id")
        leads_i = _filter_qs_by_kam_scope(LeadFact.objects.filter(doe__gte=start_i.date(), doe__lt=end_i.date()), request.user, scope_kam_id, "kam_id")
        weeks_trend.append({"week": pid_i, "sales_mt": float(_safe_decimal(inv_i.aggregate(mt=Sum("qty_mt")).get("mt"))), "visits": vis_i.count(), "calls": calls_i.count(), "collections": float(_safe_decimal(coll_i.aggregate(a=Sum("amount")).get("a"))), "leads": leads_i.count()})

    metric = (request.GET.get("metric") or "sales").strip().lower()
    rows = []
    if metric == "sales":
        qs = InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(qs.values(customer_name=F("customer__name"), kam_username=F("kam__username")).annotate(mt=Sum("qty_mt")).order_by("-mt")[:300])
    elif metric == "calls":
        qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        rows = list(qs.values("id", "call_datetime", kam_username=F("kam__username"), customer_name=F("customer__name")).order_by("-call_datetime")[:500])
    elif metric == "visits":
        qs = VisitActual.objects.filter(plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date())
        if scope_kam_id is not None:
            qs = qs.filter(plan__kam_id=scope_kam_id)
        rows = list(qs.values("id", "successful", visit_date=F("plan__visit_date"), kam_username=F("plan__kam__username"), customer_name=F("plan__customer__name")).order_by("-visit_date")[:500])

    ctx = {
        "page_title": "KAM Reports", "metric": metric, "range_label": range_label,
        "scope_label": scope_label, "can_choose_kam": _is_manager(request.user),
        "kam_options": _kam_options_for_user(request.user), "rows": rows,
        "weeks_trend": weeks_trend,
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
    }
    return render(request, "kam/reports.html", ctx)


# =====================================================================
# CSV export
# =====================================================================
@login_required
@require_kam_code("kam_export_kpi_csv")
def export_kpi_csv(request: HttpRequest) -> StreamingHttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)
    if _is_manager(request.user):
        user_q = (request.GET.get("user") or "").strip()
        if user_q:
            u = User.objects.filter(username=user_q, is_active=True).first()
            if not u or (not _is_admin(request.user) and u.id not in set(_kams_managed_by_manager(request.user))):
                kam_user_ids = []
            else:
                kam_user_ids = [u.id]
        else:
            kam_user_ids = list(InvoiceFact.objects.filter(invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date()).values_list("kam_id", flat=True).distinct()) if _is_admin(request.user) else _kams_managed_by_manager(request.user)
    else:
        kam_user_ids = [request.user.id]

    rows = [["period_type", "period_id", "kam_id", "sales_mt", "calls", "visits_actual", "collections_amount"]]
    for kam_id in kam_user_ids:
        rows.append([period_type, period_id, kam_id,
            str(_safe_decimal(InvoiceFact.objects.filter(kam_id=kam_id, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date()).aggregate(mt=Sum("qty_mt"))["mt"])),
            str(CallLog.objects.filter(kam_id=kam_id, call_datetime__gte=start_dt, call_datetime__lt=end_dt).count()),
            str(VisitActual.objects.filter(plan__kam_id=kam_id, plan__visit_date__gte=start_dt.date(), plan__visit_date__lt=end_dt.date()).count()),
            str(_safe_decimal(CollectionTxn.objects.filter(kam_id=kam_id, txn_datetime__gte=start_dt, txn_datetime__lt=end_dt).aggregate(a=Sum("amount"))["a"])),
        ])

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


# =====================================================================
# Collections Plan — FIX-3/4/5: correct field names, proper ctx
# =====================================================================
@login_required
@require_kam_code("kam_collections_plan")
def collections_plan(request: HttpRequest) -> HttpResponse:
    period_type, start_dt, end_dt, period_id = _get_period(request)
    customer_qs = _customer_qs_for_user(request.user).order_by("name")
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    if request.method == "POST":
        form = CollectionPlanForm(request.POST)
        if "customer" in form.fields:
            form.fields["customer"].queryset = customer_qs

        raw_customer_id = (request.POST.get("customer") or "").strip()
        raw_planned = (request.POST.get("planned_amount") or "").strip()
        raw_ptype = (request.POST.get("period_type") or "").strip()
        raw_pid = (request.POST.get("period_id") or "").strip()
        raw_fd = (request.POST.get("from_date") or "").strip()
        raw_td = (request.POST.get("to_date") or "").strip()

        cust = None
        if raw_customer_id.isdigit():
            cust = customer_qs.filter(id=int(raw_customer_id)).first()

        if not cust:
            messages.error(request, "Please select a valid customer.")
            ctx = _build_collections_plan_ctx(request, customer_qs, period_type, period_id, start_dt, end_dt, form, scope_label)
            return render(request, "kam/collections_plan.html", ctx)

        try:
            planned = Decimal(raw_planned) if raw_planned else None
        except Exception:
            planned = None

        if planned is None or planned < 0:
            messages.error(request, "Please enter a valid planned amount (≥ 0).")
            ctx = _build_collections_plan_ctx(request, customer_qs, period_type, period_id, start_dt, end_dt, form, scope_label)
            return render(request, "kam/collections_plan.html", ctx)

        has_period = bool(raw_ptype and raw_pid)
        fd = _parse_iso_date(raw_fd)
        td = _parse_iso_date(raw_td)
        has_range = bool(fd and td and fd <= td)

        if not has_period and not has_range:
            messages.error(request, "Please provide either a Period Type + Period ID, or a From/To date range.")
            ctx = _build_collections_plan_ctx(request, customer_qs, period_type, period_id, start_dt, end_dt, form, scope_label)
            return render(request, "kam/collections_plan.html", ctx)

        if not customer_qs.filter(id=cust.id).exists():
            return HttpResponseForbidden("403 Forbidden: Customer out of your scope.")

        owner = (cust.kam or cust.primary_kam or request.user) if _is_manager(request.user) else request.user

        with transaction.atomic():
            defaults = {"planned_amount": planned, "kam": owner}
            if has_period:
                CollectionPlan.objects.update_or_create(
                    customer=cust, period_type=raw_ptype, period_id=raw_pid,
                    defaults={**defaults, "from_date": None, "to_date": None},
                )
            else:
                CollectionPlan.objects.update_or_create(
                    customer=cust, from_date=fd, to_date=td, period_type=None, period_id=None,
                    defaults=defaults,
                )

        messages.success(request, "Collection plan saved.")
        return redirect(
            f"{reverse('kam:collections_plan')}?period={request.GET.get('period', 'month')}&asof={request.GET.get('asof', '')}"
        )

    else:
        form = CollectionPlanForm(initial={"period_type": period_type, "period_id": period_id})
        if "customer" in form.fields:
            form.fields["customer"].queryset = customer_qs

    ctx = _build_collections_plan_ctx(request, customer_qs, period_type, period_id, start_dt, end_dt, form, scope_label)
    return render(request, "kam/collections_plan.html", ctx)


def _build_collections_plan_ctx(request, customer_qs, period_type, period_id, start_dt, end_dt, form, scope_label=None):
    """FIX-4: Build rich context for collections_plan.html template."""
    plans_qs = _build_collections_plan_qs(customer_qs, period_type, period_id, start_dt, end_dt)

    # Totals
    total_planned = Decimal(0)
    total_actual = Decimal(0)
    for p in plans_qs:
        total_planned += _safe_decimal(p.planned_amount)
        total_actual += _safe_decimal(p.actual_amount)   # FIX-5: correct field

    shortfall = max(total_planned - total_actual, Decimal(0))
    achievement_pct = float((total_actual / total_planned * 100)) if total_planned else 0.0

    # Chart data
    chart_labels = []
    chart_planned = []
    chart_actual = []
    for p in plans_qs.select_related("customer")[:30]:
        chart_labels.append(p.customer.name if p.customer else "—")
        chart_planned.append(float(_safe_decimal(p.planned_amount)))
        chart_actual.append(float(_safe_decimal(p.actual_amount)))  # FIX-5

    # KAM options (for managers)
    kam_options = _kam_options_for_user(request.user)
    can_choose_kam = _is_manager(request.user)
    selected_user = _first_query_value(request, "user", "kam", "KAM", "username")

    return {
        "page_title": "Collections Plan",
        "period_type": period_type,
        "period_id": period_id,
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
        "plans": plans_qs,
        "form": form,
        "totals": {
            "planned": total_planned,
            "actual": total_actual,
            "achievement_pct": achievement_pct,
            "shortfall": shortfall,
        },
        "can_choose_kam": can_choose_kam,
        "kam_options": kam_options,
        "selected_user": selected_user,
        "scope_label": scope_label or "ALL",
        "cp_chart_data": {
            "labels": chart_labels,
            "planned": chart_planned,
            "actual": chart_actual,
        },
    }


def _build_collections_plan_qs(customer_qs, period_type, period_id, start_dt, end_dt):
    """FIX-5: Return a queryset of CollectionPlan objects for the period."""
    plan_qs = CollectionPlan.objects.select_related("customer", "kam").filter(customer__in=customer_qs)
    period_rows = plan_qs.filter(period_type=period_type, period_id=period_id)
    range_rows = plan_qs.filter(
        from_date__isnull=False, to_date__isnull=False,
        from_date__lte=end_dt.date(), to_date__gte=start_dt.date(),
    )
    return (period_rows | range_rows).distinct().order_by("customer__name")


def _build_collections_rows(customer_qs, period_type, period_id, start_dt, end_dt) -> List[Dict]:
    """Legacy row-builder kept for backward compat — now delegates to qs builder."""
    plans_qs = _build_collections_plan_qs(customer_qs, period_type, period_id, start_dt, end_dt)
    plan_customer_ids = list(plans_qs.values_list("customer_id", flat=True))

    overdue_map: Dict[int, Decimal] = {}
    if plan_customer_ids:
        latest_by_customer = OverdueSnapshot.objects.filter(customer_id__in=plan_customer_ids).values("customer_id").annotate(latest=models.Max("snapshot_date"))
        latest_map = {r["customer_id"]: r["latest"] for r in latest_by_customer if r.get("latest")}
        if latest_map:
            snaps = OverdueSnapshot.objects.filter(customer_id__in=list(latest_map.keys()), snapshot_date__in=list(set(latest_map.values()))).values("customer_id", "snapshot_date", "overdue")
            for s in snaps:
                cid = s["customer_id"]
                if latest_map.get(cid) == s["snapshot_date"]:
                    overdue_map[cid] = _safe_decimal(s["overdue"])

    rows = []
    for p in plans_qs:
        rows.append({
            "plan_id": p.id,
            "plan": p,
            "customer": p.customer,
            "kam": p.kam,
            "overdue": overdue_map.get(p.customer_id, Decimal(0)),
            "planned": _safe_decimal(p.planned_amount),
            "actual": _safe_decimal(p.actual_amount),               # FIX-5: correct field
            "collection_date": p.collection_date,                    # FIX-5: correct field
            "collection_reference": p.collection_reference,         # FIX-5: correct field
            "collection_status": p.collection_status,               # FIX-5: correct field
            "from_date": getattr(p, "from_date", None),
            "to_date": getattr(p, "to_date", None),
            "period_type": getattr(p, "period_type", None),
            "period_id": getattr(p, "period_id", None),
        })
    return rows


# =====================================================================
# Collection Plan — record actual: FIX-6 uses CollectionPlanActualForm
# =====================================================================
@login_required
@require_kam_code("kam_collections_plan")
def collection_plan_record_actual(request: HttpRequest, plan_id: int) -> HttpResponse:
    """Record actual collection amount against a collection plan."""
    plan = get_object_or_404(CollectionPlan, id=plan_id)
    customer_qs = _customer_qs_for_user(request.user)
    if not customer_qs.filter(id=plan.customer_id).exists():
        return HttpResponseForbidden("403 Forbidden: Not your plan.")

    next_url = request.GET.get("next") or reverse("kam:collections_plan")

    if request.method == "POST":
        # FIX-6: Use CollectionPlanActualForm which knows the correct fields
        form = CollectionPlanActualForm(request.POST, instance=plan)
        if form.is_valid():
            form.save()  # model.save() auto-derives collection_status
            messages.success(request, "Actual collection recorded.")
            return redirect(next_url)
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = CollectionPlanActualForm(instance=plan)

    ctx = {
        "page_title": "Record Actual Collection",
        "plan": plan,
        "form": form,
        "next_url": next_url,
    }
    return render(request, "kam/collection_plan_record_actual.html", ctx)


# =====================================================================
# Collection Plan — delete
# =====================================================================
@login_required
@require_kam_code("kam_collections_plan")
def collection_plan_delete(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    plan = get_object_or_404(CollectionPlan, id=plan_id)
    if not _customer_qs_for_user(request.user).filter(id=plan.customer_id).exists():
        return HttpResponseForbidden("403 Forbidden: Not your plan.")
    # FIX-5: use correct field name actual_amount
    if plan.actual_amount is not None and plan.actual_amount > 0:
        messages.error(request, "Cannot delete a plan that already has an actual collection recorded.")
        return redirect(reverse("kam:collections_plan"))
    plan.delete()
    messages.success(request, "Collection plan deleted.")
    return redirect(
        f"{reverse('kam:collections_plan')}?period={request.GET.get('period', 'month')}&asof={request.GET.get('asof', '')}"
    )


# =====================================================================
# Sync endpoints
# =====================================================================
@login_required
@require_kam_code("kam_sync_now")
def sync_now(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    try:
        stats = sheets.run_sync_now() or {}
        summary = stats.get("summary")
        if not summary:
            seen = stats.get("records_seen")
            created = stats.get("created")
            updated = stats.get("updated")
            summary = f"Seen={seen} Created={created} Updated={updated}" if any(v is not None for v in (seen, created, updated)) else "Sync complete."
        messages.success(request, f"Sync complete. {summary}")
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
    messages.success(request, f"Sync triggered (token={intent.token}).")
    return redirect(reverse("kam:dashboard"))


@login_required
@require_kam_code("kam_sync_step")
def sync_step(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    token = (request.GET.get("token") or request.POST.get("token") or "").strip()

    if request.method == "POST" and not token:
        try:
            stats = sheets.run_sync_now() or {}
            summary = stats.get("summary")
            if not summary:
                seen = stats.get("records_seen")
                created = stats.get("created")
                updated = stats.get("updated")
                summary = f"Seen={seen} Created={created} Updated={updated}" if any(v is not None for v in (seen, created, updated)) else "Sync complete."
            messages.success(request, f"Sync complete. {summary}")
        except Exception as e:
            messages.error(request, f"Sync failed: {e}")
        return redirect(reverse("kam:dashboard"))

    if not token:
        return JsonResponse({"ok": False, "error": "token missing"}, status=400)

    intent = get_object_or_404(SyncIntent, token=token)
    try:
        intent.status = SyncIntent.STATUS_RUNNING
        intent.step_count = int(intent.step_count or 0) + 1
        intent.last_error = ""
        intent.save(update_fields=["status", "step_count", "last_error", "updated_at"])
        try:
            result = sheets.step_sync(intent)
        except TypeError:
            result = sheets.step_sync(intent=intent)
        if not isinstance(result, dict):
            raise RuntimeError("sheets.step_sync must return a dict")
        done = bool(result.get("done"))
        intent.status = SyncIntent.STATUS_SUCCESS if done else SyncIntent.STATUS_PENDING
        intent.save(update_fields=["status", "updated_at"])
        return JsonResponse({"ok": True, "result": result})
    except Exception as e:
        intent.status = SyncIntent.STATUS_ERROR
        intent.last_error = str(e)
        intent.save(update_fields=["status", "last_error", "updated_at"])
        return JsonResponse({"ok": False, "error": str(e)}, status=500)