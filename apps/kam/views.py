# FILE: apps/kam/views.py
from __future__ import annotations

import csv
import io
import logging
import math
from datetime import date
from decimal import Decimal
from functools import wraps
from typing import Iterable, List, Dict, Optional, Tuple

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.auth.views import redirect_to_login
from django.core.mail import EmailMessage, EmailMultiAlternatives
from django.utils.html import strip_tags
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from django.db import transaction, models, connection
from django.db.models import Sum, Q, F
from django.db.models.functions import TruncDate
from django.db.utils import OperationalError, ProgrammingError
from django.http import (
    Http404,
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
from django.views.decorators.csrf import csrf_exempt
from apps.kam.analytics.services import build_kam_performance_report

# FIX 5 — explicit login_url on all login_required decorators
from django.contrib.auth.decorators import login_required as _django_login_required


def login_required(func=None, login_url="/accounts/login/", redirect_field_name="next"):
    """
    FIX 5: Wrapper that always passes login_url='/accounts/login/' so that
    unauthenticated users are redirected to the correct login page with ?next=
    preserved, regardless of project-level LOGIN_URL setting.
    """
    if func is not None:
        return _django_login_required(func, login_url=login_url, redirect_field_name=redirect_field_name)
    def decorator(view_func):
        return _django_login_required(view_func, login_url=login_url, redirect_field_name=redirect_field_name)
    return decorator


from apps.users.permissions import _user_permission_codes

from .forms import (
    VisitPlanForm,
    SingleVisitForm,
    VisitActualForm,
    CallForm,
    CollectionForm,
    TargetLineInlineForm,
    TargetSettingForm,
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
    KAMAssignment,
)

from . import sheets
from django.db.models import Sum, Q
from decimal import Decimal

User = get_user_model()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Status constants (must align with models.py choices)
# ─────────────────────────────────────────────────────────────────────────────
STATUS_DRAFT = VisitBatch.DRAFT
STATUS_PENDING_APPROVAL = VisitBatch.PENDING_APPROVAL
STATUS_PENDING_LEGACY = VisitBatch.PENDING
STATUS_APPROVED = VisitBatch.APPROVED
STATUS_REJECTED = VisitBatch.REJECTED
STATUS_COMPLETED = VisitPlan.COMPLETED

SINGLE_PREFIX = "single"
BATCH_PREFIX = "batch"

# ─────────────────────────────────────────────────────────────────────────────
# Token signers
# ─────────────────────────────────────────────────────────────────────────────
_BATCH_SIGNER = TimestampSigner(salt="kam.visitbatch.approval.v1")
_SINGLE_SIGNER = TimestampSigner(salt="kam.visitplan.single.approval.v1")

BATCH_TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7
SINGLE_TOKEN_MAX_AGE_SECONDS = 60 * 60 * 24 * 7

_APPROVAL_REQUIRED_CATEGORIES = {
    VisitBatch.CAT_CUSTOMER,
    VisitBatch.CAT_VENDOR,
    VisitBatch.CAT_SUPPLIER,
    VisitBatch.CAT_WAREHOUSE,
}

_VISIT_CATEGORY_LABELS: Dict[str, str] = dict(VisitBatch.VISIT_CATEGORY_CHOICES)


# ─────────────────────────────────────────────────────────────────────────────
# Group / role helpers
# ─────────────────────────────────────────────────────────────────────────────
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
    if not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    if _in_group(user, ("Manager", "Admin", "Finance")):
        return True
    try:
        from apps.users.permissions import _user_permission_codes
        codes = _user_permission_codes(user)
        return "kam_manager" in codes
    except Exception:
        return False

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

def _email_display_name(user) -> str:
    """
    Safe display name for email templates.
    Never raises. Always returns a readable value.
    """
    if not user:
        return "-"

    try:
        get_full_name = getattr(user, "get_full_name", None)
        if callable(get_full_name):
            full_name = (get_full_name() or "").strip()
            if full_name:
                return full_name
    except Exception:
        pass

    username = (getattr(user, "username", "") or "").strip()
    if username:
        return username

    email = (getattr(user, "email", "") or "").strip()
    if email:
        return email

    return "-"


def _email_address(user) -> str:
    """
    Safe email address for email templates.
    """
    if not user:
        return "-"
    email = (getattr(user, "email", "") or "").strip()
    return email or "-"


def _safe_email_value(value) -> str:
    """
    Convert values for email display without crashing.
    """
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"


def _display_date_range(start_value, end_value=None) -> str:
    """
    Display one date or a date range.
    """
    start_text = _safe_email_value(start_value)
    end_text = _safe_email_value(end_value)

    if end_text != "-" and end_text != start_text:
        return f"{start_text} to {end_text}"

    return start_text


def _dedupe_email_strings(emails: List[str]) -> List[str]:
    """
    Deduplicate emails case-insensitively while preserving order.
    """
    seen = set()
    output = []

    for email in emails or []:
        clean = (email or "").strip()
        if not clean:
            continue

        key = clean.lower()
        if key in seen:
            continue

        seen.add(key)
        output.append(clean)

    return output


def _emails_from_users(users: Optional[List[User]]) -> List[str]:
    """
    Convert User objects to a clean email list.
    """
    emails = []

    for user in users or []:
        email = (getattr(user, "email", "") or "").strip()
        if email:
            emails.append(email)

    return _dedupe_email_strings(emails)


def _html_to_plain_text(html: str) -> str:
    """
    Plain-text fallback for EmailMultiAlternatives.
    Gmail will show HTML, but plain text is still needed for safe email delivery.
    """
    text = strip_tags(html or "")
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines) or "BOS Lakshya."

def _send_safe_mail(
    subject: str,
    body: str,
    to_users: List[User],
    cc_users: Optional[List[User]] = None,
) -> bool:
    """
    Production-safe email sender for KAM workflows.

    Required behavior:
    - Uses Django EmailMessage.
    - Does NOT use send_mail.
    - Supports TO and CC.
    - Removes duplicate emails.
    - Prevents the same email from appearing in both TO and CC.
    - Uses fail_silently=False.
    - Logs recipient resolution, success, and exceptions.
    """

    def _email_from_user(user) -> str:
        return (getattr(user, "email", "") or "").strip()

    def _uniq_email_list(emails: List[str]) -> List[str]:
        seen = set()
        output = []

        for email in emails or []:
            clean = (email or "").strip()

            if not clean:
                continue

            key = clean.lower()

            if key in seen:
                continue

            seen.add(key)
            output.append(clean)

        return output

    try:
        to_emails = _uniq_email_list([
            _email_from_user(user)
            for user in (to_users or [])
            if _email_from_user(user)
        ])

        cc_emails = _uniq_email_list([
            _email_from_user(user)
            for user in (cc_users or [])
            if _email_from_user(user)
        ])

        # Do not keep same email in both TO and CC.
        to_email_keys = {email.lower() for email in to_emails}

        cc_emails = [
            email
            for email in cc_emails
            if email.lower() not in to_email_keys
        ]

        logger.info(
            "KAM Mail Recipient Resolution -> subject=%r to_user_ids=%s to_emails=%s cc_user_ids=%s cc_emails=%s",
            subject,
            [getattr(user, "id", None) for user in (to_users or [])],
            to_emails,
            [getattr(user, "id", None) for user in (cc_users or [])],
            cc_emails,
        )

        if not to_emails:
            logger.warning(
                "KAM email skipped because no valid TO recipient was found. subject=%r to_user_ids=%s",
                subject,
                [getattr(user, "id", None) for user in (to_users or [])],
            )
            return False

        from_email = (
            getattr(settings, "DEFAULT_FROM_EMAIL", None)
            or getattr(settings, "EMAIL_HOST_USER", None)
        )

        if not from_email:
            logger.warning(
                "KAM email sender is missing. DEFAULT_FROM_EMAIL and EMAIL_HOST_USER are both empty. subject=%r",
                subject,
            )

        email = EmailMessage(
            subject=subject,
            body=body or "BOS Lakshya ERP notification.",
            from_email=from_email,
            to=to_emails,
            cc=cc_emails,
        )

        # If body is HTML, send it as HTML.
        if "<html" in (body or "").lower():
            email.content_subtype = "html"

        sent_count = email.send(fail_silently=False)

        logger.info(
            "KAM email sent successfully. subject=%r to=%s cc=%s sent_count=%s",
            subject,
            to_emails,
            cc_emails,
            sent_count,
        )

        return bool(sent_count)

    except Exception:
        logger.exception(
            "KAM email send failed. subject=%r to_user_ids=%s cc_user_ids=%s",
            subject,
            [getattr(user, "id", None) for user in (to_users or [])],
            [getattr(user, "id", None) for user in (cc_users or [])],
        )
        return False

def _visitplan_workflow_schema_ready() -> bool:
    table_name = VisitPlan._meta.db_table
    required_columns = {
        "submitted_at", "approved_at", "approved_by_id",
        "rejected_at", "rejected_by_id", "rejection_reason",
    }
    try:
        with connection.cursor() as cursor:
            description = connection.introspection.get_table_description(cursor, table_name)
        existing = {col.name for col in description}
        return required_columns.issubset(existing)
    except Exception:
        logger.exception("Failed to inspect VisitPlan table schema.")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# FIX: Sales (F) is authoritative for qty_mt.
# ─────────────────────────────────────────────────────────────────────────────
def _sales_converted_qs(qs):
    """
    Canonical Sales dashboard queryset.

    Current Sales (F) tab is already the converted-sales source.
    Older code expected source_status='Order Converted', but the live tab now has:
      Date of Invoice, Buyer's Name, KAM, Qty(MT), Full Name

    Therefore, do not require source_status for Sales (F).
    """
    return qs.filter(source_tab="Sales (F)")


def _legacy_invoice_qs(qs):
    """
    Backward-compatible invoice queryset for Customer 360 / invoice history.

    Use this only where historical invoice view is needed.
    Do not use this for Sales KPI.
    """
    sheet1_qs = qs.filter(source_tab="Sheet1")
    if sheet1_qs.exists():
        return sheet1_qs

    salesf_qs = qs.filter(source_tab="Sales (F)")
    if salesf_qs.exists():
        return salesf_qs

    return qs.exclude(source_tab__isnull=True)

def _preferred_inv_qs(qs):
    """
    Preferred invoice queryset.

    Priority:
      1. Sales (F) for KAM sales dashboard / sales trend / Customer 360 MT
      2. Sheet1 fallback for old invoice history
      3. Any available invoice rows as final fallback

    Important:
      Current Sales (F) tab has columns:
        Date of Invoice, Buyer's Name, KAM, Qty(MT), Full Name

      It does NOT have Status = Order Converted anymore.
      So Sales (F) itself is treated as the converted sales source.
    """
    sales_f_qs = qs.filter(source_tab="Sales (F)")
    if sales_f_qs.exists():
        return sales_f_qs

    sheet1_qs = qs.filter(source_tab="Sheet1")
    if sheet1_qs.exists():
        return sheet1_qs

    return qs


def _lead_won_q():
    return (
        Q(status__iexact="WON")
        | Q(status__iexact="CONVERTED")
        | Q(status__iexact="ORDER CONVERTED")
    )

# ─────────────────────────────────────────────────────────────────────────────
# Token Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _make_single_token(plan_id: int, action: str) -> str:
    return _SINGLE_SIGNER.sign(f"{plan_id}:{(action or '').strip().upper()}")


def _parse_single_token(token: str) -> Tuple[int, str]:
    value = _SINGLE_SIGNER.unsign(token, max_age=SINGLE_TOKEN_MAX_AGE_SECONDS)
    parts = (value or "").split(":", 1)
    if len(parts) != 2:
        raise BadSignature("invalid token payload")
    plan_id = int(parts[0])
    action = (parts[1] or "").strip().upper()
    if action not in {"APPROVE", "REJECT"}:
        raise BadSignature("invalid token action")
    return plan_id, action


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


# ─────────────────────────────────────────────────────────────────────────────
# Manager / KAM lookup helpers
# ─────────────────────────────────────────────────────────────────────────────
def _active_manager_for_kam(kam_user: User) -> Optional[User]:
    if not kam_user or not getattr(kam_user, "id", None):
        return None

    try:
        from apps.leave.models import ApproverMapping
        mapping = (
            ApproverMapping.objects
            .select_related("reporting_person")
            .filter(employee=kam_user)
            .first()
        )
        if mapping and mapping.reporting_person_id:
            rp = mapping.reporting_person
            if rp and getattr(rp, "is_active", False):
                return rp
    except Exception:
        logger.exception("_active_manager_for_kam: ApproverMapping lookup failed for user_id=%s", kam_user.id)

    try:
        m = (
            KamManagerMapping.objects.select_related("manager")
            .filter(kam=kam_user, active=True)
            .order_by("-assigned_at", "-created_at")
            .first()
        )
        if m and m.manager and getattr(m.manager, "is_active", False):
            return m.manager
    except Exception:
        logger.exception("_active_manager_for_kam: KamManagerMapping lookup failed for user_id=%s", kam_user.id)

    try:
        profile = getattr(kam_user, "profile", None)
        if profile and getattr(profile, "reporting_officer_id", None):
            officer = profile.reporting_officer
            if officer and getattr(officer, "is_active", False):
                return officer
    except Exception:
        pass

    return None

def _active_cc_for_kam(kam_user: User) -> List[User]:
    """
    Resolve CC recipients for KAM visit approval email.

    Live DB source:
      ApproverMapping.employee = KAM / employee submitting visit
      ApproverMapping.cc_person = single CC user
      ApproverMapping.default_cc_users = multiple CC users

    Production rules:
      - Read both cc_person and default_cc_users.
      - Skip inactive users.
      - Skip users with blank email.
      - Remove duplicate emails.
      - Never break visit submission if CC lookup fails.
    """
    if not kam_user or not getattr(kam_user, "id", None):
        logger.warning("KAM CC Debug → invalid kam_user=%r", kam_user)
        return []

    try:
        from apps.leave.models import ApproverMapping

        mapping = (
            ApproverMapping.objects
            .select_related("employee", "reporting_person", "cc_person")
            .prefetch_related("default_cc_users")
            .filter(employee=kam_user)
            .first()
        )

        if not mapping:
            logger.warning(
                "KAM CC Debug → no ApproverMapping found for employee_id=%s email=%s",
                kam_user.id,
                getattr(kam_user, "email", None),
            )
            return []

        cc_candidates: List[User] = []

        if getattr(mapping, "cc_person_id", None):
            cc_candidates.append(mapping.cc_person)

        try:
            cc_candidates.extend(list(mapping.default_cc_users.all()))
        except Exception:
            logger.exception(
                "KAM CC Debug → failed reading default_cc_users for employee_id=%s mapping_id=%s",
                kam_user.id,
                mapping.id,
            )

        final_cc_users: List[User] = []
        seen_emails = set()

        for cc_user in cc_candidates:
            if not cc_user:
                continue

            cc_email = (getattr(cc_user, "email", "") or "").strip().lower()

            if not cc_email:
                logger.warning(
                    "KAM CC Debug → skipping CC user with blank email. employee_id=%s cc_user_id=%s",
                    kam_user.id,
                    getattr(cc_user, "id", None),
                )
                continue

            if not getattr(cc_user, "is_active", False):
                logger.warning(
                    "KAM CC Debug → skipping inactive CC user. employee_id=%s cc_user_id=%s cc_email=%s",
                    kam_user.id,
                    getattr(cc_user, "id", None),
                    cc_email,
                )
                continue

            if cc_email in seen_emails:
                continue

            seen_emails.add(cc_email)
            final_cc_users.append(cc_user)

        logger.info(
            "KAM CC Debug → final CC resolved. employee_id=%s employee_email=%s mapping_id=%s cc_emails=%s",
            kam_user.id,
            getattr(kam_user, "email", None),
            mapping.id,
            [(u.email or "").strip() for u in final_cc_users],
        )

        return final_cc_users

    except Exception:
        logger.exception(
            "_active_cc_for_kam failed for employee_id=%s email=%s",
            getattr(kam_user, "id", None),
            getattr(kam_user, "email", None),
        )
        return []

def _kams_managed_by_manager(manager_user: User) -> List[int]:
    """
    Return all active KAM / employee user IDs visible to a manager.

    Sources checked:
      1. KamManagerMapping.manager -> kam
      2. ApproverMapping.reporting_person -> employee
      3. User.profile.reporting_officer -> employee

    This function is intentionally centralized because Plan Visit,
    approval screens, visit history, and customer dropdowns must all
    resolve manager team members in the same way.
    """
    if not manager_user or not getattr(manager_user, "id", None):
        return []

    if _is_admin(manager_user):
        return list(
            User.objects
            .filter(is_active=True)
            .values_list("id", flat=True)
        )

    kam_ids = set()

    # 1. Explicit KAM manager mapping
    try:
        kam_ids.update(
            KamManagerMapping.objects
            .filter(manager=manager_user, active=True)
            .values_list("kam_id", flat=True)
        )
    except Exception:
        logger.exception(
            "_kams_managed_by_manager: KamManagerMapping lookup failed for manager_id=%s",
            getattr(manager_user, "id", None),
        )

    # 2. Leave / approval reporting hierarchy
    try:
        from apps.leave.models import ApproverMapping

        kam_ids.update(
            ApproverMapping.objects
            .filter(reporting_person=manager_user)
            .values_list("employee_id", flat=True)
        )
    except Exception:
        logger.exception(
            "_kams_managed_by_manager: ApproverMapping lookup failed for manager_id=%s",
            getattr(manager_user, "id", None),
        )

    # 3. Employee profile reporting officer hierarchy
    try:
        kam_ids.update(
            User.objects
            .filter(profile__reporting_officer=manager_user, is_active=True)
            .values_list("id", flat=True)
        )
    except Exception:
        logger.exception(
            "_kams_managed_by_manager: profile reporting_officer lookup failed for manager_id=%s",
            getattr(manager_user, "id", None),
        )

    # Remove None/blank values and return stable sorted list.
    return sorted({int(k) for k in kam_ids if k})

def _can_manager_approve_visit(manager_user: User, plan: "VisitPlan") -> bool:
    if _is_admin(manager_user):
        return True
    if plan.kam_id in set(_kams_managed_by_manager(manager_user)):
        return True
    try:
        from apps.leave.models import ApproverMapping
        if ApproverMapping.objects.filter(
            employee_id=plan.kam_id,
            reporting_person=manager_user
        ).exists():
            return True
    except Exception:
        pass
    try:
        profile = getattr(plan.kam, "profile", None)
        if profile and getattr(profile, "reporting_officer_id", None) == manager_user.id:
            return True
    except Exception:
        pass
    return False


def _safe_user_codes(u: User) -> set:
    try:
        return set(_user_permission_codes(u) or set())
    except Exception:
        return set()


def _is_manager_candidate(u: User, codes: Optional[set] = None) -> bool:
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


def _is_kam_candidate(u: User, codes: Optional[set] = None) -> bool:
    if not u or not getattr(u, "is_active", False):
        return False
    if getattr(u, "is_superuser", False):
        return False
    codes = codes if codes is not None else _safe_user_codes(u)
    has_kam_any = any((c or "").startswith("kam_") for c in codes) or ("access_kam_module" in codes)
    return has_kam_any and (not _is_manager_candidate(u, codes=codes))


def _customer_qs_for_user(user: User):
    """
    Canonical customer scope for KAM dashboard, Plan Visit, and Customer360.

    Visibility rules:
      Admin   -> all valid customers
      Manager -> customers belonging to reporting/team KAMs + own KAM customers
      KAM     -> own customers only

    Important production fix:
      Some users are classified as Manager by permissions/groups but also have
      their own KAM customers and facts. Example: pratik@blueoceansteels.com.

      Old behavior:
        If _is_manager(user) was True and manager had no reporting KAMs,
        the function returned Customer.objects.none(), hiding user's own customers.

      New behavior:
        Manager scope always includes the user's own id as well as team ids.
        This preserves manager visibility and fixes hybrid Manager/KAM users.

    PostgreSQL sources included:
      1. Customer.kam
      2. Customer.primary_kam
      3. KAMAssignment
      4. InvoiceFact.kam
      5. LeadFact.kam
      6. CollectionTxn.kam
      7. OverdueSnapshot.kam
      8. CollectionPlan.kam

    This function must not read Google Sheets directly.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return Customer.objects.none()

    try:
        from .models import KAMAssignment as _KAMAssignment
    except Exception:
        logger.exception(
            "_customer_qs_for_user: KAMAssignment model import failed. "
            "Customer assignment mapping will be skipped."
        )
        _KAMAssignment = None

    base_qs = (
        Customer.objects
        .select_related("kam", "primary_kam")
        .filter(name__isnull=False)
        .exclude(name__exact="")
    )

    if _is_admin(user):
        return base_qs.distinct().order_by("name", "code")

    def _customers_for_kam_ids(kam_ids):
        kam_ids = sorted({int(k) for k in (kam_ids or []) if k})

        if not kam_ids:
            logger.warning(
                "_customers_for_kam_ids called with no KAM IDs. user_id=%s username=%s",
                getattr(user, "id", None),
                getattr(user, "username", None),
            )
            return Customer.objects.none()

        invoice_customer_ids = (
            InvoiceFact.objects
            .filter(kam_id__in=kam_ids, customer_id__isnull=False)
            .values_list("customer_id", flat=True)
        )

        lead_customer_ids = (
            LeadFact.objects
            .filter(kam_id__in=kam_ids, customer_id__isnull=False)
            .values_list("customer_id", flat=True)
        )

        collection_customer_ids = (
            CollectionTxn.objects
            .filter(kam_id__in=kam_ids, customer_id__isnull=False)
            .values_list("customer_id", flat=True)
        )

        overdue_customer_ids = (
            OverdueSnapshot.objects
            .filter(kam_id__in=kam_ids, customer_id__isnull=False)
            .values_list("customer_id", flat=True)
        )

        collection_plan_customer_ids = (
            CollectionPlan.objects
            .filter(kam_id__in=kam_ids, customer_id__isnull=False)
            .values_list("customer_id", flat=True)
        )

        q_obj = (
            Q(kam_id__in=kam_ids)
            | Q(primary_kam_id__in=kam_ids)
            | Q(id__in=invoice_customer_ids)
            | Q(id__in=lead_customer_ids)
            | Q(id__in=collection_customer_ids)
            | Q(id__in=overdue_customer_ids)
            | Q(id__in=collection_plan_customer_ids)
        )

        if _KAMAssignment is not None:
            assignment_customer_ids = (
                _KAMAssignment.objects
                .filter(kam_id__in=kam_ids)
                .filter(
                    Q(active_to__isnull=True)
                    | Q(active_to__gte=timezone.localdate())
                )
                .values_list("customer_id", flat=True)
            )

            q_obj = q_obj | Q(id__in=assignment_customer_ids)

        return (
            base_qs
            .filter(q_obj)
            .distinct()
            .order_by("name", "code")
        )

    if _is_manager(user):
        kam_ids = set(_kams_managed_by_manager(user))

        # Critical fix:
        # Include the manager user's own id as well.
        # This fixes hybrid Manager/KAM users such as Pratik.
        if getattr(user, "id", None):
            kam_ids.add(user.id)

        if not kam_ids:
            logger.warning(
                "Manager/KAM has no team or own KAM ID. user_id=%s username=%s",
                getattr(user, "id", None),
                getattr(user, "username", None),
            )
            return Customer.objects.none()

        return _customers_for_kam_ids(kam_ids)

    return _customers_for_kam_ids([user.id])

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


def _single_visit_qs_for_user(user: User):
    qs = VisitPlan.objects.select_related("customer", "kam").filter(batch__isnull=True)
    if _is_admin(user):
        return qs
    if _is_manager(user):
        approver_mapped_ids = []
        try:
            from apps.leave.models import ApproverMapping
            approver_mapped_ids = list(
                ApproverMapping.objects.filter(
                    reporting_person=user
                ).values_list("employee_id", flat=True)
            )
        except Exception:
            logger.exception(
                "_single_visit_qs_for_user: ApproverMapping lookup failed for manager %s", user.username
            )

        kam_manager_ids = _kams_managed_by_manager(user)

        profile_ro_ids = list(
            User.objects.filter(
                profile__reporting_officer=user, is_active=True
            ).values_list("id", flat=True)
        )

        kam_ids = list(set(kam_manager_ids) | set(approver_mapped_ids) | set(profile_ro_ids))

        if not kam_ids:
            logger.warning(
                "_single_visit_qs_for_user: manager=%s has NO mapped KAM IDs via any source.",
                user.username,
            )
            return qs.none()

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
    
def _safe_ratio(numerator, denominator):
    numerator = _safe_decimal(numerator)
    denominator = _safe_decimal(denominator)

    if not denominator:
        return None

    try:
        return numerator / denominator
    except Exception:
        return None


def _customer360_identity_key(name: str) -> str:
    """
    Runtime customer alias key for Customer360 reads.

    Purpose:
    - Include existing duplicate customer rows created by sheet name variations.
    - AAM Forge Pvt. Ltd. == AAM FORGE PRIVATE LIMITED
    - AKAR AUTO INDUSTRIES LIMITED == AKAR AUTO INDUSTRIES PVT LTD

    Does not modify DB.
    Does not change display name.
    """
    import re
    import unicodedata

    text = unicodedata.normalize("NFKD", str(name or ""))
    text = text.encode("ascii", "ignore").decode()
    text = text.upper()

    replacements = {
        "&": " AND ",
        ".": " ",
        ",": " ",
        "-": " ",
        "_": " ",
        "/": " ",
        "\\": " ",
        "(": " ",
        ")": " ",
        "PRIVATE": "PVT",
        "LIMITED": "LTD",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    tokens = [token.strip() for token in text.split() if token.strip()]

    removable_suffixes = {
        "PVT",
        "LTD",
        "PRIVATE",
        "LIMITED",
        "LLP",
        "LLC",
        "INC",
        "CO",
        "COMPANY",
        "CORP",
        "CORPORATION",
    }

    tokens = [token for token in tokens if token not in removable_suffixes]

    return re.sub(r"[^A-Z0-9]", "", "".join(tokens))


def _customer360_alias_customer_ids(customer: Customer, accessible_qs=None) -> List[int]:
    """
    Return all Customer IDs representing the same real customer.

    Fixes:
    - Existing duplicate customer rows caused by sheet name variations.
    - Avoids .only() conflict when accessible_qs already has select_related().
    - Does not modify DB.
    - Does not change display name.
    """
    if not customer:
        return []

    target_key = _customer360_identity_key(customer.name)

    if not target_key:
        return [customer.id]

    base_qs = accessible_qs if accessible_qs is not None else Customer.objects.all()

    raw_name = str(customer.name or "").strip()

    first_token = ""

    for token in (
        raw_name
        .replace(".", " ")
        .replace(",", " ")
        .replace("-", " ")
        .replace("/", " ")
        .replace("(", " ")
        .replace(")", " ")
        .split()
    ):
        token = token.strip()

        if token:
            first_token = token
            break

    if first_token:
        candidate_qs = base_qs.filter(name__icontains=first_token)
    else:
        candidate_qs = base_qs.filter(id=customer.id)

    alias_ids = []

    # IMPORTANT:
    # Use values("id", "name") instead of only("id", "name").
    # .only() conflicts with select_related("kam", "primary_kam") on base_qs.
    for candidate in candidate_qs.values("id", "name"):
        if _customer360_identity_key(candidate.get("name")) == target_key:
            alias_ids.append(candidate.get("id"))

    if customer.id not in alias_ids:
        alias_ids.append(customer.id)

    return sorted({int(customer_id) for customer_id in alias_ids if customer_id})


def _parse_decimal_or_none(s: str) -> Optional[Decimal]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None
    
def _require_purpose_of_visit(value: str, *, max_length: int = 2000) -> str:
    """
    Backend guard for Purpose of Visit.

    Important:
    - DB field remains `purpose`.
    - UI/display label is `Purpose of Visit`.
    - Used in views where raw POST data is handled directly.
    """
    value = (value or "").strip()

    if not value:
        raise ValueError("Purpose of Visit is required.")

    if len(value) > max_length:
        raise ValueError(f"Purpose of Visit is too long. Max {max_length} characters allowed.")

    return value




def _get_or_create_manual_customer_for_kam(*, name: str, kam_user: User) -> Tuple[Customer, bool]:
    """
    Create/map one manually entered customer without duplicating customer data.
    Reuses Customer + KAMAssignment only.
    """
    clean_name = (name or "").strip()

    if not clean_name:
        raise ValueError("Customer name is required.")

    with transaction.atomic():
        customer_obj = (
            Customer.objects
            .select_for_update()
            .filter(name__iexact=clean_name)
            .first()
        )

        created = False

        if not customer_obj:
            customer_obj = Customer.objects.create(
                name=clean_name,
                kam=kam_user,
                primary_kam=kam_user,
                source=Customer.SOURCE_MANUAL,
                created_by=kam_user,
            )
            created = True

        changed_fields = []

        if hasattr(customer_obj, "kam_id") and not customer_obj.kam_id:
            customer_obj.kam = kam_user
            changed_fields.append("kam")

        if hasattr(customer_obj, "primary_kam_id") and not customer_obj.primary_kam_id:
            customer_obj.primary_kam = kam_user
            changed_fields.append("primary_kam")

        if hasattr(customer_obj, "source") and not getattr(customer_obj, "source", None):
            customer_obj.source = Customer.SOURCE_MANUAL
            changed_fields.append("source")

        if hasattr(customer_obj, "created_by_id") and not getattr(customer_obj, "created_by_id", None):
            customer_obj.created_by = kam_user
            changed_fields.append("created_by")

        if changed_fields:
            if hasattr(customer_obj, "updated_at"):
                changed_fields.append("updated_at")
            customer_obj.save(update_fields=changed_fields)

        KAMAssignment.objects.get_or_create(
            customer=customer_obj,
            kam=kam_user,
            defaults={"active_from": timezone.localdate()},
        )

    return customer_obj, created


def _visit_plan_customer_names(plan: "VisitPlan") -> List[str]:
    names = []

    batch_names = getattr(plan, "batch_customer_names", None)
    if batch_names:
        return list(batch_names)

    if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
        name = (getattr(plan.customer, "name", "") or "").strip()
        if name:
            names.append(name)
    else:
        name = (getattr(plan, "counterparty_name", "") or "").strip()
        if name:
            names.append(name)

    return names


def _attach_visit_customer_display(rows: List["VisitPlan"]) -> List["VisitPlan"]:
    """
    Attach customer chip data to VisitPlan rows without N+1 queries.
    For batch/multi-customer route rows, every line receives the complete
    ordered customer list from the same VisitBatch.
    """
    rows = list(rows or [])
    batch_ids = sorted({int(row.batch_id) for row in rows if getattr(row, "batch_id", None)})

    names_by_batch: Dict[int, List[str]] = {}

    if batch_ids:
        batch_lines = (
            VisitPlan.objects
            .select_related("customer")
            .filter(batch_id__in=batch_ids)
            .order_by("batch_id", "customer__name", "counterparty_name", "id")
        )

        seen_by_batch = {}

        for line in batch_lines:
            if getattr(line, "customer_id", None) and getattr(line, "customer", None):
                name = (getattr(line.customer, "name", "") or "").strip()
            else:
                name = (getattr(line, "counterparty_name", "") or "").strip()

            if not name:
                continue

            bucket = names_by_batch.setdefault(line.batch_id, [])
            seen = seen_by_batch.setdefault(line.batch_id, set())
            key = name.upper()

            if key not in seen:
                seen.add(key)
                bucket.append(name)

    for row in rows:
        if getattr(row, "batch_id", None) and row.batch_id in names_by_batch:
            names = names_by_batch.get(row.batch_id, [])
        else:
            names = _visit_plan_customer_names(row)

        row.customer_badge_names = names
        row.customer_display_name = " / ".join(names) if names else "-"
        row.recent_customer_name = row.customer_display_name

    return rows

def _post_meeting_details_complete(actual: Optional["VisitActual"]) -> bool:
    """
    Post-meeting workflow completion check.

    Outcome should be visible to manager only after:
    - Post Meeting Date & Time exists
    - Meeting Outcome exists
    - Post Meeting Details / Discussion Summary exists
    - Follow-up Notes exists
    """
    if not actual:
        return False

    if not getattr(actual, "actual_datetime", None):
        return False

    if getattr(actual, "successful", None) is None:
        return False

    if not (getattr(actual, "meeting_notes", "") or "").strip():
        return False

    if not (getattr(actual, "next_action", "") or "").strip():
        return False

    return True

def _manager_visit_business_status(plan: "VisitPlan") -> str:
    """
    Manager View business status.

    Required production lifecycle:
    - DRAFT              -> Draft
    - PENDING_APPROVAL   -> Pending
    - APPROVED           -> Pending
    - REJECTED           -> Rejected
    - COMPLETED          -> Completed

    Important:
    Manager approval alone must NOT display Completed.
    Completed is shown only after manager accepts post-visit review.
    """
    status = (getattr(plan, "approval_status", "") or "").strip().upper()

    if status == STATUS_COMPLETED:
        return "Completed"

    if status == STATUS_REJECTED:
        return "Rejected"

    if status == STATUS_DRAFT:
        return "Draft"

    return "Pending"


def _post_visit_submitted(plan: "VisitPlan") -> bool:
    """
    True only when KAM has submitted required post-visit details.
    This does NOT mean workflow is completed.
    """
    actual = getattr(plan, "actual", None)
    return bool(actual and _post_meeting_details_complete(actual))


def _post_visit_can_be_manager_reviewed(plan: "VisitPlan") -> bool:
    """
    Manager can accept post visit only after:
    - visit was manager-approved
    - KAM submitted complete post-visit details
    - workflow is not already completed
    """
    return (
        (getattr(plan, "approval_status", "") or "").strip().upper() == STATUS_APPROVED
        and _post_visit_submitted(plan)
    )


def _post_visit_completion_mail_already_sent(plan: "VisitPlan") -> bool:
    """
    Prevent duplicate post-visit completion review emails.
    Uses existing VisitApprovalAudit table.
    """
    try:
        return VisitApprovalAudit.objects.filter(
            plan=plan,
            note__icontains="[POST_VISIT_COMPLETION_MAIL_SENT]",
        ).exists()
    except Exception:
        logger.exception(
            "Failed checking post-visit mail audit. plan_id=%s",
            getattr(plan, "id", None),
        )
        return False


def _safe_actual_attachment_text(actual: Optional["VisitActual"]) -> str:
    """
    Schema-safe attachment display helper.

    Reuses whatever attachment/file field exists in production.
    If no attachment field exists, returns '-'.
    """
    if not actual:
        return "-"

    candidate_attrs = (
        "attachment",
        "attachments",
        "file",
        "files",
        "document",
        "documents",
        "proof",
        "proof_file",
    )

    for attr in candidate_attrs:
        try:
            value = getattr(actual, attr, None)
        except Exception:
            continue

        if not value:
            continue

        try:
            if hasattr(value, "all"):
                names = []
                for item in value.all():
                    name = (
                        getattr(item, "name", None)
                        or getattr(item, "filename", None)
                        or getattr(item, "file", None)
                        or str(item)
                    )
                    if name:
                        names.append(str(name))
                return ", ".join(names) if names else "-"
        except Exception:
            logger.exception(
                "Failed reading related attachments. actual_id=%s attr=%s",
                getattr(actual, "id", None),
                attr,
            )

        try:
            return str(value)
        except Exception:
            return "-"

    return "-"


def _post_visit_meeting_outcome_text(actual: Optional["VisitActual"]) -> str:
    if not actual:
        return "-"

    if getattr(actual, "successful", None) is True:
        return "Successful"

    if getattr(actual, "successful", None) is False:
        reason = getattr(actual, "not_success_reason", None)
        if reason:
            try:
                return f"Not Successful: {actual.get_not_success_reason_display()}"
            except Exception:
                return f"Not Successful: {reason}"
        return "Not Successful"

    return "-"


def _build_post_visit_completion_email(
    *,
    request: HttpRequest,
    plan: "VisitPlan",
    manager_user: User,
) -> str:
    """
    Post Visit Completion Mail.

    Required fields:
    - Customer
    - Purpose of Visit
    - Meeting Outcome
    - Discussion
    - Sales Opportunity
    - Attachments
    - Visit Date
    - Manager Review Link
    """
    actual = getattr(plan, "actual", None)

    if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
        customer_name = _safe_email_value(plan.customer.name)
    else:
        customer_name = _safe_email_value(getattr(plan, "counterparty_name", None))

    review_url = request.build_absolute_uri(
        reverse("kam:manager_view") + f"?tab=visits&focus_visit={plan.id}"
    )

    context = {
        "recipient_name": _email_display_name(manager_user),
        "visit_id": plan.id,
        "customer": customer_name,
        "kam_name": _email_display_name(getattr(plan, "kam", None)),
        "purpose": _safe_email_value(getattr(plan, "purpose", None)),
        "meeting_outcome": _post_visit_meeting_outcome_text(actual),
        "discussion": _safe_email_value(getattr(actual, "meeting_notes", None) if actual else None),
        "sales_opportunity": _safe_email_value(getattr(actual, "actual_sales_mt", None) if actual else None),
        "attachments": _safe_actual_attachment_text(actual),
        "visit_date": _display_date_range(
            getattr(plan, "visit_date", None),
            getattr(plan, "visit_date_to", None),
        ),
        "review_url": review_url,
        "remarks": _safe_email_value(getattr(actual, "next_action", None) if actual else None),
    }

    try:
        return render_to_string("kam/emails/post_visit_completion.html", context)
    except Exception:
        logger.info(
            "Optional template kam/emails/post_visit_completion.html not found or failed. "
            "Using inline fallback. plan_id=%s",
            getattr(plan, "id", None),
        )

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Post Visit Completion Review</title>
</head>
<body style="margin:0;padding:0;background:#f6f7f9;font-family:Arial,Helvetica,sans-serif;color:#111;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f7f9;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:680px;background:#ffffff;border:1px solid #e6e8ec;">
          <tr>
            <td style="background:#0b1f3a;color:#ffffff;padding:18px 20px;font-size:18px;font-weight:bold;">
              Post Visit Completion Review Required
            </td>
          </tr>
          <tr>
            <td style="padding:20px;font-size:14px;line-height:1.6;">
              <p>Hello {context["recipient_name"]},</p>
              <p>The KAM has submitted post-visit details. Please review and accept to complete the workflow.</p>

              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e6e8ec;">
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Visit ID</td><td style="padding:8px;border:1px solid #e6e8ec;">#{context["visit_id"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Customer</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["customer"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">KAM</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["kam_name"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Visit Date</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["visit_date"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Purpose of Visit</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["purpose"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Meeting Outcome</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["meeting_outcome"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Discussion</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["discussion"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Sales Opportunity</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["sales_opportunity"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Attachments</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["attachments"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Remarks / Next Action</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["remarks"]}</td></tr>
              </table>

              <p style="margin-top:18px;">
                <a href="{context["review_url"]}" style="background:#0b5cab;color:#ffffff;text-decoration:none;padding:10px 16px;display:inline-block;border-radius:4px;">
                  Review Post Visit
                </a>
              </p>

              <p style="font-size:12px;color:#667085;">System generated message. Please do not reply.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


def _send_post_visit_completion_mail(
    *,
    request: HttpRequest,
    plan: "VisitPlan",
) -> bool:
    """
    Sends second email to manager after KAM submits post-visit details.
    Does not complete the workflow.
    """
    manager_user = _active_manager_for_kam(plan.kam)

    if not manager_user or not getattr(manager_user, "email", None):
        logger.warning(
            "Post visit completion mail skipped. No active manager email. plan_id=%s kam_id=%s",
            getattr(plan, "id", None),
            getattr(plan, "kam_id", None),
        )
        return False

    if _post_visit_completion_mail_already_sent(plan):
        logger.info(
            "Post visit completion mail already sent. Skipping duplicate. plan_id=%s",
            getattr(plan, "id", None),
        )
        return True

    subject = (
        f"[KAM] Post Visit Completion Review Required: Visit #{plan.id} "
        f"({plan.visit_date}) - {plan.kam.get_full_name() or plan.kam.username}"
    )

    body = _build_post_visit_completion_email(
        request=request,
        plan=plan,
        manager_user=manager_user,
    )

    cc_users = _active_cc_for_kam(plan.kam)

    sent_ok = _send_safe_mail(
        subject,
        body,
        [manager_user],
        cc_users,
    )

    if sent_ok:
        VisitApprovalAudit.objects.create(
            plan=plan,
            actor=plan.kam,
            action=VisitApprovalAudit.ACTION_SUBMIT,
            note="[POST_VISIT_COMPLETION_MAIL_SENT] Post visit submitted to manager for review",
            actor_ip=_get_ip(request),
        )

    return sent_ok


def _build_customer_history_payload(customer_ids: List[int], actor: User) -> Dict[int, dict]:
    """
    Builds customer visit history for Manager View eye-icon modal.

    Reuses existing tables only:
    - Customer
    - VisitPlan
    - VisitActual
    - VisitApprovalAudit

    No duplicate records.
    No new workflow.
    """
    customer_ids = sorted({int(customer_id) for customer_id in (customer_ids or []) if customer_id})

    if not customer_ids:
        return {}

    visits_qs = (
        _visitplan_qs_for_user(actor)
        .select_related("customer", "kam", "actual", "approved_by", "rejected_by")
        .filter(customer_id__in=customer_ids)
        .order_by("-visit_date", "-created_at", "-id")
    )

    visits_by_customer: Dict[int, List[VisitPlan]] = {}

    for visit in visits_qs:
        if not visit.customer_id:
            continue
        visits_by_customer.setdefault(visit.customer_id, []).append(visit)

    payload: Dict[int, dict] = {}

    for customer_id, visits in visits_by_customer.items():
        if not visits:
            continue

        latest_visit = visits[0]
        first_visit = visits[-1]
        customer = latest_visit.customer

        successful_count = 0
        pending_count = 0
        rejected_count = 0
        followup_count = 0

        timeline = []
        next_meetings = []

        for visit in visits:
            actual = getattr(visit, "actual", None)
            business_status = _manager_visit_business_status(visit)

            if business_status == "Rejected":
                rejected_count += 1
            elif business_status == "Completed":
                if actual and actual.successful is True:
                    successful_count += 1
            else:
                pending_count += 1

            if actual and (getattr(actual, "next_action", "") or "").strip():
                followup_count += 1

            manager_decision = "-"

            if visit.approval_status == STATUS_REJECTED:
                manager_decision = "Rejected"
            elif visit.approval_status == STATUS_COMPLETED:
                manager_decision = "Post Visit Accepted"
            elif visit.approval_status == STATUS_APPROVED:
                manager_decision = "Visit Approved"

            meeting_outcome = _post_visit_meeting_outcome_text(actual)

            timeline.append({
                "visit": visit,
                "actual": actual,
                "visit_status": business_status,
                "manager_decision": manager_decision,
                "meeting_outcome": meeting_outcome,
            })

            if actual and getattr(actual, "next_action_date", None):
                next_meetings.append({
                    "meeting_date": actual.next_action_date,
                    "meeting_purpose": actual.next_action or "-",
                    "outcome": meeting_outcome,
                })

        payload[customer_id] = {
            "customer": customer,
            "kam_name": latest_visit.kam.get_full_name() or latest_visit.kam.username,
            "total_visits": len(visits),
            "first_visit": first_visit.visit_date if first_visit else None,
            "latest_visit": latest_visit.visit_date if latest_visit else None,
            "timeline": timeline,
            "next_meetings": next_meetings,
            "sales_summary": {
                "total_visits": len(visits),
                "successful_visits": successful_count,
                "pending_visits": pending_count,
                "rejected_visits": rejected_count,
                "followup_count": followup_count,
            },
        }

    return payload

def _current_business_quarter_bounds(today_value=None) -> Tuple[date, date]:
    """
    Current financial/business quarter.

    Business quarters:
      Q1: Apr-Jun
      Q2: Jul-Sep
      Q3: Oct-Dec
      Q4: Jan-Mar

    Requirement:
      Current quarter = quarter start date through today.
    """
    today_value = today_value or timezone.localdate()

    if today_value.month in (4, 5, 6):
        start = date(today_value.year, 4, 1)
    elif today_value.month in (7, 8, 9):
        start = date(today_value.year, 7, 1)
    elif today_value.month in (10, 11, 12):
        start = date(today_value.year, 10, 1)
    else:
        start = date(today_value.year, 1, 1)

    return start, today_value


def _visit_time_filter_bounds(time_filter: str) -> Tuple[Optional[date], Optional[date]]:
    """
    Visit History / Manager View date-range filter.

    Production-safe behavior:
      weekly     / current_week     -> Monday of current week through today
      monthly    / current_month    -> first day of current month through today
      quarterly  / current_quarter  -> current financial quarter through today
      yearly     / current_year     -> current financial year through today

    Financial year:
      Apr 1 -> Mar 31
      For current-filter use: Apr 1 of current FY through today.
    """
    today_value = timezone.localdate()
    value = (time_filter or "").strip().lower()

    if value in {"weekly", "current_week", "week"}:
        start = today_value - timezone.timedelta(days=today_value.weekday())
        return start, today_value

    if value in {"monthly", "current_month", "month"}:
        return date(today_value.year, today_value.month, 1), today_value

    if value in {"quarterly", "current_quarter", "quarter"}:
        return _current_business_quarter_bounds(today_value)

    if value in {"yearly", "current_year", "year"}:
        financial_year_start_year = today_value.year if today_value.month >= 4 else today_value.year - 1
        return date(financial_year_start_year, 4, 1), today_value

    return None, None

def _apply_visit_history_filters(qs, request: HttpRequest, *, kam_field: str, date_field: str):
    """
    Combined server-side filter helper for Visit History / Single Visits / Manager View.

    Supports:
      - Status is handled outside this helper.
      - KAM dropdown:
          kam / user / kam_id
      - Required Visit History date range dropdown:
          date_range = weekly / monthly / quarterly / yearly
      - Backward-compatible older params:
          time_filter = current_month / current_quarter / current_year
      - Optional legacy custom dates:
          from_date / from
          to_date / to

    Production rule:
      Filters combine together.
    """
    selected_kam = (
        request.GET.get("kam")
        or request.GET.get("user")
        or request.GET.get("kam_id")
        or ""
    ).strip()

    raw_from = (
        request.GET.get("from_date")
        or request.GET.get("from")
        or ""
    ).strip()

    raw_to = (
        request.GET.get("to_date")
        or request.GET.get("to")
        or ""
    ).strip()

    # New required Visit History dropdown name.
    date_range = (request.GET.get("date_range") or "").strip().lower()

    # Backward-compatible old name used by existing Manager View / Single Visit screens.
    time_filter = (request.GET.get("time_filter") or "").strip().lower()

    active_range = date_range or time_filter

    range_start, range_end = _visit_time_filter_bounds(active_range)
    explicit_from = _parse_iso_date(raw_from)
    explicit_to = _parse_iso_date(raw_to)

    start_date = range_start
    end_date = range_end

    if explicit_from:
        start_date = max(start_date, explicit_from) if start_date else explicit_from

    if explicit_to:
        end_date = min(end_date, explicit_to) if end_date else explicit_to

    if selected_kam and selected_kam.upper() not in {"ALL", "*"}:
        if selected_kam.isdigit():
            qs = qs.filter(**{kam_field: int(selected_kam)})
        else:
            relation = kam_field[:-3] if kam_field.endswith("_id") else kam_field
            qs = qs.filter(
                Q(**{f"{relation}__username__iexact": selected_kam})
                | Q(**{f"{relation}__email__iexact": selected_kam})
            )

    if start_date:
        qs = qs.filter(**{f"{date_field}__gte": start_date})

    if end_date:
        qs = qs.filter(**{f"{date_field}__lte": end_date})

    return qs

# FIX 2 — Timezone-aware datetime helpers
# All datetime construction now uses timezone.make_aware() to prevent
# RuntimeWarning: DateTimeField received a naive datetime.

def _make_aware(dt) -> timezone.datetime:
    """Ensure a datetime is timezone-aware. No-op if already aware."""
    if dt is None:
        return dt
    if timezone.is_naive(dt):
        return timezone.make_aware(dt)
    return dt


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
    """
    FIX 2 — Always return timezone-aware datetime. Previously returned naive
    datetime objects which caused RuntimeWarning in ORM date comparisons.
    """
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
        # FIX 2: use timezone.make_aware() instead of bare datetime()
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
        # FIX 2: use timezone.make_aware() for boundary datetimes
        s = timezone.make_aware(timezone.datetime(2000, 1, 1))
        e = timezone.make_aware(timezone.datetime(2100, 1, 1))
        return "ALL", s, e, "ALL"
    return ("WEEK", *_iso_week_bounds(ref))


def _get_dashboard_range(request: HttpRequest) -> Tuple[timezone.datetime, timezone.datetime, str]:
    """
    Canonical dashboard/report date-range resolver.

    Production behavior:
    - Default dashboard/report period is Current Month.
    - Existing custom from/to filters remain supported.
    - Existing range shortcuts remain supported.
    - Returned end_dt is exclusive.
    """
    now = timezone.localtime(timezone.now())
    today_local = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today_local + timezone.timedelta(days=1)

    from_s = _first_query_value(
        request,
        "from",
        "from_date",
        "start_date",
        "date_from",
        "fromDate",
        "startDate",
        "dateFrom",
    )
    to_s = _first_query_value(
        request,
        "to",
        "to_date",
        "end_date",
        "date_to",
        "toDate",
        "endDate",
        "dateTo",
    )

    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)

    if from_d and to_d and from_d <= to_d:
        start = timezone.make_aware(
            timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0)
        )
        end = timezone.make_aware(
            timezone.datetime(to_d.year, to_d.month, to_d.day, 0, 0, 0)
        ) + timezone.timedelta(days=1)

        return start, end, f"{from_d} → {to_d}"

    if from_d and not to_d:
        start = timezone.make_aware(
            timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0)
        )
        return start, start + timezone.timedelta(days=1), f"{from_d} → {from_d}"

    range_shortcut = (request.GET.get("range") or "").strip().lower()

    if range_shortcut:
        if range_shortcut in {"today", "day"}:
            return today_local, tomorrow, f"{today_local.date()} → {today_local.date()}"

        if range_shortcut in {"weekly", "week", "this_week", "current_week"}:
            start = today_local - timezone.timedelta(days=today_local.weekday())
            end = start + timezone.timedelta(days=7)
            return start, end, f"{start.date()} → {(end - timezone.timedelta(days=1)).date()}"

        if range_shortcut in {"monthly", "month", "this_month", "thismonth", "current_month"}:
            start, end, _pid = _month_bounds(now)
            return start, end, f"{start.date()} → {(end - timezone.timedelta(days=1)).date()}"

        if range_shortcut in {"quarterly", "quarter", "this_quarter", "thisquarter", "current_quarter"}:
            start, end, _pid = _quarter_bounds(now)
            return start, end, f"{start.date()} → {(end - timezone.timedelta(days=1)).date()}"

        if range_shortcut in {"yearly", "year", "this_year", "thisyear", "current_year"}:
            start, end, _pid = _year_bounds(now)
            return start, end, f"{start.date()} → {(end - timezone.timedelta(days=1)).date()}"

        if range_shortcut in {"last7", "7d", "7days"}:
            start = today_local - timezone.timedelta(days=6)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"

        if range_shortcut in {"last30", "30d", "30days"}:
            start = today_local - timezone.timedelta(days=29)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"

        if range_shortcut in {"last60", "60d"}:
            start = today_local - timezone.timedelta(days=59)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"

        if range_shortcut in {"last90", "90d", "90days", "3m"}:
            start = today_local - timezone.timedelta(days=89)
            return start, tomorrow, f"{start.date()} → {today_local.date()}"

        if range_shortcut in {"all", "*"}:
            start = timezone.make_aware(timezone.datetime(2000, 1, 1, 0, 0, 0))
            end = timezone.make_aware(timezone.datetime(2100, 1, 1, 0, 0, 0))
            return start, end, "ALL"

    start, end, _pid = _month_bounds(now)
    return start, end, f"{start.date()} → {(end - timezone.timedelta(days=1)).date()}"

def _resolve_scope(request: HttpRequest, actor: User) -> Tuple[Optional[int], str]:
    if not _is_manager(actor):
        return actor.id, actor.username

    raw_scope = _first_query_value(
        request,
        "user",
        "kam",
        "KAM",
        "username",
        "user_name",
        "kam_username",
    )
    # IMPORTANT:
    # Do NOT use generic "id" here.
    # On Customer 360, ?id=... is the customer id, not the KAM/user id.
    raw_scope_id = _first_query_value(request, "kam_id", "user_id")

    u = None
    if raw_scope_id and raw_scope_id.isdigit():
        u = User.objects.filter(id=int(raw_scope_id), is_active=True).first()
    elif raw_scope:
        if raw_scope.upper() in {"ALL", "*"}:
            return None, "ALL"

        u = User.objects.filter(
            Q(username__iexact=raw_scope) | Q(email__iexact=raw_scope),
            is_active=True,
        ).first()

        if not u and " " in raw_scope.strip():
            parts = [p for p in raw_scope.strip().split() if p]
            if len(parts) >= 2:
                u = User.objects.filter(
                    first_name__iexact=parts[0],
                    last_name__iexact=" ".join(parts[1:]),
                    is_active=True,
                ).first()

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
    """
    Customer 360 should default to ALL, not WEEK.

    Reason:
    Customer 360 is a relationship/history screen. Narrow default windows make
    valid customers appear to have 'no sales data for this period' even when
    their invoice history exists.
    """
    from_s = (request.GET.get("from") or "").strip()
    to_s = (request.GET.get("to") or "").strip()
    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)

    if from_d and to_d and from_d <= to_d:
        return "CUSTOM", from_d, to_d, f"{from_d}..{to_d}"

    p = (request.GET.get("period") or "all").strip().lower()
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

    if p == "week":
        sdt, edt, pid = _iso_week_bounds(now)
        return "WEEK", sdt.date(), (edt - timezone.timedelta(days=1)).date(), pid

    return "ALL", timezone.datetime(2000, 1, 1).date(), timezone.datetime(2100, 1, 1).date(), "ALL"


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    if m == 12:
        next_month_first = timezone.datetime(y + 1, 1, 1).date()
    else:
        next_month_first = timezone.datetime(y, m + 1, 1).date()
    last_day = next_month_first - timezone.timedelta(days=1)
    return timezone.datetime(y, m, min(d.day, last_day.day)).date()


def _calc_visits_target(start_dt: timezone.datetime, end_dt: timezone.datetime) -> int:
    delta_days = max(0, (end_dt - start_dt).days)
    if delta_days == 0:
        return 6
    return math.ceil(delta_days / 7) * 6

def _report_value(report: Optional[dict], *path, default=0):
    """
    Safe nested value reader for build_kam_performance_report() payload.

    Keeps dashboard/report merge resilient when service payload has missing keys.
    """
    current = report or {}

    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)

    return default if current is None else current


def _build_dashboard_report_charts(
    *,
    report: Optional[dict],
    kpi: dict,
    trend_rows: List[Dict],
) -> dict:
    """
    Build dashboard chart payload from:
    - existing build_kam_performance_report() output
    - existing dashboard KPI calculations

    Important:
    - No duplicate KPI service.
    - No duplicate aggregation service.
    - No duplicate report calculation.
    """
    report = report or {}

    monthly_trend = _report_value(report, "charts", "monthly_trend", default=[]) or []
    weekly_trend = _report_value(report, "charts", "weekly_trend", default=[]) or []

    if monthly_trend:
        sales_trend_labels = [
            row.get("label") or row.get("period") or row.get("month") or "-"
            for row in monthly_trend
        ]
        sales_trend_values = [
            float(row.get("sales_mt") or row.get("sales") or 0)
            for row in monthly_trend
        ]
    else:
        sales_trend_labels = [row.get("week") for row in trend_rows]
        sales_trend_values = [float(row.get("sales_mt") or 0) for row in trend_rows]

    leads_total = int(
        _report_value(
            report,
            "leads",
            "total_leads",
            default=kpi.get("leads_total_count") or 0,
        ) or 0
    )
    leads_won = int(
        _report_value(
            report,
            "leads",
            "converted_leads",
            default=kpi.get("leads_converted_count") or 0,
        ) or 0
    )
    leads_lost = int(
        _report_value(
            report,
            "leads",
            "lost_leads",
            default=0,
        ) or 0
    )
    leads_pending = max(leads_total - leads_won - leads_lost, 0)

    total_overdue = float(
        _report_value(
            report,
            "collections",
            "total_overdue",
            default=kpi.get("collection_planned") or 0,
        ) or 0
    )
    total_collected = float(
        _report_value(
            report,
            "collections",
            "total_collected",
            default=kpi.get("collection_actual") or 0,
        ) or 0
    )
    pending_collection = float(
        _report_value(
            report,
            "collections",
            "pending_collection",
            default=kpi.get("collection_pending") or 0,
        ) or 0
    )

    overall_score = float(
        _report_value(
            report,
            "score",
            "overall_score",
            default=0,
        ) or 0
    )

    sales_achievement = float(kpi.get("sales_ach_pct") or 0)
    achieved_sales = min(max(sales_achievement, 0), 100)
    remaining_sales = max(100 - achieved_sales, 0)

    return {
        "monthly_sales_trend": {
            "labels": sales_trend_labels,
            "values": sales_trend_values,
        },
        "visits_completed": {
            "labels": ["Completed", "Target"],
            "values": [
                int(kpi.get("visits_actual") or 0),
                int(kpi.get("visits_target") or 0),
            ],
        },
        "calls_completed": {
            "labels": ["Completed", "Target"],
            "values": [
                int(kpi.get("calls") or 0),
                int(kpi.get("calls_target") or 0),
            ],
        },
        "lead_conversion": {
            "labels": ["Won", "Lost", "Pending"],
            "values": [leads_won, leads_lost, leads_pending],
        },
        "collections": {
            "labels": ["Collected", "Pending", "Overdue"],
            "values": [total_collected, pending_collection, total_overdue],
        },
        "customer_visit_distribution": {
            "labels": ["Customer Visit", "Vendor Visit", "Plant Visit", "Other"],
            "values": [
                int(_report_value(report, "visits", "customer_visits", default=0) or 0),
                int(_report_value(report, "visits", "vendor_visits", default=0) or 0),
                int(_report_value(report, "visits", "plant_visits", default=0) or 0),
                int(_report_value(report, "visits", "other_visits", default=0) or 0),
            ],
        },
        "visit_approval_status": {
            "labels": ["Approved", "Pending", "Rejected", "Completed"],
            "values": [
                int(_report_value(report, "visits", "approved_visits", default=0) or 0),
                int(_report_value(report, "visits", "pending_visits", default=0) or 0),
                int(_report_value(report, "visits", "rejected_visits", default=0) or 0),
                int(kpi.get("visits_actual") or 0),
            ],
        },
        "performance_score": {
            "labels": ["Overall KPI %", "Remaining"],
            "values": [
                min(max(overall_score, 0), 100),
                max(100 - overall_score, 0),
            ],
        },
        "target_achievement": {
            "labels": ["Achieved", "Remaining"],
            "values": [achieved_sales, remaining_sales],
        },
        "monthly_comparison": {
            "labels": ["Sales", "Visits", "Collections", "Calls"],
            "values": [
                float(kpi.get("sales_mt") or 0),
                int(kpi.get("visits_actual") or 0),
                total_collected,
                int(kpi.get("calls") or 0),
            ],
        },
        "weekly_trend": weekly_trend,
        "monthly_trend": monthly_trend,
    }


def _dashboard_selected_report_kam(
    *,
    request: HttpRequest,
    scope_kam_id: Optional[int],
) -> Optional[User]:
    """
    Resolve the KAM used by the embedded dashboard report section.

    Rules:
    - If dashboard is scoped to one KAM, use that KAM.
    - If dashboard is ALL, reuse existing report resolver.
    """
    if scope_kam_id:
        return (
            User.objects
            .filter(id=scope_kam_id, is_active=True)
            .exclude(is_superuser=True)
            .exclude(username__iexact="admin")
            .exclude(email__icontains="admin")
            .first()
        )

    return _resolve_selected_kam_for_performance_report(request)


def _kam_options_for_user(user: User) -> List[str]:
    if not _is_manager(user):
        return []
    if _is_admin(user):
        return list(User.objects.filter(is_active=True).order_by("username").values_list("username", flat=True))
    allowed_ids = _kams_managed_by_manager(user)
    return list(
        User.objects.filter(is_active=True, id__in=allowed_ids).order_by("username").values_list("username", flat=True)
    )


def _manager_customer_dropdown_options():
    """
    Manager View customer filter source.

    Reuses the canonical Customer Master table populated by Google Sheet sync
    and manual ERP customer creation. This intentionally does not hardcode any
    customer names and does not duplicate customer data.
    """
    return (
        Customer.objects
        .filter(name__isnull=False)
        .exclude(name__exact="")
        .only("id", "name", "code")
        .order_by("name", "code", "id")
    )


def _selected_manager_customer_id(request: HttpRequest) -> Optional[int]:
    raw_customer = (
        request.GET.get("customer")
        or request.GET.get("customer_id")
        or ""
    ).strip()

    if not raw_customer or raw_customer.upper() in {"ALL", "*"}:
        return None

    if not raw_customer.isdigit():
        return None

    customer_id = int(raw_customer)

    exists = (
        Customer.objects
        .filter(id=customer_id, name__isnull=False)
        .exclude(name__exact="")
        .exists()
    )

    return customer_id if exists else None


def _apply_manager_customer_filter(qs, customer_id: Optional[int], field_name: str = "customer_id"):
    if not customer_id:
        return qs
    return qs.filter(**{field_name: customer_id})


def _manager_visit_customer_name(plan: "VisitPlan") -> str:
    """
    Customer Name column for Manager View.

    For Customer Visit rows, display the exact Customer Master name selected
    during Plan Visit. Do not fall back to counterparty/entity text.
    """
    if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
        name = (getattr(plan.customer, "name", "") or "").strip()
        if name:
            return name
    return "No customer selected"


def _visit_approval_status_label(plan: "VisitPlan") -> str:
    try:
        return plan.get_approval_status_display()
    except Exception:
        return (getattr(plan, "approval_status", "") or "-").replace("_", " ").title()



def _safe_display_attr(obj, *attrs: str, default: str = "-") -> str:
    """
    Read optional model attributes safely for templates.

    Production reason:
    VisitActual fields differ across deployed schemas. Templates must not access
    optional/nonexistent fields such as competitor_info directly because strict
    template rendering can raise VariableDoesNotExist.
    """
    if not obj:
        return default

    for attr in attrs:
        if not attr:
            continue
        try:
            value = getattr(obj, attr, None)
        except Exception:
            value = None

        if value is None:
            continue

        text = str(value).strip()
        if text:
            return text

    return default


def _safe_display_decimal_attr(obj, *attrs: str, default: str = "-") -> str:
    if not obj:
        return default

    for attr in attrs:
        try:
            value = getattr(obj, attr, None)
        except Exception:
            value = None

        if value in (None, ""):
            continue

        try:
            return str(value)
        except Exception:
            continue

    return default

def _attach_manager_visit_readonly_details(visits: List["VisitPlan"]) -> List["VisitPlan"]:
    """
    Attach read-only display attributes for Manager View without N+1 queries.

    Uses existing VisitPlan / VisitActual / CallLog / CollectionTxn data only.
    Call and collection detail rows are grouped by customer + KAM + visit date.
    """
    visits = list(visits or [])

    for visit in visits:
        actual = getattr(visit, "actual", None)
        visit.manager_customer_name = _manager_visit_customer_name(visit)
        visit.manager_approval_status_label = _visit_approval_status_label(visit)
        visit.manager_meeting_outcome = _post_visit_meeting_outcome_text(actual)
        visit.manager_attachment_text = _safe_actual_attachment_text(actual)

        visit.manager_post_datetime = getattr(actual, "actual_datetime", None) if actual else None
        visit.manager_next_meeting_date = getattr(actual, "next_action_date", None) if actual else None
        visit.manager_sales_opportunity = getattr(actual, "actual_sales_mt", None) if actual else None
        visit.manager_collection_amount = getattr(actual, "actual_collection", None) if actual else None
        visit.manager_followup_text = _safe_display_attr(
            actual,
            "next_action",
            "follow_up_details",
            "followup_details",
            "follow_up",
        )

        visit.manager_quantity_text = _safe_display_decimal_attr(
            actual,
            "quantity",
            "qty",
            "actual_quantity",
            "actual_sales_mt",
        )
        visit.manager_meeting_summary_text = _safe_display_attr(
            actual,
            "summary",
            "meeting_summary",
            "meeting_notes",
        )
        visit.manager_discussion_text = _safe_display_attr(
            actual,
            "meeting_notes",
            "discussion",
            "discussion_summary",
            "customer_feedback",
        )

        if actual and getattr(actual, "not_success_reason", None):
            try:
                visit.manager_issues_text = actual.get_not_success_reason_display()
            except Exception:
                visit.manager_issues_text = _safe_display_attr(actual, "not_success_reason")
        else:
            visit.manager_issues_text = _safe_display_attr(actual, "issues", "issue")

        visit.manager_challenges_text = _safe_display_attr(
            actual,
            "challenges",
            "challenge",
            "customer_challenges",
        )
        visit.manager_competitor_information_text = _safe_display_attr(
            actual,
            "competitor_information",
            "competitor_info",
            "competitor",
            "competitor_details",
        )
        visit.manager_products_discussed_text = _safe_display_attr(
            actual,
            "products_discussed",
            "product_discussed",
            "products",
            "product_details",
        )
        visit.manager_remarks_text = _safe_display_attr(
            actual,
            "remarks",
            "remark",
            "summary",
            "meeting_notes",
        )

        visit.manager_call_rows = []
        visit.manager_collection_rows = []

    customer_ids = sorted({
        int(getattr(visit, "customer_id", 0))
        for visit in visits
        if getattr(visit, "customer_id", None)
    })
    kam_ids = sorted({
        int(getattr(visit, "kam_id", 0))
        for visit in visits
        if getattr(visit, "kam_id", None)
    })
    visit_dates = [
        getattr(visit, "visit_date", None)
        for visit in visits
        if getattr(visit, "visit_date", None)
    ]

    if not customer_ids or not kam_ids or not visit_dates:
        return visits

    min_visit_date = min(visit_dates)
    max_visit_date = max(visit_dates)

    start_dt = timezone.make_aware(
        timezone.datetime(min_visit_date.year, min_visit_date.month, min_visit_date.day, 0, 0, 0)
    )
    end_dt = timezone.make_aware(
        timezone.datetime(max_visit_date.year, max_visit_date.month, max_visit_date.day, 0, 0, 0)
    ) + timezone.timedelta(days=1)

    calls_by_key: Dict[Tuple[int, int, date], List[CallLog]] = {}

    call_rows = (
        CallLog.objects
        .select_related("customer", "kam")
        .filter(
            customer_id__in=customer_ids,
            kam_id__in=kam_ids,
            call_datetime__gte=start_dt,
            call_datetime__lt=end_dt,
        )
        .order_by("-call_datetime", "-id")
    )

    for call in call_rows:
        try:
            call_date = timezone.localtime(call.call_datetime).date()
        except Exception:
            call_date = call.call_datetime.date() if call.call_datetime else None

        if not call_date:
            continue

        calls_by_key.setdefault(
            (call.customer_id, call.kam_id, call_date),
            [],
        ).append(call)

    collections_by_key: Dict[Tuple[int, int, date], List[CollectionTxn]] = {}

    collection_rows = (
        CollectionTxn.objects
        .select_related("customer", "kam")
        .filter(
            customer_id__in=customer_ids,
            kam_id__in=kam_ids,
            txn_datetime__gte=start_dt,
            txn_datetime__lt=end_dt,
        )
        .order_by("-txn_datetime", "-id")
    )

    for txn in collection_rows:
        try:
            txn_date = timezone.localtime(txn.txn_datetime).date()
        except Exception:
            txn_date = txn.txn_datetime.date() if txn.txn_datetime else None

        if not txn_date:
            continue

        collections_by_key.setdefault(
            (txn.customer_id, txn.kam_id, txn_date),
            [],
        ).append(txn)

    for visit in visits:
        key = (
            getattr(visit, "customer_id", None),
            getattr(visit, "kam_id", None),
            getattr(visit, "visit_date", None),
        )
        visit.manager_call_rows = calls_by_key.get(key, [])
        visit.manager_collection_rows = collections_by_key.get(key, [])

    return visits

def _kam_dropdown_options_for_user(user: User) -> List[User]:
    """
    KAM objects for the Visits & Calls filter dropdown.
    Admin -> all active users. Manager -> own team + self. KAM -> empty
    (dropdown hidden in template; filter is forced to self).
    """
    if _is_admin(user):
        return list(
            User.objects.filter(is_active=True)
            .order_by("first_name", "last_name", "username")
        )
    if _is_manager(user):
        ids = set(_kams_managed_by_manager(user))
        ids.add(user.id)
        return list(
            User.objects.filter(is_active=True, id__in=ids)
            .order_by("first_name", "last_name", "username")
        )
    return []


def _resolve_visits_kam_filter(request: HttpRequest, user: User) -> Tuple[Optional[int], Optional[User]]:
    """
    Resolves which kam_id Visits & Calls should be filtered by, respecting
    permissions. Returns (kam_id_or_None, kam_user_or_None).
    None kam_id means "no restriction" (only possible for admin/manager
    when nothing / ALL is selected).
    """
    if not _is_manager(user):
        return user.id, user

    raw = (request.GET.get("kam") or "").strip()
    if not raw or raw.upper() in {"ALL", "*"}:
        return None, None
    if not raw.isdigit():
        return None, None

    kam_id = int(raw)
    allowed_ids = {u.id for u in _kam_dropdown_options_for_user(user)}
    if kam_id not in allowed_ids:
        return None, None

    kam_user = User.objects.filter(id=kam_id, is_active=True).first()
    return (kam_id, kam_user) if kam_user else (None, None)

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


def _build_batch_approval_email(
    *,
    request,
    batch,
    kam_user,
    visit_category_label,
    remarks,
    approve_url,
    reject_url,
    customers=None,
    counterparty_names=None,
    manager_user=None,
    cc_users=None,
) -> str:
    """
    Build professional HTML email for batch approval.

    Important:
      - Uses direct signed approval/rejection URLs.
      - Does not expose raw URLs in the HTML body.
      - Passes complete structured business context.
      - Logs template rendering failures.
      - Falls back to a safe HTML body, not raw plain URLs.
    """
    direct_approve_url = request.build_absolute_uri(
        reverse("kam:direct_batch_approve", args=[_make_batch_token(batch.id, "APPROVE")])
    )
    direct_reject_url = request.build_absolute_uri(
        reverse("kam:direct_batch_reject", args=[_make_batch_token(batch.id, "REJECT")])
    )

    customers = customers or []
    counterparty_names = counterparty_names or []
    cc_users = cc_users or []

    cc_emails = _emails_from_users(cc_users)

    line_rows = []

    try:
        plans = list(
            VisitPlan.objects
            .select_related("customer")
            .filter(batch=batch)
            .order_by("id")
        )

        for plan in plans:
            if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
                entity_name = _safe_email_value(plan.customer.name)
                location = _safe_email_value(
                    getattr(plan, "location", None)
                    or getattr(plan.customer, "address", None)
                )
            else:
                entity_name = _safe_email_value(getattr(plan, "counterparty_name", None))
                location = _safe_email_value(getattr(plan, "location", None))

            line_rows.append({
                "index": len(line_rows) + 1,
                "entity": entity_name,
                "location": location,
                "date": _display_date_range(
                    getattr(plan, "visit_date", None),
                    getattr(plan, "visit_date_to", None),
                ),
                "category": _safe_email_value(visit_category_label),
                "purpose": _safe_email_value(getattr(plan, "purpose", None) or remarks),
            })

    except Exception:
        logger.exception(
            "Failed to build VisitPlan line rows for batch approval email. batch_id=%s",
            getattr(batch, "id", None),
        )

    if not line_rows and customers:
        for customer in customers:
            line_rows.append({
                "index": len(line_rows) + 1,
                "entity": _safe_email_value(getattr(customer, "name", None)),
                "location": _safe_email_value(getattr(customer, "address", None)),
                "date": _display_date_range(getattr(batch, "from_date", None), getattr(batch, "to_date", None)),
                "category": _safe_email_value(visit_category_label),
                "purpose": _safe_email_value(remarks),
            })

    if not line_rows and counterparty_names:
        for name in counterparty_names:
            line_rows.append({
                "index": len(line_rows) + 1,
                "entity": _safe_email_value(name),
                "location": "-",
                "date": _display_date_range(getattr(batch, "from_date", None), getattr(batch, "to_date", None)),
                "category": _safe_email_value(visit_category_label),
                "purpose": _safe_email_value(remarks),
            })

    line_count = len(line_rows)
    if line_count == 1:
        customer_summary = line_rows[0]["entity"]
        location_summary = line_rows[0]["location"]
    elif line_count > 1:
        customer_summary = f"{line_count} customers/entities"
        location_summary = "Multiple - see visit lines"
    else:
        customer_summary = "-"
        location_summary = "-"

    context = {
        "batch": batch,
        "kam_user": kam_user,
        "manager_user": manager_user,
        "recipient_name": _email_display_name(manager_user) if manager_user else "Manager",

        "employee_name": _email_display_name(kam_user),
        "employee_email": _email_address(kam_user),
        "kam_name": _email_display_name(kam_user),

        "visit_category_label": _safe_email_value(visit_category_label),
        "date_range": _display_date_range(getattr(batch, "from_date", None), getattr(batch, "to_date", None)),
        "remarks": _safe_email_value(remarks),
        "purpose_of_visit": _safe_email_value(remarks),

        "customer_summary": customer_summary,
        "location_summary": location_summary,
        "line_rows": line_rows,
        "line_count": line_count,

        "customers": customers,
        "counterparty_names": counterparty_names,

        "approve_url": direct_approve_url,
        "reject_url": direct_reject_url,

        "cc_users": cc_users,
        "cc_emails": cc_emails,
    }

    try:
        return render_to_string("kam/emails/visit_batch_approval.html", context)
    except Exception:
        logger.exception(
            "Failed to render batch approval email template. batch_id=%s",
            getattr(batch, "id", None),
        )

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Batch Approval Required</title>
</head>
<body style="margin:0;padding:0;background:#f6f7f9;font-family:Arial,Helvetica,sans-serif;color:#111;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f7f9;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;background:#ffffff;border:1px solid #e6e8ec;">
          <tr>
            <td style="background:#0b1f3a;color:#ffffff;padding:18px 20px;font-size:18px;font-weight:bold;">
              Batch Approval Required
            </td>
          </tr>
          <tr>
            <td style="padding:20px;font-size:14px;line-height:1.6;">
              <p>Hello {context["recipient_name"]},</p>
              <p>A batch requires your approval.</p>
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e6e8ec;">
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Batch ID</td><td style="padding:8px;border:1px solid #e6e8ec;">#{batch.id}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Employee Name</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["employee_name"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Employee Email</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["employee_email"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Date</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["date_range"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Category</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["visit_category_label"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Purpose of Visit</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["purpose"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Purpose of Visit</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["purpose_of_visit"]}</td></tr>
              </table>
              <p style="margin-top:18px;">
                <a href="{direct_approve_url}" style="background:#0b5cab;color:#ffffff;text-decoration:none;padding:10px 16px;display:inline-block;border-radius:4px;margin-right:8px;">Approve</a>
                <a href="{direct_reject_url}" style="background:#b42318;color:#ffffff;text-decoration:none;padding:10px 16px;display:inline-block;border-radius:4px;">Reject</a>
              </p>
              <p style="font-size:12px;color:#667085;">System generated message. Please do not reply.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


def _build_single_visit_approval_email(
    *,
    request,
    plan,
    kam_user,
    manager_user,
    approve_url,
    reject_url,
    cc_users=None,
) -> str:
    """
    Build professional HTML email for single visit approval.

    Important:
      - Uses direct signed approval/rejection URLs.
      - Does not expose raw URLs in the HTML body.
      - Passes complete structured business context.
      - Logs template rendering failures.
      - Falls back to a safe HTML body, not raw plain URLs.
    """
    direct_approve_url = request.build_absolute_uri(
        reverse("kam:direct_single_visit_approve", args=[_make_single_token(plan.id, "APPROVE")])
    )
    direct_reject_url = request.build_absolute_uri(
        reverse("kam:direct_single_visit_reject", args=[_make_single_token(plan.id, "REJECT")])
    )

    cc_users = cc_users or []
    cc_emails = _emails_from_users(cc_users)

    visit_category_label = _VISIT_CATEGORY_LABELS.get(
        getattr(plan, "visit_category", None),
        getattr(plan, "visit_category", "-"),
    )

    if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
        counterparty = _safe_email_value(plan.customer.name)
        location = _safe_email_value(getattr(plan, "location", None) or getattr(plan.customer, "address", None))
    else:
        counterparty = _safe_email_value(getattr(plan, "counterparty_name", None))
        location = _safe_email_value(getattr(plan, "location", None))

    context = {
        "plan": plan,
        "kam_user": kam_user,
        "manager_user": manager_user,
        "recipient_name": _email_display_name(manager_user) if manager_user else "Manager",

        "employee_name": _email_display_name(kam_user),
        "employee_email": _email_address(kam_user),
        "kam_name": _email_display_name(kam_user),

        "visit_category_label": _safe_email_value(visit_category_label),
        "visit_date_display": _display_date_range(
            getattr(plan, "visit_date", None),
            getattr(plan, "visit_date_to", None),
        ),
        "counterparty": counterparty,
        "location": location,
        "purpose": _safe_email_value(getattr(plan, "purpose", None)),

        "approve_url": direct_approve_url,
        "reject_url": direct_reject_url,

        "cc_users": cc_users,
        "cc_emails": cc_emails,
    }

    try:
        return render_to_string("kam/emails/single_visit_approval.html", context)
    except Exception:
        logger.exception(
            "Failed to render single visit approval email template. plan_id=%s",
            getattr(plan, "id", None),
        )

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Visit Approval Required</title>
</head>
<body style="margin:0;padding:0;background:#f6f7f9;font-family:Arial,Helvetica,sans-serif;color:#111;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f6f7f9;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;background:#ffffff;border:1px solid #e6e8ec;">
          <tr>
            <td style="background:#0b1f3a;color:#ffffff;padding:18px 20px;font-size:18px;font-weight:bold;">
              Visit Approval Required
            </td>
          </tr>
          <tr>
            <td style="padding:20px;font-size:14px;line-height:1.6;">
              <p>Hello {context["recipient_name"]},</p>
              <p>A visit requires your approval.</p>
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e6e8ec;">
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Visit ID</td><td style="padding:8px;border:1px solid #e6e8ec;">#{plan.id}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Employee Name</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["employee_name"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Employee Email</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["employee_email"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Date</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["visit_date_display"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Customer</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["counterparty"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Location</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["location"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Category</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["visit_category_label"]}</td></tr>
                <tr><td style="padding:8px;border:1px solid #e6e8ec;background:#f8fafc;font-weight:bold;">Purpose of Visit</td><td style="padding:8px;border:1px solid #e6e8ec;">{context["purpose"]}</td></tr>
              </table>
              <p style="margin-top:18px;">
                <a href="{direct_approve_url}" style="background:#0b5cab;color:#ffffff;text-decoration:none;padding:10px 16px;display:inline-block;border-radius:4px;margin-right:8px;">Approve</a>
                <a href="{direct_reject_url}" style="background:#b42318;color:#ffffff;text-decoration:none;padding:10px 16px;display:inline-block;border-radius:4px;">Reject</a>
              </p>
              <p style="font-size:12px;color:#667085;">System generated message. Please do not reply.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


def _notify_kam_single_visit_decision(*, request, plan, actor, status, rejection_reason="") -> None:
    """
    Notify KAM when single visit is approved/rejected.

    Fixed:
    - Purpose of Visit included in email template context.
    - Purpose of Visit included in plain-text fallback.
    """
    try:
        kam_user = plan.kam

        if not kam_user or not getattr(kam_user, "email", None):
            return

        visit_category_label = _VISIT_CATEGORY_LABELS.get(
            plan.visit_category,
            plan.visit_category,
        )

        counterparty = (
            plan.customer.name
            if plan.customer_id and plan.customer
            else (plan.counterparty_name or "—")
        )

        purpose_of_visit = (plan.purpose or "").strip() or "—"

        plan_url = request.build_absolute_uri(
            reverse("kam:single_visit_detail", args=[plan.id])
        )

        subject = (
            f"[KAM] Single Visit #{plan.id} {status}: "
            f"{plan.visit_date} — {visit_category_label}"
        )

        try:
            body = render_to_string(
                "kam/emails/single_visit_status.html",
                {
                    "plan": plan,
                    "kam_user": kam_user,
                    "actor": actor,
                    "status": status,
                    "visit_category_label": visit_category_label,
                    "counterparty": counterparty,
                    "purpose_of_visit": purpose_of_visit,
                    "rejection_reason": rejection_reason,
                    "plan_url": plan_url,
                },
            )
        except Exception:
            logger.exception(
                "Failed rendering single visit status email. plan_id=%s",
                getattr(plan, "id", None),
            )

            body = (
                f"Single Visit #{plan.id} has been {status}.\n"
                f"Category: {visit_category_label}\n"
                f"Entity: {counterparty}\n"
                f"Date: {plan.visit_date}\n"
                f"Purpose of Visit: {purpose_of_visit}\n"
                f"Decided by: {actor.get_full_name() or actor.username}\n"
            )

            if rejection_reason:
                body += f"\nRejection Reason:\n{rejection_reason}\n"

            body += f"\nView visit: {plan_url}\n"

        _send_safe_mail(subject, body, [kam_user])

    except Exception:
        logger.exception(
            "Failed to notify KAM single visit decision. plan_id=%s",
            getattr(plan, "id", None),
        )


def _notify_kam_batch_decision(*, request, batch, actor, status, rejection_reason="") -> None:
    """
    Notify KAM when batch is approved/rejected.

    Fixed:
    - Purpose of Visit included in email template context.
    - Purpose of Visit included in plain-text fallback.
    """
    try:
        kam_user = batch.kam

        if not kam_user or not getattr(kam_user, "email", None):
            return

        visit_category_label = _VISIT_CATEGORY_LABELS.get(
            batch.visit_category,
            batch.visit_category,
        )

        purpose_of_visit = (batch.purpose or "").strip() or "—"

        batch_url = request.build_absolute_uri(
            reverse("kam:visit_batch_detail", args=[batch.id])
        )

        subject = (
            f"[KAM] Batch #{batch.id} {status}: "
            f"{batch.from_date}..{batch.to_date} — {visit_category_label}"
        )

        try:
            body = render_to_string(
                "kam/emails/visit_batch_status.html",
                {
                    "batch": batch,
                    "kam_user": kam_user,
                    "actor": actor,
                    "status": status,
                    "visit_category_label": visit_category_label,
                    "purpose_of_visit": purpose_of_visit,
                    "rejection_reason": rejection_reason,
                    "batch_url": batch_url,
                },
            )
        except Exception:
            logger.exception(
                "Failed rendering batch status email. batch_id=%s",
                getattr(batch, "id", None),
            )

            body = (
                f"Batch #{batch.id} has been {status}.\n"
                f"Category: {visit_category_label}\n"
                f"Date Range: {batch.from_date} to {batch.to_date}\n"
                f"Purpose of Visit: {purpose_of_visit}\n"
                f"Decided by: {actor.get_full_name() or actor.username}\n"
            )

            if rejection_reason:
                body += f"\nRejection Reason:\n{rejection_reason}\n"

            body += f"\nView batch: {batch_url}\n"

        _send_safe_mail(subject, body, [kam_user])

    except Exception:
        logger.exception(
            "Failed to notify KAM batch decision. batch_id=%s",
            getattr(batch, "id", None),
        )

# =====================================================================
# ADMIN: KAM → Manager Mapping
# =====================================================================

@login_required(login_url="/accounts/login/")
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
# DASHBOARD
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_dashboard")
def dashboard(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    selected_user = _first_query_value(
        request,
        "user",
        "kam",
        "KAM",
        "username",
        "user_name",
        "kam_username",
    )

    sales_target_mt = Decimal(0)
    calls_target = 0
    leads_target_mt = Decimal(0)
    collections_plan_amount = Decimal(0)

    if scope_kam_id:
        start_date_ts = start_dt.date()
        end_date_ts_inc = (end_dt - timezone.timedelta(days=1)).date()
        ts = _target_setting_for_kam_window(
            scope_kam_id,
            start_date_ts,
            end_date_ts_inc,
        )

        if ts:
            sales_target_mt = _safe_decimal(ts.sales_target_mt)
            calls_target = int(ts.calls_target or 0)
            leads_target_mt = _safe_decimal(ts.leads_target_mt)
            collections_plan_amount = _safe_decimal(ts.collections_target_amount)

    visits_target = _calc_visits_target(start_dt, end_dt)
    start_date = start_dt.date()
    end_date = end_dt.date()

    inv_qs = InvoiceFact.objects.filter(
        invoice_date__gte=start_date,
        invoice_date__lt=end_date,
    )
    inv_qs = _filter_qs_by_kam_scope(
        inv_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )
    inv_qs = _sales_converted_qs(inv_qs)

    visit_plan_qs = (
        VisitPlan.objects
        .select_related("kam", "customer", "batch")
        .filter(
            visit_date__gte=start_date,
            visit_date__lt=end_date,
        )
    )
    visit_act_qs = (
        VisitActual.objects
        .select_related("plan", "plan__kam", "plan__customer")
        .filter(
            plan__visit_date__gte=start_date,
            plan__visit_date__lt=end_date,
        )
    )
    call_qs = (
        CallLog.objects
        .select_related("kam", "customer")
        .filter(
            call_datetime__gte=start_dt,
            call_datetime__lt=end_dt,
        )
    )
    lead_qs = (
        LeadFact.objects
        .select_related("kam", "customer")
        .filter(
            doe__gte=start_date,
            doe__lt=end_date,
        )
    )
    coll_qs = (
        CollectionTxn.objects
        .select_related("kam", "customer")
        .filter(
            txn_datetime__gte=start_dt,
            txn_datetime__lt=end_dt,
        )
    )

    visit_plan_qs = _filter_qs_by_kam_scope(
        visit_plan_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )
    visit_act_qs = _filter_qs_by_kam_scope(
        visit_act_qs,
        request.user,
        scope_kam_id,
        "plan__kam_id",
    )
    call_qs = _filter_qs_by_kam_scope(
        call_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )
    lead_qs = _filter_qs_by_kam_scope(
        lead_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )
    coll_qs = _filter_qs_by_kam_scope(
        coll_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )

    sales_mt = _safe_decimal(
        inv_qs.aggregate(mt=Sum("qty_mt")).get("mt")
    )

    visits_planned = visit_plan_qs.count()
    visits_actual = visit_act_qs.count()
    visits_successful = visit_act_qs.filter(successful=True).count()

    calls_total = call_qs.count()
    calls_successful = call_qs.filter(
        outcome__isnull=False,
    ).exclude(
        outcome="",
    ).count()

    won_status_q = _lead_won_q()

    leads_agg = lead_qs.aggregate(
        total_mt=Sum("qty_mt"),
        won_mt=Sum("qty_mt", filter=won_status_q),
    )
    leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
    leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))

    leads_total_count = lead_qs.count()
    leads_converted_count = lead_qs.filter(won_status_q).count()
    leads_converted_value = _safe_decimal(
        lead_qs
        .filter(won_status_q)
        .aggregate(v=Sum("qty_mt"))
        .get("v")
    )

    collections_actual = _safe_decimal(
        coll_qs.aggregate(total_amt=Sum("amount")).get("total_amt")
    )

    if scope_kam_id is not None:
        customer_ids_for_scope = list(
            Customer.objects
            .filter(
                Q(kam_id=scope_kam_id)
                | Q(primary_kam_id=scope_kam_id)
            )
            .values_list("id", flat=True)
        )
    else:
        customer_ids_for_scope = list(
            _customer_qs_for_user(request.user)
            .values_list("id", flat=True)
        )

    cp_qs = (
        CollectionPlan.objects
        .select_related("kam", "customer")
        .filter(overdue_amount__gt=0)
    )
    cp_qs = _filter_qs_by_kam_scope(
        cp_qs,
        request.user,
        scope_kam_id,
        "kam_id",
    )

    cp_agg = cp_qs.aggregate(
        total_overdue=Sum("overdue_amount"),
        total_actual=Sum("actual_amount"),
    )

    collection_total_customers = cp_qs.count()
    collection_overdue = _safe_decimal(cp_agg.get("total_overdue"))
    collection_actual_plan = _safe_decimal(cp_agg.get("total_actual"))
    collection_pending = max(
        collection_overdue - collection_actual_plan,
        Decimal("0"),
    )

    collection_planned = collection_overdue
    collections_planned = collection_overdue

    overdue_snapshot_date = None
    prev_overdue_snapshot_date = None
    credit_limit_sum = Decimal(0)
    exposure_sum = Decimal(0)
    overdue_sum = collection_overdue
    prev_overdue_sum = Decimal(0)

    if customer_ids_for_scope:
        credit_limit_sum = _safe_decimal(
            Customer.objects
            .filter(id__in=customer_ids_for_scope)
            .aggregate(total_cl=Sum("credit_limit"))
            .get("total_cl")
        )

        end_date_inclusive = (end_dt - timezone.timedelta(days=1)).date()
        start_date_inclusive = start_dt.date()

        overdue_snapshot_date = (
            OverdueSnapshot.objects
            .filter(
                customer_id__in=customer_ids_for_scope,
                snapshot_date__lte=end_date_inclusive,
            )
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )

        if overdue_snapshot_date:
            agg = (
                OverdueSnapshot.objects
                .filter(
                    customer_id__in=customer_ids_for_scope,
                    snapshot_date=overdue_snapshot_date,
                )
                .aggregate(
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
                ageing_sum = sum(
                    _safe_decimal(agg.get(k))
                    for k in ("a0", "a1", "a2", "a3")
                )
                if ageing_sum:
                    exposure_sum = ageing_sum

            if not exposure_sum and overdue_sum:
                exposure_sum = overdue_sum

        prev_overdue_snapshot_date = (
            OverdueSnapshot.objects
            .filter(
                customer_id__in=customer_ids_for_scope,
                snapshot_date__lt=start_date_inclusive,
            )
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )

        if prev_overdue_snapshot_date:
            agg2 = (
                OverdueSnapshot.objects
                .filter(
                    customer_id__in=customer_ids_for_scope,
                    snapshot_date=prev_overdue_snapshot_date,
                )
                .aggregate(total_overdue=Sum("overdue"))
            )
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

    prod_by_grade = list(
        inv_qs
        .values("grade")
        .annotate(mt=Sum("qty_mt"))
        .order_by("-mt")
    )
    prod_by_size = list(
        inv_qs
        .values("size")
        .annotate(mt=Sum("qty_mt"))
        .order_by("-mt")
    )

    if sales_mt:
        for row in prod_by_grade:
            row["pct"] = round(
                float(
                    (_safe_decimal(row.get("mt")) / sales_mt)
                    * Decimal("100")
                ),
                1,
            )

        for row in prod_by_size:
            row["pct"] = round(
                float(
                    (_safe_decimal(row.get("mt")) / sales_mt)
                    * Decimal("100")
                ),
                1,
            )

    trend_rows: List[Dict] = []
    anchor_end = _last_completed_ms_week_end(timezone.now())

    for k in (3, 2, 1, 0):
        end_i = anchor_end - timezone.timedelta(days=7 * k)
        start_i = end_i - timezone.timedelta(days=7)
        _a, _b, pid_i = _ms_week_bounds(start_i)

        inv_i = _filter_qs_by_kam_scope(
            InvoiceFact.objects.filter(
                invoice_date__gte=start_i.date(),
                invoice_date__lt=end_i.date(),
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )
        inv_i = _sales_converted_qs(inv_i)

        vis_i = _filter_qs_by_kam_scope(
            VisitPlan.objects.filter(
                visit_date__gte=start_i.date(),
                visit_date__lt=end_i.date(),
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        calls_i = _filter_qs_by_kam_scope(
            CallLog.objects.filter(
                call_datetime__gte=start_i,
                call_datetime__lt=end_i,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        coll_i = _filter_qs_by_kam_scope(
            CollectionTxn.objects.filter(
                txn_datetime__gte=start_i,
                txn_datetime__lt=end_i,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        trend_rows.append({
            "week": pid_i,
            "sales_mt": _safe_decimal(
                inv_i.aggregate(mt=Sum("qty_mt")).get("mt")
            ),
            "visits": vis_i.count(),
            "calls": calls_i.count(),
            "collections": _safe_decimal(
                coll_i.aggregate(a=Sum("amount")).get("a")
            ),
        })

    kpi = {
        "sales_mt": sales_mt,
        "sales_target_mt": sales_target_mt,
        "sales_ach_pct": sales_ach_pct,

        "visits_target": visits_target,
        "visits_planned": visits_planned,
        "visits_actual": visits_actual,
        "visit_ach_pct": visit_ach_pct,
        "visit_success_pct": visit_success_pct,

        "calls": calls_total,
        "calls_successful": calls_successful,
        "calls_target": calls_target,
        "call_ach_pct": call_ach_pct,
        "calls_conversion_pct": calls_conversion_pct,

        "leads_total_mt": leads_total_mt,
        "leads_won_mt": leads_won_mt,
        "leads_target_mt": leads_target_mt,
        "lead_conv_pct": lead_conv_pct,
        "leads_total_count": leads_total_count,
        "leads_converted_count": leads_converted_count,
        "lead_count_conv_pct": lead_count_conv_pct,
        "leads_converted_value": leads_converted_value,

        "collections_actual": collections_actual,
        "collections_planned": collections_planned,
        "collections_eff_pct": coll_eff_pct,

        "collection_planned": collection_planned,
        "collection_actual": collection_actual_plan,
        "collection_pending": collection_pending,
        "collection_total_customers": collection_total_customers,
        "collection_ach_pct": collection_ach_pct,
        "collection_overdue_amt": overdue_sum,

        "overdue_sum": overdue_sum,
        "prev_overdue_sum": prev_overdue_sum,
        "overdue_reduction_pct": overdue_reduction_pct,
        "credit_limit_sum": credit_limit_sum,
        "exposure_sum": exposure_sum,
        "overdue_risk_ratio": overdue_risk_ratio,
        "overdue_snapshot_date": overdue_snapshot_date,
        "prev_overdue_snapshot_date": prev_overdue_snapshot_date,
    }

    selected_report_kam = _dashboard_selected_report_kam(
        request=request,
        scope_kam_id=scope_kam_id,
    )

    performance_report = None

    if selected_report_kam:
        performance_report = build_kam_performance_report(
            kam_id=selected_report_kam.id,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    dashboard_report_charts = _build_dashboard_report_charts(
        report=performance_report,
        kpi=kpi,
        trend_rows=trend_rows,
    )

    ctx = {
        "page_title": "KAM Dashboard",
        "range_label": range_label,
        "can_choose_kam": _is_manager(request.user),
        "scope_label": scope_label,
        "kam_options": _kam_options_for_user(request.user),
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
        "selected_user": selected_user,

        "kpi": kpi,
        "prod_by_grade": prod_by_grade,
        "prod_by_size": prod_by_size,
        "trend_rows": trend_rows,

        "lead_analysis_data": {
            "total": leads_total_count,
            "converted": leads_converted_count,
        },
        "collection_analysis_data": {
            "planned": float(collection_planned),
            "actual": float(collection_actual_plan),
            "overdue": float(collection_overdue),
            "pending": float(collection_pending),
            "customers": collection_total_customers,
        },

        "kam_report": performance_report,
        "selected_report_kam": selected_report_kam,
        "dashboard_report_charts": dashboard_report_charts,
        "report_kam_options": _kam_options_for_performance_report(request.user),
    }

    return render(request, "kam/kam_dashboard.html", ctx)


# =====================================================================
# TODAY'S DETAILS
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def manager_dashboard(request: HttpRequest) -> HttpResponse:
    """
    Today's Details.

    Fixed:
    - Purpose of Visit is available through plan.purpose.
    - Outcome counts only post-meeting-complete visits.
    - Manager dashboard does not treat incomplete actuals as completed outcome.
    """
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    today_start = timezone.localtime(timezone.now()).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    tomorrow = today_start + timezone.timedelta(days=1)

    kam_ids = _kams_managed_by_manager(request.user)

    def _scope(qs, field):
        if _is_admin(request.user):
            return qs
        return qs.filter(**{f"{field}__in": kam_ids})

    today_visit_plans_qs = _scope(
        VisitPlan.objects
        .select_related("customer", "kam", "actual")
        .filter(
            visit_date__gte=today_start.date(),
            visit_date__lt=tomorrow.date(),
        ),
        "kam_id",
    )

    visits_actual_qs = _scope(
        VisitActual.objects
        .select_related("plan__customer", "plan__kam")
        .filter(
            plan__visit_date__gte=today_start.date(),
            plan__visit_date__lt=tomorrow.date(),
        ),
        "plan__kam_id",
    )

    completed_actuals = [
        actual
        for actual in visits_actual_qs[:300]
        if _post_meeting_details_complete(actual)
    ]

    calls_today_qs = _scope(
        CallLog.objects
        .select_related("customer", "kam")
        .filter(call_datetime__gte=today_start, call_datetime__lt=tomorrow),
        "kam_id",
    )

    leads_today_qs = _scope(
        LeadFact.objects.filter(doe=today_start.date()),
        "kam_id",
    )

    collections_today_qs = _scope(
        CollectionTxn.objects
        .select_related("customer", "kam")
        .filter(txn_datetime__gte=today_start, txn_datetime__lt=tomorrow),
        "kam_id",
    )

    collections_today_total = _safe_decimal(
        collections_today_qs.aggregate(a=Sum("amount")).get("a")
    )

    kam_rows = []

    if _is_admin(request.user):
        kams = User.objects.filter(is_active=True).order_by("username")
    else:
        kams = User.objects.filter(is_active=True, id__in=kam_ids).order_by("username")

    for k in kams:
        v_actual_qs = VisitActual.objects.select_related("plan").filter(
            plan__kam=k,
            plan__visit_date__gte=today_start.date(),
            plan__visit_date__lt=tomorrow.date(),
        )

        complete_visit_count = sum(
            1
            for actual in v_actual_qs
            if _post_meeting_details_complete(actual)
        )

        c_count = CallLog.objects.filter(
            kam=k,
            call_datetime__gte=today_start,
            call_datetime__lt=tomorrow,
        ).count()

        l_count = LeadFact.objects.filter(
            kam=k,
            doe=today_start.date(),
        ).count()

        coll_amt = _safe_decimal(
            CollectionTxn.objects.filter(
                kam=k,
                txn_datetime__gte=today_start,
                txn_datetime__lt=tomorrow,
            ).aggregate(a=Sum("amount")).get("a")
        )

        if complete_visit_count or c_count or l_count or coll_amt:
            kam_rows.append(
                {
                    "kam": k,
                    "visits": complete_visit_count,
                    "calls": c_count,
                    "leads": l_count,
                    "collections": coll_amt,
                }
            )

    ctx = {
        "page_title": "Today's Details",
        "today": today_start.date(),

        "today_visits_count": today_visit_plans_qs.count(),
        "today_visits_success": sum(
            1
            for actual in completed_actuals
            if actual.successful is True
        ),

        "today_calls_count": calls_today_qs.count(),
        "today_leads_count": leads_today_qs.count(),
        "today_collections_amount": collections_today_total,
        "today_collections_count": collections_today_qs.count(),

        # Template expects actual rows with v.plan.
        "today_visits": completed_actuals[:50],
        "today_calls": list(calls_today_qs[:50]),
        "today_leads": list(leads_today_qs[:50]),
        "today_collections": list(collections_today_qs[:50]),
        "kam_rows": kam_rows,
    }

    return render(request, "kam/manager_dashboard.html", ctx)


# =====================================================================
# MANAGER KPIs
# =====================================================================
@login_required(login_url="/accounts/login/")
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
        inv_qs = _preferred_inv_qs(inv_qs)
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
# PLAN VISIT — Single + Batch
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_plan")
def weekly_plan(request: HttpRequest) -> HttpResponse:
    """
    Plan Visit page.

    Production-safe update:
    - KAM sees only scoped/assigned customers through _customer_qs_for_user.
    - Purpose of Visit mandatory for Single Visit Save Draft.
    - Purpose of Visit mandatory for Single Visit Submit to Manager.
    - New customer is auto-created only once.
    - New customer is mapped to KAM through Customer.kam, Customer.primary_kam, and KAMAssignment.
    - New customer becomes available in future existing customer dropdown.
    - Batch manual customers also receive KAMAssignment.
    - Existing approval email workflow is preserved.
    - Existing batch workflow is preserved.
    - No duplicate workflows.
    - No unrelated modules touched.
    """
    schema_ready = _visitplan_workflow_schema_ready()
    user = request.user

    try:
        customer_qs = _customer_qs_for_user(user).order_by("name", "code")
    except Exception:
        logger.exception(
            "Plan Visit customer queryset failed for user_id=%s username=%s",
            getattr(user, "id", None),
            getattr(user, "username", None),
        )
        messages.error(
            request,
            "Customer list could not be loaded. Please contact admin; details are logged.",
        )
        customer_qs = Customer.objects.none()

    single_form = SingleVisitForm(
        prefix=SINGLE_PREFIX,
        customer_queryset=customer_qs,
    )

    batch_form = VisitBatchForm(
        prefix=BATCH_PREFIX,
        customer_queryset=customer_qs,
    )

    if "customer" in single_form.fields:
        single_form.fields["customer"].queryset = customer_qs

    if "customers" in batch_form.fields:
        batch_form.fields["customers"].queryset = customer_qs

    if request.method == "POST" and not schema_ready:
        messages.error(
            request,
            "Visit workflow database fields are not migrated yet. "
            "Run: python manage.py makemigrations kam && python manage.py migrate",
        )
        return redirect(reverse("kam:plan"))

    post_mode = (
        request.POST.get("mode")
        or request.POST.get("form_kind")
        or ""
    ).strip().lower()

    # ---------------------------------------------------------------------
    # SINGLE VISIT SUBMIT
    # ---------------------------------------------------------------------
    if request.method == "POST" and post_mode == "single":
        single_form = SingleVisitForm(
            request.POST,
            prefix=SINGLE_PREFIX,
            customer_queryset=customer_qs,
        )

        if "customer" in single_form.fields:
            single_form.fields["customer"].queryset = customer_qs

        manual_customer_name = (request.POST.get("manual_customer") or "").strip()

        raw_category = (
            request.POST.get(f"{SINGLE_PREFIX}-visit_category")
            or request.POST.get("visit_category")
            or ""
        ).strip().upper()

        selected_ids: List[int] = []
        for raw_id in (
            request.POST.getlist("customers_selected[]")
            or request.POST.getlist("customers_selected")
            or request.POST.getlist(f"{SINGLE_PREFIX}-customers")
            or request.POST.getlist("customers")
        ):
            try:
                selected_ids.append(int(raw_id))
            except Exception:
                continue

        legacy_customer_id = (
            request.POST.get(f"{SINGLE_PREFIX}-customer")
            or request.POST.get("customer")
            or ""
        ).strip()

        if legacy_customer_id.isdigit():
            selected_ids.append(int(legacy_customer_id))

        selected_ids = list(dict.fromkeys(selected_ids))

        if "customer" in single_form.fields:
            if manual_customer_name or selected_ids or raw_category != "CUSTOMER":
                single_form.fields["customer"].required = False

        if single_form.is_valid():
            submit_action = (
                request.POST.get("submit_action")
                or request.POST.get("action")
                or "save_draft"
            ).strip().lower()

            if submit_action == "draft":
                submit_action = "save_draft"

            if submit_action == "submit":
                submit_action = "submit_to_manager"

            base_plan: VisitPlan = single_form.save(commit=False)
            base_plan.kam = user
            base_plan.batch = None

            try:
                base_plan.purpose = _require_purpose_of_visit(base_plan.purpose)
            except ValueError as exc:
                messages.error(request, str(exc))
                return redirect(reverse("kam:plan"))

            reporting_officer = None

            try:
                profile = getattr(user, "profile", None)
                if profile and getattr(profile, "reporting_officer_id", None):
                    reporting_officer = profile.reporting_officer
            except Exception:
                logger.exception(
                    "Failed to resolve reporting officer on single visit draft. user_id=%s",
                    getattr(user, "id", None),
                )

            if submit_action not in {"save_draft", "submit_to_manager"}:
                messages.error(request, "Invalid submit action.")
                return redirect(reverse("kam:plan"))

            approval_status = (
                VisitPlan.DRAFT
                if submit_action == "save_draft"
                else VisitPlan.PENDING_APPROVAL
            )

            proceed_flag = submit_action == "submit_to_manager"
            mgr_user = None
            cc_users: List[User] = []

            if proceed_flag:
                mgr_user = _active_manager_for_kam(user)

                if not mgr_user or not getattr(mgr_user, "email", None):
                    messages.error(
                        request,
                        "No reporting officer assigned to your profile. "
                        "Contact admin to set your Reporting Officer in your profile.",
                    )
                    return redirect(reverse("kam:plan"))

                cc_users = _active_cc_for_kam(user)

            created_manual_customer = False

            if base_plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if manual_customer_name:
                    try:
                        manual_customer, created_manual_customer = _get_or_create_manual_customer_for_kam(
                            name=manual_customer_name,
                            kam_user=user,
                        )
                    except Exception as exc:
                        logger.exception(
                            "Failed to create/map manual customer. name=%s user_id=%s",
                            manual_customer_name,
                            getattr(user, "id", None),
                        )
                        messages.error(
                            request,
                            f"Could not create customer '{manual_customer_name}': {exc}",
                        )
                        return redirect(reverse("kam:plan"))

                    selected_ids.append(manual_customer.id)
                    selected_ids = list(dict.fromkeys(selected_ids))

                if not selected_ids:
                    messages.error(
                        request,
                        "Customer is required. Select one or more existing customers or enter a new one.",
                    )
                    return redirect(reverse("kam:plan"))

                refreshed_customer_qs = _customer_qs_for_user(user).order_by("name", "code")
                valid_selected_customers = list(
                    refreshed_customer_qs
                    .filter(id__in=selected_ids)
                    .order_by("name", "code")
                )

                valid_ids = {customer.id for customer in valid_selected_customers}
                invalid_ids = [cid for cid in selected_ids if cid not in valid_ids]

                if invalid_ids:
                    messages.error(
                        request,
                        "One or more selected customers are outside your allowed scope.",
                    )
                    return redirect(reverse("kam:plan"))

                if not valid_selected_customers:
                    messages.error(
                        request,
                        "Customer is required. Select one or more existing customers or enter a new one.",
                    )
                    return redirect(reverse("kam:plan"))

                create_route_batch = len(valid_selected_customers) > 1

                with transaction.atomic():
                    batch = None

                    if create_route_batch:
                        batch = VisitBatch.objects.create(
                            kam=user,
                            from_date=base_plan.visit_date,
                            to_date=base_plan.visit_date,
                            visit_category=VisitBatch.CAT_CUSTOMER,
                            purpose=base_plan.purpose,
                            approval_status=approval_status,
                            submitted_at=timezone.now() if proceed_flag else None,
                        )

                    created_plans: List[VisitPlan] = []

                    for customer in valid_selected_customers:
                        plan = VisitPlan(
                            batch=batch,
                            customer=customer,
                            counterparty_name="",
                            kam=user,
                            visit_date=base_plan.visit_date,
                            visit_date_to=getattr(base_plan, "visit_date_to", None),
                            visit_type=VisitPlan.PLANNED,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            purpose=base_plan.purpose,
                            location=(base_plan.location or customer.address or "").strip() or None,
                            expected_sales_mt=getattr(base_plan, "expected_sales_mt", None),
                            expected_collection=getattr(base_plan, "expected_collection", None),
                            approval_status=approval_status,
                            submitted_at=timezone.now() if proceed_flag else None,
                        )

                        if reporting_officer is not None and hasattr(plan, "reporting_officer"):
                            plan.reporting_officer = reporting_officer

                        plan.save()
                        created_plans.append(plan)

                        VisitApprovalAudit.objects.create(
                            plan=plan,
                            batch=batch,
                            actor=user,
                            action=VisitApprovalAudit.ACTION_SUBMIT,
                            note=(
                                "Submitted to manager for approval"
                                if proceed_flag
                                else "Saved as draft"
                            ),
                            actor_ip=_get_ip(request),
                        )

                    if batch:
                        VisitApprovalAudit.objects.create(
                            batch=batch,
                            actor=user,
                            action=VisitApprovalAudit.ACTION_SUBMIT,
                            note=(
                                "Multi-customer route submitted to manager"
                                if proceed_flag
                                else "Multi-customer route saved as draft"
                            ),
                            actor_ip=_get_ip(request),
                        )

                    if proceed_flag and batch:
                        batch_id = batch.id
                        selected_customer_ids_for_email = [customer.id for customer in valid_selected_customers]

                        def _send_route_batch_approval_after_commit():
                            try:
                                fresh_batch = VisitBatch.objects.select_related("kam").get(id=batch_id)
                                email_customers = list(
                                    Customer.objects
                                    .filter(id__in=selected_customer_ids_for_email)
                                    .order_by("name", "code")
                                )

                                approve_url = request.build_absolute_uri(
                                    reverse(
                                        "kam:direct_batch_approve",
                                        args=[_make_batch_token(fresh_batch.id, "APPROVE")],
                                    )
                                )

                                reject_url = request.build_absolute_uri(
                                    reverse(
                                        "kam:direct_batch_reject",
                                        args=[_make_batch_token(fresh_batch.id, "REJECT")],
                                    )
                                )

                                html_body = _build_batch_approval_email(
                                    request=request,
                                    batch=fresh_batch,
                                    kam_user=user,
                                    visit_category_label=_VISIT_CATEGORY_LABELS.get(
                                        fresh_batch.visit_category,
                                        fresh_batch.visit_category,
                                    ),
                                    remarks=base_plan.purpose,
                                    approve_url=approve_url,
                                    reject_url=reject_url,
                                    customers=email_customers,
                                    counterparty_names=[],
                                    manager_user=mgr_user,
                                    cc_users=cc_users,
                                )

                                subject = (
                                    f"[KAM] Approval Required: Visit Route #{fresh_batch.id} "
                                    f"({fresh_batch.from_date}) - {user.get_full_name() or user.username}"
                                )

                                sent_ok = _send_safe_mail(subject, html_body, [mgr_user], cc_users)

                                if not sent_ok:
                                    logger.warning(
                                        "Approval email could not be sent for multi-customer route batch #%s",
                                        fresh_batch.id,
                                    )
                            except Exception:
                                logger.exception(
                                    "Multi-customer route approval email failed after commit. batch_id=%s",
                                    batch_id,
                                )

                        transaction.on_commit(_send_route_batch_approval_after_commit)

                    elif proceed_flag:
                        plan_id = created_plans[0].id

                        def _send_single_approval_after_commit():
                            try:
                                fresh_plan = (
                                    VisitPlan.objects
                                    .select_related("customer", "kam")
                                    .get(id=plan_id)
                                )

                                approve_token = _make_single_token(fresh_plan.id, "APPROVE")
                                reject_token = _make_single_token(fresh_plan.id, "REJECT")

                                approve_url = request.build_absolute_uri(
                                    reverse("kam:single_visit_approve_link", args=[approve_token])
                                )
                                reject_url = request.build_absolute_uri(
                                    reverse("kam:single_visit_reject_link", args=[reject_token])
                                )

                                subject = (
                                    f"[KAM] Approval Required: Single Visit #{fresh_plan.id} "
                                    f"({fresh_plan.visit_date}) - {user.get_full_name() or user.username}"
                                )

                                html_body = _build_single_visit_approval_email(
                                    request=request,
                                    plan=fresh_plan,
                                    kam_user=user,
                                    manager_user=mgr_user,
                                    approve_url=approve_url,
                                    reject_url=reject_url,
                                    cc_users=cc_users,
                                )

                                sent_ok = _send_safe_mail(subject, html_body, [mgr_user], cc_users)

                                if not sent_ok:
                                    logger.warning(
                                        "Approval email could not be sent for single visit #%s",
                                        fresh_plan.id,
                                    )
                            except Exception:
                                logger.exception(
                                    "Single visit approval email failed after commit. plan_id=%s",
                                    plan_id,
                                )

                        transaction.on_commit(_send_single_approval_after_commit)

                if created_manual_customer:
                    messages.info(
                        request,
                        f"New customer '{manual_customer_name}' created automatically.",
                    )

                if proceed_flag:
                    messages.success(
                        request,
                        f"Visit submitted for manager approval with {len(valid_selected_customers)} customer(s).",
                    )
                else:
                    messages.success(
                        request,
                        f"Visit saved as Draft with {len(valid_selected_customers)} customer(s).",
                    )

                return redirect(reverse("kam:plan"))

            base_plan.customer = None
            base_plan.counterparty_name = (base_plan.counterparty_name or "").strip() or None
            base_plan.location = (base_plan.location or "").strip() or None
            base_plan.approval_status = approval_status
            base_plan.submitted_at = timezone.now() if proceed_flag else None

            if reporting_officer is not None and hasattr(base_plan, "reporting_officer"):
                base_plan.reporting_officer = reporting_officer

            with transaction.atomic():
                base_plan.save()

                VisitApprovalAudit.objects.create(
                    plan=base_plan,
                    actor=user,
                    action=VisitApprovalAudit.ACTION_SUBMIT,
                    note=(
                        "Submitted to manager for approval"
                        if proceed_flag
                        else "Saved as draft"
                    ),
                    actor_ip=_get_ip(request),
                )

                if proceed_flag:
                    plan_id = base_plan.id

                    def _send_non_customer_single_approval_after_commit():
                        try:
                            fresh_plan = VisitPlan.objects.select_related("customer", "kam").get(id=plan_id)
                            approve_token = _make_single_token(fresh_plan.id, "APPROVE")
                            reject_token = _make_single_token(fresh_plan.id, "REJECT")
                            approve_url = request.build_absolute_uri(
                                reverse("kam:single_visit_approve_link", args=[approve_token])
                            )
                            reject_url = request.build_absolute_uri(
                                reverse("kam:single_visit_reject_link", args=[reject_token])
                            )
                            subject = (
                                f"[KAM] Approval Required: Single Visit #{fresh_plan.id} "
                                f"({fresh_plan.visit_date}) - {user.get_full_name() or user.username}"
                            )
                            html_body = _build_single_visit_approval_email(
                                request=request,
                                plan=fresh_plan,
                                kam_user=user,
                                manager_user=mgr_user,
                                approve_url=approve_url,
                                reject_url=reject_url,
                                cc_users=cc_users,
                            )
                            sent_ok = _send_safe_mail(subject, html_body, [mgr_user], cc_users)
                            if not sent_ok:
                                logger.warning("Approval email could not be sent for single visit #%s", fresh_plan.id)
                        except Exception:
                            logger.exception("Single non-customer approval email failed after commit. plan_id=%s", plan_id)

                    transaction.on_commit(_send_non_customer_single_approval_after_commit)

            messages.success(
                request,
                f"Single visit #{base_plan.id} {'submitted for manager approval' if proceed_flag else 'saved as Draft'}.",
            )
            return redirect(reverse("kam:plan"))

        messages.error(request, "Single visit has errors. Please correct and save again.")

    # ---------------------------------------------------------------------
    # BATCH VISIT SUBMIT
    # ---------------------------------------------------------------------
    if request.method == "POST" and post_mode == "batch":
        batch_form = VisitBatchForm(
            request.POST,
            prefix=BATCH_PREFIX,
            customer_queryset=customer_qs,
        )

        if "customers" in batch_form.fields:
            batch_form.fields["customers"].queryset = customer_qs

        action = (
            request.POST.get("action")
            or request.POST.get("submit_action")
            or ""
        ).strip().lower()

        proceed_flag = action in {
            "submit",
            "submit_to_manager",
            "proceed",
            "proceed_to_manager",
            "proceed-manager",
            "manager",
            "proceed_to_manager_btn",
        }

        if not batch_form.is_valid():
            messages.error(
                request,
                "Batch submission has errors. Please correct and re-submit.",
            )

        else:
            visit_category = batch_form.cleaned_data.get("visit_category")
            from_date = batch_form.cleaned_data.get("from_date")
            to_date = batch_form.cleaned_data.get("to_date")

            try:
                remarks = _require_purpose_of_visit(
                    batch_form.cleaned_data.get("purpose"),
                    max_length=1000,
                )
            except ValueError as exc:
                messages.error(request, str(exc))
                return redirect(reverse("kam:plan"))

            selected_ids: List[int] = []

            if request.POST.getlist("customers_selected[]"):
                for raw_id in request.POST.getlist("customers_selected[]"):
                    try:
                        selected_ids.append(int(raw_id))
                    except Exception:
                        continue
            else:
                selected_customers = batch_form.cleaned_data.get("customers") or []
                selected_ids = [customer.id for customer in selected_customers]

            manual_names = [
                name.strip()
                for name in request.POST.getlist("batch_manual_customer[]")
                if (name or "").strip()
            ]

            for manual_name in manual_names:
                try:
                    with transaction.atomic():
                        manual_customer = (
                            Customer.objects
                            .select_for_update()
                            .filter(name__iexact=manual_name)
                            .first()
                        )

                        created = False

                        if not manual_customer:
                            manual_customer = Customer.objects.create(
                                name=manual_name,
                                kam=user,
                                primary_kam=user,
                                source=Customer.SOURCE_MANUAL,
                                created_by=user,
                            )
                            created = True

                        changed_fields = []

                        if hasattr(manual_customer, "kam_id") and not manual_customer.kam_id:
                            manual_customer.kam = user
                            changed_fields.append("kam")

                        if hasattr(manual_customer, "primary_kam_id") and not manual_customer.primary_kam_id:
                            manual_customer.primary_kam = user
                            changed_fields.append("primary_kam")

                        if hasattr(manual_customer, "source") and not getattr(manual_customer, "source", None):
                            manual_customer.source = Customer.SOURCE_MANUAL
                            changed_fields.append("source")

                        if hasattr(manual_customer, "created_by_id") and not getattr(manual_customer, "created_by_id", None):
                            manual_customer.created_by = user
                            changed_fields.append("created_by")

                        if changed_fields:
                            if hasattr(manual_customer, "updated_at"):
                                changed_fields.append("updated_at")
                            manual_customer.save(update_fields=changed_fields)

                        KAMAssignment.objects.get_or_create(
                            customer=manual_customer,
                            kam=user,
                            defaults={
                                "active_from": timezone.localdate(),
                            },
                        )

                    if manual_customer.id not in selected_ids:
                        selected_ids.append(manual_customer.id)

                    if created:
                        logger.info(
                            "Batch manual customer created and mapped: id=%s name=%r by user=%s",
                            manual_customer.id,
                            manual_name,
                            user.username,
                        )

                except Exception as exc:
                    logger.exception(
                        "Failed to create/map batch manual customer: %r",
                        manual_name,
                    )
                    messages.error(
                        request,
                        f"Could not create customer '{manual_name}': {exc}",
                    )
                    return redirect(reverse("kam:plan"))

            valid_selected_customers = []

            if visit_category == VisitBatch.CAT_CUSTOMER:
                if not selected_ids:
                    messages.error(
                        request,
                        "Select at least one customer or add a manual customer.",
                    )
                    return redirect(reverse("kam:plan"))

                refreshed_customer_qs = _customer_qs_for_user(user).order_by("name", "code")

                valid_selected_customers = list(
                    refreshed_customer_qs
                    .filter(id__in=selected_ids)
                    .order_by("name", "code")
                )

                valid_ids = {customer.id for customer in valid_selected_customers}
                invalid_ids = [cid for cid in selected_ids if cid not in valid_ids]

                if invalid_ids:
                    messages.error(
                        request,
                        "One or more selected customers are outside your allowed scope.",
                    )
                    return redirect(reverse("kam:plan"))

                for customer in valid_selected_customers:
                    try:
                        _require_purpose_of_visit(
                            request.POST.get(f"purpose_{customer.id}"),
                            max_length=500,
                        )
                    except ValueError:
                        messages.error(
                            request,
                            f"Purpose of Visit is required for customer: {customer.name}",
                        )
                        return redirect(reverse("kam:plan"))

            non_customer_lines = []

            if visit_category != VisitBatch.CAT_CUSTOMER:
                names = request.POST.getlist("counterparty_name[]")
                locations = request.POST.getlist("counterparty_location[]")
                purposes = request.POST.getlist("counterparty_purpose[]")

                max_len = max(len(names), len(locations), len(purposes), 0)

                for idx in range(max_len):
                    data = {
                        "counterparty_name": names[idx] if idx < len(names) else "",
                        "counterparty_location": locations[idx] if idx < len(locations) else "",
                        "counterparty_purpose": purposes[idx] if idx < len(purposes) else "",
                    }

                    line_form = MultiVisitPlanLineForm(data)

                    if line_form.is_valid():
                        try:
                            _require_purpose_of_visit(
                                line_form.cleaned_data.get("counterparty_purpose"),
                                max_length=500,
                            )
                        except ValueError as exc:
                            messages.error(request, str(exc))
                            return redirect(reverse("kam:plan"))

                        non_customer_lines.append(line_form)
                    else:
                        messages.error(
                            request,
                            "One or more non-customer batch lines are invalid.",
                        )
                        return redirect(reverse("kam:plan"))

                if not non_customer_lines:
                    messages.error(
                        request,
                        "Add at least one line to save a non-customer batch visit.",
                    )
                    return redirect(reverse("kam:plan"))

            approval_status = (
                VisitBatch.PENDING_APPROVAL
                if proceed_flag
                else VisitBatch.DRAFT
            )

            mgr_user = None
            cc_users: List[User] = []

            if proceed_flag:
                mgr_user = _active_manager_for_kam(user)

                if not mgr_user:
                    logger.warning(
                        "KAM batch submit blocked: reporting manager not resolved. employee_id=%s employee_email=%s",
                        getattr(user, "id", None),
                        getattr(user, "email", None),
                    )
                    messages.error(
                        request,
                        "No reporting officer is assigned to your profile. "
                        "Please contact admin to set your Reporting Officer.",
                    )
                    return redirect(reverse("kam:plan"))

                manager_email = (getattr(mgr_user, "email", "") or "").strip()

                if not manager_email:
                    logger.warning(
                        "KAM batch submit blocked: reporting manager has blank email. employee_id=%s manager_id=%s manager_username=%s",
                        getattr(user, "id", None),
                        getattr(mgr_user, "id", None),
                        getattr(mgr_user, "username", None),
                    )
                    messages.error(
                        request,
                        "Your reporting officer does not have an email address. "
                        "Please contact admin to update the manager email.",
                    )
                    return redirect(reverse("kam:plan"))

                cc_users = _active_cc_for_kam(user)

                logger.info(
                    "KAM batch submit manager resolved. employee_id=%s employee_email=%s manager_id=%s manager_email=%s cc_emails=%s",
                    getattr(user, "id", None),
                    getattr(user, "email", None),
                    getattr(mgr_user, "id", None),
                    manager_email,
                    [(getattr(cc_user, "email", "") or "").strip() for cc_user in cc_users],
                )

            with transaction.atomic():
                batch = VisitBatch.objects.create(
                    kam=user,
                    from_date=from_date,
                    to_date=to_date,
                    visit_category=visit_category,
                    purpose=remarks,
                    approval_status=approval_status,
                    submitted_at=timezone.now() if proceed_flag else None,
                )

                created_lines = 0

                if visit_category == VisitBatch.CAT_CUSTOMER:
                    for customer in valid_selected_customers:
                        purpose = _require_purpose_of_visit(
                            request.POST.get(f"purpose_{customer.id}"),
                            max_length=500,
                        )

                        expected_sales_mt_raw = (
                            request.POST.get(f"expected_sales_mt_{customer.id}") or ""
                        )

                        expected_collection_raw = (
                            request.POST.get(f"expected_collection_{customer.id}") or ""
                        )

                        expected_sales = _parse_decimal_or_none(expected_sales_mt_raw)
                        expected_collection = _parse_decimal_or_none(expected_collection_raw)

                        if expected_sales_mt_raw.strip() != "" and expected_sales is None:
                            messages.error(
                                request,
                                f"Expected Sales (MT) is invalid for customer: {customer.name}",
                            )
                            transaction.set_rollback(True)
                            return redirect(reverse("kam:plan"))

                        if expected_collection_raw.strip() != "" and expected_collection is None:
                            messages.error(
                                request,
                                f"Expected Collection (₹) is invalid for customer: {customer.name}",
                            )
                            transaction.set_rollback(True)
                            return redirect(reverse("kam:plan"))

                        location = (customer.address or "").strip()

                        VisitPlan.objects.create(
                            batch=batch,
                            customer=customer,
                            kam=user,
                            visit_date=from_date,
                            visit_date_to=to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=VisitPlan.CAT_CUSTOMER,
                            purpose=purpose,
                            expected_sales_mt=expected_sales,
                            expected_collection=expected_collection,
                            location=location,
                            approval_status=approval_status,
                            submitted_at=timezone.now() if proceed_flag else None,
                        )

                        created_lines += 1

                else:
                    for line_form in non_customer_lines:
                        line_purpose = _require_purpose_of_visit(
                            line_form.cleaned_data.get("counterparty_purpose"),
                            max_length=500,
                        )

                        VisitPlan.objects.create(
                            batch=batch,
                            customer=None,
                            counterparty_name=line_form.cleaned_data["counterparty_name"],
                            kam=user,
                            visit_date=from_date,
                            visit_date_to=to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=visit_category,
                            purpose=line_purpose,
                            location=(
                                line_form.cleaned_data.get("counterparty_location")
                                or ""
                            ).strip(),
                            approval_status=approval_status,
                            submitted_at=timezone.now() if proceed_flag else None,
                        )

                        created_lines += 1

                VisitApprovalAudit.objects.create(
                    batch=batch,
                    actor=user,
                    action=VisitApprovalAudit.ACTION_SUBMIT,
                    note=(
                        "Batch submitted to manager"
                        if proceed_flag
                        else "Batch saved as draft"
                    ),
                    actor_ip=_get_ip(request),
                )

                if proceed_flag:
                    batch_id = batch.id
                    selected_customer_ids_for_email = [
                        customer.id
                        for customer in valid_selected_customers
                    ]
                    counterparty_names_for_email = [
                        line_form.cleaned_data["counterparty_name"]
                        for line_form in non_customer_lines
                    ]

                    def _send_batch_approval_after_commit():
                        try:
                            fresh_batch = (
                                VisitBatch.objects
                                .select_related("kam")
                                .get(id=batch_id)
                            )

                            email_customers = list(
                                Customer.objects
                                .filter(id__in=selected_customer_ids_for_email)
                                .order_by("name", "code")
                            )

                            approve_url = request.build_absolute_uri(
                                reverse(
                                    "kam:direct_batch_approve",
                                    args=[_make_batch_token(fresh_batch.id, "APPROVE")],
                                )
                            )

                            reject_url = request.build_absolute_uri(
                                reverse(
                                    "kam:direct_batch_reject",
                                    args=[_make_batch_token(fresh_batch.id, "REJECT")],
                                )
                            )

                            visit_category_label = _VISIT_CATEGORY_LABELS.get(
                                fresh_batch.visit_category,
                                fresh_batch.visit_category,
                            )

                            subject = (
                                f"[KAM] Approval Required: Batch #{fresh_batch.id} "
                                f"({fresh_batch.from_date}..{fresh_batch.to_date}) - "
                                f"{user.get_full_name() or user.username}"
                            )

                            html_body = _build_batch_approval_email(
                                request=request,
                                batch=fresh_batch,
                                kam_user=user,
                                visit_category_label=visit_category_label,
                                remarks=remarks,
                                approve_url=approve_url,
                                reject_url=reject_url,
                                customers=email_customers,
                                counterparty_names=counterparty_names_for_email,
                                manager_user=mgr_user,
                                cc_users=cc_users,
                            )

                            sent_ok = _send_safe_mail(
                                subject,
                                html_body,
                                [mgr_user],
                                cc_users,
                            )

                            if sent_ok:
                                logger.info(
                                    "KAM batch approval email triggered successfully. batch_id=%s manager_id=%s manager_email=%s",
                                    fresh_batch.id,
                                    getattr(mgr_user, "id", None),
                                    getattr(mgr_user, "email", None),
                                )
                            else:
                                logger.warning(
                                    "KAM batch approval email returned False. batch_id=%s manager_id=%s manager_email=%s",
                                    fresh_batch.id,
                                    getattr(mgr_user, "id", None),
                                    getattr(mgr_user, "email", None),
                                )

                        except Exception:
                            logger.exception(
                                "KAM batch approval email trigger failed after DB commit. batch_id=%s",
                                batch_id,
                            )

                    transaction.on_commit(_send_batch_approval_after_commit)

            if proceed_flag:
                messages.success(
                    request,
                    f"Batch submitted to Manager: {created_lines} lines (Batch #{batch.id}). "
                    "Approval email has been triggered.",
                )
            else:
                messages.success(
                    request,
                    f"Batch saved as Draft: {created_lines} lines (Batch #{batch.id}).",
                )

            return redirect(reverse("kam:plan"))

    # ---------------------------------------------------------------------
    # GET PAGE RENDER
    # ---------------------------------------------------------------------
    if schema_ready:
        today_local = timezone.localtime(timezone.now()).date()
        plan_window_start = today_local - timezone.timedelta(days=7)
        plan_window_end = today_local + timezone.timedelta(days=7)

        # Recent Visit Plans must be rendered from VisitPlan rows only.
        # One VisitPlan row = one visit = one customer/entity + one stored purpose.
        my_plans = _attach_visit_customer_display(list(
            _visitplan_qs_for_user(user)
            .filter(
                visit_date__gte=plan_window_start,
                visit_date__lte=plan_window_end,
            )
            .select_related("customer", "kam", "batch", "actual")
            .order_by("-visit_date", "-created_at", "-id")
            .distinct()[:25]
        ))

        for plan in my_plans:
            if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
                recent_customer_name = (getattr(plan.customer, "name", "") or "").strip()
            else:
                recent_customer_name = (getattr(plan, "counterparty_name", "") or "").strip()

            recent_purpose = (getattr(plan, "purpose", "") or "").strip()

            try:
                recent_status = plan.get_approval_status_display()
            except Exception:
                recent_status = (getattr(plan, "approval_status", "") or "").strip()

            kam_obj = getattr(plan, "kam", None)
            if kam_obj:
                recent_kam_name = (kam_obj.get_full_name() or getattr(kam_obj, "username", "") or "").strip()
            else:
                recent_kam_name = ""

            if not getattr(plan, "recent_customer_name", None):
                plan.recent_customer_name = recent_customer_name or "-"
            plan.recent_purpose = recent_purpose or "-"
            plan.recent_status = recent_status or "-"
            plan.recent_kam_name = recent_kam_name or "-"
    else:
        my_plans = []

    ctx = {
        "page_title": "Plan Visit",
        "form": single_form,
        "single_form": single_form,
        "batch_form": batch_form,
        "plans": my_plans,
        "recent_plans": my_plans,
        "recent_visits": my_plans,
        "customers": list(customer_qs),
        "SINGLE_PREFIX": SINGLE_PREFIX,
        "BATCH_PREFIX": BATCH_PREFIX,
        "visitplan_schema_ready": schema_ready,
        "status_constants": {
            "DRAFT": STATUS_DRAFT,
            "PENDING_APPROVAL": STATUS_PENDING_APPROVAL,
            "APPROVED": STATUS_APPROVED,
            "REJECTED": STATUS_REJECTED,
            "COMPLETED": STATUS_COMPLETED,
        },
    }

    return render(request, "kam/plan_visit.html", ctx)


@login_required(login_url="/accounts/login/")
def single_visit_reject_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        messages.error(request, "Reject link has expired (7-day limit).")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid reject link.")
        return redirect(reverse("kam:visit_batches"))
    if action != "REJECT":
        messages.error(request, "This link is not a rejection link.")
        return redirect(reverse("kam:visit_batches"))

    plan = get_object_or_404(VisitPlan, id=plan_id, batch__isnull=True)

    if request.method == "GET":
        if plan.approval_status == VisitPlan.REJECTED:
            messages.info(request, f"Single Visit #{plan.id} is already rejected.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        return render(request, "kam/single_visit_reject_reason.html", {
            "plan": plan, "token": token,
            "page_title": f"Reject Single Visit #{plan_id}",
            "visit_category_label": _VISIT_CATEGORY_LABELS.get(plan.visit_category, plan.visit_category),
        })

    reason = (request.POST.get("reason") or "").strip()
    if not reason:
        messages.error(request, "Rejection reason is required.")
        return render(request, "kam/single_visit_reject_reason.html", {
            "plan": plan, "token": token,
            "page_title": f"Reject Single Visit #{plan_id}",
            "visit_category_label": _VISIT_CATEGORY_LABELS.get(plan.visit_category, plan.visit_category),
            "error": "Rejection reason is required.",
        })

    with transaction.atomic():
        plan = get_object_or_404(VisitPlan.objects.select_for_update(), id=plan_id, batch__isnull=True)
        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
        if plan.approval_status == VisitPlan.REJECTED:
            messages.info(request, f"Single Visit #{plan.id} is already rejected.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        if plan.approval_status != VisitPlan.PENDING_APPROVAL:
            messages.error(request, f"Single Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        now_ts = timezone.now()
        plan.approval_status = VisitPlan.REJECTED
        plan.rejected_by = request.user
        plan.rejected_at = now_ts
        plan.rejection_reason = reason
        plan.save(update_fields=["approval_status", "rejected_by", "rejected_at", "rejection_reason", "updated_at"])
        VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note=reason[:255], actor_ip=_get_ip(request))

    _notify_kam_single_visit_decision(request=request, plan=plan, actor=request.user, status="REJECTED", rejection_reason=reason)
    messages.info(request, f"Single Visit #{plan_id} rejected. KAM has been notified.")
    return redirect(reverse("kam:single_visit_detail", args=[plan_id]))


# =====================================================================
# SINGLE VISIT LIST
# =====================================================================
@login_required(login_url="/accounts/login/")
def single_visit_list(request: HttpRequest) -> HttpResponse:
    """
    Single Visit list.

    Fixed:
    - Purpose of Visit visible in template context.
    - KAM filter supported.
    - Date range filter supported.
    - Time filter supported.
    - Filters combine together.
    """
    if not _visitplan_workflow_schema_ready():
        messages.error(
            request,
            "Single Visit workflow DB fields are not migrated yet. Run migrations before using this page.",
        )
        return render(
            request,
            "kam/single_visit_list.html",
            {
                "page_title": "Single Visits",
                "rows": [],
                "status_filter": "",
                "selected_kam": "",
                "filter_from": "",
                "filter_to": "",
                "time_filter": "",
                "can_approve": _is_manager(request.user),
                "status_choices": VisitPlan.APPROVAL_STATUS_CHOICES,
                "kam_dropdown_options": [],
            },
        )

    qs = (
        _single_visit_qs_for_user(request.user)
        .select_related("customer", "kam", "actual")
        .order_by("-created_at")
    )

    status_filter = (request.GET.get("status") or "").strip().upper()

    if status_filter:
        qs = qs.filter(approval_status=status_filter)

    qs = _apply_visit_history_filters(
        qs,
        request,
        kam_field="kam_id",
        date_field="visit_date",
    )

    kam_dropdown_options = []

    if _is_manager(request.user):
        try:
            kam_ids = _kams_managed_by_manager(request.user)
            if _is_admin(request.user):
                kam_dropdown_options = list(
                    User.objects.filter(is_active=True).order_by("first_name", "last_name", "username")
                )
            else:
                kam_dropdown_options = list(
                    User.objects.filter(is_active=True, id__in=kam_ids).order_by("first_name", "last_name", "username")
                )
        except Exception:
            logger.exception("Failed to build single visit KAM dropdown.")
            kam_dropdown_options = []

    ctx = {
        "page_title": "Single Visits",
        "rows": list(qs[:300]),
        "status_filter": status_filter,
        "selected_kam": request.GET.get("kam", "") or request.GET.get("user", ""),
        "filter_from": request.GET.get("from_date", "") or request.GET.get("from", ""),
        "filter_to": request.GET.get("to_date", "") or request.GET.get("to", ""),
        "time_filter": request.GET.get("time_filter", ""),
        "can_approve": _is_manager(request.user),
        "status_choices": VisitPlan.APPROVAL_STATUS_CHOICES,
        "kam_dropdown_options": kam_dropdown_options,
    }

    return render(request, "kam/single_visit_list.html", ctx)


# =====================================================================
# SINGLE VISIT APPROVE / REJECT (form POST)
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def single_visit_approve(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    with transaction.atomic():
        plan = get_object_or_404(VisitPlan.objects.select_for_update(), id=plan_id, batch__isnull=True)
        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
        if plan.approval_status == VisitPlan.APPROVED:
            messages.info(request, f"Visit #{plan.id} already approved.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        if plan.approval_status != VisitPlan.PENDING_APPROVAL:
            messages.error(request, f"Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        now_ts = timezone.now()
        plan.approval_status = VisitPlan.APPROVED
        plan.approved_by = request.user
        plan.approved_at = now_ts
        plan.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])
        VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved from detail page", actor_ip=_get_ip(request))

    _notify_kam_single_visit_decision(request=request, plan=plan, actor=request.user, status="APPROVED")
    messages.success(request, f"Single Visit #{plan.id} approved.")
    return redirect(reverse("kam:single_visit_detail", args=[plan.id]))


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def single_visit_reject(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    reason = (request.POST.get("reason") or "").strip() or "Rejected by manager"

    with transaction.atomic():
        plan = get_object_or_404(VisitPlan.objects.select_for_update(), id=plan_id, batch__isnull=True)
        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
        if plan.approval_status == VisitPlan.REJECTED:
            messages.info(request, f"Visit #{plan.id} already rejected.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        if plan.approval_status != VisitPlan.PENDING_APPROVAL:
            messages.error(request, f"Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        now_ts = timezone.now()
        plan.approval_status = VisitPlan.REJECTED
        plan.rejected_by = request.user
        plan.rejected_at = now_ts
        plan.rejection_reason = reason
        plan.save(update_fields=["approval_status", "rejected_by", "rejected_at", "rejection_reason", "updated_at"])
        VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note=reason[:255], actor_ip=_get_ip(request))

    _notify_kam_single_visit_decision(request=request, plan=plan, actor=request.user, status="REJECTED", rejection_reason=reason)
    messages.info(request, f"Single Visit #{plan.id} rejected.")
    return redirect(reverse("kam:single_visit_detail", args=[plan.id]))


# =====================================================================
# Customer APIs + CRUD
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_plan")
def customers_api(request: HttpRequest) -> JsonResponse:
    """
    Role-scoped customer API for Plan Visit dropdown / Select2.

    Returns both:
      - customers: existing frontend format
      - results: Select2-compatible format
    """
    user = request.user

    try:
        qs = _customer_qs_for_user(user).order_by("name", "code")
    except Exception:
        logger.exception(
            "customers_api queryset failed for user_id=%s username=%s",
            getattr(user, "id", None),
            getattr(user, "username", None),
        )
        return JsonResponse(
            {"ok": False, "count": 0, "customers": [], "results": []},
            status=500,
        )

    if _is_manager(user):
        kam_u = (request.GET.get("kam") or "").strip()
        if kam_u:
            u = User.objects.filter(username=kam_u, is_active=True).first()
            allowed_kam_ids = set(_kams_managed_by_manager(user))
            if u and (_is_admin(user) or u.id in allowed_kam_ids):
                qs = qs.filter(Q(kam=u) | Q(primary_kam=u))
            else:
                qs = qs.none()

    q = (request.GET.get("q") or "").strip()
    if q:
        qs = qs.filter(
            Q(name__icontains=q)
            | Q(code__icontains=q)
            | Q(mobile__icontains=q)
            | Q(address__icontains=q)
        )

    source = (request.GET.get("source") or "").strip().upper()
    if source:
        qs = qs.filter(source=source)

    customers = []
    results = []

    for c in qs[:500]:
        code = (getattr(c, "code", "") or "").strip()
        mobile = (getattr(c, "mobile", "") or "").strip()
        address = (getattr(c, "address", "") or "").strip()
        source_value = (getattr(c, "source", "") or "").upper()

        city_or_address = address.split(",")[0].strip() if address else ""

        label_parts = [(c.name or "").strip()]
        if code:
            label_parts.append(code)
        if city_or_address:
            label_parts.append(city_or_address)

        label = " — ".join([p for p in label_parts if p])

        row = {
            "id": c.id,
            "name": c.name,
            "code": code,
            "mobile": mobile,
            "address": address,
            "source": source_value,
            "text": label,
        }

        customers.append(row)
        results.append({
            "id": c.id,
            "text": label,
        })

    return JsonResponse({
        "ok": True,
        "count": len(customers),
        "customers": customers,
        "results": results,
    })


@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
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
# Customer Search API  (NEW — 2026-04-14)
# Powers AJAX typeahead in Collection Plan
# =====================================================================
@login_required(login_url="/accounts/login/")
def customer_search_api(request: HttpRequest) -> JsonResponse:
    """
    AJAX customer search with role-based scoping.
    GET ?q=<search_term>
    Returns: { "results": [{ "id", "name", "code", "mobile" }, ...] }
    """
    query = (request.GET.get("q") or "").strip()

    # Role-scoped base queryset — reuses existing helper, never bypasses roles
    qs = _customer_qs_for_user(request.user).order_by("name")

    if query:
        qs = qs.filter(
            Q(name__icontains=query) |
            Q(mobile__icontains=query) |
            Q(code__icontains=query)
        )

    data = list(
        qs.values("id", "name", "code", "mobile")[:20]
    )

    # Normalise None → empty string so JS JSON is clean
    for row in data:
        row["code"]   = row.get("code")   or ""
        row["mobile"] = row.get("mobile") or ""

    return JsonResponse({"results": data})


# =====================================================================
# Customer 360 API  (NEW — 2026-04-14)
# Inline panel on Collection Plan page — summary card per customer
# =====================================================================
@login_required(login_url="/accounts/login/")
def customer_360_api(request: HttpRequest, customer_id: int) -> JsonResponse:
    """
    Lightweight Customer 360 API.

    Reads PostgreSQL only.
    Uses alias customer IDs so all historical synced data is included even when
    Google Sheet tabs used slightly different customer legal names.
    """
    accessible_qs = _customer_qs_for_user(request.user).select_related("kam", "primary_kam")

    try:
        customer = accessible_qs.get(id=customer_id)
    except Customer.DoesNotExist:
        return JsonResponse({"error": "Customer not found or access denied"}, status=404)

    alias_customer_ids = _customer360_alias_customer_ids(customer, accessible_qs)

    scoped_overdue_qs = _filter_qs_by_kam_scope(
        OverdueSnapshot.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_sales_qs = _filter_qs_by_kam_scope(
        InvoiceFact.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_collection_qs = _filter_qs_by_kam_scope(
        CollectionTxn.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_collection_plan_qs = _filter_qs_by_kam_scope(
        CollectionPlan.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_visit_qs = _filter_qs_by_kam_scope(
        VisitPlan.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_call_qs = _filter_qs_by_kam_scope(
        CallLog.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )
    scoped_lead_qs = _filter_qs_by_kam_scope(
        LeadFact.objects.filter(customer_id__in=alias_customer_ids),
        request.user,
        None,
        "kam_id",
    )

    latest_snapshot_date = (
        scoped_overdue_qs
        .order_by("-snapshot_date")
        .values_list("snapshot_date", flat=True)
        .first()
    )

    snapshot_agg = {
        "exposure": Decimal(0),
        "overdue": Decimal(0),
        "a0_30": Decimal(0),
        "a31_60": Decimal(0),
        "a61_90": Decimal(0),
        "a90_plus": Decimal(0),
    }

    if latest_snapshot_date:
        snapshot_agg = (
            scoped_overdue_qs
            .filter(snapshot_date=latest_snapshot_date)
            .aggregate(
                exposure=Sum("exposure"),
                overdue=Sum("overdue"),
                a0_30=Sum("ageing_0_30"),
                a31_60=Sum("ageing_31_60"),
                a61_90=Sum("ageing_61_90"),
                a90_plus=Sum("ageing_90_plus"),
            )
        )

    exposure = _safe_decimal(snapshot_agg.get("exposure"))
    overdue = _safe_decimal(snapshot_agg.get("overdue"))
    ageing_0_30 = _safe_decimal(snapshot_agg.get("a0_30"))
    ageing_31_60 = _safe_decimal(snapshot_agg.get("a31_60"))
    ageing_61_90 = _safe_decimal(snapshot_agg.get("a61_90"))
    ageing_90_plus = _safe_decimal(snapshot_agg.get("a90_plus"))

    credit_limit = _safe_decimal(customer.credit_limit)

    if not credit_limit:
        credit_limit = _safe_decimal(
            Customer.objects
            .filter(id__in=alias_customer_ids)
            .aggregate(s=Sum("credit_limit"))
            .get("s")
        )

    if not exposure:
        exposure = _safe_decimal(
            Customer.objects
            .filter(id__in=alias_customer_ids)
            .aggregate(s=Sum("total_exposure"))
            .get("s")
        )

    if not exposure:
        age_sum = ageing_0_30 + ageing_31_60 + ageing_61_90 + ageing_90_plus

        if age_sum:
            exposure = age_sum
        elif overdue:
            exposure = overdue

    sales_qs = _preferred_inv_qs(scoped_sales_qs)

    sales_agg = sales_qs.aggregate(
        total_mt=Sum("qty_mt"),
        total_value=Sum("invoice_value"),
    )

    total_sales_mt = _safe_decimal(sales_agg.get("total_mt"))
    total_sales_value = _safe_decimal(sales_agg.get("total_value"))

    today = timezone.localdate()
    month_start = today.replace(day=1)
    year_start = date(today.year, 1, 1)

    monthly_sales = _safe_decimal(
        _preferred_inv_qs(
            scoped_sales_qs.filter(
                invoice_date__gte=month_start,
                invoice_date__lte=today,
            )
        ).aggregate(s=Sum("qty_mt")).get("s")
    )

    yearly_sales = _safe_decimal(
        _preferred_inv_qs(
            scoped_sales_qs.filter(
                invoice_date__gte=year_start,
                invoice_date__lte=today,
            )
        ).aggregate(s=Sum("qty_mt")).get("s")
    )

    collected = _safe_decimal(
        scoped_collection_qs
        .aggregate(received=Sum("amount"))
        .get("received")
    )

    plan_agg = (
        scoped_collection_plan_qs
        .aggregate(
            planned=Sum("planned_amount"),
            actual=Sum("actual_amount"),
            plan_overdue=Sum("overdue_amount"),
        )
    )

    planned_collection = _safe_decimal(plan_agg.get("planned"))
    plan_actual = _safe_decimal(plan_agg.get("actual"))
    plan_overdue = _safe_decimal(plan_agg.get("plan_overdue"))

    if not collected and plan_actual:
        collected = plan_actual

    if not overdue and plan_overdue:
        overdue = plan_overdue

    outstanding = exposure - collected
    pending_collection = outstanding if outstanding > 0 else Decimal(0)

    visits = scoped_visit_qs
    planned_visits = visits.count()
    completed_visits = visits.filter(actual__isnull=False).count()
    pending_visits = max(planned_visits - completed_visits, 0)
    last_visit = visits.order_by("-visit_date").first()

    calls = scoped_call_qs
    call_count = calls.count()
    last_call = calls.order_by("-call_datetime").first()

    leads = scoped_lead_qs
    lead_count = leads.count()
    lead_qty = _safe_decimal(leads.aggregate(s=Sum("qty_mt")).get("s"))

    last_invoice = sales_qs.order_by("-invoice_date").first()

    kam_name = ""

    if customer.kam_id:
        kam_name = customer.kam.get_full_name() or customer.kam.username
    elif customer.primary_kam_id:
        kam_name = customer.primary_kam.get_full_name() or customer.primary_kam.username

    risk_ratio = _safe_ratio(exposure, credit_limit)

    data = {
        "id": customer.id,
        "alias_customer_ids": alias_customer_ids,
        "name": customer.name,
        "kam": kam_name,
        "code": getattr(customer, "code", None) or "",
        "mobile": getattr(customer, "mobile", None) or "",
        "phone": getattr(customer, "mobile", None) or "",
        "email": getattr(customer, "email", None) or "",
        "address": getattr(customer, "address", None) or "",
        "contact_person": getattr(customer, "contact_person", None) or "",

        "credit_limit": float(credit_limit),
        "exposure": float(exposure),
        "outstanding": float(outstanding),
        "overdue": float(overdue),
        "collected": float(collected),
        "pending_collection": float(pending_collection),
        "planned_collection": float(planned_collection),
        "risk_ratio": float(risk_ratio) if risk_ratio is not None else None,

        "ageing": {
            "0_30": float(ageing_0_30),
            "31_60": float(ageing_31_60),
            "61_90": float(ageing_61_90),
            "90_plus": float(ageing_90_plus),
        },

        "total_sales_mt": float(total_sales_mt),
        "total_sales_value": float(total_sales_value),
        "monthly_sales": float(monthly_sales),
        "yearly_sales": float(yearly_sales),
        "last_invoice_date": str(last_invoice.invoice_date) if last_invoice else None,

        "planned_visits": planned_visits,
        "completed_visits": completed_visits,
        "pending_visits": pending_visits,
        "last_visit_date": str(last_visit.visit_date) if last_visit else None,

        "call_count": call_count,
        "last_call": str(last_call.call_datetime) if last_call else None,

        "lead_count": lead_count,
        "lead_qty": float(lead_qty),
    }

    return JsonResponse(data)

# =====================================================================
# Visit batches / Visit History
# =====================================================================
def _wants_json(request: HttpRequest) -> bool:
    fmt = (request.GET.get("format") or "").strip().lower()
    if fmt in {"json", "api"}:
        return True
    return "application/json" in (request.headers.get("Accept") or "").lower()


def _visit_history_customer_options_for_user(user: User):
    """
    Customer Master source for Visit History customer filter.

    Production rule:
    - Admin sees all valid customers.
    - Manager sees customers in permitted reporting scope.
    - KAM sees only customers assigned to that KAM through the existing
      Customer Master / KAMAssignment / synced fact ownership sources.

    Reuses _customer_qs_for_user() so newly synced/manual customers appear
    automatically and no duplicate customer data is created.
    """
    try:
        qs = (
            _customer_qs_for_user(user)
            .filter(name__isnull=False)
            .exclude(name__exact="")
            .distinct()
            .order_by("name", "code")
        )

        # Defence-in-depth for pure KAM users: never let this dropdown fall
        # back to global Customer.objects. The canonical helper already scopes
        # by KAM; this guard keeps the contract explicit for Visit History.
        if not _is_manager(user) and not _is_admin(user):
            return qs.filter(
                Q(kam=user)
                | Q(primary_kam=user)
                | Q(id__in=KAMAssignment.objects.filter(
                    kam=user,
                ).filter(
                    Q(active_to__isnull=True)
                    | Q(active_to__gte=timezone.localdate())
                ).values_list("customer_id", flat=True))
                | Q(id__in=VisitPlan.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
                | Q(id__in=InvoiceFact.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
                | Q(id__in=LeadFact.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
                | Q(id__in=CollectionTxn.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
                | Q(id__in=OverdueSnapshot.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
                | Q(id__in=CollectionPlan.objects.filter(
                    kam=user,
                    customer_id__isnull=False,
                ).values_list("customer_id", flat=True))
            ).distinct().order_by("name", "code")

        return qs

    except Exception:
        logger.exception(
            "Failed to build Visit History customer dropdown. user_id=%s",
            getattr(user, "id", None),
        )
        return Customer.objects.none()


def _selected_visit_history_customer_id(request: HttpRequest, customer_qs) -> Optional[int]:
    """
    Resolve selected Visit History customer safely inside the user's customer scope.
    Filtering is by exact Customer.id stored on VisitPlan.customer_id.
    """
    raw_customer = (
        request.GET.get("customer")
        or request.GET.get("customer_id")
        or ""
    ).strip()

    if not raw_customer or raw_customer.upper() in {"ALL", "*"}:
        return None

    if not raw_customer.isdigit():
        return None

    customer_id = int(raw_customer)

    try:
        if customer_qs.filter(id=customer_id).exists():
            return customer_id
    except Exception:
        logger.exception(
            "Failed to validate Visit History customer filter. customer_id=%s user_id=%s",
            customer_id,
            getattr(getattr(request, "user", None), "id", None),
        )

    return None


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches(request: HttpRequest) -> HttpResponse:
    return visit_batches_api(request) if _wants_json(request) else visit_batches_page(request)


@login_required(login_url="/accounts/login/")
def visit_batches_page(request: HttpRequest) -> HttpResponse:
    """
    Visit History page.

    Production-safe update:
    - Uses VisitPlan as the Visit History row source because customer selected
      during Plan Visit is stored on VisitPlan.customer.
    - Reuses _customer_qs_for_user() as the canonical Customer Master source
      for the Customer dropdown.
    - Applies exact Customer.id filtering against VisitPlan.customer_id.
    - Keeps existing scoping, filters, workflow, permissions, URLs, and APIs.
    - Uses select_related and grouped aggregate lookups to avoid N+1 queries.
    - Does not change approval, post visit, collection, or call log logic.
    """
    can_view_all = _is_manager(request.user)

    customer_dropdown_options_qs = _visit_history_customer_options_for_user(request.user)
    selected_customer_id = _selected_visit_history_customer_id(
        request,
        customer_dropdown_options_qs,
    )
    selected_customer = str(selected_customer_id or "")
    customer_all_label = "All Customers" if can_view_all else "All Assigned Customers"
    customer_all_help = (
        "Show visits for every customer in permitted scope"
        if can_view_all
        else "Show visits only for customers assigned to you"
    )

    if not _visitplan_workflow_schema_ready():
        messages.error(
            request,
            "Visit workflow DB fields are not migrated yet. Run migrations before using this page.",
        )
        return render(
            request,
            "kam/visit_batches.html",
            {
                "rows": [],
                "can_view_all": can_view_all,
                "status_filter": "",
                "selected_kam": "",
                "selected_customer": selected_customer,
                "date_range": "",
                "time_filter": "",
                "filter_from": "",
                "filter_to": "",
                "kam_dropdown_options": [],
                "customer_dropdown_options": list(customer_dropdown_options_qs),
                "customer_all_label": customer_all_label,
                "customer_all_help": customer_all_help,
            },
        )

    qs = (
        _visitplan_qs_for_user(request.user)
        .select_related("customer", "kam", "batch", "actual", "approved_by", "rejected_by")
        .order_by("-visit_date", "-created_at", "-id")
    )

    status_filter = (request.GET.get("status") or "").strip().upper()

    if status_filter:
        qs = qs.filter(approval_status=status_filter)

    qs = _apply_visit_history_filters(
        qs,
        request,
        kam_field="kam_id",
        date_field="visit_date",
    )

    if selected_customer_id:
        qs = qs.filter(customer_id=selected_customer_id)

    rows = _attach_visit_customer_display(list(qs[:300]))

    customer_ids = sorted(
        {
            int(plan.customer_id)
            for plan in rows
            if getattr(plan, "customer_id", None)
        }
    )

    kam_ids = sorted(
        {
            int(plan.kam_id)
            for plan in rows
            if getattr(plan, "kam_id", None)
        }
    )

    visit_dates = sorted(
        {
            plan.visit_date
            for plan in rows
            if getattr(plan, "visit_date", None)
        }
    )

    calls_map = {}

    if customer_ids and kam_ids and visit_dates:
        calls_qs = (
            CallLog.objects
            .filter(
                customer_id__in=customer_ids,
                kam_id__in=kam_ids,
                call_datetime__date__in=visit_dates,
            )
            .annotate(activity_date=TruncDate("call_datetime"))
            .values("customer_id", "kam_id", "activity_date")
            .annotate(total=models.Count("id"))
        )

        calls_map = {
            (row["customer_id"], row["kam_id"], row["activity_date"]): row["total"]
            for row in calls_qs
        }

    collections_map = {}

    if customer_ids and kam_ids and visit_dates:
        collections_qs = (
            CollectionTxn.objects
            .filter(
                customer_id__in=customer_ids,
                kam_id__in=kam_ids,
                txn_datetime__date__in=visit_dates,
            )
            .annotate(activity_date=TruncDate("txn_datetime"))
            .values("customer_id", "kam_id", "activity_date")
            .annotate(total=Sum("amount"))
        )

        collections_map = {
            (row["customer_id"], row["kam_id"], row["activity_date"]): _safe_decimal(row["total"])
            for row in collections_qs
        }

    for plan in rows:
        customer_name = ""

        if getattr(plan, "customer_id", None) and getattr(plan, "customer", None):
            customer_name = (getattr(plan.customer, "name", "") or "").strip()

        if not customer_name:
            customer_name = (getattr(plan, "counterparty_name", "") or "").strip()

        plan.customer_display_name = customer_name
        plan.customer_badge_names = [customer_name] if customer_name else []
        plan.manager_status_display = _manager_visit_business_status(plan)
        plan.visit_status_display = _manager_visit_business_status(plan)

        actual = getattr(plan, "actual", None)

        if plan.approval_status == STATUS_COMPLETED:
            plan.post_visit_status_display = "Accepted"
        elif _post_visit_submitted(plan):
            plan.post_visit_status_display = "Submitted"
        else:
            plan.post_visit_status_display = "Pending"

        plan.next_meeting_display = (
            getattr(actual, "next_action_date", None)
            if actual
            else None
        )

        aggregate_key = (
            getattr(plan, "customer_id", None),
            getattr(plan, "kam_id", None),
            getattr(plan, "visit_date", None),
        )

        # Strict date binding: row-level Post Visit History metrics must belong
        # only to the opened/selected Visit Date. Do not mix calls or
        # collections from other dates for the same customer/KAM.
        plan.total_calls = calls_map.get(aggregate_key, 0)
        plan.total_collections = collections_map.get(aggregate_key, Decimal(0))

    kam_dropdown_options = []

    if can_view_all:
        try:
            if _is_admin(request.user):
                kam_dropdown_options = list(
                    User.objects
                    .filter(is_active=True)
                    .order_by("first_name", "last_name", "username")
                )
            else:
                kam_ids_for_dropdown = _kams_managed_by_manager(request.user)
                kam_dropdown_options = list(
                    User.objects
                    .filter(is_active=True, id__in=kam_ids_for_dropdown)
                    .order_by("first_name", "last_name", "username")
                )
        except Exception:
            logger.exception("Failed to build visit history KAM dropdown.")
            kam_dropdown_options = []

    selected_kam = (
        request.GET.get("kam", "")
        or request.GET.get("user", "")
        or request.GET.get("kam_id", "")
    )

    date_range = (request.GET.get("date_range") or "").strip().lower()
    time_filter = (request.GET.get("time_filter") or "").strip().lower()

    ctx = {
        "rows": rows,
        "can_view_all": can_view_all,
        "status_filter": status_filter,
        "selected_kam": selected_kam,
        "selected_customer": selected_customer,
        "date_range": date_range,
        "time_filter": time_filter,
        "filter_from": request.GET.get("from_date", "") or request.GET.get("from", ""),
        "filter_to": request.GET.get("to_date", "") or request.GET.get("to", ""),
        "kam_dropdown_options": kam_dropdown_options,
        "customer_dropdown_options": list(customer_dropdown_options_qs),
        "customer_all_label": customer_all_label,
        "customer_all_help": customer_all_help,
    }

    return render(request, "kam/visit_batches.html", ctx)

@login_required(login_url="/accounts/login/")
def visit_batches_api(request: HttpRequest) -> JsonResponse:
    """
    Existing Visit History API.

    Production-safe update:
    - Keeps same endpoint.
    - Keeps same response key: batches.
    - Adds support for date_range:
        weekly / monthly / quarterly / yearly
    - Keeps old time_filter support.
    - Keeps purpose_of_visit and remarks.
    - Avoids N+1 for line count by using prefetched lines.
    """
    if not _visitplan_workflow_schema_ready():
        return JsonResponse({"batches": []})

    qs = (
        _visitbatch_qs_for_user(request.user)
        .select_related("kam")
        .prefetch_related("lines", "lines__customer", "lines__actual")
        .order_by("-created_at")
    )

    status_filter = (request.GET.get("status") or "").strip().upper()

    if status_filter:
        qs = qs.filter(approval_status=status_filter)

    qs = _apply_visit_history_filters(
        qs,
        request,
        kam_field="kam_id",
        date_field="from_date",
    )

    customer_dropdown_options_qs = _visit_history_customer_options_for_user(request.user)
    selected_customer_id = _selected_visit_history_customer_id(
        request,
        customer_dropdown_options_qs,
    )

    if selected_customer_id:
        qs = qs.filter(lines__customer_id=selected_customer_id).distinct()

    batches = []

    for batch in qs[:300]:
        purpose = (batch.purpose or "").strip()

        try:
            line_count = len(list(batch.lines.all()))
        except Exception:
            line_count = batch.lines.count() if hasattr(batch, "lines") else 0

        batches.append(
            {
                "id": batch.id,
                "kam": batch.kam.get_full_name() or batch.kam.username,
                "kam_username": batch.kam.username,
                "visit_category": batch.visit_category,
                "visit_category_label": batch.get_visit_category_display(),
                "from_date": batch.from_date.isoformat() if batch.from_date else "",
                "to_date": batch.to_date.isoformat() if batch.to_date else "",
                "purpose_of_visit": purpose,
                "remarks": purpose,
                "approval_status": batch.approval_status,
                "line_count": line_count,
            }
        )

    return JsonResponse({"batches": batches})

@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def visit_history_edit(request: HttpRequest, plan_id: int) -> HttpResponse:
    """
    Edit Visit History item.

    Production lifecycle:
    - KAM submits post-visit details after approval.
    - Post-visit submit sends second email to manager.
    - Post-visit submit does NOT mark Completed.
    - Manager review acceptance is the only Completed transition.
    - Next Meeting Date is hidden until workflow is Completed.
    """
    user = request.user

    if _is_manager(user) and not _is_admin(user):
        return HttpResponseForbidden("403 Forbidden: Managers can only view visit records.")

    if _is_admin(user):
        plan = get_object_or_404(
            VisitPlan.objects.select_related("customer", "kam", "batch", "actual"),
            id=plan_id,
        )
    else:
        plan = get_object_or_404(
            VisitPlan.objects.select_related("customer", "kam", "batch", "actual"),
            id=plan_id,
            kam=user,
        )

    if not _is_manager(user) and plan.approval_status not in {
        STATUS_APPROVED,
        STATUS_COMPLETED,
    }:
        messages.error(
            request,
            "Only approved or completed visits can be edited for post-visit details.",
        )
        return redirect(reverse("kam:visit_batches"))

    existing_actual = getattr(plan, "actual", None)
    workflow_completed = plan.approval_status == STATUS_COMPLETED

    if request.method == "POST":
        new_visit_date = _parse_iso_date((request.POST.get("visit_date") or "").strip())
        new_visit_date_to = _parse_iso_date((request.POST.get("visit_date_to") or "").strip())
        new_location = (request.POST.get("location") or "").strip() or None

        try:
            new_purpose = _require_purpose_of_visit(request.POST.get("purpose"))
        except ValueError as exc:
            messages.error(request, str(exc))

            actual_form = VisitActualForm(
                request.POST,
                instance=existing_actual,
                workflow_completed=workflow_completed,
            )

            return render(
                request,
                "kam/visit_history_edit.html",
                {
                    "page_title": "Edit Visit",
                    "plan": plan,
                    "actual_form": actual_form,
                    "existing_actual": existing_actual,
                    "can_edit_plan_fields": True,
                    "workflow_completed": workflow_completed,
                },
            )

        if not new_visit_date:
            messages.error(request, "Visit Date is required.")

            actual_form = VisitActualForm(
                request.POST,
                instance=existing_actual,
                workflow_completed=workflow_completed,
            )

            return render(
                request,
                "kam/visit_history_edit.html",
                {
                    "page_title": "Edit Visit",
                    "plan": plan,
                    "actual_form": actual_form,
                    "existing_actual": existing_actual,
                    "can_edit_plan_fields": True,
                    "workflow_completed": workflow_completed,
                },
            )

        actual_form = VisitActualForm(
            request.POST,
            instance=existing_actual,
            workflow_completed=workflow_completed,
        )

        if actual_form.is_valid():
            with transaction.atomic():
                plan.visit_date = new_visit_date
                plan.visit_date_to = new_visit_date_to
                plan.purpose = new_purpose
                plan.location = new_location

                plan.save(
                    update_fields=[
                        "visit_date",
                        "visit_date_to",
                        "purpose",
                        "location",
                        "updated_at",
                    ]
                )

                actual: VisitActual = actual_form.save(commit=False)
                actual.plan = plan

                if plan.approval_status != STATUS_COMPLETED:
                    actual.next_action_date = None

                actual.save()

                VisitApprovalAudit.objects.create(
                    plan=plan,
                    actor=user,
                    action=VisitApprovalAudit.ACTION_SUBMIT,
                    note="[POST_VISIT_SUBMITTED] Post visit details submitted by KAM",
                    actor_ip=_get_ip(request),
                )

            if plan.approval_status == STATUS_APPROVED and _post_meeting_details_complete(actual):
                sent_ok = _send_post_visit_completion_mail(
                    request=request,
                    plan=plan,
                )

                if sent_ok:
                    messages.success(
                        request,
                        f"Post visit details for Visit #{plan.id} submitted. Manager review mail sent.",
                    )
                else:
                    messages.warning(
                        request,
                        f"Post visit details for Visit #{plan.id} submitted, but manager email could not be sent. Please check mail configuration.",
                    )
            else:
                messages.success(
                    request,
                    f"Visit #{plan.id} updated successfully.",
                )

            return redirect(reverse("kam:visit_batches"))

        messages.error(request, "Please correct the errors below.")

    else:
        actual_form = VisitActualForm(
            instance=existing_actual,
            workflow_completed=workflow_completed,
        )

    return render(
        request,
        "kam/visit_history_edit.html",
        {
            "page_title": "Edit Visit",
            "plan": plan,
            "actual_form": actual_form,
            "existing_actual": existing_actual,
            "can_edit_plan_fields": True,
            "workflow_completed": workflow_completed,
        },
    )

@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def visit_batch_detail(request: HttpRequest, batch_id: int) -> HttpResponse:
    b = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    lines = _attach_visit_customer_display(list(VisitPlan.objects.select_related("customer", "kam", "batch").filter(batch=b).order_by("customer__name", "counterparty_name", "id")))
    can_approve = _is_manager(request.user)
    return render(request, "kam/visit_batch_detail.html", {"page_title": f"Visit History — Batch #{b.id}", "batch": b, "lines": lines, "can_approve": can_approve, "can_edit": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT, STATUS_REJECTED}), "can_delete": (not _is_manager(request.user)) and (b.approval_status in {STATUS_DRAFT})})


@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
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
    _notify_kam_batch_decision(request=request, batch=batch, actor=request.user, status="APPROVED")
    messages.success(request, f"Batch #{batch.id} approved.")
    return redirect(reverse("kam:visit_batches"))


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def visit_batch_reject(request: HttpRequest, batch_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    batch = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    reason = (request.POST.get("reason") or "").strip() or "Rejected by manager"
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
        batch.rejected_by = request.user
        batch.rejected_at = now_ts
        batch.rejection_reason = reason
        batch.save(update_fields=["approval_status", "rejected_by", "rejected_at", "rejection_reason", "updated_at"])
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_REJECTED, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note=reason[:255], actor_ip=_get_ip(request))
    _notify_kam_batch_decision(request=request, batch=batch, actor=request.user, status="REJECTED", rejection_reason=reason)
    messages.info(request, f"Batch #{batch.id} rejected.")
    return redirect(reverse("kam:visit_batches"))


# =====================================================================
# EMAIL APPROVAL LINKS (Batch)
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_visit_approve")
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
    _notify_kam_batch_decision(request=request, batch=batch, actor=request.user, status="APPROVED")
    messages.success(request, f"Batch #{batch_id} approved.")
    return redirect(reverse("kam:visit_batches"))


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_visit_reject")
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

    if request.method == "GET":
        with transaction.atomic():
            batch = get_object_or_404(VisitBatch, id=batch_id)
        if batch.approval_status == STATUS_REJECTED:
            messages.info(request, f"Batch #{batch.id} is already rejected.")
            return redirect(reverse("kam:visit_batches"))
        return render(request, "kam/visit_batch_reject_reason.html", {"batch": batch, "token": token, "page_title": f"Reject Batch #{batch_id}"})

    reason = (request.POST.get("reason") or "").strip() or "Rejected via email link"
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
        batch.rejected_by = request.user
        batch.rejected_at = now_ts
        batch.rejection_reason = reason
        batch.save(update_fields=["approval_status", "rejected_by", "rejected_at", "rejection_reason", "updated_at"])
        VisitPlan.objects.filter(batch=batch).update(approval_status=STATUS_REJECTED, updated_at=now_ts)
        VisitApprovalAudit.objects.create(batch=batch, actor=request.user, action=VisitApprovalAudit.ACTION_REJECT, note=reason[:255], actor_ip=_get_ip(request))
    _notify_kam_batch_decision(request=request, batch=batch, actor=request.user, status="REJECTED", rejection_reason=reason)
    messages.info(request, f"Batch #{batch_id} rejected.")
    return redirect(reverse("kam:visit_batches"))


# =====================================================================
# VISITS & CALLS
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def visits(request: HttpRequest) -> HttpResponse:
    """
    Visits & Calls page.

    Production lifecycle:
    - Only approved/completed visits are visible for post-visit update.
    - KAM post-visit save does not complete workflow.
    - Manager review acceptance completes workflow.
    - Next Meeting Date is hidden unless workflow is completed.
    """
    user = request.user
    can_choose_kam = _is_manager(user) or _is_admin(user)

    selected_kam_id = (request.GET.get("kam") or "").strip()
    selected_plan_id = (
        request.GET.get("plan")
        or request.POST.get("plan_id")
        or ""
    ).strip()

    rows_qs = (
        _visitplan_qs_for_user(user)
        .select_related("customer", "kam", "actual")
        .filter(approval_status__in=[STATUS_APPROVED, STATUS_COMPLETED])
        .order_by("-visit_date", "-created_at", "-id")
    )

    rows_qs = _apply_visit_history_filters(
        rows_qs,
        request,
        kam_field="kam_id",
        date_field="visit_date",
    )

    rows = _attach_visit_customer_display(list(rows_qs[:300]))

    selected_plan = None

    if selected_plan_id.isdigit():
        selected_plan = (
            rows_qs
            .filter(id=int(selected_plan_id))
            .select_related("customer", "kam", "actual")
            .first()
        )

    if selected_plan is None and rows:
        selected_plan = rows[0]

    if selected_plan:
        _attach_visit_customer_display([selected_plan])

    existing_actual = getattr(selected_plan, "actual", None) if selected_plan else None
    workflow_completed = bool(
        selected_plan and selected_plan.approval_status == STATUS_COMPLETED
    )

    if request.method == "POST":
        if not selected_plan:
            messages.error(request, "Please select a visit first.")
            return redirect(reverse("kam:visits"))

        try:
            new_purpose = _require_purpose_of_visit(request.POST.get("purpose"))
        except ValueError as exc:
            messages.error(request, str(exc))
            actual_form = VisitActualForm(
                request.POST,
                instance=existing_actual,
                workflow_completed=workflow_completed,
            )
        else:
            actual_form = VisitActualForm(
                request.POST,
                instance=existing_actual,
                workflow_completed=workflow_completed,
            )

            if actual_form.is_valid():
                with transaction.atomic():
                    selected_plan.purpose = new_purpose
                    selected_plan.save(update_fields=["purpose", "updated_at"])

                    actual: VisitActual = actual_form.save(commit=False)
                    actual.plan = selected_plan

                    if selected_plan.approval_status != STATUS_COMPLETED:
                        actual.next_action_date = None

                    actual.save()

                    VisitApprovalAudit.objects.create(
                        plan=selected_plan,
                        actor=user,
                        action=VisitApprovalAudit.ACTION_SUBMIT,
                        note="[POST_VISIT_SUBMITTED] Post visit details submitted by KAM",
                        actor_ip=_get_ip(request),
                    )

                if (
                    selected_plan.approval_status == STATUS_APPROVED
                    and _post_meeting_details_complete(actual)
                ):
                    sent_ok = _send_post_visit_completion_mail(
                        request=request,
                        plan=selected_plan,
                    )

                    if sent_ok:
                        messages.success(
                            request,
                            f"Post visit details for Visit #{selected_plan.id} submitted. Manager review mail sent.",
                        )
                    else:
                        messages.warning(
                            request,
                            f"Post visit details for Visit #{selected_plan.id} submitted, but manager email could not be sent.",
                        )
                else:
                    messages.success(
                        request,
                        f"Visit #{selected_plan.id} updated successfully.",
                    )

                return redirect(reverse("kam:visits") + f"?plan={selected_plan.id}")

            messages.error(request, "Please correct the errors below.")

    else:
        actual_form = VisitActualForm(
            instance=existing_actual,
            workflow_completed=workflow_completed,
        )

    kam_dropdown_options = []

    if can_choose_kam:
        kam_ids = _kams_managed_by_manager(user)
        if _is_admin(user):
            kam_dropdown_options = list(
                User.objects
                .filter(is_active=True)
                .order_by("first_name", "last_name", "username")
            )
        else:
            kam_dropdown_options = list(
                User.objects
                .filter(id__in=kam_ids, is_active=True)
                .order_by("first_name", "last_name", "username")
            )

    context = {
        "page_title": "Visits & Calls",
        "rows": rows,
        "selected_plan": selected_plan,
        "actual_form": actual_form,
        "workflow_completed": workflow_completed,

        "can_choose_kam": can_choose_kam,
        "kam_dropdown_options": kam_dropdown_options,
        "selected_kam_id": selected_kam_id,

        "filter_from": request.GET.get("from_date", ""),
        "filter_to": request.GET.get("to_date", ""),
        "time_filter": request.GET.get("time_filter", ""),
    }

    return render(request, "kam/visit_actual.html", context)

# =====================================================================
# MANAGER VIEW
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def manager_view(request: HttpRequest) -> HttpResponse:
    """
    Manager View.

    Production lifecycle rule:
    - Visit created/submitted              -> Pending
    - Manager approved                     -> Still Pending
    - KAM submitted post visit             -> Still Pending
    - Manager accepted post-visit review   -> Completed

    Manager approval alone never marks the workflow completed.
    """
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    active_tab = (request.GET.get("tab") or "visits").strip().lower()

    tabs = [
        ("visits", "Visits"),
        ("calls", "Calls"),
        ("collections", "Collections"),
        ("leads", "Leads"),
    ]

    if active_tab not in {key for key, _label in tabs}:
        active_tab = "visits"

    kam_options = _kam_options_for_user(request.user)
    customer_dropdown_options = list(_manager_customer_dropdown_options())

    selected_user = request.GET.get("user", "") or request.GET.get("kam", "")
    selected_customer_id = _selected_manager_customer_id(request)
    selected_customer = str(selected_customer_id or "")
    selected_customer_label = "ALL"

    if selected_customer_id:
        for customer_option in customer_dropdown_options:
            if customer_option.id == selected_customer_id:
                selected_customer_label = customer_option.name
                break

    time_filter = request.GET.get("time_filter", "")
    focus_visit = (request.GET.get("focus_visit") or "").strip()

    visits_data = []
    customer_history_map = {}
    visits_summary = {
        "total_planned": 0,
        "total_actual": 0,
        "successful": 0,
        "success_pct": None,
    }

    calls_data = []
    calls_summary = {
        "total_calls": 0,
        "connected": 0,
        "followups": 0,
        "conversion_pct": None,
    }

    collections_data = []
    collections_summary = {
        "target": Decimal(0),
        "collected": Decimal(0),
        "pending": Decimal(0),
        "achievement_pct": None,
    }

    leads_data = []
    leads_summary = {
        "total": 0,
        "won": 0,
        "lost": 0,
        "open": 0,
    }

    if active_tab == "visits":
        visit_qs = (
            _visitplan_qs_for_user(request.user)
            .select_related("customer", "kam", "actual", "approved_by", "rejected_by")
            .order_by("-visit_date", "-created_at", "-id")
        )

        visit_qs = _apply_visit_history_filters(
            visit_qs,
            request,
            kam_field="kam_id",
            date_field="visit_date",
        )

        visit_qs = _apply_manager_customer_filter(visit_qs, selected_customer_id)

        visits_data = _attach_manager_visit_readonly_details(list(visit_qs[:300]))

        for visit in visits_data:
            visit.business_status = _manager_visit_business_status(visit)
            visit.post_visit_submitted = _post_visit_submitted(visit)
            visit.can_manager_review_post_visit = _post_visit_can_be_manager_reviewed(visit)

        completed_actuals = [
            visit.actual
            for visit in visits_data
            if getattr(visit, "actual", None)
            and visit.approval_status == STATUS_COMPLETED
            and _post_meeting_details_complete(visit.actual)
        ]

        total_actual = len(completed_actuals)
        successful = sum(1 for actual in completed_actuals if actual.successful is True)

        visits_summary = {
            "total_planned": len(visits_data),
            "total_actual": total_actual,
            "successful": successful,
            "success_pct": (successful / total_actual * 100) if total_actual else None,
        }

        customer_ids = [
            visit.customer_id
            for visit in visits_data
            if getattr(visit, "customer_id", None)
        ]
        customer_history_map = _build_customer_history_payload(customer_ids, request.user)

    elif active_tab == "calls":
        call_qs = CallLog.objects.select_related("customer", "kam").order_by("-call_datetime")
        scope_kam_id, _scope_label = _resolve_scope(request, request.user)

        call_qs = _filter_qs_by_kam_scope(
            call_qs,
            request.user,
            scope_kam_id,
            "kam_id",
        )

        call_qs = _apply_manager_customer_filter(call_qs, selected_customer_id)

        from_d = _parse_iso_date(request.GET.get("from_date") or request.GET.get("from") or "")
        to_d = _parse_iso_date(request.GET.get("to_date") or request.GET.get("to") or "")
        time_start, time_end = _visit_time_filter_bounds(time_filter)

        start_d = from_d or time_start
        end_d = to_d or time_end

        if start_d:
            start_dt = timezone.make_aware(
                timezone.datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0)
            )
            call_qs = call_qs.filter(call_datetime__gte=start_dt)

        if end_d:
            end_dt = timezone.make_aware(
                timezone.datetime(end_d.year, end_d.month, end_d.day, 0, 0, 0)
            ) + timezone.timedelta(days=1)
            call_qs = call_qs.filter(call_datetime__lt=end_dt)

        calls_data = list(call_qs[:300])
        connected = sum(1 for call in calls_data if (getattr(call, "outcome", "") or "").strip())

        calls_summary = {
            "total_calls": len(calls_data),
            "connected": connected,
            "followups": sum(
                1
                for call in calls_data
                if (getattr(call, "next_action", "") or "").strip()
            ),
            "conversion_pct": (connected / len(calls_data) * 100) if calls_data else None,
        }

    elif active_tab == "collections":
        collection_qs = CollectionTxn.objects.select_related("customer", "kam").order_by("-txn_datetime")
        scope_kam_id, _scope_label = _resolve_scope(request, request.user)

        collection_qs = _filter_qs_by_kam_scope(
            collection_qs,
            request.user,
            scope_kam_id,
            "kam_id",
        )

        collection_qs = _apply_manager_customer_filter(collection_qs, selected_customer_id)

        from_d = _parse_iso_date(request.GET.get("from_date") or request.GET.get("from") or "")
        to_d = _parse_iso_date(request.GET.get("to_date") or request.GET.get("to") or "")
        time_start, time_end = _visit_time_filter_bounds(time_filter)

        start_d = from_d or time_start
        end_d = to_d or time_end

        if start_d:
            start_dt = timezone.make_aware(
                timezone.datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0)
            )
            collection_qs = collection_qs.filter(txn_datetime__gte=start_dt)

        if end_d:
            end_dt = timezone.make_aware(
                timezone.datetime(end_d.year, end_d.month, end_d.day, 0, 0, 0)
            ) + timezone.timedelta(days=1)
            collection_qs = collection_qs.filter(txn_datetime__lt=end_dt)

        collections_data = list(collection_qs[:300])
        collected = _safe_decimal(collection_qs.aggregate(total=Sum("amount")).get("total"))

        collections_summary = {
            "target": Decimal(0),
            "collected": collected,
            "pending": Decimal(0),
            "achievement_pct": None,
        }

    elif active_tab == "leads":
        lead_qs = LeadFact.objects.select_related("customer", "kam").order_by("-doe")
        scope_kam_id, _scope_label = _resolve_scope(request, request.user)

        lead_qs = _filter_qs_by_kam_scope(
            lead_qs,
            request.user,
            scope_kam_id,
            "kam_id",
        )

        lead_qs = _apply_manager_customer_filter(lead_qs, selected_customer_id)

        from_d = _parse_iso_date(request.GET.get("from_date") or request.GET.get("from") or "")
        to_d = _parse_iso_date(request.GET.get("to_date") or request.GET.get("to") or "")
        time_start, time_end = _visit_time_filter_bounds(time_filter)

        start_d = from_d or time_start
        end_d = to_d or time_end

        if start_d:
            lead_qs = lead_qs.filter(doe__gte=start_d)

        if end_d:
            lead_qs = lead_qs.filter(doe__lte=end_d)

        leads_data = list(lead_qs[:300])

        leads_summary = {
            "total": len(leads_data),
            "won": sum(1 for lead in leads_data if (lead.status or "").upper() == "WON"),
            "lost": sum(1 for lead in leads_data if (lead.status or "").upper() == "LOST"),
            "open": sum(
                1
                for lead in leads_data
                if (lead.status or "").upper() not in {"WON", "LOST"}
            ),
        }

    context = {
        "page_title": "Manager View",
        "active_tab": active_tab,
        "tabs": tabs,

        "range_label": "Filtered View",
        "scope_label": selected_user or "ALL",
        "selected_customer_label": selected_customer_label,

        "filter_from": request.GET.get("from_date", "") or request.GET.get("from", ""),
        "filter_to": request.GET.get("to_date", "") or request.GET.get("to", ""),
        "time_filter": time_filter,
        "focus_visit": focus_visit,

        "selected_user": selected_user,
        "kam_options": kam_options,
        "selected_customer": selected_customer,
        "customer_dropdown_options": customer_dropdown_options,

        "visits_data": visits_data,
        "customer_history_map": customer_history_map,
        "visits_summary": visits_summary,

        "calls_data": calls_data,
        "calls_summary": calls_summary,

        "collections_data": collections_data,
        "collections_summary": collections_summary,

        "leads_data": leads_data,
        "leads_summary": leads_summary,
    }

    return render(request, "kam/manager_view.html", context)

@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def manager_accept_post_visit(request: HttpRequest, plan_id: int) -> HttpResponse:
    """
    Manager accepts KAM post-visit submission.

    Production lifecycle:
    Visit Plan
      -> Manager Approval
      -> Visit
      -> Post Visit
      -> Manager Review
      -> Completion

    This function is the ONLY transition to COMPLETED after post-visit review.
    """
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")

    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    with transaction.atomic():
        plan = get_object_or_404(
            VisitPlan.objects
            .select_for_update()
            .select_related("kam", "customer", "actual"),
            id=plan_id,
        )

        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")

        if plan.approval_status == STATUS_REJECTED:
            messages.error(
                request,
                f"Visit #{plan.id} is rejected and cannot be completed.",
            )
            return redirect(reverse("kam:manager_view") + "?tab=visits")

        if plan.approval_status == STATUS_COMPLETED:
            messages.info(
                request,
                f"Visit #{plan.id} is already completed.",
            )
            return redirect(reverse("kam:manager_view") + f"?tab=visits&focus_visit={plan.id}")

        actual = getattr(plan, "actual", None)

        if not _post_meeting_details_complete(actual):
            messages.error(
                request,
                f"Visit #{plan.id} cannot be completed because post-visit details are incomplete.",
            )
            return redirect(reverse("kam:manager_view") + f"?tab=visits&focus_visit={plan.id}")

        if plan.approval_status != STATUS_APPROVED:
            messages.error(
                request,
                f"Visit #{plan.id} must be manager-approved before post-visit review can be accepted.",
            )
            return redirect(reverse("kam:manager_view") + f"?tab=visits&focus_visit={plan.id}")

        plan.approval_status = STATUS_COMPLETED
        plan.save(update_fields=["approval_status", "updated_at"])

        VisitApprovalAudit.objects.create(
            plan=plan,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_APPROVE,
            note="[POST_VISIT_REVIEW_ACCEPTED] Manager accepted post visit. Workflow completed.",
            actor_ip=_get_ip(request),
        )

    messages.success(
        request,
        f"Visit #{plan.id} marked Completed after post-visit manager review.",
    )
    return redirect(reverse("kam:manager_view") + f"?tab=visits&focus_visit={plan.id}")
# =====================================================================
# Legacy approve/reject
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_visit_approve")
def visit_approve(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    plan = get_object_or_404(VisitPlan, id=plan_id)
    if not _can_manager_approve_visit(request.user, plan):
        return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
    plan.approval_status = STATUS_APPROVED
    plan.approved_by = request.user
    plan.approved_at = timezone.now()
    plan.save(update_fields=["approval_status", "approved_by", "approved_at"])
    VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved", actor_ip=_get_ip(request))
    messages.success(request, "Visit approved.")
    return redirect(reverse("kam:manager_kpis"))


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_visit_reject")
def visit_reject(request: HttpRequest, plan_id: int) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    plan = get_object_or_404(VisitPlan, id=plan_id)
    if not _can_manager_approve_visit(request.user, plan):
        return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
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
@login_required(login_url="/accounts/login/")
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
            obj.source = CollectionTxn.SOURCE_ERP
            obj.save()
            messages.success(request, "Call saved.")
            return redirect(reverse("kam:dashboard"))
    else:
        form = CallForm()
        if "customer" in form.fields:
            form.fields["customer"].queryset = qs
    return render(request, "kam/call_new.html", {"page_title": "Log Call", "form": form})


@login_required(login_url="/accounts/login/")
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
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_customers")
def customers(request: HttpRequest) -> HttpResponse:
    """
    Customer 360.

    Fixed for all customers:
    - Uses PostgreSQL only.
    - Includes data linked to duplicate customer aliases.
    - Exposes all expected financial/sales/visits/calls/leads/ageing values.
    - Removes duplicate query blocks.
    - Does not change business logic.
    """
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    customer_id = request.GET.get("id")

    base_qs = _customer_qs_for_user(request.user).select_related("kam", "primary_kam")

    if scope_kam_id is not None:
        scoped_invoice_customer_ids = (
            InvoiceFact.objects
            .filter(kam_id=scope_kam_id)
            .values_list("customer_id", flat=True)
        )

        scoped_lead_customer_ids = (
            LeadFact.objects
            .filter(kam_id=scope_kam_id)
            .values_list("customer_id", flat=True)
        )

        scoped_collection_customer_ids = (
            CollectionTxn.objects
            .filter(kam_id=scope_kam_id)
            .values_list("customer_id", flat=True)
        )

        base_qs = (
            base_qs
            .filter(
                Q(kam_id=scope_kam_id)
                | Q(primary_kam_id=scope_kam_id)
                | Q(id__in=scoped_invoice_customer_ids)
                | Q(id__in=scoped_lead_customer_ids)
                | Q(id__in=scoped_collection_customer_ids)
            )
            .distinct()
        )

    customer_list = list(base_qs.order_by("name")[:300])

    customer = None

    if customer_id:
        try:
            customer = base_qs.filter(id=int(customer_id)).first()
        except (ValueError, TypeError):
            customer = None

    if customer is None:
        customer = customer_list[0] if customer_list else None

    period_type, start_date, end_date, period_id = _get_customer360_range(request)

    exposure = Decimal(0)
    overdue = Decimal(0)
    credit_limit = Decimal(0)
    outstanding = Decimal(0)
    collected = Decimal(0)
    pending_collection = Decimal(0)
    planned_collection = Decimal(0)

    ageing = {
        "a0_30": Decimal(0),
        "a31_60": Decimal(0),
        "a61_90": Decimal(0),
        "a90_plus": Decimal(0),
    }

    total_sales = Decimal(0)
    monthly_sales = Decimal(0)
    yearly_sales = Decimal(0)
    total_sales_value = Decimal(0)

    planned_visits = 0
    completed_visits = 0
    pending_visits = 0

    call_count = 0
    last_call = None

    lead_count = 0
    lead_qty = Decimal(0)

    sales_history = []
    collections_history = []
    visit_history = []
    call_history = []
    lead_history = []
    overdue_history = []
    followups = []

    risk_ratio = None
    alias_customer_ids = []

    if customer:
        alias_customer_ids = _customer360_alias_customer_ids(customer, base_qs)

        latest_dt = (
            OverdueSnapshot.objects
            .filter(customer_id__in=alias_customer_ids)
            .order_by("-snapshot_date")
            .values_list("snapshot_date", flat=True)
            .first()
        )

        if latest_dt:
            snap_agg = (
                OverdueSnapshot.objects
                .filter(customer_id__in=alias_customer_ids, snapshot_date=latest_dt)
                .aggregate(
                    exposure=Sum("exposure"),
                    overdue=Sum("overdue"),
                    a0_30=Sum("ageing_0_30"),
                    a31_60=Sum("ageing_31_60"),
                    a61_90=Sum("ageing_61_90"),
                    a90_plus=Sum("ageing_90_plus"),
                )
            )

            exposure = _safe_decimal(snap_agg.get("exposure"))
            overdue = _safe_decimal(snap_agg.get("overdue"))
            ageing = {
                "a0_30": _safe_decimal(snap_agg.get("a0_30")),
                "a31_60": _safe_decimal(snap_agg.get("a31_60")),
                "a61_90": _safe_decimal(snap_agg.get("a61_90")),
                "a90_plus": _safe_decimal(snap_agg.get("a90_plus")),
            }

        credit_limit = _safe_decimal(customer.credit_limit)

        if not credit_limit:
            credit_limit = _safe_decimal(
                Customer.objects
                .filter(id__in=alias_customer_ids)
                .aggregate(s=Sum("credit_limit"))
                .get("s")
            )

        if not exposure:
            exposure = _safe_decimal(
                Customer.objects
                .filter(id__in=alias_customer_ids)
                .aggregate(s=Sum("total_exposure"))
                .get("s")
            )

        if not exposure:
            age_sum = (
                ageing["a0_30"]
                + ageing["a31_60"]
                + ageing["a61_90"]
                + ageing["a90_plus"]
            )

            if age_sum:
                exposure = age_sum
            elif overdue:
                exposure = overdue

        risk_ratio = _safe_ratio(exposure, credit_limit)

        sales_base = InvoiceFact.objects.filter(
            customer_id__in=alias_customer_ids,
            invoice_date__gte=start_date,
            invoice_date__lte=end_date,
        )

        sales_qs = _preferred_inv_qs(sales_base)

        sales_totals = sales_qs.aggregate(
            qty=Sum("qty_mt"),
            value=Sum("invoice_value"),
        )

        total_sales = _safe_decimal(sales_totals.get("qty"))
        total_sales_value = _safe_decimal(sales_totals.get("value"))

        today = timezone.localdate()
        month_start = today.replace(day=1)
        year_start = date(today.year, 1, 1)

        monthly_sales = _safe_decimal(
            _preferred_inv_qs(
                InvoiceFact.objects.filter(
                    customer_id__in=alias_customer_ids,
                    invoice_date__gte=month_start,
                    invoice_date__lte=today,
                )
            ).aggregate(s=Sum("qty_mt")).get("s")
        )

        yearly_sales = _safe_decimal(
            _preferred_inv_qs(
                InvoiceFact.objects.filter(
                    customer_id__in=alias_customer_ids,
                    invoice_date__gte=year_start,
                    invoice_date__lte=today,
                )
            ).aggregate(s=Sum("qty_mt")).get("s")
        )

        sales_history = [
            {
                "year": row["invoice_date__year"],
                "month": row["invoice_date__month"],
                "mt": _safe_decimal(row["mt"]),
            }
            for row in (
                sales_qs
                .values("invoice_date__year", "invoice_date__month")
                .annotate(mt=Sum("qty_mt"))
                .order_by("invoice_date__year", "invoice_date__month")
            )
        ]

        collection_qs = CollectionTxn.objects.filter(
            customer_id__in=alias_customer_ids,
            txn_datetime__date__gte=start_date,
            txn_datetime__date__lte=end_date,
        )

        collected = _safe_decimal(
            collection_qs.aggregate(s=Sum("amount")).get("s")
        )

        plan_agg = (
            CollectionPlan.objects
            .filter(customer_id__in=alias_customer_ids)
            .aggregate(
                planned=Sum("planned_amount"),
                actual=Sum("actual_amount"),
                plan_overdue=Sum("overdue_amount"),
            )
        )

        planned_collection = _safe_decimal(plan_agg.get("planned"))
        plan_actual = _safe_decimal(plan_agg.get("actual"))
        plan_overdue = _safe_decimal(plan_agg.get("plan_overdue"))

        if not collected and plan_actual:
            collected = plan_actual

        if not overdue and plan_overdue:
            overdue = plan_overdue

        outstanding = exposure - collected
        pending_collection = outstanding if outstanding > 0 else Decimal(0)

        collections_history = [
            {
                "year": row["txn_datetime__year"],
                "month": row["txn_datetime__month"],
                "amount": _safe_decimal(row["amount"]),
            }
            for row in (
                collection_qs
                .values("txn_datetime__year", "txn_datetime__month")
                .annotate(amount=Sum("amount"))
                .order_by("txn_datetime__year", "txn_datetime__month")
            )
        ]

        visits_qs = (
            VisitPlan.objects
            .select_related("actual", "kam", "customer")
            .filter(
                customer_id__in=alias_customer_ids,
                visit_date__gte=start_date,
                visit_date__lte=end_date,
            )
        )

        planned_visits = visits_qs.count()
        completed_visits = visits_qs.filter(actual__isnull=False).count()
        pending_visits = max(planned_visits - completed_visits, 0)

        visit_history = list(
            visits_qs.order_by("-visit_date", "-created_at", "-id")[:20]
        )

        calls_qs = (
            CallLog.objects
            .select_related("kam", "customer")
            .filter(
                customer_id__in=alias_customer_ids,
                call_datetime__date__gte=start_date,
                call_datetime__date__lte=end_date,
            )
        )

        call_count = calls_qs.count()
        last_call = calls_qs.order_by("-call_datetime").first()

        call_history = list(
            calls_qs.order_by("-call_datetime")[:20]
        )

        leads_qs = (
            LeadFact.objects
            .select_related("customer", "kam")
            .filter(
                customer_id__in=alias_customer_ids,
                doe__gte=start_date,
                doe__lte=end_date,
            )
        )

        lead_count = leads_qs.count()
        lead_qty = _safe_decimal(leads_qs.aggregate(s=Sum("qty_mt")).get("s"))

        lead_history = list(
            leads_qs.order_by("-doe")[:20]
        )

        overdue_history = list(
            OverdueSnapshot.objects
            .select_related("customer", "kam")
            .filter(customer_id__in=alias_customer_ids)
            .order_by("-snapshot_date")[:12]
        )

        followups = list(
            VisitActual.objects
            .select_related("plan", "plan__customer", "plan__kam")
            .filter(
                plan__customer_id__in=alias_customer_ids,
                next_action__isnull=False,
                next_action__gt="",
                next_action_date__isnull=False,
                next_action_date__gte=today,
            )
            .order_by("next_action_date")[:10]
        )

    ctx = {
        "page_title": "Customer 360",
        "period_type": period_type,
        "period_id": period_id,
        "scope_label": scope_label,
        "kam_options": _kam_options_for_user(request.user),
        "customer_list": customer_list,
        "customer": customer,

        "alias_customer_ids": alias_customer_ids,

        "exposure": exposure,
        "overdue": overdue,
        "credit_limit": credit_limit,
        "outstanding": outstanding,
        "collected": collected,
        "pending_collection": pending_collection,
        "planned_collection": planned_collection,
        "risk_ratio": risk_ratio,
        "ageing": ageing,

        "total_sales": total_sales,
        "total_sales_value": total_sales_value,
        "monthly_sales": monthly_sales,
        "yearly_sales": yearly_sales,
        "sales_history": sales_history,
        "sales_last12": sales_history,

        "planned_visits": planned_visits,
        "completed_visits": completed_visits,
        "pending_visits": pending_visits,
        "visit_history": visit_history,
        "recent_visits": visit_history,

        "call_count": call_count,
        "last_call": last_call,
        "call_history": call_history,
        "recent_calls": call_history,

        "lead_count": lead_count,
        "lead_qty": lead_qty,
        "lead_history": lead_history,

        "collections_history": collections_history,
        "collections_last12": collections_history,
        "overdue_history": overdue_history,
        "followups": followups,
    }

    return render(request, "kam/customer_360.html", ctx)

# =====================================================================
# TARGETS
# =====================================================================
@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_targets_lines")
def targets_lines(request: HttpRequest) -> HttpResponse:
    return redirect(reverse("kam:targets"))

def _kam_ids_with_real_kam_data() -> List[int]:
    kam_ids = set()

    kam_ids.update(
        KamManagerMapping.objects
        .filter(active=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
    )

    kam_ids.update(
        InvoiceFact.objects
        .filter(source_tab="Sales (F)")
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        LeadFact.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        VisitPlan.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        CallLog.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        CollectionPlan.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        CollectionTxn.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    kam_ids.update(
        TargetSetting.objects
        .exclude(kam_id__isnull=True)
        .exclude(kam__is_superuser=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

    return sorted([int(k) for k in kam_ids if k])


def _kam_options_for_performance_report(user: User) -> List[User]:
    real_kam_ids = _kam_ids_with_real_kam_data()

    base_qs = (
        User.objects
        .filter(is_active=True, id__in=real_kam_ids)
        .exclude(is_superuser=True)
        .exclude(username__iexact="admin")
        .exclude(email__icontains="admin")
        .order_by("first_name", "last_name", "username")
    )

    if _is_admin(user):
        return list(base_qs)

    if _is_manager(user):
        managed_ids = set(_kams_managed_by_manager(user))
        allowed_ids = sorted(set(real_kam_ids).intersection(managed_ids))
        return list(base_qs.filter(id__in=allowed_ids))

    if user.id in real_kam_ids and not user.is_superuser:
        return list(base_qs.filter(id=user.id))

    return []


def _resolve_selected_kam_for_performance_report(request: HttpRequest) -> Optional[User]:
    actor = request.user
    kam_options = _kam_options_for_performance_report(actor)

    if not kam_options:
        return None

    allowed_ids = {u.id for u in kam_options}

    raw_kam_id = _first_query_value(request, "kam_id", "kam", "user_id")

    if raw_kam_id and str(raw_kam_id).isdigit():
        requested_kam_id = int(raw_kam_id)

        if requested_kam_id not in allowed_ids:
            return None

        return (
            User.objects
            .filter(id=requested_kam_id, is_active=True)
            .exclude(is_superuser=True)
            .exclude(username__iexact="admin")
            .exclude(email__icontains="admin")
            .first()
        )

    if not _is_manager(actor):
        if actor.id in allowed_ids and not actor.is_superuser:
            return actor

    return kam_options[0]

# =====================================================================
# REPORTS
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    """
    Legacy KAM Reports route.

    UI has been merged into KAM Dashboard.
    Backend URL is preserved for backward compatibility.
    """
    query = request.GET.urlencode()
    url = reverse("kam:dashboard")

    if query:
        url = f"{url}?{query}"

    return redirect(url)

@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_reports", "kam_dashboard")
def kam_performance_report_api(request: HttpRequest) -> JsonResponse:
    """
    Existing report API preserved.

    Dashboard can reuse this API.
    No duplicate dashboard/report API is created.
    """
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    selected_kam = _resolve_selected_kam_for_performance_report(request)

    if not selected_kam:
        return JsonResponse(
            {
                "ok": False,
                "error": "You are not allowed to view this KAM report.",
            },
            status=403,
        )

    report = build_kam_performance_report(
        kam_id=selected_kam.id,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    return JsonResponse(
        {
            "ok": True,
            "range_label": range_label,
            "report": report,
        }
    )
# =====================================================================
# CSV export
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_export_kpi_csv", "kam_dashboard", "kam_reports")
def export_kpi_csv(request: HttpRequest) -> HttpResponse:
    """
    Backward-compatible export endpoint.

    Existing URL/name preserved:
      kam:export_kpi_csv

    Supported formats:
      ?format=csv
      ?format=xlsx
      ?format=excel
      ?format=pdf

    This avoids creating duplicate export APIs.
    """
    export_format = (request.GET.get("format") or "csv").strip().lower()

    start_dt, end_dt, range_label = _get_dashboard_range(request)

    if _is_manager(request.user):
        raw_user = (request.GET.get("user") or "").strip()
        raw_kam_id = (request.GET.get("kam_id") or "").strip()

        if raw_kam_id.isdigit():
            u = User.objects.filter(
                id=int(raw_kam_id),
                is_active=True,
            ).first()
        elif raw_user:
            u = User.objects.filter(
                username__iexact=raw_user,
                is_active=True,
            ).first()
        else:
            u = None

        if u:
            if _is_admin(request.user) or u.id in set(_kams_managed_by_manager(request.user)):
                kam_user_ids = [u.id]
            else:
                kam_user_ids = []
        else:
            if _is_admin(request.user):
                kam_user_ids = _kam_ids_with_real_kam_data()
            else:
                kam_user_ids = _kams_managed_by_manager(request.user)
    else:
        kam_user_ids = [request.user.id]

    reports = []

    for kam_id in kam_user_ids:
        kam = (
            User.objects
            .filter(id=kam_id, is_active=True)
            .exclude(is_superuser=True)
            .exclude(username__iexact="admin")
            .exclude(email__icontains="admin")
            .first()
        )

        if not kam:
            continue

        report = build_kam_performance_report(
            kam_id=kam.id,
            start_dt=start_dt,
            end_dt=end_dt,
        )

        reports.append(report)

    reports.sort(
        key=lambda r: float(_report_value(r, "score", "overall_score", default=0) or 0),
        reverse=True,
    )

    rows = [[
        "Reporting Period",
        "KAM",
        "Designation",
        "Manager",
        "Sales MT",
        "Visits",
        "Calls",
        "Collections",
        "Leads",
        "Conversion %",
        "Target %",
        "Performance %",
        "Overdues",
        "Risk",
    ]]

    for report in reports:
        rows.append([
            range_label,
            _report_value(report, "basic", "name", default="-"),
            _report_value(report, "basic", "designation", default="-"),
            _report_value(report, "basic", "manager", default="-"),
            _report_value(report, "sales", "total_sales_mt", default=0),
            _report_value(report, "visits", "actual_visits", default=0),
            _report_value(report, "calls", "total_calls", default=0),
            _report_value(report, "collections", "total_collected", default=0),
            _report_value(report, "leads", "total_leads", default=0),
            _report_value(report, "leads", "conversion_ratio", default=0),
            _report_value(report, "sales", "achievement_pct", default=0),
            _report_value(report, "score", "overall_score", default=0),
            _report_value(report, "collections", "total_overdue", default=0),
            _report_value(report, "risk", "risk_customers", default=0),
        ])

    filename_period = f"{start_dt.date()}_to_{(end_dt - timezone.timedelta(days=1)).date()}"

    if export_format in {"xlsx", "excel"}:
        try:
            from openpyxl import Workbook
        except Exception:
            logger.exception("openpyxl is not installed. Excel export failed.")
            return HttpResponse(
                "Excel export requires openpyxl to be installed.",
                status=500,
                content_type="text/plain",
            )

        wb = Workbook()
        ws = wb.active
        ws.title = "KAM Performance"

        for row in rows:
            ws.append(row)

        for column_cells in ws.columns:
            max_length = 0
            col_letter = column_cells[0].column_letter

            for cell in column_cells:
                value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(value))

            ws.column_dimensions[col_letter].width = min(max_length + 2, 36)

        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        response = HttpResponse(
            buffer.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="kam_performance_{filename_period}.xlsx"'
        )
        return response

    if export_format == "pdf":
        html = render_to_string(
            "kam/emails/monthly_kam_performance_report.html",
            {
                "reporting_period": range_label,
                "summary_table": [
                    {
                        "rank": idx,
                        "kam": _report_value(report, "basic", "name", default="-"),
                        "sales": _report_value(report, "sales", "total_sales_mt", default=0),
                        "visits": _report_value(report, "visits", "actual_visits", default=0),
                        "collections": _report_value(report, "collections", "total_collected", default=0),
                        "target_pct": _report_value(report, "sales", "achievement_pct", default=0),
                        "performance_pct": _report_value(report, "score", "overall_score", default=0),
                    }
                    for idx, report in enumerate(reports, start=1)
                ],
                "kam_sections": [
                    {
                        "name": _report_value(report, "basic", "name", default="-"),
                        "designation": _report_value(report, "basic", "designation", default="-"),
                        "manager": _report_value(report, "basic", "manager", default="-"),
                        "sales": _report_value(report, "sales", "total_sales_mt", default=0),
                        "visits": _report_value(report, "visits", "actual_visits", default=0),
                        "calls": _report_value(report, "calls", "total_calls", default=0),
                        "collections": _report_value(report, "collections", "total_collected", default=0),
                        "leads": _report_value(report, "leads", "total_leads", default=0),
                        "conversion": _report_value(report, "leads", "conversion_ratio", default=0),
                        "targets": _report_value(report, "sales", "target_mt", default=0),
                        "achievement_pct": _report_value(report, "sales", "achievement_pct", default=0),
                        "overdues": _report_value(report, "collections", "total_overdue", default=0),
                        "risk": _report_value(report, "risk", "risk_customers", default=0),
                        "performance_pct": _report_value(report, "score", "overall_score", default=0),
                        "chart_cids": [],
                    }
                    for report in reports
                ],
                "management_summary": {
                    "top_performer": (
                        _report_value(reports[0], "basic", "name", default="-")
                        if reports
                        else "-"
                    ),
                    "needs_improvement": (
                        _report_value(reports[-1], "basic", "name", default="-")
                        if reports
                        else "-"
                    ),
                    "recommendations": (
                        "Review sales target gaps, pending visits, overdue exposure, "
                        "collection delays, and lead conversion follow-up."
                    ),
                },
            },
        )

        try:
            from weasyprint import HTML

            pdf_bytes = HTML(string=html).write_pdf()
            response = HttpResponse(pdf_bytes, content_type="application/pdf")
            response["Content-Disposition"] = (
                f'attachment; filename="kam_performance_{filename_period}.pdf"'
            )
            return response

        except Exception:
            logger.exception("PDF export failed. Returning HTML fallback.")

            response = HttpResponse(html, content_type="text/html")
            response["Content-Disposition"] = (
                f'attachment; filename="kam_performance_{filename_period}.html"'
            )
            return response

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    for row in rows:
        writer.writerow(row)

    response = HttpResponse(buffer.getvalue(), content_type="text/csv")
    response["Content-Disposition"] = (
        f'attachment; filename="kam_performance_{filename_period}.csv"'
    )
    return response

# =====================================================================
# Collections Plan
# =====================================================================

@login_required(login_url="/accounts/login/")
@require_kam_code("kam_collections_plan")
def collections_plan(request: HttpRequest) -> HttpResponse:
    """
    NEW ARCHITECTURE: Overdue-driven collection tracking.

    DATA FLOW:
      Google Sheet Overdues tab (cols A–C)
        → sync → CollectionPlan.overdue_amount
        → KAM fills actual_amount, collection_date, payment_details, utr_number
        → pending = overdue - actual

    ROLE-BASED ACCESS:
      KAM     → own customers (filtered by plan.kam_id = request.user.id)
      Manager → mapped team's customers (via KamManagerMapping)
      Admin   → all

    REMOVED: Manual customer selection, manual planned amount, "Add New Entry" form.
    """
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    # ── POST: record actual collection ─────────────────────────────────────
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip().lower()

        if action == "record_actual":
            plan_id = (request.POST.get("plan_id") or "").strip()
            if not plan_id.isdigit():
                messages.error(request, "Invalid plan reference.")
                return redirect(reverse("kam:collections_plan"))

            plan = get_object_or_404(CollectionPlan, id=int(plan_id))

            # ── Role-based security check ──────────────────────────────────
            if not _is_admin(request.user):
                if _is_manager(request.user):
                    allowed_kams = set(_kams_managed_by_manager(request.user))
                    if plan.kam_id not in allowed_kams:
                        return HttpResponseForbidden("403 Forbidden: Not your team's plan.")
                else:
                    if plan.kam_id != request.user.id:
                        return HttpResponseForbidden("403 Forbidden: Not your plan.")

            actual_raw       = (request.POST.get("actual_amount")   or "").strip()
            cdate_raw        = (request.POST.get("collection_date")  or "").strip()
            payment_details  = (request.POST.get("payment_details")  or "").strip() or None
            utr_number       = (request.POST.get("utr_number")       or "").strip() or None

            # Parse amount
            try:
                actual = Decimal(actual_raw) if actual_raw else Decimal("0")
            except Exception:
                messages.error(request, "Invalid amount — enter a valid number.")
                return redirect(reverse("kam:collections_plan"))

            if actual < 0:
                messages.error(request, "Amount cannot be negative.")
                return redirect(reverse("kam:collections_plan"))

            cdate = _parse_iso_date(cdate_raw)
            if not cdate:
                messages.error(request, "Collection date is required.")
                return redirect(reverse("kam:collections_plan"))

            with transaction.atomic():
                plan.actual_amount        = actual
                plan.collection_date      = cdate
                plan.payment_details      = payment_details
                plan.utr_number           = utr_number
                plan.collection_reference = utr_number  # backward compat
                plan.save()

            messages.success(
                request,
                f"Collection recorded: {plan.customer.name} — ₹{actual:,.0f} on {cdate}."
            )
            return redirect(reverse("kam:collections_plan"))

        if action == "sync_from_sheet":
            if not _is_manager(request.user):
                return HttpResponseForbidden("403 Forbidden: Manager access required.")
            try:
                result = _sync_overdue_collection_plans()
                messages.success(
                    request,
                    f"Sync complete — {result['synced']} customers synced, "
                    f"{result['skipped']} skipped, "
                    f"{result['unknown_kam']} unknown KAM."
                )
            except Exception as exc:
                logger.exception("Sync from sheet failed")
                messages.error(request, f"Sync failed: {exc}")
            return redirect(reverse("kam:collections_plan"))

        messages.error(request, "Unknown action.")
        return redirect(reverse("kam:collections_plan"))

    # ── GET: build view ─────────────────────────────────────────────────────
    qs = _build_overdue_collection_qs(request.user, scope_kam_id)

    # Status filter
    status_filter = (request.GET.get("status") or "").strip().upper()
    if status_filter in ("OPEN", "PARTIAL", "COLLECTED"):
        qs = qs.filter(collection_status=status_filter)

    # KAM filter (managers/admin only)
    kam_filter = (
        request.GET.get("user") or
        request.GET.get("kam")  or
        ""
    ).strip()
    if kam_filter and _is_manager(request.user):
        filter_user = User.objects.filter(username__iexact=kam_filter, is_active=True).first()
        if filter_user:
            qs = qs.filter(kam=filter_user)

    plans = list(qs.select_related("customer", "kam").order_by("kam__username", "customer__name"))

    # ── Aggregations: Sum / Count — always dynamic ─────────────────────────
    plan_ids = [p.id for p in plans]
    agg = CollectionPlan.objects.filter(id__in=plan_ids).aggregate(
        total_overdue=Sum("overdue_amount"),
        total_collected=Sum("actual_amount"),
    )
    total_customers = len(plans)
    total_overdue   = _safe_decimal(agg.get("total_overdue"))
    total_collected = _safe_decimal(agg.get("total_collected"))
    total_pending   = max(total_overdue - total_collected, Decimal("0"))

    # ── KAM-wise grouping (for manager/admin view) ─────────────────────────
    kam_groups: Dict[str, dict] = {}
    for plan in plans:
        kam_key = plan.kam.username if plan.kam_id else "Unknown"
        if kam_key not in kam_groups:
            kam_groups[kam_key] = {
                "kam":             plan.kam,
                "plans":           [],
                "total_overdue":   Decimal("0"),
                "total_collected": Decimal("0"),
            }
        kam_groups[kam_key]["plans"].append(plan)
        kam_groups[kam_key]["total_overdue"]   += _safe_decimal(plan.overdue_amount)
        kam_groups[kam_key]["total_collected"] += _safe_decimal(plan.actual_amount)

    for g in kam_groups.values():
        g["total_pending"] = max(g["total_overdue"] - g["total_collected"], Decimal("0"))

    ctx = {
        "page_title":     "Collection Tracking",
        "plans":          plans,
        "kam_groups":     list(kam_groups.values()),
        "scope_label":    scope_label,
        "can_choose_kam": _is_manager(request.user),
        "is_manager":     _is_manager(request.user),
        "is_admin":       _is_admin(request.user),
        "kam_options":    _kam_options_for_user(request.user),
        "selected_user":  kam_filter,
        "status_filter":  status_filter,
        "totals": {
            "customers": total_customers,
            "overdue":   total_overdue,
            "collected": total_collected,
            "pending":   total_pending,
        },
    }
    return render(request, "kam/collections_plan.html", ctx)


def _build_overdue_collection_qs(user: User, scope_kam_id: Optional[int]):
    """
    Return CollectionPlan queryset filtered to overdue-driven entries only.
    Scoped by role: KAM → own, Manager → team, Admin → all.
    """
    qs = CollectionPlan.objects.select_related("customer", "kam").filter(
        overdue_amount__gt=0      # only sheet-synced entries
    )
    kam_ids = _scoped_kam_ids(user, scope_kam_id)
    if kam_ids is None:
        return qs                 # admin: all
    if not kam_ids:
        return qs.none()
    return qs.filter(kam_id__in=kam_ids)


def _sync_overdue_collection_plans() -> dict:
    """
    Trigger Google Sheet Overdues tab → CollectionPlan sync.
    Called by manager via POST action='sync_from_sheet'.
    """
    from . import sheets_adapter
    sheet_id    = sheets_adapter._require_env("KAM_SALES_SHEET_ID")
    service     = sheets_adapter.build_sheets_service()
    tab_mapping = sheets_adapter._load_kam_names_tab(service, sheet_id)
    db_lookup   = sheets_adapter._build_user_lookup()
    env_usermap = sheets_adapter._load_env_usermap()
    local_cache: Dict = {}

    stats = sheets_adapter._sync_overdues_to_collection_plan(
        service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache
    )
    return {
        "synced":      stats.customers_upserted,
        "skipped":     stats.skipped,
        "unknown_kam": stats.unknown_kam,
        "notes":       stats.notes,
    }


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_collections_plan")
def collection_plan_record_actual(request: HttpRequest, plan_id: int) -> HttpResponse:
    """Standalone page for recording actual collection (used by direct URL)."""
    plan = get_object_or_404(CollectionPlan, id=plan_id)

    # Security
    if not _is_admin(request.user):
        if _is_manager(request.user):
            if plan.kam_id not in set(_kams_managed_by_manager(request.user)):
                return HttpResponseForbidden("403 Forbidden: Not your team's plan.")
        else:
            if plan.kam_id != request.user.id:
                return HttpResponseForbidden("403 Forbidden: Not your plan.")

    next_url = request.GET.get("next") or reverse("kam:collections_plan")

    if request.method == "POST":
        form = CollectionPlanActualForm(request.POST, instance=plan)
        if form.is_valid():
            form.save()
            messages.success(request, "Actual collection recorded.")
            return redirect(next_url)
        messages.error(request, "Please correct the errors below.")
    else:
        form = CollectionPlanActualForm(instance=plan)

    ctx = {
        "page_title": "Record Actual Collection",
        "plan":       plan,
        "form":       form,
        "next_url":   next_url,
    }
    return render(request, "kam/collection_plan_record_actual.html", ctx)


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_collections_plan")
def collection_plan_delete(request: HttpRequest, plan_id: int) -> HttpResponse:
    """Admin/manager only: delete a collection plan entry that has no actual collected."""
    if request.method != "POST":
        return HttpResponseForbidden("403 Forbidden: POST required.")
    if not _is_admin(request.user):
        return HttpResponseForbidden("403 Forbidden: Admin only.")
    plan = get_object_or_404(CollectionPlan, id=plan_id)
    if plan.actual_amount and plan.actual_amount > 0:
        messages.error(request, "Cannot delete — actual collection already recorded.")
        return redirect(reverse("kam:collections_plan"))
    plan.delete()
    messages.success(request, "Entry deleted.")
    return redirect(reverse("kam:collections_plan"))





@login_required(login_url="/accounts/login/")
@require_kam_code("kam_manager")
def collection_report(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    qs = CollectionPlan.objects.select_related("customer", "kam").filter(overdue_amount__gt=0)

    if not _is_admin(request.user):
        qs = qs.filter(kam_id__in=_kams_managed_by_manager(request.user))

    kam_id = (request.GET.get("kam") or "").strip()
    if kam_id and kam_id.isdigit():
        qs = qs.filter(kam_id=int(kam_id))

    status = (request.GET.get("status") or "").strip().upper()
    if status in {"OPEN", "PARTIAL", "COLLECTED"}:
        qs = qs.filter(collection_status=status)

    agg = qs.aggregate(
        total_overdue=Sum("overdue_amount"),
        total_collected=Sum("actual_amount"),
    )

    total_overdue = _safe_decimal(agg.get("total_overdue"))
    total_collected = _safe_decimal(agg.get("total_collected"))
    total_pending = max(total_overdue - total_collected, Decimal("0"))

    achievement_pct = float((total_collected / total_overdue) * 100) if total_overdue else 0.0

    kam_users = (
        User.objects.filter(is_active=True).order_by("username")
        if _is_admin(request.user)
        else User.objects.filter(
            is_active=True,
            id__in=_kams_managed_by_manager(request.user),
        ).order_by("username")
    )

    ctx = {
        "page_title": "Collection Report",
        "data": qs.order_by("kam__username", "customer__name"),
        "total_overdue": total_overdue,
        "total_collected": total_collected,
        "total_pending": total_pending,
        "achievement_pct": achievement_pct,
        "kam_users": kam_users,
        "selected_kam": kam_id,
        "selected_status": status,

        # Backward-compatible aliases.
        "planned": total_overdue,
        "actual": total_collected,
        "shortfall": total_pending,
    }

    return render(request, "kam/collection_report.html", ctx)

@login_required(login_url="/accounts/login/")
@require_kam_code("kam_collections_plan")
def update_actual_collection(request: HttpRequest, pk: int) -> HttpResponse:
    obj = get_object_or_404(CollectionPlan, pk=pk)

    # Security: KAM can only update their own; superuser/manager can update all
    if not request.user.is_superuser and not _is_manager(request.user):
        if obj.kam_id != request.user.id:
            return HttpResponseForbidden("403 Forbidden: Not your plan.")

    if request.method == "POST":
        actual = request.POST.get("actual_amount")
        if actual:
            try:
                obj.actual_amount = Decimal(actual)
                obj.save(update_fields=["actual_amount"])
                messages.success(request, "Actual amount saved.")
            except Exception:
                messages.error(request, "Invalid amount.")

    return redirect(reverse("kam:collections_plan"))

def _direct_action_page(title: str, message: str, color: str, icon: str) -> HttpResponse:
    """Standalone result page after a direct email action. No f-string CSS."""
    html = (
        "<!DOCTYPE html><html lang='en'><head>"
        "<meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
        "<title>" + title + "</title>"
        "<style>"
        "* { margin:0; padding:0; box-sizing:border-box; }"
        "body { font-family:'Segoe UI',Arial,sans-serif; background:#f0f2f5;"
        "       display:flex; align-items:center; justify-content:center;"
        "       min-height:100vh; padding:20px; }"
        ".card { background:#fff; border-radius:16px; padding:48px 40px;"
        "        max-width:480px; width:100%; text-align:center;"
        "        box-shadow:0 8px 32px rgba(0,0,0,.12); }"
        ".icon { font-size:56px; margin-bottom:16px; }"
        "h1 { font-size:22px; font-weight:700; color:#1e293b; margin-bottom:10px; }"
        "p  { font-size:14px; color:#64748b; line-height:1.6; }"
        ".close-note { margin-top:24px; font-size:12px; color:#94a3b8; }"
        "</style></head><body>"
        "<div class='card'>"
        "<div class='icon'>" + icon + "</div>"
        "<h1>" + title + "</h1>"
        "<p>" + message + "</p>"
        "<p class='close-note'>You can close this tab now.</p>"
        "</div></body></html>"
    )
    return HttpResponse(html)


@csrf_exempt
def direct_single_visit_approve(request: HttpRequest, token: str) -> HttpResponse:
    """
    Direct email approval — no login required.
    Token validates identity. Shows simple result page.
    """
    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        return _direct_action_page(
            "Link Expired", "This approval link has expired (7-day limit). Please ask the KAM to resubmit.",
            "#f59e0b", "⏰"
        )
    except BadSignature:
        return _direct_action_page(
            "Invalid Link", "This link is not valid or has already been used.",
            "#ef4444", "❌"
        )

    if action != "APPROVE":
        return _direct_action_page("Wrong Link", "This link is not an approval link.", "#ef4444", "")

    try:
        with transaction.atomic():
            plan = VisitPlan.objects.select_for_update().filter(id=plan_id, batch__isnull=True).first()
            if not plan:
                return _direct_action_page("Not Found", "Visit plan not found.", "#ef4444", "")
            if plan.approval_status == VisitPlan.APPROVED:
                return _direct_action_page(
                    "Already Approved",
                    f"Single Visit #{plan.id} was already approved earlier.",
                    "#22c55e", "✅"
                )
            if plan.approval_status != VisitPlan.PENDING_APPROVAL:
                return _direct_action_page(
                    "Cannot Approve",
                    f"Visit #{plan.id} is not pending approval (status: {plan.approval_status}).",
                    "#f59e0b", "⚠️"
                )
            now_ts = timezone.now()
            plan.approval_status = VisitPlan.APPROVED
            plan.approved_at = now_ts
            plan.save(update_fields=["approval_status", "approved_at", "updated_at"])
            VisitApprovalAudit.objects.create(
                plan=plan,
                actor=plan.kam,   # log as action by the kam's record (no auth user here)
                action=VisitApprovalAudit.ACTION_APPROVE,
                note="Approved via direct email link (no login required)",
            )
        logger.info("Direct email approval: VisitPlan #%s approved via token", plan_id)
        return _direct_action_page(
            "Visit Approved ✓",
            f"Single Visit #{plan_id} has been approved successfully. The KAM has been notified.",
            "#22c55e", "✅"
        )
    except Exception as exc:
        logger.exception("direct_single_visit_approve failed for plan_id=%s", plan_id)
        return _direct_action_page("Error", f"Something went wrong: {exc}", "#ef4444", "")


@csrf_exempt
def direct_single_visit_reject(request: HttpRequest, token: str) -> HttpResponse:
    """
    Direct email rejection — no login required.
    GET → shows rejection reason form.
    POST → applies rejection.
    """
    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        return _direct_action_page(
            "Link Expired",
            "This reject link has expired (7-day limit).",
            "#f59e0b",
            "⏰",
        )
    except BadSignature:
        return _direct_action_page(
            "Invalid Link",
            "This link is not valid.",
            "#ef4444",
            "❌",
        )

    if action != "REJECT":
        return _direct_action_page(
            "Wrong Link",
            "This link is not a rejection link.",
            "#ef4444",
            "❌",
        )

    plan = VisitPlan.objects.select_related("kam", "customer").filter(
        id=plan_id,
        batch__isnull=True,
    ).first()

    if not plan:
        return _direct_action_page(
            "Not Found",
            "Visit plan not found.",
            "#ef4444",
            "❌",
        )

    if plan.approval_status == VisitPlan.REJECTED:
        return _direct_action_page(
            "Already Rejected",
            f"Visit #{plan_id} was already rejected.",
            "#f59e0b",
            "⚠️",
        )

    if request.method == "GET":
        kam_name = (
            plan.kam.get_full_name()
            or plan.kam.username
            if plan.kam
            else "KAM"
        )

        counterparty = (
            plan.customer.name
            if plan.customer_id and plan.customer
            else (plan.counterparty_name or "—")
        )

        html = (
            "<!DOCTYPE html>"
            "<html lang='en'><head>"
            "<meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
            "<title>Reject Visit #" + str(plan_id) + "</title>"
            "<style>"
            "* { margin:0; padding:0; box-sizing:border-box; }"
            "body { font-family:'Segoe UI',Arial,sans-serif; background:#f0f2f5;"
            "       display:flex; align-items:center; justify-content:center;"
            "       min-height:100vh; padding:20px; }"
            ".card { background:#fff; border-radius:16px; padding:40px;"
            "        max-width:500px; width:100%; box-shadow:0 8px 32px rgba(0,0,0,.12); }"
            "h1 { font-size:20px; font-weight:700; color:#1e293b; margin-bottom:6px; }"
            ".sub { font-size:13px; color:#64748b; margin-bottom:24px; line-height:1.5; }"
            ".info { background:#fef2f2; border:1px solid #fecaca; border-radius:10px;"
            "        padding:14px 16px; font-size:13px; color:#991b1b; margin-bottom:20px; }"
            "label { font-size:12px; font-weight:700; color:#374151; text-transform:uppercase;"
            "        letter-spacing:.05em; display:block; margin-bottom:6px; }"
            "textarea { width:100%; padding:12px; border:1px solid #d1d5db; border-radius:8px;"
            "           font-size:13px; font-family:inherit; resize:vertical; outline:none; }"
            "textarea:focus { border-color:#ef4444; }"
            ".btn-reject { display:block; width:100%; padding:14px; background:#dc2626;"
            "              color:#fff; border:none; border-radius:8px; font-size:15px;"
            "              font-weight:700; cursor:pointer; margin-top:16px; }"
            "</style></head><body>"
            "<div class='card'>"
            "<h1>Reject Visit #" + str(plan_id) + "</h1>"
            "<p class='sub'>KAM: " + str(kam_name) + "<br>"
            "Entity: " + str(counterparty) + "<br>"
            "Visit Date: " + str(plan.visit_date) + "</p>"
            "<div class='info'>"
            "<strong>You are about to reject this visit.</strong><br>"
            "Please provide a reason so the KAM can address it and resubmit."
            "</div>"
            "<form method='post'>"
            "<label for='reason'>Rejection Reason *</label>"
            "<textarea id='reason' name='reason' rows='4'"
            "  placeholder='Enter reason for rejection...' required></textarea>"
            "<button type='submit' class='btn-reject'>Reject This Visit</button>"
            "</form>"
            "</div></body></html>"
        )
        return HttpResponse(html)

    reason = (request.POST.get("reason") or "").strip()

    if not reason:
        return _direct_action_page(
            "Reason Required",
            "Please go back and provide a rejection reason.",
            "#f59e0b",
            "⚠️",
        )

    try:
        with transaction.atomic():
            plan = VisitPlan.objects.select_for_update().filter(
                id=plan_id,
                batch__isnull=True,
            ).first()

            if not plan:
                return _direct_action_page(
                    "Not Found",
                    "Visit plan not found.",
                    "#ef4444",
                    "❌",
                )

            if plan.approval_status != VisitPlan.PENDING_APPROVAL:
                return _direct_action_page(
                    "Cannot Reject",
                    f"Visit #{plan_id} is not pending approval.",
                    "#f59e0b",
                    "⚠️",
                )

            now_ts = timezone.now()

            plan.approval_status = VisitPlan.REJECTED
            plan.rejected_at = now_ts
            plan.rejection_reason = reason
            plan.save(
                update_fields=[
                    "approval_status",
                    "rejected_at",
                    "rejection_reason",
                    "updated_at",
                ]
            )

            VisitApprovalAudit.objects.create(
                plan=plan,
                actor=plan.kam,
                action=VisitApprovalAudit.ACTION_REJECT,
                note=f"[DIRECT EMAIL] {reason[:255]}",
            )

        logger.info(
            "Direct email rejection: VisitPlan #%s rejected via token. Reason: %s",
            plan_id,
            reason[:100],
        )

        return _direct_action_page(
            "Visit Rejected",
            f"Visit #{plan_id} has been rejected. Reason recorded: {reason[:100]}. The KAM will be notified.",
            "#ef4444",
            "❌",
        )

    except Exception as exc:
        logger.exception(
            "direct_single_visit_reject failed for plan_id=%s",
            plan_id,
        )
        return _direct_action_page(
            "Error",
            f"Something went wrong: {exc}",
            "#ef4444",
            "❌",
        )


@csrf_exempt
def direct_batch_approve(request: HttpRequest, token: str) -> HttpResponse:
    """Direct email batch approval — no login required."""
    try:
        batch_id, action = _parse_batch_token(token)
    except SignatureExpired:
        return _direct_action_page("Link Expired", "This approval link expired (7-day limit).", "#f59e0b", "")
    except BadSignature:
        return _direct_action_page("Invalid Link", "This link is not valid.", "#ef4444", "")

    if action != "APPROVE":
        return _direct_action_page("Wrong Link", "This is not an approval link.", "#ef4444", "")

    try:
        with transaction.atomic():
            batch = VisitBatch.objects.select_for_update().filter(id=batch_id).first()
            if not batch:
                return _direct_action_page("Not Found", "Batch not found.", "#ef4444", "")
            if batch.approval_status == STATUS_APPROVED:
                return _direct_action_page("Already Approved", f"Batch #{batch_id} was already approved.", "#22c55e", "")
            if batch.approval_status not in {STATUS_PENDING_APPROVAL, STATUS_PENDING_LEGACY}:
                return _direct_action_page("Cannot Approve", f"Batch #{batch_id} is not pending approval.", "#f59e0b", "")
            now_ts = timezone.now()
            batch.approval_status = STATUS_APPROVED
            batch.approved_at = now_ts
            batch.save(update_fields=["approval_status", "approved_at", "updated_at"])
            VisitPlan.objects.filter(batch=batch).update(
                approval_status=STATUS_APPROVED, approved_at=now_ts, updated_at=now_ts
            )
            VisitApprovalAudit.objects.create(
                batch=batch, actor=batch.kam,
                action=VisitApprovalAudit.ACTION_APPROVE,
                note="Approved via direct email link (no login required)",
            )
        logger.info("Direct email approval: Batch #%s approved", batch_id)
        return _direct_action_page(
            "Batch Approved ✓",
            f"Visit Batch #{batch_id} has been approved. All visits in this batch are now approved.",
            "#22c55e", ""
        )
    except Exception as exc:
        logger.exception("direct_batch_approve failed for batch_id=%s", batch_id)
        return _direct_action_page("Error", f"Something went wrong: {exc}", "#ef4444", "")


@csrf_exempt
def direct_batch_reject(request: HttpRequest, token: str) -> HttpResponse:
    """
    Direct email batch rejection — no login required.
    GET → shows rejection reason form.
    POST → applies rejection.
    """
    try:
        batch_id, action = _parse_batch_token(token)
    except SignatureExpired:
        return _direct_action_page(
            "Link Expired",
            "This reject link expired (7-day limit).",
            "#f59e0b",
            "⏰",
        )
    except BadSignature:
        return _direct_action_page(
            "Invalid Link",
            "This link is not valid.",
            "#ef4444",
            "❌",
        )

    if action != "REJECT":
        return _direct_action_page(
            "Wrong Link",
            "This is not a rejection link.",
            "#ef4444",
            "❌",
        )

    batch = VisitBatch.objects.select_related("kam").filter(id=batch_id).first()

    if not batch:
        return _direct_action_page(
            "Not Found",
            "Batch not found.",
            "#ef4444",
            "❌",
        )

    if batch.approval_status == STATUS_REJECTED:
        return _direct_action_page(
            "Already Rejected",
            f"Batch #{batch_id} was already rejected.",
            "#f59e0b",
            "⚠️",
        )

    if request.method == "GET":
        kam_name = (
            batch.kam.get_full_name()
            or batch.kam.username
            if batch.kam
            else "KAM"
        )

        html = (
            "<!DOCTYPE html>"
            "<html lang='en'><head>"
            "<meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
            "<title>Reject Batch #" + str(batch_id) + "</title>"
            "<style>"
            "* { margin:0; padding:0; box-sizing:border-box; }"
            "body { font-family:'Segoe UI',Arial,sans-serif; background:#f0f2f5;"
            "       display:flex; align-items:center; justify-content:center;"
            "       min-height:100vh; padding:20px; }"
            ".card { background:#fff; border-radius:16px; padding:40px;"
            "        max-width:500px; width:100%; box-shadow:0 8px 32px rgba(0,0,0,.12); }"
            "h1 { font-size:20px; font-weight:700; color:#1e293b; margin-bottom:6px; }"
            ".sub { font-size:13px; color:#64748b; margin-bottom:24px; }"
            ".info { background:#fef2f2; border:1px solid #fecaca; border-radius:10px;"
            "        padding:14px 16px; font-size:13px; color:#991b1b; margin-bottom:20px; }"
            "label { font-size:12px; font-weight:700; color:#374151; text-transform:uppercase;"
            "        letter-spacing:.05em; display:block; margin-bottom:6px; }"
            "textarea { width:100%; padding:12px; border:1px solid #d1d5db; border-radius:8px;"
            "           font-size:13px; font-family:inherit; resize:vertical; outline:none; }"
            "textarea:focus { border-color:#ef4444; }"
            ".btn-reject { display:block; width:100%; padding:14px; background:#dc2626;"
            "              color:#fff; border:none; border-radius:8px; font-size:15px;"
            "              font-weight:700; cursor:pointer; margin-top:16px; }"
            "</style></head><body>"
            "<div class='card'>"
            "<h1>Reject Batch #" + str(batch_id) + "</h1>"
            "<p class='sub'>KAM: " + str(kam_name) + " &mdash; "
            + str(batch.from_date) + " &rarr; " + str(batch.to_date) + "</p>"
            "<div class='info'>"
            "<strong>You are about to reject this visit batch.</strong><br>"
            "Please provide a reason so the KAM can address it and resubmit."
            "</div>"
            "<form method='post'>"
            "<label for='reason'>Rejection Reason *</label>"
            "<textarea id='reason' name='reason' rows='4'"
            "  placeholder='Enter reason for rejection...' required></textarea>"
            "<button type='submit' class='btn-reject'>Reject This Batch</button>"
            "</form>"
            "</div></body></html>"
        )
        return HttpResponse(html)

    reason = (request.POST.get("reason") or "").strip()

    if not reason:
        return _direct_action_page(
            "Reason Required",
            "Please provide a rejection reason.",
            "#f59e0b",
            "⚠️",
        )

    try:
        with transaction.atomic():
            batch = VisitBatch.objects.select_for_update().filter(id=batch_id).first()

            if not batch:
                return _direct_action_page(
                    "Not Found",
                    "Batch not found.",
                    "#ef4444",
                    "❌",
                )

            if batch.approval_status not in {
                STATUS_PENDING_APPROVAL,
                STATUS_PENDING_LEGACY,
            }:
                return _direct_action_page(
                    "Cannot Reject",
                    "Batch is not pending approval.",
                    "#f59e0b",
                    "⚠️",
                )

            now_ts = timezone.now()

            batch.approval_status = STATUS_REJECTED
            batch.rejected_at = now_ts
            batch.rejection_reason = reason
            batch.save(
                update_fields=[
                    "approval_status",
                    "rejected_at",
                    "rejection_reason",
                    "updated_at",
                ]
            )

            VisitPlan.objects.filter(batch=batch).update(
                approval_status=STATUS_REJECTED,
                updated_at=now_ts,
            )

            VisitApprovalAudit.objects.create(
                batch=batch,
                actor=batch.kam,
                action=VisitApprovalAudit.ACTION_REJECT,
                note=f"[DIRECT EMAIL] {reason[:255]}",
            )

        logger.info(
            "Direct email rejection: Batch #%s rejected. Reason: %s",
            batch_id,
            reason[:100],
        )

        return _direct_action_page(
            "Batch Rejected",
            f"Batch #{batch_id} rejected. Reason: {reason[:100]}. KAM will be notified.",
            "#ef4444",
            "❌",
        )

    except Exception as exc:
        logger.exception(
            "direct_batch_reject failed for batch_id=%s",
            batch_id,
        )
        return _direct_action_page(
            "Error",
            f"Something went wrong: {exc}",
            "#ef4444",
            "❌",
        )


# =====================================================================
# Sync endpoints
# =====================================================================
@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
@require_kam_code("kam_sync_trigger")
def sync_trigger(request: HttpRequest) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    token = timezone.now().strftime("%Y%m%d%H%M%S") + f"_{request.user.id}"
    intent = SyncIntent.objects.create(token=token, created_by=request.user, scope=SyncIntent.SCOPE_TEAM)
    messages.success(request, f"Sync triggered (token={intent.token}).")
    return redirect(reverse("kam:dashboard"))


@login_required(login_url="/accounts/login/")
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

# =====================================================================
# URL BACKWARD-COMPATIBILITY FIXES
# Required by apps/kam/urls.py
# =====================================================================

@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def single_visit_detail(request: HttpRequest, plan_id: int) -> HttpResponse:
    return visit_history_edit(request, plan_id)


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def single_visit_edit(request: HttpRequest, plan_id: int) -> HttpResponse:
    return visit_history_edit(request, plan_id)


@login_required(login_url="/accounts/login/")
def single_visit_approve_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        messages.error(request, "Approval link has expired.")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid approval link.")
        return redirect(reverse("kam:visit_batches"))

    if action != "APPROVE":
        messages.error(request, "Invalid approval action.")
        return redirect(reverse("kam:visit_batches"))

    with transaction.atomic():
        plan = get_object_or_404(
            VisitPlan.objects.select_for_update().select_related("customer", "kam"),
            id=plan_id,
        )

        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")

        if plan.approval_status == VisitPlan.APPROVED:
            messages.info(request, f"Visit #{plan.id} is already approved.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

        if plan.approval_status not in {
            VisitPlan.PENDING_APPROVAL,
            getattr(VisitPlan, "PENDING", "PENDING"),
        }:
            messages.error(request, f"Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

        now_ts = timezone.now()

        plan.approval_status = VisitPlan.APPROVED
        plan.approved_by = request.user
        plan.approved_at = now_ts
        plan.rejected_by = None
        plan.rejected_at = None
        plan.rejection_reason = None

        plan.save(
            update_fields=[
                "approval_status",
                "approved_by",
                "approved_at",
                "rejected_by",
                "rejected_at",
                "rejection_reason",
                "updated_at",
            ]
        )

        VisitApprovalAudit.objects.create(
            plan=plan,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_APPROVE,
            note="Approved via single visit email approval link",
            actor_ip=_get_ip(request),
        )

    _notify_kam_single_visit_decision(
        request=request,
        plan=plan,
        actor=request.user,
        status="APPROVED",
    )

    messages.success(request, f"Visit #{plan.id} approved successfully.")
    return redirect(reverse("kam:single_visit_detail", args=[plan.id]))


@login_required(login_url="/accounts/login/")
def single_visit_reject_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")

    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        messages.error(request, "Reject link has expired.")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid reject link.")
        return redirect(reverse("kam:visit_batches"))

    if action != "REJECT":
        messages.error(request, "Invalid reject action.")
        return redirect(reverse("kam:visit_batches"))

    plan = get_object_or_404(
        VisitPlan.objects.select_related("customer", "kam"),
        id=plan_id,
    )

    if request.method == "GET":
        return render(
            request,
            "kam/single_visit_reject_reason.html",
            {
                "plan": plan,
                "token": token,
                "page_title": f"Reject Visit #{plan.id}",
                "visit_category_label": _VISIT_CATEGORY_LABELS.get(
                    plan.visit_category,
                    plan.visit_category,
                ),
            },
        )

    reason = (request.POST.get("reason") or "").strip()

    if not reason:
        messages.error(request, "Rejection reason is required.")
        return render(
            request,
            "kam/single_visit_reject_reason.html",
            {
                "plan": plan,
                "token": token,
                "page_title": f"Reject Visit #{plan.id}",
                "visit_category_label": _VISIT_CATEGORY_LABELS.get(
                    plan.visit_category,
                    plan.visit_category,
                ),
                "error": "Rejection reason is required.",
            },
        )

    with transaction.atomic():
        plan = get_object_or_404(
            VisitPlan.objects.select_for_update().select_related("customer", "kam"),
            id=plan_id,
        )

        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")

        if plan.approval_status == VisitPlan.REJECTED:
            messages.info(request, f"Visit #{plan.id} is already rejected.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

        if plan.approval_status not in {
            VisitPlan.PENDING_APPROVAL,
            getattr(VisitPlan, "PENDING", "PENDING"),
        }:
            messages.error(request, f"Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

        now_ts = timezone.now()

        plan.approval_status = VisitPlan.REJECTED
        plan.rejected_by = request.user
        plan.rejected_at = now_ts
        plan.rejection_reason = reason

        plan.save(
            update_fields=[
                "approval_status",
                "rejected_by",
                "rejected_at",
                "rejection_reason",
                "updated_at",
            ]
        )

        VisitApprovalAudit.objects.create(
            plan=plan,
            actor=request.user,
            action=VisitApprovalAudit.ACTION_REJECT,
            note=reason[:255],
            actor_ip=_get_ip(request),
        )

    _notify_kam_single_visit_decision(
        request=request,
        plan=plan,
        actor=request.user,
        status="REJECTED",
        rejection_reason=reason,
    )

    messages.info(request, f"Visit #{plan.id} rejected successfully.")
    return redirect(reverse("kam:single_visit_detail", args=[plan.id]))