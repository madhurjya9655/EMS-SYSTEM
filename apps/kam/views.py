# FILE: apps/kam/views.py
# FIXES APPLIED:
#   FIX 2 — All datetime objects made timezone-aware via timezone.make_aware()
#   FIX 3 — Customer 404 replaced with safe fallback (never crash on invalid ?id=)
#   FIX 5 — All @login_required use explicit login_url='/accounts/login/'
from __future__ import annotations

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

    This sender:
    - sends proper HTML email when body contains HTML
    - also sends plain-text fallback using EmailMultiAlternatives
    - keeps TO and CC clean
    - removes duplicate emails
    - prevents same email appearing in both TO and CC
    - logs failures instead of hiding them silently
    """
    try:
        def _email_from_user(user) -> str:
            return (getattr(user, "email", "") or "").strip()

        def _uniq_email_list(emails: List[str]) -> List[str]:
            seen = set()
            output = []

            for email in emails:
                clean = (email or "").strip()
                if not clean:
                    continue

                key = clean.lower()
                if key in seen:
                    continue

                seen.add(key)
                output.append(clean)

            return output

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

        # Do not keep the same email in both TO and CC.
        to_email_keys = {email.lower() for email in to_emails}
        cc_emails = [
            email
            for email in cc_emails
            if email.lower() not in to_email_keys
        ]

        if not to_emails and not cc_emails:
            logger.warning(
                "KAM email skipped because no valid recipients were found. subject=%r",
                subject,
            )
            return False

        # If only CC exists, move CC to TO so email delivery still works.
        final_to = to_emails or cc_emails
        final_cc = cc_emails if to_emails else []

        from_email = (
            getattr(settings, "DEFAULT_FROM_EMAIL", None)
            or getattr(settings, "EMAIL_HOST_USER", None)
        )

        is_html = "<html" in (body or "").lower()

        if is_html:
            plain_body = strip_tags(body or "")
            plain_body = "\n".join(
                line.strip()
                for line in plain_body.splitlines()
                if line.strip()
            )
            if not plain_body:
                plain_body = "BOS Lakshya ERP notification."
        else:
            plain_body = body or "BOS Lakshya ERP notification."

        logger.info(
            "KAM Mail Debug -> subject=%r TO=%s CC=%s is_html=%s",
            subject,
            final_to,
            final_cc,
            is_html,
        )

        email = EmailMultiAlternatives(
            subject=subject,
            body=plain_body,
            from_email=from_email,
            to=final_to,
            cc=final_cc,
        )

        if is_html:
            email.attach_alternative(body, "text/html")

        sent_count = email.send(fail_silently=False)

        logger.info(
            "KAM email send attempted. subject=%r to=%s cc=%s sent_count=%s",
            subject,
            final_to,
            final_cc,
            sent_count,
        )

        return bool(sent_count)

    except Exception:
        logger.exception(
            "KAM email send failed. subject=%r to_users=%s cc_users=%s",
            subject,
            [getattr(user, "username", None) for user in (to_users or [])],
            [getattr(user, "username", None) for user in (cc_users or [])],
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
    if not manager_user or not getattr(manager_user, "id", None):
        return []
    if _is_admin(manager_user):
        return list(User.objects.filter(is_active=True).values_list("id", flat=True))
    return list(
        KamManagerMapping.objects.filter(manager=manager_user, active=True)
        .values_list("kam_id", flat=True)
        .distinct()
    )

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
    Canonical customer scope for KAM Plan Visit and customer APIs.

    Visibility rules:
      Admin   -> all valid customers
      Manager -> customers owned by mapped/team KAMs
      KAM     -> own customers only

    Sources used:
      1. Customer.kam / Customer.primary_kam
      2. KAMAssignment
      3. InvoiceFact.kam
      4. LeadFact.kam
      5. CollectionTxn.kam

    Important:
      Sheet-synced customers may have kam/primary_kam NULL, so related facts
      and assignment table are also considered.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return Customer.objects.none()

    base_qs = (
        Customer.objects
        .filter(name__isnull=False)
        .exclude(name__exact="")
    )

    if _is_admin(user):
        return base_qs.distinct()

    def _customers_for_kam_ids(kam_ids):
        kam_ids = [int(k) for k in kam_ids or [] if k]
        if not kam_ids:
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

        assignment_customer_ids = (
            KAMAssignment.objects
            .filter(kam_id__in=kam_ids)
            .filter(Q(active_to__isnull=True) | Q(active_to__gte=timezone.localdate()))
            .values_list("customer_id", flat=True)
        )

        return (
            base_qs
            .filter(
                Q(kam_id__in=kam_ids)
                | Q(primary_kam_id__in=kam_ids)
                | Q(id__in=invoice_customer_ids)
                | Q(id__in=lead_customer_ids)
                | Q(id__in=collection_customer_ids)
                | Q(id__in=assignment_customer_ids)
            )
            .distinct()
        )

    if _is_manager(user):
        kam_ids = set(_kams_managed_by_manager(user))

        try:
            from apps.leave.models import ApproverMapping
            kam_ids.update(
                ApproverMapping.objects
                .filter(reporting_person=user)
                .values_list("employee_id", flat=True)
            )
        except Exception:
            logger.exception(
                "_customer_qs_for_user: ApproverMapping lookup failed for manager_id=%s",
                getattr(user, "id", None),
            )

        try:
            kam_ids.update(
                User.objects
                .filter(profile__reporting_officer=user, is_active=True)
                .values_list("id", flat=True)
            )
        except Exception:
            logger.exception(
                "_customer_qs_for_user: profile reporting officer lookup failed for manager_id=%s",
                getattr(user, "id", None),
            )

        return _customers_for_kam_ids(kam_ids).order_by("name", "code")

    return _customers_for_kam_ids([user.id]).order_by("name", "code")

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


def _parse_decimal_or_none(s: str) -> Optional[Decimal]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


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
            # FIX 2: timezone-aware boundary datetimes
            s = timezone.make_aware(timezone.datetime(2000, 1, 1))
            e = timezone.make_aware(timezone.datetime(2100, 1, 1))
            return s, e, "ALL"

    from_s = _first_query_value(request, "from", "from_date", "start_date", "date_from", "fromDate", "startDate", "dateFrom")
    to_s = _first_query_value(request, "to", "to_date", "end_date", "date_to", "toDate", "endDate", "dateTo")
    from_d = _parse_iso_date(from_s)
    to_d = _parse_iso_date(to_s)

    if from_d and to_d and from_d <= to_d:
        # FIX 2: make_aware on all constructed datetimes
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        end = timezone.make_aware(timezone.datetime(to_d.year, to_d.month, to_d.day, 0, 0, 0)) + timezone.timedelta(days=1)
        return start, end, f"{from_d} → {to_d}"
    if from_d and not to_d:
        start = timezone.make_aware(timezone.datetime(from_d.year, from_d.month, from_d.day, 0, 0, 0))
        return start, start + timezone.timedelta(days=1), f"{from_d} → {from_d}"

    today_local = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
    m, y = today_local.month, today_local.year
    fy_start_year = y if m >= 4 else y - 1
    # FIX 2: make_aware for fiscal year start
    fy_start = timezone.make_aware(timezone.datetime(fy_start_year, 4, 1, 0, 0, 0))
    fy_end = today_local + timezone.timedelta(days=1)
    return fy_start, fy_end, f"{fy_start.date()} → {today_local.date()} (Fiscal YTD)"


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
    try:
        kam_user = plan.kam
        if not kam_user or not getattr(kam_user, "email", None):
            return
        visit_category_label = _VISIT_CATEGORY_LABELS.get(plan.visit_category, plan.visit_category)
        counterparty = plan.customer.name if plan.customer_id else (plan.counterparty_name or "—")
        plan_url = request.build_absolute_uri(reverse("kam:single_visit_detail", args=[plan.id]))
        subject = f"[KAM] Single Visit #{plan.id} {status}: {plan.visit_date} — {visit_category_label}"
        try:
            body = render_to_string("kam/emails/single_visit_status.html", {
                "plan": plan, "kam_user": kam_user, "actor": actor, "status": status,
                "visit_category_label": visit_category_label, "counterparty": counterparty,
                "rejection_reason": rejection_reason, "plan_url": plan_url,
            })
        except Exception:
            body = (
                f"Single Visit #{plan.id} has been {status}.\n"
                f"Category: {visit_category_label}\nEntity: {counterparty}\n"
                f"Date: {plan.visit_date}\nDecided by: {actor.get_full_name() or actor.username}\n"
            )
            if rejection_reason:
                body += f"\nRejection Reason:\n{rejection_reason}\n"
            body += f"\nView visit: {plan_url}\n"
        _send_safe_mail(subject, body, [kam_user])
    except Exception:
        pass


def _notify_kam_batch_decision(*, request, batch, actor, status, rejection_reason="") -> None:
    try:
        kam_user = batch.kam
        if not kam_user or not getattr(kam_user, "email", None):
            return
        visit_category_label = _VISIT_CATEGORY_LABELS.get(batch.visit_category, batch.visit_category)
        batch_url = request.build_absolute_uri(reverse("kam:visit_batch_detail", args=[batch.id]))
        subject = f"[KAM] Batch #{batch.id} {status}: {batch.from_date}..{batch.to_date} — {visit_category_label}"
        try:
            body = render_to_string("kam/emails/visit_batch_status.html", {
                "batch": batch, "kam_user": kam_user, "actor": actor, "status": status,
                "visit_category_label": visit_category_label,
                "rejection_reason": rejection_reason, "batch_url": batch_url,
            })
        except Exception:
            body = (
                f"Batch #{batch.id} has been {status}.\n"
                f"Category: {visit_category_label}\n"
                f"Date Range: {batch.from_date} to {batch.to_date}\n"
                f"Decided by: {actor.get_full_name() or actor.username}\n"
            )
            if rejection_reason:
                body += f"\nRejection Reason:\n{rejection_reason}\n"
            body += f"\nView batch: {batch_url}\n"
        _send_safe_mail(subject, body, [kam_user])
    except Exception:
        pass


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

    inv_qs = InvoiceFact.objects.filter(
    invoice_date__gte=start_date,
    invoice_date__lt=end_date,
    )
    inv_qs = _filter_qs_by_kam_scope(inv_qs, request.user, scope_kam_id, "kam_id")
    inv_qs = _sales_converted_qs(inv_qs)

    visit_plan_qs = VisitPlan.objects.filter(visit_date__gte=start_date, visit_date__lt=end_date)
    visit_act_qs = VisitActual.objects.filter(plan__visit_date__gte=start_date, plan__visit_date__lt=end_date)
    call_qs = CallLog.objects.filter(call_datetime__gte=start_dt, call_datetime__lt=end_dt)
    lead_qs = LeadFact.objects.filter(doe__gte=start_date, doe__lt=end_date)
    coll_qs = CollectionTxn.objects.filter(txn_datetime__gte=start_dt, txn_datetime__lt=end_dt)

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

    won_status_q = _lead_won_q()
    leads_agg = lead_qs.aggregate(
        total_mt=Sum("qty_mt"),
        won_mt=Sum("qty_mt", filter=Q(status="WON")),
    )
    leads_total_mt = _safe_decimal(leads_agg.get("total_mt"))
    leads_won_mt = _safe_decimal(leads_agg.get("won_mt"))
    leads_total_count = lead_qs.count()
    leads_converted_count = lead_qs.filter(status="WON").count()
    leads_converted_value = _safe_decimal(
        lead_qs.filter(status="WON").aggregate(v=Sum("qty_mt")).get("v")
    )
    collections_actual = _safe_decimal(coll_qs.aggregate(total_amt=Sum("amount")).get("total_amt"))

    if scope_kam_id is not None:
        customer_ids_for_scope = list(
            Customer.objects.filter(Q(kam_id=scope_kam_id) | Q(primary_kam_id=scope_kam_id)).values_list("id", flat=True)
        )
    else:
        customer_ids_for_scope = list(_customer_qs_for_user(request.user).values_list("id", flat=True))

    # ── Collection Plan Aggregation (overdue-driven, sheet = source of truth) ──
    cp_qs = CollectionPlan.objects.filter(overdue_amount__gt=0)
    cp_qs = _filter_qs_by_kam_scope(cp_qs, request.user, scope_kam_id, "kam_id")
    cp_agg = cp_qs.aggregate(
        total_overdue=Sum("overdue_amount"),
        total_actual=Sum("actual_amount"),
    )
    collection_total_customers = cp_qs.count()
    collection_overdue         = _safe_decimal(cp_agg.get("total_overdue"))
    collection_actual_plan     = _safe_decimal(cp_agg.get("total_actual"))
    collection_pending         = max(collection_overdue - collection_actual_plan, Decimal("0"))

    # Backward-compat aliases used in ctx and _pct() calls below
    collection_planned  = collection_overdue
    collections_planned = collection_overdue

    overdue_sum = collection_overdue

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
        inv_i = _filter_qs_by_kam_scope(
            InvoiceFact.objects.filter(invoice_date__gte=start_i.date(), invoice_date__lt=end_i.date()),
            request.user, scope_kam_id, "kam_id"
        )
        inv_i = _preferred_inv_qs(inv_i)
        vis_i = _filter_qs_by_kam_scope(VisitPlan.objects.filter(visit_date__gte=start_i.date(), visit_date__lt=end_i.date()), request.user, scope_kam_id, "kam_id")
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
            "overdue_sum": overdue_sum, "prev_overdue_sum": prev_overdue_sum,
            "overdue_reduction_pct": overdue_reduction_pct, "credit_limit_sum": credit_limit_sum,
            "exposure_sum": exposure_sum, "overdue_risk_ratio": overdue_risk_ratio,
            "overdue_snapshot_date": overdue_snapshot_date,
            "prev_overdue_snapshot_date": prev_overdue_snapshot_date,
        },
        "prod_by_grade": prod_by_grade,
        "prod_by_size": prod_by_size,
        "trend_rows": trend_rows,
        "lead_analysis_data": {"total": leads_total_count, "converted": leads_converted_count},
        "collection_analysis_data": {
        "planned": float(collection_planned),
        "actual": float(collection_actual_plan),
        "overdue": float(collection_overdue),
        "pending": float(collection_pending),
        "customers": collection_total_customers,
        },
    }
    return render(request, "kam/kam_dashboard.html", ctx)


# =====================================================================
# TODAY'S DETAILS
# =====================================================================
@login_required(login_url="/accounts/login/")
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
def weekly_plan(request: HttpRequest) -> HttpResponse:
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

    batch_form = VisitBatchForm(prefix=BATCH_PREFIX)

    if "customer" in single_form.fields:
        single_form.fields["customer"].queryset = customer_qs

    if "customers" in batch_form.fields:
        batch_form.fields["customers"].queryset = customer_qs

    if "purpose" in batch_form.fields:
        batch_form.fields["purpose"].required = False

    if request.method == "POST" and not schema_ready:
        messages.error(
            request,
            "Visit workflow database fields are not migrated yet. "
            "Run: python manage.py makemigrations kam && python manage.py migrate",
        )
        return redirect(reverse("kam:plan"))

    # ---------------------------------------------------------------------
    # SINGLE VISIT SUBMIT
    # ---------------------------------------------------------------------
    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "single":
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

        if "customer" in single_form.fields:
            if manual_customer_name or raw_category != "CUSTOMER":
                single_form.fields["customer"].required = False

        if single_form.is_valid():
            submit_action = (
                request.POST.get("submit_action") or "save_draft"
            ).strip().lower()

            plan: VisitPlan = single_form.save(commit=False)
            plan.kam = user
            plan.batch = None

            try:
                profile = getattr(user, "profile", None)
                if profile and getattr(profile, "reporting_officer_id", None):
                    plan.reporting_officer = profile.reporting_officer
            except Exception:
                logger.exception(
                    "Failed to assign reporting officer on single visit draft. user_id=%s",
                    getattr(user, "id", None),
                )

            if plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if manual_customer_name:
                    try:
                        customer_obj, created = Customer.objects.get_or_create(
                            name__iexact=manual_customer_name,
                            defaults={
                                "name": manual_customer_name,
                                "kam": user,
                                "primary_kam": user,
                                "source": Customer.SOURCE_MANUAL,
                                "created_by": user,
                            },
                        )

                        changed = False

                        if not customer_obj.kam_id:
                            customer_obj.kam = user
                            changed = True

                        if not customer_obj.primary_kam_id:
                            customer_obj.primary_kam = user
                            changed = True

                        if changed:
                            customer_obj.save(update_fields=["kam", "primary_kam", "updated_at"])

                        plan.customer = customer_obj

                        if created:
                            messages.info(
                                request,
                                f"New customer '{customer_obj.name}' created automatically.",
                            )

                    except Exception as exc:
                        logger.exception(
                            "Failed to get_or_create manual customer: %s",
                            manual_customer_name,
                        )
                        messages.error(
                            request,
                            f"Could not create customer '{manual_customer_name}': {exc}",
                        )
                        return redirect(reverse("kam:plan"))

                elif plan.customer_id:
                    if not customer_qs.filter(id=plan.customer_id).exists():
                        messages.error(
                            request,
                            "Invalid customer selection (out of your scope).",
                        )
                        return redirect(reverse("kam:plan"))

                else:
                    messages.error(
                        request,
                        "Customer is required. Select an existing customer or enter a new one.",
                    )
                    return redirect(reverse("kam:plan"))

            else:
                plan.customer = None

            if not (plan.location or "").strip():
                if (
                    plan.visit_category == VisitPlan.CAT_CUSTOMER
                    and plan.customer
                    and plan.customer.address
                ):
                    plan.location = plan.customer.address

            if submit_action == "save_draft":
                plan.approval_status = VisitPlan.DRAFT
                plan.save()

                VisitApprovalAudit.objects.create(
                    plan=plan,
                    actor=user,
                    action=VisitApprovalAudit.ACTION_SUBMIT,
                    note="Saved as draft",
                    actor_ip=_get_ip(request),
                )

                messages.success(request, f"Single visit saved as Draft (#{plan.id}).")
                return redirect(reverse("kam:plan"))

            if submit_action == "submit_to_manager":
                mgr_user = _active_manager_for_kam(user)

                if not mgr_user or not getattr(mgr_user, "email", None):
                    messages.error(
                        request,
                        "No reporting officer assigned to your profile. "
                        "Contact admin to set your Reporting Officer in your profile.",
                    )
                    return redirect(reverse("kam:plan"))

                with transaction.atomic():
                    plan.approval_status = VisitPlan.PENDING_APPROVAL
                    plan.submitted_at = timezone.now()
                    plan.save()

                    VisitApprovalAudit.objects.create(
                        plan=plan,
                        actor=user,
                        action=VisitApprovalAudit.ACTION_SUBMIT,
                        note="Submitted to manager for approval",
                        actor_ip=_get_ip(request),
                    )

                approve_token = _make_single_token(plan.id, "APPROVE")
                reject_token = _make_single_token(plan.id, "REJECT")

                approve_url = request.build_absolute_uri(
                    reverse("kam:single_visit_approve_link", args=[approve_token])
                )

                reject_url = request.build_absolute_uri(
                    reverse("kam:single_visit_reject_link", args=[reject_token])
                )

                subject = (
                    f"[KAM] Approval Required: Single Visit #{plan.id} "
                    f"({plan.visit_date}) - {user.get_full_name() or user.username}"
                )

                cc_users = _active_cc_for_kam(user)

                html_body = _build_single_visit_approval_email(
                    request=request,
                    plan=plan,
                    kam_user=user,
                    manager_user=mgr_user,
                    approve_url=approve_url,
                    reject_url=reject_url,
                    cc_users=cc_users,
                )

                sent_ok = _send_safe_mail(
                    subject,
                    html_body,
                    [mgr_user],
                    cc_users,
                )

                if not sent_ok:
                    logger.warning(
                        "Approval email could not be sent for single visit #%s",
                        plan.id,
                    )

                messages.success(
                    request,
                    f"Single visit #{plan.id} submitted for manager approval.",
                )
                return redirect(reverse("kam:plan"))

            messages.error(request, "Invalid submit action.")
            return redirect(reverse("kam:plan"))

        messages.error(request, "Single visit has errors. Please correct and save again.")

    # ---------------------------------------------------------------------
    # BATCH VISIT SUBMIT
    # ---------------------------------------------------------------------
    if request.method == "POST" and (request.POST.get("mode") or "").strip().lower() == "batch":
        batch_form = VisitBatchForm(request.POST, prefix=BATCH_PREFIX)

        if "customers" in batch_form.fields:
            batch_form.fields["customers"].queryset = customer_qs

        if "purpose" in batch_form.fields:
            batch_form.fields["purpose"].required = False

        action = (
            request.POST.get("action")
            or request.POST.get("submit_action")
            or ""
        ).strip().lower()

        proceed_flag = action in {
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
            remarks = (batch_form.cleaned_data.get("purpose") or "").strip()

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
                    manual_customer, created = Customer.objects.get_or_create(
                        name__iexact=manual_name,
                        defaults={
                            "name": manual_name,
                            "kam": user,
                            "primary_kam": user,
                            "source": Customer.SOURCE_MANUAL,
                            "created_by": user,
                        },
                    )

                    changed = False

                    if not manual_customer.kam_id:
                        manual_customer.kam = user
                        changed = True

                    if not manual_customer.primary_kam_id:
                        manual_customer.primary_kam = user
                        changed = True

                    if changed:
                        manual_customer.save(update_fields=["kam", "primary_kam", "updated_at"])

                    if manual_customer.id not in selected_ids:
                        selected_ids.append(manual_customer.id)

                    if created:
                        logger.info(
                            "Batch manual customer created: id=%s name=%r by user=%s",
                            manual_customer.id,
                            manual_name,
                            user.username,
                        )

                except Exception as exc:
                    logger.exception(
                        "Failed to get_or_create batch manual customer: %r",
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

                valid_selected_customers = list(
                    customer_qs
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
                        non_customer_lines.append(line_form)
                    else:
                        messages.error(
                            request,
                            "One or more non-customer batch lines are invalid.",
                        )
                        return redirect(reverse("kam:plan"))

            approval_status = (
                VisitBatch.PENDING_APPROVAL
                if proceed_flag
                else VisitBatch.DRAFT
            )

            with transaction.atomic():
                batch = VisitBatch.objects.create(
                    kam=user,
                    from_date=from_date,
                    to_date=to_date,
                    visit_category=visit_category,
                    purpose=remarks or None,
                    approval_status=approval_status,
                    submitted_at=timezone.now() if proceed_flag else None,
                )

                created_lines = 0

                if visit_category == VisitBatch.CAT_CUSTOMER:
                    for customer in valid_selected_customers:
                        purpose = (
                            request.POST.get(f"purpose_{customer.id}") or ""
                        ).strip()

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
                            purpose=purpose or (remarks or None),
                            expected_sales_mt=expected_sales,
                            expected_collection=expected_collection,
                            location=location,
                            approval_status=approval_status,
                            submitted_at=timezone.now() if proceed_flag else None,
                        )

                        created_lines += 1

                else:
                    if not non_customer_lines:
                        messages.error(
                            request,
                            "Add at least one line to save a non-customer batch draft.",
                        )
                        transaction.set_rollback(True)
                        return redirect(reverse("kam:plan"))

                    for line_form in non_customer_lines:
                        VisitPlan.objects.create(
                            batch=batch,
                            customer=None,
                            counterparty_name=line_form.cleaned_data["counterparty_name"],
                            kam=user,
                            visit_date=from_date,
                            visit_date_to=to_date,
                            visit_type=VisitPlan.PLANNED,
                            visit_category=visit_category,
                            purpose=(
                                line_form.cleaned_data.get("counterparty_purpose")
                                or remarks
                                or None
                            ),
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
                messages.success(
                    request,
                    f"Batch submitted to Manager: {created_lines} lines (Batch #{batch.id}).",
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
    week_start, week_end, _ = _iso_week_bounds(timezone.now())

    if schema_ready:
        today_local = timezone.localtime(timezone.now()).date()
        plan_window_start = today_local - timezone.timedelta(days=7)
        plan_window_end = today_local + timezone.timedelta(days=7)

        my_plans = (
            _visitplan_qs_for_user(user)
            .filter(
                batch__isnull=True,
                visit_date__gte=plan_window_start,
                visit_date__lte=plan_window_end,
            )
            .order_by("visit_date", "customer__name")
        )
    else:
        my_plans = []

    ctx = {
        "page_title": "Plan Visit",
        "form": single_form,
        "single_form": single_form,
        "batch_form": batch_form,
        "plans": my_plans,
        "customers": list(customer_qs),
        "SINGLE_PREFIX": SINGLE_PREFIX,
        "BATCH_PREFIX": BATCH_PREFIX,
        "visitplan_schema_ready": schema_ready,
        "status_constants": {
            "DRAFT": STATUS_DRAFT,
            "PENDING_APPROVAL": STATUS_PENDING_APPROVAL,
            "APPROVED": STATUS_APPROVED,
            "REJECTED": STATUS_REJECTED,
        },
    }

    return render(request, "kam/plan_visit.html", ctx)

# =====================================================================
# SINGLE VISIT DETAIL
# =====================================================================
@login_required(login_url="/accounts/login/")
def single_visit_detail(request: HttpRequest, plan_id: int) -> HttpResponse:
    qs = _single_visit_qs_for_user(request.user)
    plan = qs.filter(id=plan_id).first()
    if plan is None:
        if not _is_manager(request.user):
            plan = get_object_or_404(
                VisitPlan.objects.select_related("customer", "kam"),
                id=plan_id,
                kam=request.user,
                batch__isnull=True,
            )
        else:
            raise Http404
    audit_log = list(VisitApprovalAudit.objects.filter(plan=plan).select_related("actor").order_by("created_at"))
    ctx = {
        "page_title": f"Single Visit #{plan.id}", "plan": plan, "audit_log": audit_log,
        "can_edit": plan.can_submit and not _is_manager(request.user),
        "can_approve": _is_manager(request.user) and plan.approval_status == VisitPlan.PENDING_APPROVAL,
        "visit_category_label": _VISIT_CATEGORY_LABELS.get(plan.visit_category, plan.visit_category),
    }
    return render(request, "kam/single_visit_detail.html", ctx)


# =====================================================================
# SINGLE VISIT EDIT
# =====================================================================
@login_required(login_url="/accounts/login/")
def single_visit_edit(request: HttpRequest, plan_id: int) -> HttpResponse:
    plan = get_object_or_404(
        _single_visit_qs_for_user(request.user).filter(kam=request.user),
        id=plan_id,
    )

    if plan.is_locked:
        messages.error(
            request,
            f"Visit #{plan.id} is locked ({plan.approval_status}) and cannot be edited.",
        )
        return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

    customer_qs = _customer_qs_for_user(request.user).order_by("name")

    if request.method == "POST":
        form = SingleVisitForm(request.POST, instance=plan)

        if "customer" in form.fields:
            form.fields["customer"].queryset = customer_qs

        if form.is_valid():
            submit_action = (
                request.POST.get("submit_action") or "save_draft"
            ).strip().lower()

            updated_plan: VisitPlan = form.save(commit=False)
            updated_plan.kam = request.user

            if updated_plan.visit_category == VisitPlan.CAT_CUSTOMER:
                if updated_plan.customer_id and not customer_qs.filter(id=updated_plan.customer_id).exists():
                    messages.error(request, "Invalid customer selection.")
                    return redirect(reverse("kam:single_visit_edit", args=[plan.id]))
            else:
                updated_plan.customer = None

            if not (updated_plan.location or "").strip():
                if (
                    updated_plan.visit_category == VisitPlan.CAT_CUSTOMER
                    and updated_plan.customer
                    and updated_plan.customer.address
                ):
                    updated_plan.location = updated_plan.customer.address

            if submit_action == "save_draft":
                updated_plan.approval_status = VisitPlan.DRAFT
                updated_plan.rejected_by = None
                updated_plan.rejected_at = None
                updated_plan.rejection_reason = None
                updated_plan.save()

                messages.success(request, f"Visit #{plan.id} updated as Draft.")
                return redirect(reverse("kam:single_visit_detail", args=[plan.id]))

            if submit_action == "submit_to_manager":
                mgr_user = _active_manager_for_kam(request.user)

                if not mgr_user or not getattr(mgr_user, "email", None):
                    messages.error(request, "No manager assigned. Contact admin.")
                    return redirect(reverse("kam:single_visit_edit", args=[plan.id]))

                with transaction.atomic():
                    updated_plan.approval_status = VisitPlan.PENDING_APPROVAL
                    updated_plan.submitted_at = timezone.now()
                    updated_plan.rejected_by = None
                    updated_plan.rejected_at = None
                    updated_plan.rejection_reason = None
                    updated_plan.save()

                    VisitApprovalAudit.objects.create(
                        plan=updated_plan,
                        actor=request.user,
                        action=VisitApprovalAudit.ACTION_SUBMIT,
                        note="Resubmitted after rejection",
                        actor_ip=_get_ip(request),
                    )

                approve_token = _make_single_token(updated_plan.id, "APPROVE")
                reject_token = _make_single_token(updated_plan.id, "REJECT")

                approve_url = request.build_absolute_uri(
                    reverse("kam:single_visit_approve_link", args=[approve_token])
                )
                reject_url = request.build_absolute_uri(
                    reverse("kam:single_visit_reject_link", args=[reject_token])
                )

                subject = (
                    f"[KAM] Re-Approval Required: Single Visit #{updated_plan.id} "
                    f"({updated_plan.visit_date}) - "
                    f"{request.user.get_full_name() or request.user.username}"
                )

                cc_users = _active_cc_for_kam(request.user)

                html_body = _build_single_visit_approval_email(
                    request=request,
                    plan=updated_plan,
                    kam_user=request.user,
                    manager_user=mgr_user,
                    approve_url=approve_url,
                    reject_url=reject_url,
                    cc_users=cc_users,
                )

                sent_ok = _send_safe_mail(
                    subject,
                    html_body,
                    [mgr_user],
                    cc_users,
                )

                if not sent_ok:
                    logger.warning(
                        "Approval email could not be sent for resubmitted single visit #%s",
                        updated_plan.id,
                    )

                messages.success(
                    request,
                    f"Visit #{updated_plan.id} resubmitted for manager approval.",
                )
                return redirect(reverse("kam:single_visit_detail", args=[updated_plan.id]))

        else:
            messages.error(request, "Please correct the errors below.")

    else:
        form = SingleVisitForm(instance=plan)

        if "customer" in form.fields:
            form.fields["customer"].queryset = customer_qs

    ctx = {
        "page_title": f"Edit Single Visit #{plan.id}",
        "plan": plan,
        "form": form,
        "visit_category_label": _VISIT_CATEGORY_LABELS.get(
            plan.visit_category,
            plan.visit_category,
        ),
        "is_resubmit": plan.approval_status == VisitPlan.REJECTED,
    }

    return render(request, "kam/single_visit_edit.html", ctx)


# =====================================================================
# SINGLE VISIT APPROVE / REJECT LINKS
# =====================================================================
@login_required(login_url="/accounts/login/")
def single_visit_approve_link(request: HttpRequest, token: str) -> HttpResponse:
    if not _is_manager(request.user):
        return HttpResponseForbidden("403 Forbidden: Manager access required.")
    try:
        plan_id, action = _parse_single_token(token)
    except SignatureExpired:
        messages.error(request, "Approval link has expired (7-day limit).")
        return redirect(reverse("kam:visit_batches"))
    except BadSignature:
        messages.error(request, "Invalid approval link.")
        return redirect(reverse("kam:visit_batches"))
    if action != "APPROVE":
        messages.error(request, "This link is not an approval link.")
        return redirect(reverse("kam:visit_batches"))

    with transaction.atomic():
        plan = get_object_or_404(VisitPlan.objects.select_for_update(), id=plan_id, batch__isnull=True)
        if not _can_manager_approve_visit(request.user, plan):
            return HttpResponseForbidden("403 Forbidden: This visit is not in your approval scope.")
        if plan.approval_status == VisitPlan.APPROVED:
            messages.info(request, f"Single Visit #{plan.id} is already approved.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        if plan.approval_status != VisitPlan.PENDING_APPROVAL:
            messages.error(request, f"Single Visit #{plan.id} is not pending approval.")
            return redirect(reverse("kam:single_visit_detail", args=[plan.id]))
        now_ts = timezone.now()
        plan.approval_status = VisitPlan.APPROVED
        plan.approved_by = request.user
        plan.approved_at = now_ts
        plan.save(update_fields=["approval_status", "approved_by", "approved_at", "updated_at"])
        VisitApprovalAudit.objects.create(plan=plan, actor=request.user, action=VisitApprovalAudit.ACTION_APPROVE, note="Approved via email link", actor_ip=_get_ip(request))

    _notify_kam_single_visit_decision(request=request, plan=plan, actor=request.user, status="APPROVED")
    messages.success(request, f"Single Visit #{plan_id} approved successfully.")
    return redirect(reverse("kam:single_visit_detail", args=[plan_id]))


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
    if not _visitplan_workflow_schema_ready():
        messages.error(request, "Single Visit workflow DB fields are not migrated yet. Run migrations before using this page.")
        return render(request, "kam/single_visit_list.html", {
            "page_title": "Single Visits", "rows": [], "status_filter": "",
            "can_approve": _is_manager(request.user),
            "status_choices": VisitPlan.APPROVAL_STATUS_CHOICES,
        })

    qs = _single_visit_qs_for_user(request.user).order_by("-created_at")
    status_filter = (request.GET.get("status") or "").strip().upper()
    if status_filter:
        qs = qs.filter(approval_status=status_filter)

    ctx = {
        "page_title": "Single Visits", "rows": list(qs[:300]),
        "status_filter": status_filter,
        "can_approve": _is_manager(request.user),
        "status_choices": VisitPlan.APPROVAL_STATUS_CHOICES,
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
    Lightweight Customer 360 API used by inline widgets.

    Must stay aligned with:
    - _customer_qs_for_user()
    - _preferred_inv_qs()
    """
    accessible_qs = _customer_qs_for_user(request.user)
    try:
        customer = accessible_qs.get(id=customer_id)
    except Customer.DoesNotExist:
        return JsonResponse({"error": "Customer not found or access denied"}, status=404)

    plans = CollectionPlan.objects.filter(customer_id=customer.id)
    if not _is_admin(request.user):
        if _is_manager(request.user):
            allowed_kam_ids = _kams_managed_by_manager(request.user)
            plans = plans.filter(kam_id__in=allowed_kam_ids)
        else:
            plans = plans.filter(kam_id=request.user.id)

    plan_agg = plans.aggregate(
        planned=Sum("planned_amount"),
        actual=Sum("actual_amount"),
    )
    total_planned = _safe_decimal(plan_agg.get("planned"))
    total_collected = _safe_decimal(plan_agg.get("actual"))
    outstanding = total_planned - total_collected
    last_collection = plans.order_by("-updated_at").first()

    sales_qs = _preferred_inv_qs(
        InvoiceFact.objects.filter(customer=customer)
    )
    sales_agg = sales_qs.aggregate(
        total_mt=Sum("qty_mt"),
        total_value=Sum("invoice_value"),
    )
    total_sales_mt = _safe_decimal(sales_agg.get("total_mt"))
    total_sales_value = _safe_decimal(sales_agg.get("total_value"))
    last_invoice = sales_qs.order_by("-invoice_date").first()

    visits = VisitPlan.objects.filter(customer_id=customer.id)
    last_visit = visits.order_by("-visit_date").first()

    kam_name = ""
    if customer.kam_id:
        try:
            kam_name = customer.kam.get_full_name() or customer.kam.username
        except Exception:
            pass

    last_coll_date = None
    if last_collection and last_collection.updated_at:
        try:
            last_coll_date = timezone.localtime(last_collection.updated_at).strftime("%d %b %Y")
        except Exception:
            last_coll_date = str(last_collection.updated_at.date())

    data = {
        "name": customer.name,
        "kam": kam_name,
        "code": getattr(customer, "code", None) or "",
        "mobile": getattr(customer, "mobile", None) or "",
        "total_planned": float(total_planned),
        "total_collected": float(total_collected),
        "outstanding": float(outstanding),
        "total_sales_mt": float(total_sales_mt),
        "total_sales_value": float(total_sales_value),
        "last_invoice_date": str(last_invoice.invoice_date) if last_invoice else None,
        "last_collection_date": last_coll_date,
        "last_visit_date": str(last_visit.visit_date) if last_visit else None,
        "total_visits": visits.count(),
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


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches(request: HttpRequest) -> HttpResponse:
    return visit_batches_api(request) if _wants_json(request) else visit_batches_page(request)


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_page(request: HttpRequest) -> HttpResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")
    status = (request.GET.get("status") or "").strip().upper()
    if status:
        qs = qs.filter(approval_status=status)
    return render(request, "kam/visit_batches.html", {"page_title": "Visit History", "rows": list(qs[:300]), "can_view_all": _is_manager(user)})


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_plan", "kam_manager")
def visit_batches_api(request: HttpRequest) -> JsonResponse:
    user = request.user
    qs = _visitbatch_qs_for_user(user).order_by("-created_at")
    rows = [{"id": b.id, "kam": b.kam.username if b.kam_id else None, "from_date": str(b.from_date), "to_date": str(b.to_date), "visit_category": b.visit_category, "visit_category_label": b.get_visit_category_display(), "approval_status": b.approval_status, "remarks": b.purpose or "", "created_at": timezone.localtime(b.created_at).isoformat() if b.created_at else None} for b in qs[:300]]
    return JsonResponse({"ok": True, "count": len(rows), "batches": rows})


@login_required(login_url="/accounts/login/")
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


@login_required(login_url="/accounts/login/")
@require_any_kam_code("kam_manager", "kam_plan")
def visit_batch_detail(request: HttpRequest, batch_id: int) -> HttpResponse:
    b = get_object_or_404(_visitbatch_qs_for_user(request.user), id=batch_id)
    lines = list(VisitPlan.objects.select_related("customer").filter(batch=b).order_by("customer__name"))
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
                update_fields = ["updated_at"]
                if exp_sales is not None:
                    plan.expected_sales_mt = exp_sales
                    update_fields.append("expected_sales_mt")
                if exp_coll is not None:
                    plan.expected_collection = exp_coll
                    update_fields.append("expected_collection")
                plan.save(update_fields=update_fields)
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
@login_required(login_url="/accounts/login/")
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
        qs = _filter_qs_by_kam_scope(
            VisitPlan.objects.select_related("customer", "kam").filter(
                visit_date__gte=start_date,
                visit_date__lt=end_date,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        visits_data = list(qs.order_by("-visit_date")[:500])
        total_actuals = VisitActual.objects.filter(plan__in=qs).count()
        successful = VisitActual.objects.filter(plan__in=qs, successful=True).count()

        visits_summary = {
            "total_planned": qs.count(),
            "total_actual": total_actuals,
            "successful": successful,
            "success_pct": _pct(Decimal(successful), Decimal(total_actuals)) if total_actuals else None,
        }

    if active_tab == "calls":
        qs = _filter_qs_by_kam_scope(
            CallLog.objects.select_related("customer", "kam").filter(
                call_datetime__gte=start_dt,
                call_datetime__lt=end_dt,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        calls_data = list(qs.order_by("-call_datetime")[:500])
        total_calls = qs.count()
        successful_calls = qs.exclude(outcome="").exclude(outcome__isnull=True).count()

        calls_summary = {
            "total": total_calls,
            "successful": successful_calls,
            "conversion_pct": _pct(Decimal(successful_calls), Decimal(total_calls)) if total_calls else None,
        }

    if active_tab == "sales":
        qs = _filter_qs_by_kam_scope(
            InvoiceFact.objects.filter(
                invoice_date__gte=start_date,
                invoice_date__lt=end_date,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )
        qs = _sales_converted_qs(qs)

        sales_data = list(
            qs.values(
                customer_name=F("customer__name"),
                kam_username=F("kam__username"),
            )
            .annotate(mt=Sum("qty_mt"))
            .order_by("-mt")[:300]
        )

        sales_summary = {
            "total_mt": _safe_decimal(qs.aggregate(mt=Sum("qty_mt")).get("mt")),
            "customer_count": len(sales_data),
        }

    if active_tab == "leads":
        qs = _filter_qs_by_kam_scope(
            LeadFact.objects.select_related("customer", "kam").filter(
                doe__gte=start_date,
                doe__lt=end_date,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        leads_data = list(qs.order_by("-doe")[:500])

        leads_summary = {
            "total_count": qs.count(),
            "won_count": qs.filter(status="WON").count(),
            "total_mt": _safe_decimal(qs.aggregate(mt=Sum("qty_mt")).get("mt")),
            "won_mt": _safe_decimal(qs.filter(status="WON").aggregate(mt=Sum("qty_mt")).get("mt")),
        }

    if active_tab == "collections":
        qs = CollectionPlan.objects.select_related("customer", "kam").filter(
            overdue_amount__gt=0,
        )
        qs = _filter_qs_by_kam_scope(qs, request.user, scope_kam_id, "kam_id")

        collections_data = list(
            qs.order_by("kam__username", "customer__name")[:500]
        )

        agg = qs.aggregate(
            total_overdue=Sum("overdue_amount"),
            total_collected=Sum("actual_amount"),
        )

        total_overdue = _safe_decimal(agg.get("total_overdue"))
        total_collected = _safe_decimal(agg.get("total_collected"))
        total_pending = max(total_overdue - total_collected, Decimal("0"))

        collections_summary = {
            "total_overdue": total_overdue,
            "total_collected": total_collected,
            "total_pending": total_pending,
            "customer_count": qs.count(),

            # Backward-compatible aliases for older template variables.
            "total_amount": total_collected,
            "transaction_count": qs.count(),
        }

    ctx = {
        "page_title": "Manager View",
        "range_label": range_label,
        "scope_label": scope_label,
        "active_tab": active_tab,
        "tabs": [
            ("visits", "Visits"),
            ("calls", "Calls"),
            ("sales", "Sales"),
            ("leads", "Leads"),
            ("collections", "Collections"),
        ],
        "kam_options": _kam_options_for_user(request.user),
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
        "selected_user": scope_label if scope_label != "ALL" else "",
        "visits_data": visits_data,
        "visits_summary": visits_summary,
        "calls_data": calls_data,
        "calls_summary": calls_summary,
        "sales_data": sales_data,
        "sales_summary": sales_summary,
        "leads_data": leads_data,
        "leads_summary": leads_summary,
        "collections_data": collections_data,
        "collections_summary": collections_summary,
    }

    return render(request, "kam/manager_view.html", ctx)

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
    FIX 3 — Safe customer fallback.
    NEVER crash on ?id=<invalid>. If the provided ID does not exist or is
    out of scope, fall back to the first customer in the queryset (or None).
    This prevents 404 errors when the frontend sends stale/invalid IDs.
    """
    scope_kam_id, scope_label = _resolve_scope(request, request.user)
    customer_id = request.GET.get("id")

    base_qs = _customer_qs_for_user(request.user)

    if scope_kam_id is not None:
        scoped_invoice_customer_ids = InvoiceFact.objects.filter(
            kam_id=scope_kam_id
        ).values_list("customer_id", flat=True)

        scoped_lead_customer_ids = LeadFact.objects.filter(
            kam_id=scope_kam_id
        ).values_list("customer_id", flat=True)

        scoped_collection_customer_ids = CollectionTxn.objects.filter(
            kam_id=scope_kam_id
        ).values_list("customer_id", flat=True)

        base_qs = base_qs.filter(
            Q(kam_id=scope_kam_id)
            | Q(primary_kam_id=scope_kam_id)
            | Q(id__in=scoped_invoice_customer_ids)
            | Q(id__in=scoped_lead_customer_ids)
            | Q(id__in=scoped_collection_customer_ids)
        ).distinct()

    customer_list = list(base_qs.order_by("name")[:300])

    # FIX 3 — Safe customer lookup: never crash on invalid/missing ID
    customer = None
    if customer_id:
        try:
            customer = base_qs.filter(id=int(customer_id)).first()
        except (ValueError, TypeError):
            customer = None

    # Fallback: use first customer in list if no valid ID was provided
    if customer is None:
        customer = customer_list[0] if customer_list else None

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
                risk_ratio = exposure / credit_limit
            except Exception:
                risk_ratio = None

        sales_base = InvoiceFact.objects.filter(
            customer=customer,
            invoice_date__gte=start_date,
            invoice_date__lte=end_date,
        )
        sales_qs = _preferred_inv_qs(sales_base)
        sales = (
            sales_qs
            .values("invoice_date__year", "invoice_date__month")
            .annotate(mt=Sum("qty_mt"))
            .order_by("invoice_date__year", "invoice_date__month")
        )
        sales_history = [
            {
                "year": r["invoice_date__year"],
                "month": r["invoice_date__month"],
                "mt": _safe_decimal(r["mt"]),
            }
            for r in sales
        ]

        colls = (
            CollectionTxn.objects
            .filter(
                customer=customer,
                txn_datetime__date__gte=start_date,
                txn_datetime__date__lte=end_date,
            )
            .values("txn_datetime__year", "txn_datetime__month")
            .annotate(amount=Sum("amount"))
            .order_by("txn_datetime__year", "txn_datetime__month")
        )
        collections_history = [
            {
                "year": r["txn_datetime__year"],
                "month": r["txn_datetime__month"],
                "amount": _safe_decimal(r["amount"]),
            }
            for r in colls
        ]

        visit_history = list(
            VisitPlan.objects
            .select_related("actual", "kam")
            .filter(customer=customer, visit_date__gte=start_date, visit_date__lte=end_date)
            .order_by("-visit_date")[:20]
        )
        call_history = list(
            CallLog.objects
            .select_related("kam")
            .filter(customer=customer, call_datetime__date__gte=start_date, call_datetime__date__lte=end_date)
            .order_by("-call_datetime")[:20]
        )
        lead_history = list(
            LeadFact.objects
            .filter(customer=customer, doe__gte=start_date, doe__lte=end_date)
            .order_by("-doe")[:20]
        )
        overdue_history = list(
            OverdueSnapshot.objects
            .filter(customer=customer)
            .order_by("-snapshot_date")[:12]
        )
        today = timezone.localdate()
        followups = list(
            VisitActual.objects
            .filter(
                plan__customer=customer,
                next_action__isnull=False,
                next_action__gt="",
                next_action_date__isnull=False,
                next_action_date__gte=today,
            )
            .order_by("next_action_date")[:10]
        )
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


# =====================================================================
# REPORTS
# =====================================================================
@login_required(login_url="/accounts/login/")
@require_kam_code("kam_reports")
def reports(request: HttpRequest) -> HttpResponse:
    start_dt, end_dt, range_label = _get_dashboard_range(request)
    scope_kam_id, scope_label = _resolve_scope(request, request.user)

    anchor_end = _last_completed_ms_week_end(timezone.now())
    weeks_trend: List[Dict] = []

    for k in (3, 2, 1, 0):
        week_end = anchor_end - timezone.timedelta(days=7 * k)
        week_start = week_end - timezone.timedelta(days=7)
        _, __, week_label = _ms_week_bounds(week_start)

        inv_qs = _filter_qs_by_kam_scope(
            InvoiceFact.objects.filter(
                invoice_date__gte=week_start.date(),
                invoice_date__lt=week_end.date(),
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )
        inv_qs = _preferred_inv_qs(inv_qs)

        visit_actual_qs = _filter_qs_by_kam_scope(
            VisitActual.objects.filter(
                plan__visit_date__gte=week_start.date(),
                plan__visit_date__lt=week_end.date(),
            ),
            request.user,
            scope_kam_id,
            "plan__kam_id",
        )

        call_qs = _filter_qs_by_kam_scope(
            CallLog.objects.filter(
                call_datetime__gte=week_start,
                call_datetime__lt=week_end,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        lead_qs = _filter_qs_by_kam_scope(
            LeadFact.objects.filter(
                doe__gte=week_start.date(),
                doe__lt=week_end.date(),
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        collection_qs = _filter_qs_by_kam_scope(
            CollectionTxn.objects.filter(
                txn_datetime__gte=week_start,
                txn_datetime__lt=week_end,
            ),
            request.user,
            scope_kam_id,
            "kam_id",
        )

        weeks_trend.append({
            "week": week_label,
            "sales_mt": float(_safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt")).get("mt"))),
            "visits": int(visit_actual_qs.count()),
            "calls": int(call_qs.count()),
            "leads": int(lead_qs.count()),
            "collections": float(_safe_decimal(collection_qs.aggregate(a=Sum("amount")).get("a"))),
        })

    metric = (request.GET.get("metric") or "sales").strip().lower()
    rows = []

    if metric == "sales":
        qs = InvoiceFact.objects.filter(
            invoice_date__gte=start_dt.date(),
            invoice_date__lt=end_dt.date(),
        )
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)
        qs = _sales_converted_qs(qs)

        rows = list(
            qs.values(
                customer_name=F("customer__name"),
                kam_username=F("kam__username"),
            )
            .annotate(mt=Sum("qty_mt"))
            .order_by("-mt")[:300]
        )

    elif metric == "calls":
        qs = CallLog.objects.filter(
            call_datetime__gte=start_dt,
            call_datetime__lt=end_dt,
        )
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "call_datetime",
                kam_username=F("kam__username"),
                customer_name=F("customer__name"),
            ).order_by("-call_datetime")[:500]
        )

    elif metric == "visits":
        qs = VisitActual.objects.filter(
            plan__visit_date__gte=start_dt.date(),
            plan__visit_date__lt=end_dt.date(),
        )
        if scope_kam_id is not None:
            qs = qs.filter(plan__kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "successful",
                visit_date=F("plan__visit_date"),
                kam_username=F("plan__kam__username"),
                customer_name=F("plan__customer__name"),
            ).order_by("-visit_date")[:500]
        )

    elif metric == "leads":
        qs = LeadFact.objects.filter(
            doe__isnull=False,
            doe__gte=start_dt.date(),
            doe__lt=end_dt.date(),
        )
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "doe",
                "status",
                "qty_mt",
                kam_username=F("kam__username"),
                customer_name=F("customer__name"),
            ).order_by("-doe")[:500]
        )

    elif metric == "collections":
        qs = CollectionTxn.objects.filter(
            txn_datetime__gte=start_dt,
            txn_datetime__lt=end_dt,
        )
        if scope_kam_id is not None:
            qs = qs.filter(kam_id=scope_kam_id)

        rows = list(
            qs.values(
                "id",
                "txn_datetime",
                "amount",
                kam_username=F("kam__username"),
                customer_name=F("customer__name"),
            ).order_by("-txn_datetime")[:500]
        )

    ctx = {
        "page_title": "KAM Reports",
        "metric": metric,
        "range_label": range_label,
        "scope_label": scope_label,
        "can_choose_kam": _is_manager(request.user),
        "kam_options": _kam_options_for_user(request.user),
        "rows": rows,
        "weeks_trend": weeks_trend,
        "filter_from": start_dt.date().isoformat(),
        "filter_to": (end_dt - timezone.timedelta(days=1)).date().isoformat(),
    }
    return render(request, "kam/reports.html", ctx)

# =====================================================================
# CSV export
# =====================================================================
@login_required(login_url="/accounts/login/")
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
        inv_qs = InvoiceFact.objects.filter(kam_id=kam_id, invoice_date__gte=start_dt.date(), invoice_date__lt=end_dt.date())
        inv_qs = _preferred_inv_qs(inv_qs)
        rows.append([period_type, period_id, kam_id,
            str(_safe_decimal(inv_qs.aggregate(mt=Sum("qty_mt"))["mt"])),
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