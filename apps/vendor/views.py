# apps/vendor/views.py
from __future__ import annotations

import logging

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.exceptions import ValidationError
from django.core.mail import EmailMessage
from django.template.loader import render_to_string
from django.conf import settings
from django.http import JsonResponse

from .models import Vendor, VendorPaymentRequest, VendorApprovalConfig
from .forms import VendorPaymentRequestForm, VendorApprovalConfigForm
from .services import send_vendor_payment_submission_email
from apps.users.permissions import _user_permission_codes

logger = logging.getLogger(__name__)


# ── Google Sheet Sync helper ─────────────────────────────────────────────────

def _sync_vendor_payment_sheet(obj: VendorPaymentRequest) -> None:
    """
    Production-safe Vendor Payment Google Sheet sync trigger.

    Same principle as Reimbursement:
    - View saves database object first.
    - Sync service runs after DB commit.
    - Google errors must not break ERP workflow.
    - If integration file/env is missing, workflow still continues.
    """
    try:
        from apps.vendor.integrations.sheets import sync_request

        sync_request(obj)
    except Exception:
        logger.exception(
            "Vendor Payment Google Sheet sync trigger failed for request pk=%s",
            getattr(obj, "pk", None),
        )


# ── Permission helpers ────────────────────────────────────────────────────────

def _user_codes(user):
    """
    Safe wrapper around the ERP permission system.

    Profile.permissions is the main source.
    Superuser normally returns {"*"} from _user_permission_codes().
    """
    try:
        return _user_permission_codes(user)
    except Exception:
        return set()


def _is_profile_admin(user):
    """
    ERP Admin check.

    Allows:
    1. Django superuser
    2. Wildcard permission: "*" or "all"
    3. Profile role = Admin
    4. Django staff user

    This makes the Vendor module accessible to Admin.
    Admin can then give Vendor permissions to other users from the User module.
    """
    if getattr(user, "is_superuser", False):
        return True

    codes = _user_codes(user)

    if {"*", "all"} & codes:
        return True

    profile = getattr(user, "profile", None)
    role = str(getattr(profile, "role", "") or "").strip().lower()

    if role == "admin":
        return True

    if getattr(user, "is_staff", False):
        return True

    return False


def _is_vendor_admin(user):
    """
    Vendor Admin access.

    Vendor Admin can:
    - Access Vendor module
    - Access Vendor Admin Setup
    - Manage Vendor Master
    - Configure Vendor approvers
    - See all Vendor Payment Requests
    """
    if _is_profile_admin(user):
        return True

    codes = _user_codes(user)

    return "vendor_admin" in codes


def _can_access_vendor(user):
    """
    True if user can access Vendor Payments module at all.

    Admin/Vendor Admin always pass.
    Other users need at least one Vendor permission.
    """
    if _is_vendor_admin(user):
        return True

    codes = _user_codes(user)

    vendor_codes = {
        "vendor_create",
        "vendor_view_own",
        "vendor_finance_approve",
        "vendor_final_approve",
        "vendor_admin",
    }

    return bool(codes & vendor_codes)


def _can_create(user):
    """
    User can create Vendor Payment Requests.
    """
    if _is_vendor_admin(user):
        return True

    codes = _user_codes(user)

    return "vendor_create" in codes


def _can_view_own(user):
    """
    User can view own Vendor Payment Requests.
    """
    if _is_vendor_admin(user):
        return True

    codes = _user_codes(user)

    return "vendor_view_own" in codes or "vendor_create" in codes


def _is_finance(user):
    """
    Finance approver check.

    Important:
    Vendor Admin can view all records but does not automatically become
    finance approver unless superuser or explicitly configured.
    """
    if getattr(user, "is_superuser", False):
        return True

    codes = _user_codes(user)

    if "vendor_finance_approve" in codes:
        return True

    config = VendorApprovalConfig.get_config()

    if config.finance_users.filter(pk=user.pk).exists():
        return True

    if user.email:
        manual = [
            e.strip().lower()
            for e in (config.finance_manual_emails or "").split(",")
            if e.strip()
        ]

        if user.email.lower() in manual:
            return True

    return False


def _is_senior(user):
    """
    Senior authority check.

    Important:
    Vendor Admin can view all records but does not automatically become
    senior approver unless superuser or explicitly configured.
    """
    if getattr(user, "is_superuser", False):
        return True

    codes = _user_codes(user)

    if "vendor_final_approve" in codes:
        return True

    config = VendorApprovalConfig.get_config()

    return config.senior_authority_id == user.pk


def _is_admin_user(user):
    """
    Full Vendor audit visibility.
    """
    return _is_vendor_admin(user)


# ── Email helpers ─────────────────────────────────────────────────────────────

def _send_submission_email(request, obj):
    """
    Backward-compatible wrapper.

    Keep this function name because new_request() and resubmit()
    already call _send_submission_email().

    Actual production email logic is now inside apps/vendor/services.py.
    """
    send_vendor_payment_submission_email(request, obj)


def _send_finance_approved_email(request, obj):
    config = VendorApprovalConfig.get_config()

    if not config.senior_authority or not config.senior_authority.email:
        return

    subject = f"Vendor Payment Ready for Final Approval — {obj.request_id}"

    body = (
        f"A vendor payment has cleared finance review and awaits your final approval.\n\n"
        f"Request ID       : {obj.request_id}\n"
        f"Vendor           : {obj.vendor_display_name}\n"
        f"Vendor Type      : {obj.vendor_type_display_safe}\n"
        f"Invoice No       : {obj.invoice_number}\n"
        f"Total Amount     : INR {obj.total_amount}\n"
        f"Finance Approved : {obj.finance_approved_by.get_full_name() or obj.finance_approved_by.username}\n\n"
        f"Please log in to give final approval.\n{request.build_absolute_uri('/')}"
    )

    try:
        EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[config.senior_authority.email],
        ).send(fail_silently=True)
    except Exception:
        logger.exception(
            "Vendor finance-approved email failed for request pk=%s",
            getattr(obj, "pk", None),
        )


def _send_final_approval_email(request, obj):
    config = VendorApprovalConfig.get_config()
    to_list = config.get_mumbai_email_list()
    cc_list = config.get_cc_email_list()

    if not to_list:
        return

    subject = f"Vendor Payment Approved — {obj.request_id}"

    body = render_to_string(
        "vendor/email/final_approval.txt",
        {
            "obj": obj,
            "site_url": request.build_absolute_uri("/"),
        },
    )

    try:
        EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_list,
            cc=cc_list,
        ).send(fail_silently=True)
    except Exception:
        logger.exception(
            "Vendor final approval email failed for request pk=%s",
            getattr(obj, "pk", None),
        )


def _send_rejection_email(request, obj):
    if not obj.created_by.email:
        return

    subject = f"Vendor Payment Request Rejected — {obj.request_id}"

    body = (
        f"Your vendor payment request has been rejected.\n\n"
        f"Request ID : {obj.request_id}\n"
        f"Vendor     : {obj.vendor_display_name}\n"
        f"Vendor Type: {obj.vendor_type_display_safe}\n"
        f"Amount     : INR {obj.total_amount}\n"
        f"Remarks    : {obj.remarks or '—'}\n\n"
        f"Please contact your approver for details.\n{request.build_absolute_uri('/')}"
    )

    try:
        EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[obj.created_by.email],
        ).send(fail_silently=True)
    except Exception:
        logger.exception(
            "Vendor rejection email failed for request pk=%s",
            getattr(obj, "pk", None),
        )


# ── API Views ────────────────────────────────────────────────────────────────

@login_required
def vendor_type_api(request, vendor_id):
    """
    AJAX endpoint for Vendor Type auto-fill.

    Frontend uses this only for display.
    Backend still forces vendor_type from Vendor Master during save.
    """
    if not _can_access_vendor(request.user):
        return JsonResponse({"error": "Access denied"}, status=403)

    vendor = get_object_or_404(Vendor, pk=vendor_id, is_active=True)

    return JsonResponse(
        {
            "id": vendor.pk,
            "name": vendor.name,
            "type": vendor.type,
            "type_display": vendor.get_type_display(),
        }
    )


# ── Main Views ───────────────────────────────────────────────────────────────

@login_required
def dashboard(request):
    user = request.user

    if not _can_access_vendor(user):
        messages.error(
            request,
            "You do not have access to the Vendor Payments module.",
        )
        return redirect("dashboard:home")

    is_vendor_admin = _is_vendor_admin(user)
    is_finance = _is_finance(user)
    is_senior = _is_senior(user)

    if is_vendor_admin:
        my_count = VendorPaymentRequest.objects.count()
    else:
        my_count = VendorPaymentRequest.objects.filter(created_by=user).count()

    pending_finance = (
        VendorPaymentRequest.objects.filter(status="submitted").count()
        if is_finance or is_vendor_admin
        else 0
    )

    pending_senior = (
        VendorPaymentRequest.objects.filter(status="finance_approved").count()
        if is_senior or is_vendor_admin
        else 0
    )

    paid_count = (
        VendorPaymentRequest.objects.filter(status="paid").count()
        if is_finance or is_vendor_admin
        else 0
    )

    return render(
        request,
        "vendor/dashboard.html",
        {
            "my_count": my_count,
            "pending_finance": pending_finance,
            "pending_senior": pending_senior,
            "paid_count": paid_count,
            "is_finance": is_finance,
            "is_senior": is_senior,
            "can_create": _can_create(user),
            "is_vendor_admin": is_vendor_admin,
        },
    )


@login_required
def new_request(request):
    if not _can_create(request.user):
        messages.error(
            request,
            "You do not have permission to create vendor payment requests.",
        )
        return redirect("vendor:dashboard")

    if request.method == "POST":
        form = VendorPaymentRequestForm(request.POST, request.FILES)

        if form.is_valid():
            obj = form.save(commit=False)
            obj.created_by = request.user

            action = request.POST.get("action")
            obj.status = "submitted" if action == "submit" else "draft"

            # Production safety:
            # Never trust browser-posted vendor_type.
            # Vendor type must come from Vendor Master.
            if obj.vendor_id:
                obj.vendor_type = obj.vendor.type
                obj.vendor_name_manual = ""

            obj.save()

            # Google Sheet sync:
            # Create row for draft/submitted request.
            # Same row will be updated later using request_id.
            _sync_vendor_payment_sheet(obj)

            if obj.status == "submitted":
                _send_submission_email(request, obj)
                messages.success(
                    request,
                    f"Request {obj.request_id} submitted. Finance team notified.",
                )
            else:
                messages.success(
                    request,
                    f"Request {obj.request_id} saved as draft.",
                )

            return redirect("vendor:my_requests")
    else:
        form = VendorPaymentRequestForm()

    return render(
        request,
        "vendor/new_request.html",
        {
            "form": form,
        },
    )


@login_required
def my_requests(request):
    if not _can_access_vendor(request.user):
        messages.error(request, "Access denied.")
        return redirect("dashboard:home")

    is_admin_view = _is_admin_user(request.user)

    if is_admin_view:
        qs = VendorPaymentRequest.objects.all().select_related(
            "vendor",
            "created_by",
            "finance_approved_by",
            "final_approved_by",
            "paid_by",
        )
    else:
        if not _can_view_own(request.user):
            messages.error(
                request,
                "You do not have permission to view vendor requests.",
            )
            return redirect("vendor:dashboard")

        qs = VendorPaymentRequest.objects.filter(
            created_by=request.user
        ).select_related("vendor", "paid_by")

    status_filter = request.GET.get("status", "")

    if status_filter:
        qs = qs.filter(status=status_filter)

    return render(
        request,
        "vendor/my_requests.html",
        {
            "requests": qs,
            "status_filter": status_filter,
            "status_choices": VendorPaymentRequest.STATUS_CHOICES,
            "is_admin_view": is_admin_view,
        },
    )


@login_required
def approval_queue(request):
    user = request.user

    is_fin = _is_finance(user)
    is_sen = _is_senior(user)
    is_admin = _is_admin_user(user)

    if not (is_fin or is_sen or is_admin):
        messages.warning(request, "You are not configured as an approver.")
        return redirect("vendor:dashboard")

    qs = VendorPaymentRequest.objects.select_related(
        "vendor",
        "created_by",
        "finance_approved_by",
        "final_approved_by",
        "paid_by",
    )

    status_filter = request.GET.get("status", "")

    if is_admin:
        # Admin sees every request for audit/tracking.
        if status_filter:
            qs = qs.filter(status=status_filter)
    elif is_fin and is_sen:
        qs = qs.filter(status__in=["submitted", "finance_approved", "final_approved"])
        status_filter = ""
    elif is_fin:
        # Finance handles first approval and final payment.
        qs = qs.filter(status__in=["submitted", "final_approved"])
        status_filter = ""
    else:
        qs = qs.filter(status="finance_approved")
        status_filter = "finance_approved"

    return render(
        request,
        "vendor/approval_queue.html",
        {
            "requests": qs,
            "is_finance": is_fin,
            "is_senior": is_sen,
            "is_admin": is_admin,
            "status_choices": VendorPaymentRequest.STATUS_CHOICES,
            "status_filter": status_filter,
        },
    )


@login_required
def detail(request, pk):
    obj = get_object_or_404(
        VendorPaymentRequest.objects.select_related(
            "vendor",
            "created_by",
            "finance_approved_by",
            "final_approved_by",
            "paid_by",
        ),
        pk=pk,
    )

    is_fin = _is_finance(request.user)
    is_sen = _is_senior(request.user)
    is_admin = _is_admin_user(request.user)

    can_view = (
        obj.created_by == request.user
        or is_fin
        or is_sen
        or is_admin
    )

    if not can_view:
        messages.error(request, "Access denied.")
        return redirect("vendor:dashboard")

    return render(
        request,
        "vendor/detail.html",
        {
            "obj": obj,
            "can_finance_action": is_fin and obj.status == "submitted",
            "can_senior_action": is_sen and obj.status == "finance_approved",
            "can_mark_paid": is_fin and obj.status == "final_approved",
            "can_resubmit": obj.created_by == request.user and obj.status == "draft",
            "is_admin": is_admin,
        },
    )


@login_required
def resubmit(request, pk):
    obj = get_object_or_404(
        VendorPaymentRequest,
        pk=pk,
        created_by=request.user,
        status="draft",
    )

    obj.status = "submitted"

    if obj.vendor_id:
        obj.vendor_type = obj.vendor.type
        obj.vendor_name_manual = ""

    obj.save()

    # Google Sheet sync:
    # Update existing draft row to Submitted.
    _sync_vendor_payment_sheet(obj)

    _send_submission_email(request, obj)

    messages.success(
        request,
        f"{obj.request_id} submitted. Finance team notified.",
    )

    return redirect("vendor:detail", pk=pk)


@login_required
def finance_action(request, pk):
    if request.method != "POST":
        return redirect("vendor:approval_queue")

    obj = get_object_or_404(VendorPaymentRequest, pk=pk)

    if not _is_finance(request.user) or obj.status != "submitted":
        messages.error(request, "Not authorized or invalid status.")
        return redirect("vendor:approval_queue")

    action = request.POST.get("action")
    remarks = request.POST.get("remarks", "").strip()

    if action == "approve":
        obj.status = "finance_approved"
        obj.finance_approved_by = request.user
        obj.remarks = remarks
        obj.save()

        # Google Sheet sync:
        # Update same row to Finance Approved.
        _sync_vendor_payment_sheet(obj)

        _send_finance_approved_email(request, obj)

        messages.success(
            request,
            f"{obj.request_id} finance approved. Senior authority notified.",
        )

    elif action == "reject":
        obj.status = "rejected"
        obj.remarks = remarks
        obj.save()

        # Google Sheet sync:
        # Update same row to Rejected.
        _sync_vendor_payment_sheet(obj)

        _send_rejection_email(request, obj)

        messages.warning(request, f"{obj.request_id} rejected.")

    else:
        messages.error(request, "Invalid finance action.")

    return redirect("vendor:approval_queue")


@login_required
def senior_action(request, pk):
    if request.method != "POST":
        return redirect("vendor:approval_queue")

    obj = get_object_or_404(VendorPaymentRequest, pk=pk)

    if not _is_senior(request.user) or obj.status != "finance_approved":
        messages.error(request, "Not authorized or invalid status.")
        return redirect("vendor:approval_queue")

    action = request.POST.get("action")
    remarks = request.POST.get("remarks", "").strip()

    if action == "approve":
        obj.status = "final_approved"
        obj.final_approved_by = request.user
        obj.remarks = remarks
        obj.save()

        # Google Sheet sync:
        # Update same row to Final Approved.
        _sync_vendor_payment_sheet(obj)

        _send_final_approval_email(request, obj)

        messages.success(
            request,
            f"{obj.request_id} finally approved. Accounts team notified.",
        )

    elif action == "reject":
        obj.status = "rejected"
        obj.remarks = remarks
        obj.save()

        # Google Sheet sync:
        # Update same row to Rejected.
        _sync_vendor_payment_sheet(obj)

        _send_rejection_email(request, obj)

        messages.warning(request, f"{obj.request_id} rejected.")

    else:
        messages.error(request, "Invalid senior action.")

    return redirect("vendor:approval_queue")


@login_required
def mark_paid(request, pk):
    """
    Finance marks a final-approved Vendor Payment Request as Paid.

    This is intentionally separate from finance approval:
    - finance_action: submitted -> finance_approved
    - senior_action: finance_approved -> final_approved
    - mark_paid: final_approved -> paid

    After payment, Google Sheet updates the same row using request_id.
    """
    if request.method != "POST":
        return redirect("vendor:detail", pk=pk)

    obj = get_object_or_404(
        VendorPaymentRequest.objects.select_related(
            "vendor",
            "created_by",
            "finance_approved_by",
            "final_approved_by",
            "paid_by",
        ),
        pk=pk,
    )

    if not _is_finance(request.user):
        messages.error(request, "Only Finance can mark vendor payment as Paid.")
        return redirect("vendor:detail", pk=pk)

    if obj.status != "final_approved":
        messages.error(
            request,
            "Only Final Approved vendor payments can be marked as Paid.",
        )
        return redirect("vendor:detail", pk=pk)

    reference = (
        request.POST.get("payment_reference")
        or request.POST.get("reference")
        or ""
    ).strip()
    remarks = request.POST.get("remarks", "").strip()

    if not reference:
        messages.error(request, "Payment reference is required.")
        return redirect("vendor:detail", pk=pk)

    try:
        if hasattr(obj, "mark_paid"):
            obj.mark_paid(
                actor=request.user,
                reference=reference,
                remarks=remarks,
            )
        else:
            obj.status = "paid"
            obj.payment_reference = reference
            obj.paid_by = request.user
            if remarks:
                obj.remarks = remarks
            obj.save()

    except ValidationError as exc:
        messages.error(
            request,
            exc.messages[0] if hasattr(exc, "messages") else str(exc),
        )
        return redirect("vendor:detail", pk=pk)
    except Exception:
        logger.exception(
            "Vendor Payment mark_paid failed for request pk=%s",
            getattr(obj, "pk", None),
        )
        messages.error(request, "Unable to mark this vendor payment as Paid.")
        return redirect("vendor:detail", pk=pk)

    # Google Sheet sync:
    # Update same row to Paid.
    _sync_vendor_payment_sheet(obj)

    messages.success(
        request,
        f"{obj.request_id} marked as Paid.",
    )

    return redirect("vendor:detail", pk=pk)


# ── Admin: Approval Config ───────────────────────────────────────────────────

@login_required
def admin_setup(request):
    if not _is_vendor_admin(request.user):
        messages.error(request, "Admin access required.")
        return redirect("vendor:dashboard")

    config = VendorApprovalConfig.get_config()

    if request.method == "POST" and "save_config" in request.POST:
        form = VendorApprovalConfigForm(request.POST, instance=config)

        if form.is_valid():
            form.save()
            messages.success(request, "Approval configuration saved successfully.")
            return redirect("vendor:admin_setup")

        messages.error(request, "Please fix the errors below.")
    else:
        form = VendorApprovalConfigForm(instance=config)

    config = VendorApprovalConfig.get_config()
    vendors = Vendor.objects.all().order_by("name")

    return render(
        request,
        "vendor/admin_setup.html",
        {
            "form": form,
            "vendors": vendors,
            "vendor_type_choices": Vendor.VENDOR_TYPE_CHOICES,
            "config": config,
            "finance_manual_emails_val": config.finance_manual_emails or "",
            "mumbai_manual_emails_val": config.mumbai_manual_emails or "",
        },
    )


# ── Admin: Vendor CRUD ───────────────────────────────────────────────────────

@login_required
def add_vendor(request):
    if not _is_vendor_admin(request.user):
        messages.error(request, "Admin access required.")
        return redirect("vendor:dashboard")

    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        vtype = request.POST.get("vendor_type", "other").strip()
        valid_types = dict(Vendor.VENDOR_TYPE_CHOICES)

        if vtype not in valid_types:
            messages.error(request, "Invalid vendor type selected.")
        elif not name:
            messages.error(request, "Vendor name is required.")
        elif Vendor.objects.filter(name__iexact=name).exists():
            messages.warning(request, f'Vendor "{name}" already exists.')
        else:
            Vendor.objects.create(name=name, type=vtype)
            messages.success(request, f'Vendor "{name}" added successfully.')

    return redirect("vendor:admin_setup")


@login_required
def edit_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, "Admin access required.")
        return redirect("vendor:dashboard")

    vendor = get_object_or_404(Vendor, pk=pk)

    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        vtype = request.POST.get("vendor_type", vendor.type).strip()
        valid_types = dict(Vendor.VENDOR_TYPE_CHOICES)

        if vtype not in valid_types:
            messages.error(request, "Invalid vendor type selected.")
        elif not name:
            messages.error(request, "Vendor name cannot be empty.")
        elif Vendor.objects.filter(name__iexact=name).exclude(pk=pk).exists():
            messages.warning(
                request,
                f'Another vendor named "{name}" already exists.',
            )
        else:
            vendor.name = name
            vendor.type = vtype
            vendor.save()

            messages.success(
                request,
                f'Vendor "{vendor.name}" updated successfully.',
            )

    return redirect("vendor:admin_setup")


@login_required
def delete_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, "Admin access required.")
        return redirect("vendor:dashboard")

    vendor = get_object_or_404(Vendor, pk=pk)

    if request.method == "POST":
        if VendorPaymentRequest.objects.filter(vendor=vendor).exists():
            vendor.is_active = False
            vendor.save()

            messages.warning(
                request,
                f'"{vendor.name}" has linked payment requests and cannot be deleted. '
                f"It has been deactivated instead.",
            )
        else:
            name = vendor.name
            vendor.delete()

            messages.success(
                request,
                f'Vendor "{name}" deleted permanently.',
            )

    return redirect("vendor:admin_setup")


@login_required
def toggle_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, "Admin access required.")
        return redirect("vendor:dashboard")

    vendor = get_object_or_404(Vendor, pk=pk)
    vendor.is_active = not vendor.is_active
    vendor.save()

    messages.success(
        request,
        f'Vendor "{vendor.name}" {"activated" if vendor.is_active else "deactivated"}.',
    )

    return redirect("vendor:admin_setup")