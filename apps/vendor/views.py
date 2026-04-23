from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.mail import EmailMessage
from django.template.loader import render_to_string
from django.conf import settings

from .models import Vendor, VendorPaymentRequest, VendorApprovalConfig
from .forms import VendorPaymentRequestForm, VendorApprovalConfigForm
from apps.users.permissions import _user_permission_codes


# ── Permission helpers ────────────────────────────────────────────────────────

def _can_access_vendor(user):
    """
    True if user can access the vendor module at all.
    Superusers always pass. Others need at least one vendor permission.
    """
    if getattr(user, 'is_superuser', False):
        return True

    codes = _user_permission_codes(user)
    vendor_codes = {
        'vendor_create',
        'vendor_view_own',
        'vendor_finance_approve',
        'vendor_final_approve',
        'vendor_admin',
    }
    return bool(codes & vendor_codes)


def _can_create(user):
    """User can submit new vendor payment requests."""
    if getattr(user, 'is_superuser', False):
        return True

    codes = _user_permission_codes(user)
    return 'vendor_create' in codes


def _is_finance(user):
    """
    Finance approver check — sources:
      1. Superuser
      2. Profile has 'vendor_finance_approve'
      3. Listed in VendorApprovalConfig.finance_users
      4. Email in VendorApprovalConfig.finance_manual_emails
    """
    if getattr(user, 'is_superuser', False):
        return True

    codes = _user_permission_codes(user)
    if 'vendor_finance_approve' in codes:
        return True

    config = VendorApprovalConfig.get_config()
    if config.finance_users.filter(pk=user.pk).exists():
        return True

    if user.email:
        manual = [e.strip() for e in (config.finance_manual_emails or '').split(',') if e.strip()]
        if user.email in manual:
            return True

    return False


def _is_senior(user):
    """
    Senior authority check — sources:
      1. Superuser
      2. Profile has 'vendor_final_approve'
      3. Set as senior_authority in VendorApprovalConfig
    """
    if getattr(user, 'is_superuser', False):
        return True

    codes = _user_permission_codes(user)
    if 'vendor_final_approve' in codes:
        return True

    config = VendorApprovalConfig.get_config()
    return config.senior_authority_id == user.pk


def _is_vendor_admin(user):
    """Can access vendor admin setup page."""
    if getattr(user, 'is_superuser', False):
        return True

    codes = _user_permission_codes(user)
    return 'vendor_admin' in codes


# ── Email helpers ─────────────────────────────────────────────────────────────

def _send_submission_email(request, obj):
    config = VendorApprovalConfig.get_config()
    to_list = config.get_finance_email_list()
    if not to_list:
        return

    subject = f'New Vendor Payment Request — {obj.request_id}'
    body = (
        f"A new vendor payment request has been submitted and requires finance approval.\n\n"
        f"Request ID   : {obj.request_id}\n"
        f"Vendor       : {obj.vendor_display_name}\n"
        f"Invoice No   : {obj.invoice_number}\n"
        f"Total Amount : INR {obj.total_amount}\n"
        f"Submitted By : {obj.created_by.get_full_name() or obj.created_by.username}\n\n"
        f"Please log in to review and approve.\n{request.build_absolute_uri('/')}"
    )
    try:
        EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_list,
        ).send(fail_silently=True)
    except Exception:
        pass


def _send_finance_approved_email(request, obj):
    config = VendorApprovalConfig.get_config()
    if not config.senior_authority or not config.senior_authority.email:
        return

    subject = f'Vendor Payment Ready for Final Approval — {obj.request_id}'
    body = (
        f"A vendor payment has cleared finance review and awaits your final approval.\n\n"
        f"Request ID       : {obj.request_id}\n"
        f"Vendor           : {obj.vendor_display_name}\n"
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
        pass


def _send_final_approval_email(request, obj):
    """TO: mumbai system users + manual emails, CC: cc_emails"""
    config = VendorApprovalConfig.get_config()
    to_list = config.get_mumbai_email_list()
    cc_list = config.get_cc_email_list()
    if not to_list:
        return

    subject = f'Vendor Payment Approved — {obj.request_id}'
    body = render_to_string('vendor/email/final_approval.txt', {
        'obj': obj,
        'site_url': request.build_absolute_uri('/'),
    })
    try:
        EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_list,
            cc=cc_list,
        ).send(fail_silently=True)
    except Exception:
        pass


def _send_rejection_email(request, obj):
    if not obj.created_by.email:
        return

    subject = f'Vendor Payment Request Rejected — {obj.request_id}'
    body = (
        f"Your vendor payment request has been rejected.\n\n"
        f"Request ID : {obj.request_id}\n"
        f"Vendor     : {obj.vendor_display_name}\n"
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
        pass


# ── Views ─────────────────────────────────────────────────────────────────────

@login_required
def dashboard(request):
    user = request.user

    if not _can_access_vendor(user):
        messages.error(request, 'You do not have access to the Vendor Payments module.')
        return redirect('dashboard:home')

    my_count = VendorPaymentRequest.objects.filter(created_by=user).count()
    pending_finance = VendorPaymentRequest.objects.filter(status='submitted').count() if _is_finance(user) else 0
    pending_senior = VendorPaymentRequest.objects.filter(status='finance_approved').count() if _is_senior(user) else 0

    return render(request, 'vendor/dashboard.html', {
        'my_count': my_count,
        'pending_finance': pending_finance,
        'pending_senior': pending_senior,
        'is_finance': _is_finance(user),
        'is_senior': _is_senior(user),
        'can_create': _can_create(user),
        'is_vendor_admin': _is_vendor_admin(user),
    })


@login_required
def new_request(request):
    if not _can_create(request.user):
        messages.error(request, 'You do not have permission to create vendor payment requests.')
        return redirect('vendor:dashboard')

    if request.method == 'POST':
        form = VendorPaymentRequestForm(request.POST, request.FILES)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.created_by = request.user
            obj.status = 'submitted' if request.POST.get('action') == 'submit' else 'draft'
            obj.save()

            if obj.status == 'submitted':
                _send_submission_email(request, obj)
                messages.success(request, f'Request {obj.request_id} submitted. Finance team notified.')
            else:
                messages.success(request, f'Request {obj.request_id} saved as draft.')

            return redirect('vendor:my_requests')
    else:
        form = VendorPaymentRequestForm()

    return render(request, 'vendor/new_request.html', {'form': form})


@login_required
def my_requests(request):
    if not _can_access_vendor(request.user):
        messages.error(request, 'Access denied.')
        return redirect('dashboard:home')

    qs = VendorPaymentRequest.objects.filter(created_by=request.user).select_related('vendor')
    sf = request.GET.get('status', '')
    if sf:
        qs = qs.filter(status=sf)

    return render(request, 'vendor/my_requests.html', {
        'requests': qs,
        'status_filter': sf,
        'status_choices': VendorPaymentRequest.STATUS_CHOICES,
    })


@login_required
def approval_queue(request):
    user = request.user
    is_fin = _is_finance(user)
    is_sen = _is_senior(user)

    if not (is_fin or is_sen):
        messages.warning(request, 'You are not configured as an approver.')
        return redirect('vendor:dashboard')

    if is_fin and is_sen:
        qs = VendorPaymentRequest.objects.filter(
            status__in=['submitted', 'finance_approved']
        ).select_related('vendor', 'created_by')
    elif is_fin:
        qs = VendorPaymentRequest.objects.filter(
            status='submitted'
        ).select_related('vendor', 'created_by')
    else:
        qs = VendorPaymentRequest.objects.filter(
            status='finance_approved'
        ).select_related('vendor', 'created_by')

    return render(request, 'vendor/approval_queue.html', {
        'requests': qs,
        'is_finance': is_fin,
        'is_senior': is_sen,
    })


@login_required
def detail(request, pk):
    obj = get_object_or_404(VendorPaymentRequest, pk=pk)
    is_fin = _is_finance(request.user)
    is_sen = _is_senior(request.user)

    if not (obj.created_by == request.user or is_fin or is_sen or request.user.is_superuser):
        messages.error(request, 'Access denied.')
        return redirect('vendor:dashboard')

    return render(request, 'vendor/detail.html', {
        'obj': obj,
        'can_finance_action': is_fin and obj.status == 'submitted',
        'can_senior_action': is_sen and obj.status == 'finance_approved',
        'can_resubmit': obj.created_by == request.user and obj.status == 'draft',
    })


@login_required
def resubmit(request, pk):
    obj = get_object_or_404(
        VendorPaymentRequest,
        pk=pk,
        created_by=request.user,
        status='draft',
    )
    obj.status = 'submitted'
    obj.save()
    _send_submission_email(request, obj)
    messages.success(request, f'{obj.request_id} submitted. Finance team notified.')
    return redirect('vendor:detail', pk=pk)


@login_required
def finance_action(request, pk):
    if request.method != 'POST':
        return redirect('vendor:approval_queue')

    obj = get_object_or_404(VendorPaymentRequest, pk=pk)
    if not _is_finance(request.user) or obj.status != 'submitted':
        messages.error(request, 'Not authorized or invalid status.')
        return redirect('vendor:approval_queue')

    action = request.POST.get('action')
    remarks = request.POST.get('remarks', '').strip()

    if action == 'approve':
        obj.status = 'finance_approved'
        obj.finance_approved_by = request.user
        obj.remarks = remarks
        obj.save()
        _send_finance_approved_email(request, obj)
        messages.success(request, f'{obj.request_id} finance approved. Senior authority notified.')
    elif action == 'reject':
        obj.status = 'rejected'
        obj.remarks = remarks
        obj.save()
        _send_rejection_email(request, obj)
        messages.warning(request, f'{obj.request_id} rejected.')

    return redirect('vendor:approval_queue')


@login_required
def senior_action(request, pk):
    if request.method != 'POST':
        return redirect('vendor:approval_queue')

    obj = get_object_or_404(VendorPaymentRequest, pk=pk)
    if not _is_senior(request.user) or obj.status != 'finance_approved':
        messages.error(request, 'Not authorized or invalid status.')
        return redirect('vendor:approval_queue')

    action = request.POST.get('action')
    remarks = request.POST.get('remarks', '').strip()

    if action == 'approve':
        obj.status = 'final_approved'
        obj.final_approved_by = request.user
        obj.remarks = remarks
        obj.save()
        _send_final_approval_email(request, obj)
        messages.success(request, f'{obj.request_id} finally approved. Accounts team notified.')
    elif action == 'reject':
        obj.status = 'rejected'
        obj.remarks = remarks
        obj.save()
        _send_rejection_email(request, obj)
        messages.warning(request, f'{obj.request_id} rejected.')

    return redirect('vendor:approval_queue')


# ── Admin: Approval Config ────────────────────────────────────────────────────

@login_required
def admin_setup(request):
    if not _is_vendor_admin(request.user):
        messages.error(request, 'Admin access required.')
        return redirect('vendor:dashboard')

    config = VendorApprovalConfig.get_config()

    if request.method == 'POST' and 'save_config' in request.POST:
        form = VendorApprovalConfigForm(request.POST, instance=config)
        if form.is_valid():
            form.save()
            messages.success(request, 'Approval configuration saved successfully.')
            return redirect('vendor:admin_setup')
        messages.error(request, 'Please fix the errors below.')
    else:
        form = VendorApprovalConfigForm(instance=config)

    config = VendorApprovalConfig.get_config()
    vendors = Vendor.objects.all().order_by('name')

    return render(request, 'vendor/admin_setup.html', {
        'form': form,
        'vendors': vendors,
        'vendor_type_choices': Vendor.VENDOR_TYPE_CHOICES,
        'config': config,
        'finance_manual_emails_val': config.finance_manual_emails or '',
        'mumbai_manual_emails_val': config.mumbai_manual_emails or '',
    })


# ── Admin: Vendor CRUD ────────────────────────────────────────────────────────

@login_required
def add_vendor(request):
    if not _is_vendor_admin(request.user):
        messages.error(request, 'Admin access required.')
        return redirect('vendor:dashboard')

    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        vtype = request.POST.get('vendor_type', 'other').strip()

        if not name:
            messages.error(request, 'Vendor name is required.')
        elif Vendor.objects.filter(name__iexact=name).exists():
            messages.warning(request, f'Vendor "{name}" already exists.')
        else:
            Vendor.objects.create(name=name, type=vtype)
            messages.success(request, f'Vendor "{name}" added successfully.')

    return redirect('vendor:admin_setup')


@login_required
def edit_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, 'Admin access required.')
        return redirect('vendor:dashboard')

    v = get_object_or_404(Vendor, pk=pk)

    if request.method == 'POST':
        name = request.POST.get('name', '').strip()
        vtype = request.POST.get('vendor_type', v.type).strip()

        if not name:
            messages.error(request, 'Vendor name cannot be empty.')
        elif Vendor.objects.filter(name__iexact=name).exclude(pk=pk).exists():
            messages.warning(request, f'Another vendor named "{name}" already exists.')
        else:
            v.name = name
            v.type = vtype
            v.save()
            messages.success(request, f'Vendor "{v.name}" updated successfully.')

    return redirect('vendor:admin_setup')


@login_required
def delete_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, 'Admin access required.')
        return redirect('vendor:dashboard')

    v = get_object_or_404(Vendor, pk=pk)

    if request.method == 'POST':
        if VendorPaymentRequest.objects.filter(vendor=v).exists():
            v.is_active = False
            v.save()
            messages.warning(
                request,
                f'"{v.name}" has linked payment requests and cannot be deleted. '
                f'It has been deactivated instead.'
            )
        else:
            name = v.name
            v.delete()
            messages.success(request, f'Vendor "{name}" deleted permanently.')

    return redirect('vendor:admin_setup')


@login_required
def toggle_vendor(request, pk):
    if not _is_vendor_admin(request.user):
        messages.error(request, 'Admin access required.')
        return redirect('vendor:dashboard')

    v = get_object_or_404(Vendor, pk=pk)
    v.is_active = not v.is_active
    v.save()
    messages.success(request, f'Vendor "{v.name}" {"activated" if v.is_active else "deactivated"}.')

    return redirect('vendor:admin_setup')