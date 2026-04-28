# D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\recruitment\views.py
from __future__ import annotations

from datetime import date
from typing import Tuple

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import transaction
from django.http import HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse, reverse_lazy
from django.views.generic import CreateView, DeleteView, DetailView, ListView, UpdateView

from .forms import (
    CandidateForm,
    CandidateStatusForm,
    EmployeeForm,
    InterviewFeedbackForm,
    InterviewScheduleForm,
)
from .models import Candidate, Employee, InterviewFeedback, InterviewSchedule
from apps.leave.models import ApproverMapping, CCConfiguration
from apps.users.models import Profile

User = get_user_model()


# ---------------------------------------------------------------------
# Employees page (list) — shows routing columns
# ---------------------------------------------------------------------
class EmployeeListView(LoginRequiredMixin, ListView):
    login_url = "login"
    model = Employee
    template_name = "recruitment/employee_list.html"
    context_object_name = "employees"

    def get_queryset(self):
        # Suppress default object_list; template uses 'rows' we provide.
        return Employee.objects.none()

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # Fetch ALL users (active + inactive) so admins see the full picture.
        # Filter by is_active in template using user.is_active — single source of truth.
        users = (
            User.objects.select_related("profile", "employee_record")
            .order_by("username")
        )

        # Prefetch ApproverMapping into a dict by user_id.
        # Important: prefetch default_cc_users so Employee page shows all assigned CC users.
        mappings = {
            m.employee_id: m
            for m in ApproverMapping.objects.select_related(
                "employee",
                "reporting_person",
                "cc_person",
            ).prefetch_related("default_cc_users")
        }

        # Prefetch CCConfiguration into a dict by user_id.
        cc_configs = {
            c.user_id: c
            for c in CCConfiguration.objects.select_related("user")
        }

        def full_name(u) -> str:
            name = (getattr(u, "get_full_name", lambda: "")() or "").strip()
            if name:
                return name

            first = (getattr(u, "first_name", "") or "").strip()
            last = (getattr(u, "last_name", "") or "").strip()
            combo = f"{first} {last}".strip()

            return combo or (u.username or "").strip()

        def label_with_email(u) -> str:
            if not u:
                return ""

            nm = full_name(u)
            em = (u.email or "").strip()

            return f"{nm} ({em})" if em else nm

        rows = []

        for u in users:
            prof = getattr(u, "profile", None)
            mapping = mappings.get(u.id)
            cc_config = cc_configs.get(u.id)

            rp_name = ""
            rp_email = ""

            if mapping and mapping.reporting_person:
                rp = mapping.reporting_person
                rp_name = full_name(rp) or (rp.username or "")
                rp_email = (rp.email or "").strip()

            # Show all CC users:
            # 1. Old single CC field: mapping.cc_person
            # 2. New multi CC field: mapping.default_cc_users
            cc_users = []

            if mapping:
                if mapping.cc_person:
                    cc_users.append(mapping.cc_person)

                try:
                    cc_users.extend(list(mapping.default_cc_users.all()))
                except Exception:
                    pass

            seen_cc_emails = set()
            cc_labels = []

            for cc_user in cc_users:
                cc_email = (getattr(cc_user, "email", "") or "").strip().lower()

                if not cc_email:
                    continue

                if cc_email in seen_cc_emails:
                    continue

                seen_cc_emails.add(cc_email)
                cc_labels.append(label_with_email(cc_user))

            cc_label = ", ".join(cc_labels)

            rows.append(
                {
                    "user_id": u.id,
                    "email": (u.email or "").strip(),
                    "name": full_name(u),
                    "mobile": (getattr(prof, "phone", None) or ""),
                    "md_name": rp_name,
                    "reporting_officer": (
                        f"{rp_name} ({rp_email})" if (rp_name or rp_email) else ""
                    ),
                    "cc_label": cc_label,
                    "cc_config_active": cc_config.is_active if cc_config else False,
                    "designation": (getattr(prof, "role", "") or ""),
                    "mapping_id": mapping.id if mapping else None,
                    # STATUS: always from User.is_active — single source of truth.
                    "is_active": u.is_active,
                }
            )

        ctx["rows"] = rows
        return ctx


# ---------------------------------------------------------------------
# Minimal add/delete endpoints used by the Employees list page
# ---------------------------------------------------------------------
def _split_name(full: str) -> Tuple[str, str]:
    full = (full or "").strip()

    if not full:
        return "", ""

    parts = full.split()

    if len(parts) == 1:
        return parts[0], ""

    return " ".join(parts[:-1]), parts[-1]


@login_required
@user_passes_test(lambda u: u.is_superuser)
@transaction.atomic
def employee_create(request):
    """
    POST-only endpoint backing the Employees page.

    Creates/updates:
      - auth User
      - Profile
      - Employee record
      - ApproverMapping.reporting_person
      - ApproverMapping.cc_person
      - ApproverMapping.default_cc_users
      - CCConfiguration when add_to_cc_config is selected

    Important CC behavior:
      - The submitted selected CC users are treated as final source of truth.
      - If admin changes Amreen to XYZ, Amreen is removed and XYZ is saved.
      - If admin clears CC selection, old CC is cleared from cc_person and default_cc_users.
    """
    if request.method != "POST":
        return HttpResponseForbidden("POST required")

    next_url = request.POST.get("next") or reverse("recruitment:employee_list")

    email = (request.POST.get("email") or "").strip().lower()
    name = (request.POST.get("name") or "").strip()
    mobile = (request.POST.get("mobile") or "").strip()
    designation = (request.POST.get("designation") or "").strip()

    reporting_person_id = (request.POST.get("reporting_person_id") or "").strip()
    cc_person_id = (request.POST.get("cc_person_id") or "").strip()

    # Supports multiple possible template field names.
    # Your template can use any one of these:
    #   default_cc_users
    #   default_cc_users[]
    #   cc_users
    #   cc_users[]
    #   assigned_cc_users
    #   assigned_cc_users[]
    default_cc_user_ids = (
        request.POST.getlist("default_cc_users")
        or request.POST.getlist("default_cc_users[]")
        or request.POST.getlist("cc_users")
        or request.POST.getlist("cc_users[]")
        or request.POST.getlist("assigned_cc_users")
        or request.POST.getlist("assigned_cc_users[]")
    )

    add_to_cc_config = bool(request.POST.get("add_to_cc_config"))

    if not email or not name:
        messages.error(request, "E-mail and Employee Name are required.")
        return redirect(next_url)

    username_base = (email.split("@")[0] or name.split()[0] or "user").lower()
    username = username_base

    user = User.objects.filter(email__iexact=email).first()

    if user is None:
        i = 1

        while User.objects.filter(username__iexact=username).exists():
            i += 1
            username = f"{username_base}{i}"

        user = User(username=username, email=email)

        first, last = _split_name(name)
        user.first_name = first
        user.last_name = last
        user.set_unusable_password()
        user.save()

        created = True

    else:
        first, last = _split_name(name)
        changed = False

        if first != (user.first_name or ""):
            user.first_name = first
            changed = True

        if last != (user.last_name or ""):
            user.last_name = last
            changed = True

        if email != (user.email or "").strip().lower():
            user.email = email
            changed = True

        if changed:
            user.save(update_fields=["first_name", "last_name", "email"])

        created = False

    profile, _ = Profile.objects.get_or_create(user=user)

    prof_changed = False

    if mobile and profile.phone != mobile:
        profile.phone = mobile
        prof_changed = True

    if designation and profile.role != designation:
        profile.role = designation
        prof_changed = True

    if prof_changed:
        profile.save()

    from apps.recruitment.models import Employee  # noqa: PLC0415

    employee, emp_created = Employee.objects.get_or_create(
        email=email,
        defaults={
            "user": user,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "phone": mobile,
            "department": designation,
            "is_active": user.is_active,
        },
    )

    if not emp_created:
        update_fields = []

        if employee.user_id != user.pk:
            employee.user = user
            update_fields.append("user")

        if employee.first_name != user.first_name:
            employee.first_name = user.first_name
            update_fields.append("first_name")

        if employee.last_name != user.last_name:
            employee.last_name = user.last_name
            update_fields.append("last_name")

        if mobile and employee.phone != mobile:
            employee.phone = mobile
            update_fields.append("phone")

        if designation and employee.department != designation:
            employee.department = designation
            update_fields.append("department")

        if employee.is_active != user.is_active:
            employee.is_active = user.is_active
            update_fields.append("is_active")

        if update_fields:
            employee.save(update_fields=update_fields)

    reporting_person = None

    if reporting_person_id.isdigit():
        reporting_person = User.objects.filter(
            id=int(reporting_person_id),
            is_active=True,
        ).first()

    # ------------------------------------------------------------------
    # CC resolution from Employee page form
    # ------------------------------------------------------------------
    cc_person = None

    if cc_person_id.isdigit():
        cc_person = User.objects.filter(
            id=int(cc_person_id),
            is_active=True,
        ).exclude(email="").first()

    default_cc_ids = []

    for raw_id in default_cc_user_ids:
        raw_id = (raw_id or "").strip()

        if raw_id.isdigit():
            default_cc_ids.append(int(raw_id))

    # Remove duplicate IDs while preserving submitted order.
    seen_ids = set()
    cleaned_default_cc_ids = []

    for uid in default_cc_ids:
        if uid in seen_ids:
            continue

        seen_ids.add(uid)
        cleaned_default_cc_ids.append(uid)

    selected_cc_users_qs = (
        User.objects
        .filter(id__in=cleaned_default_cc_ids, is_active=True)
        .exclude(email="")
    )

    selected_cc_by_id = {
        u.id: u
        for u in selected_cc_users_qs
    }

    default_cc_users = [
        selected_cc_by_id[uid]
        for uid in cleaned_default_cc_ids
        if uid in selected_cc_by_id
    ]

    # Compatibility rule:
    # If template sends cc_person_id separately, include it in multi-CC also.
    # This makes selected cc_person work even if the template has only a single CC dropdown.
    if cc_person and cc_person.id not in {u.id for u in default_cc_users}:
        default_cc_users.insert(0, cc_person)

    # Source-of-truth rule:
    # Whatever is selected now becomes the full CC list.
    # If nothing is selected, cc_person becomes None and default_cc_users becomes empty.
    if default_cc_users:
        cc_person = default_cc_users[0]
    else:
        cc_person = None

    mapping, _ = ApproverMapping.objects.get_or_create(
        employee=user,
        defaults={
            "reporting_person": reporting_person,
            "cc_person": cc_person,
        },
    )

    mapping_changed = False

    if mapping.reporting_person_id != (reporting_person.id if reporting_person else None):
        mapping.reporting_person = reporting_person
        mapping_changed = True

    if mapping.cc_person_id != (cc_person.id if cc_person else None):
        mapping.cc_person = cc_person
        mapping_changed = True

    if mapping_changed:
        mapping.save()

    # This is the most important line.
    # It REPLACES old CC users with the newly selected CC users.
    # Example:
    #   Old: Amreen
    #   New selected: XYZ
    # Result:
    #   Amreen removed, XYZ saved.
    mapping.default_cc_users.set(default_cc_users)

    if add_to_cc_config:
        cc_config, _ = CCConfiguration.objects.get_or_create(
            user=user,
            defaults={
                "is_active": True,
                "department": designation or "Other",
                "sort_order": 20,
            },
        )

        cc_config_changed = False

        if not cc_config.is_active:
            cc_config.is_active = True
            cc_config_changed = True

        if designation and cc_config.department != designation:
            cc_config.department = designation
            cc_config_changed = True

        if cc_config_changed:
            cc_config.save(update_fields=["is_active", "department", "updated_at"])

    cc_email_list = [
        (u.email or "").strip()
        for u in default_cc_users
        if (u.email or "").strip()
    ]

    messages.success(
        request,
        f"{'Created' if created else 'Updated'} employee: {name} ({email}). "
        f"Reporting To: {getattr(reporting_person, 'email', None) or '—'}, "
        f"CC: {', '.join(cc_email_list) if cc_email_list else '—'}"
    )

    return redirect(next_url)


@login_required
@user_passes_test(lambda u: u.is_superuser)
@transaction.atomic
def employee_delete(request, user_id: int):
    """
    POST-only: deletes the Django auth User.

    Employee record is deleted automatically via CASCADE.
    Cleans up mappings and CC config first.
    """
    if request.method != "POST":
        return HttpResponseForbidden("POST required")

    next_url = request.POST.get("next") or reverse("recruitment:employee_list")
    user = get_object_or_404(User, pk=user_id)

    # Clean up ApproverMapping references.
    ApproverMapping.objects.filter(employee_id=user.id).delete()
    ApproverMapping.objects.filter(reporting_person_id=user.id).update(reporting_person=None)
    ApproverMapping.objects.filter(cc_person_id=user.id).update(cc_person=None)

    # Clean up ManyToMany references where this user is assigned as default CC.
    for mapping in ApproverMapping.objects.filter(default_cc_users=user):
        mapping.default_cc_users.remove(user)

    # Clean up CC Configuration.
    CCConfiguration.objects.filter(user_id=user.id).delete()

    name = getattr(user, "get_full_name", lambda: "")() or user.username or user.email

    # Deleting User cascades to Employee via OneToOneField(on_delete=CASCADE).
    user.delete()

    messages.success(request, f"Deleted employee: {name}")
    return redirect(next_url)


# ---------------------------------------------------------------------
# Legacy CRUD around Employee model (kept as-is, not used by list page)
# ---------------------------------------------------------------------
class EmployeeCreateView(LoginRequiredMixin, CreateView):
    login_url = "login"
    model = Employee
    form_class = EmployeeForm
    template_name = "recruitment/employee_form.html"

    def get_success_url(self):
        return reverse("employee_detail", args=[self.object.pk])


class EmployeeDetailView(LoginRequiredMixin, DetailView):
    login_url = "login"
    model = Employee
    template_name = "recruitment/employee_detail.html"
    context_object_name = "employee"


class EmployeeUpdateView(LoginRequiredMixin, UpdateView):
    login_url = "login"
    model = Employee
    form_class = EmployeeForm
    template_name = "recruitment/employee_form.html"
    success_url = reverse_lazy("employee_list")


class EmployeeDeleteView(LoginRequiredMixin, DeleteView):
    login_url = "login"
    model = Employee
    template_name = "recruitment/employee_confirm_delete.html"
    success_url = reverse_lazy("employee_list")


# ---------------------------------------------------------------------
# Candidates / Interviews (unchanged)
# ---------------------------------------------------------------------
@login_required
def candidate_list(request):
    qs = Candidate.objects.all()
    return render(request, "recruitment/candidate_list.html", {"candidates": qs})


@login_required
def candidate_detail(request, pk):
    obj = get_object_or_404(Candidate, pk=pk)
    return render(request, "recruitment/candidate_detail.html", {"candidate": obj})


hr_or_manager_or_super = lambda u: u.is_superuser or u.groups.filter(name__in=["HR", "Manager"]).exists()


@login_required
@user_passes_test(hr_or_manager_or_super)
def update_status(request, pk):
    obj = get_object_or_404(Candidate, pk=pk)

    if request.method == "POST":
        form = CandidateStatusForm(request.POST, instance=obj)

        if form.is_valid():
            form.save()
            return redirect("candidate_detail", pk=pk)

    else:
        form = CandidateStatusForm(instance=obj)

    return render(request, "recruitment/update_status.html", {"form": form, "candidate": obj})


@login_required
@user_passes_test(hr_or_manager_or_super)
def schedule_interview(request):
    if request.method == "POST":
        form = InterviewScheduleForm(request.POST)

        if form.is_valid():
            form.save()
            return redirect("interview_list")

    else:
        form = InterviewScheduleForm()

    return render(request, "recruitment/schedule_interview.html", {"form": form})


@login_required
def interview_list(request):
    qs = InterviewSchedule.objects.all()
    return render(request, "recruitment/interview_list.html", {"interviews": qs})


@login_required
def interview_feedback(request, pk):
    interview = get_object_or_404(InterviewSchedule, pk=pk)

    if request.method == "POST":
        form = InterviewFeedbackForm(request.POST)

        if form.is_valid():
            form.save()
            return redirect("interview_list")

    else:
        form = InterviewFeedbackForm(
            initial={
                "interview": interview,
                "reviewer": request.user,
            }
        )

    return render(
        request,
        "recruitment/interview_feedback.html",
        {
            "form": form,
            "interview": interview,
        },
    )


@login_required
def offer_letter(request, pk):
    candidate = get_object_or_404(Candidate, pk=pk)
    context = {
        "candidate": candidate,
        "date": date.today(),
    }

    html = render_to_string("recruitment/offer_letter.html", context)
    resp = HttpResponse(html, content_type="application/msword")
    resp["Content-Disposition"] = f'attachment; filename="OfferLetter_{candidate.name}.doc"'

    return resp