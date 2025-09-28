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
    # We won't use the ORM list from this model; we compose rows from AUTH users.
    model = Employee
    template_name = "recruitment/employee_list.html"
    context_object_name = "employees"

    def get_queryset(self):
        # Suppress default object_list; template uses 'rows' we provide.
        return Employee.objects.none()

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # IMPORTANT: no .only(...) with select_related('profile') to avoid deferred clash
        users = User.objects.select_related("profile").order_by("username")

        # Prefetch ApproverMapping into a dict by user_id
        mappings = {
            m.employee_id: m
            for m in ApproverMapping.objects.select_related("employee", "reporting_person", "cc_person")
        }

        # Prefetch CCConfiguration into a dict by user_id
        cc_configs = {
            c.user_id: c
            for c in CCConfiguration.objects.select_related("user")
        }

        def full_name(u: User) -> str:
            name = (getattr(u, "get_full_name", lambda: "")() or "").strip()
            if name:
                return name
            first = (getattr(u, "first_name", "") or "").strip()
            last = (getattr(u, "last_name", "") or "").strip()
            combo = f"{first} {last}".strip()
            return combo or (u.username or "").strip()

        rows = []
        for u in users:
            prof: Profile | None = getattr(u, "profile", None)
            mapping: ApproverMapping | None = mappings.get(u.id)
            cc_config: CCConfiguration | None = cc_configs.get(u.id)

            # Reporting officer (from ApproverMapping)
            rp_name = rp_email = ""
            if mapping and mapping.reporting_person:
                rp = mapping.reporting_person
                rp_name = full_name(rp) or (rp.username or "")
                rp_email = (rp.email or "").strip()

            # CC email (single, from ApproverMapping)
            cc_email = ""
            if mapping and mapping.cc_person:
                cc = mapping.cc_person
                cc_email = (cc.email or "").strip()

            # "MD Name" — mirror Reporting officer (as per requirement)
            md_name = rp_name

            rows.append(
                {
                    "user_id": u.id,  # used by the custom Edit Mapping page
                    "email": (u.email or "").strip(),
                    "name": full_name(u),
                    "mobile": (getattr(prof, "phone", None) or ""),
                    "md_name": md_name,
                    "reporting_officer": f"{rp_name} ({rp_email})" if rp_name or rp_email else "",
                    "cc_email": cc_email,
                    "cc_config_active": cc_config.is_active if cc_config else False,
                    # Use role as designation if no explicit designation field exists
                    "designation": (getattr(prof, "role", "") or ""),
                    # admin edit shortcut (fallback link)
                    "mapping_id": mapping.id if mapping else None,
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
    POST-only endpoint backing the 'Add Employee' modal.
    Creates (or updates) a Django auth User + Profile + CC Configuration.
    """
    if request.method != "POST":
        return HttpResponseForbidden("POST required")

    next_url = request.POST.get("next") or reverse("recruitment:employee_list")

    email = (request.POST.get("email") or "").strip().lower()
    name = (request.POST.get("name") or "").strip()
    mobile = (request.POST.get("mobile") or "").strip()
    designation = (request.POST.get("designation") or "").strip()
    add_to_cc_config = bool(request.POST.get("add_to_cc_config"))

    if not email or not name:
        messages.error(request, "E-mail and Employee Name are required.")
        return redirect(next_url)

    username_base = (email.split("@")[0] or name.split()[0] or "user").lower()
    username = username_base

    # If a user with the email exists, update it; else create a new one.
    user = User.objects.filter(email__iexact=email).first()
    if user is None:
        # Make sure username is unique
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
        # Update name if provided
        first, last = _split_name(name)
        if first or last:
            user.first_name = first
            user.last_name = last
            user.save(update_fields=["first_name", "last_name"])
        created = False

    # Ensure Profile exists and update fields we show in the grid
    profile, _ = Profile.objects.get_or_create(user=user)
    changed = False
    if mobile and profile.phone != mobile:
        profile.phone = mobile
        changed = True
    if designation and profile.role != designation:
        profile.role = designation
        changed = True
    if changed:
        profile.save()

    # Add to CC Configuration if requested
    if add_to_cc_config:
        cc_config, cc_created = CCConfiguration.objects.get_or_create(
            user=user,
            defaults={
                'is_active': True,
                'department': designation or 'Other',
                'sort_order': 20,  # Default sort order for new users
            }
        )
        if cc_created:
            messages.info(request, f"Added {name} to CC configuration options.")

    messages.success(request, f"{'Created' if created else 'Updated'} employee: {name} ({email})")
    return redirect(next_url)


@login_required
@user_passes_test(lambda u: u.is_superuser)
@transaction.atomic
def employee_delete(request, user_id: int):
    """
    POST-only: deletes the Django auth User and detaches any ApproverMapping links and CC config.
    """
    if request.method != "POST":
        return HttpResponseForbidden("POST required")

    next_url = request.POST.get("next") or reverse("recruitment:employee_list")
    user = get_object_or_404(User, pk=user_id)

    # Clean up mappings that reference this user
    # 1) If the user is an employee in mapping → delete those rows
    ApproverMapping.objects.filter(employee_id=user.id).delete()
    # 2) If used as reporting/cc → null them out (if nullable)
    ApproverMapping.objects.filter(reporting_person_id=user.id).update(reporting_person=None)
    ApproverMapping.objects.filter(cc_person_id=user.id).update(cc_person=None)

    # Clean up CC Configuration
    CCConfiguration.objects.filter(user_id=user.id).delete()

    name = getattr(user, "get_full_name", lambda: "")() or user.username or user.email
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
        form = InterviewFeedbackForm(initial={"interview": interview, "reviewer": request.user})
    return render(request, "recruitment/interview_feedback.html", {"form": form, "interview": interview})


@login_required
def offer_letter(request, pk):
    candidate = get_object_or_404(Candidate, pk=pk)
    context = {"candidate": candidate, "date": date.today()}
    html = render_to_string("recruitment/offer_letter.html", context)
    resp = HttpResponse(html, content_type="application/msword")
    resp["Content-Disposition"] = f'attachment; filename="OfferLetter_{candidate.name}.doc"'
    return resp
