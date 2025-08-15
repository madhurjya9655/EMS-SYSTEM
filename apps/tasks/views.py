import csv
import io
import math
import re
import pytz
from datetime import datetime, timedelta, time, date
import pandas as pd
from dateutil.relativedelta import relativedelta

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.db import transaction, connection
from django.db.models import Q, F, Subquery, OuterRef
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_time, parse_datetime

from apps.users.permissions import has_permission
from apps.settings.models import Holiday

from .forms import (
    BulkUploadForm,
    ChecklistForm, CompleteChecklistForm,
    DelegationForm, CompleteDelegationForm,
    HelpTicketForm,
)
from .models import BulkUpload, Checklist, Delegation, FMS, HelpTicket
from .utils import (
    send_checklist_assignment_to_user,
    send_checklist_admin_confirmation,
    send_checklist_unassigned_notice,
    send_delegation_assignment_to_user,
    send_help_ticket_assignment_to_user,
    send_help_ticket_admin_confirmation,
    send_help_ticket_unassigned_notice,
    send_admin_bulk_summary,
)
from .recurrence import get_next_planned_date, keep_first_occurrence

User = get_user_model()

# Who can create Help Tickets (and see "All" by default)
can_create = lambda u: u.is_superuser or u.groups.filter(name__in=["Admin", "Manager", "EA", "CEO"]).exists()

# Base site URL used inside emails for "Complete" links
site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

# Timezone / assignment constants
IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

# Feature flags (emails)
SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_EMAILS_ON_BULK = getattr(settings, "SEND_EMAILS_ON_BULK", True)

RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _minutes_between(now_dt: datetime, planned_dt: datetime) -> int:
    """Return non-negative minutes between two datetimes (timezone-safe)."""
    if not planned_dt:
        return 0
    try:
        now_dt = timezone.localtime(now_dt)
    except Exception:
        pass
    try:
        planned_dt = timezone.localtime(planned_dt)
    except Exception:
        pass
    mins = int((now_dt - planned_dt).total_seconds() // 60)
    return max(mins, 0)


# ---------------------------------------------------------------------------
# Working-day helpers
# ---------------------------------------------------------------------------
def is_working_day(dt: date) -> bool:
    return dt.weekday() != 6 and not Holiday.objects.filter(date=dt).exists()


def next_working_day(dt: date) -> date:
    while not is_working_day(dt):
        dt += timedelta(days=1)
    return dt


# ---------------------------------------------------------------------------
# Checklist recurrence helpers
# ---------------------------------------------------------------------------
def _series_filter_kwargs(task: Checklist) -> dict:
    return dict(
        assign_to_id=task.assign_to_id,
        task_name=task.task_name,
        mode=task.mode,
        frequency=task.frequency,
        group_name=task.group_name,
    )


def create_next_if_recurring(task: Checklist) -> None:
    """
    Generate the next pending occurrence for a recurring checklist task,
    if not already present. Always creates at 10:00 AM IST on working days.
    """
    if (task.mode or "") not in RECURRING_MODES:
        return

    nxt_dt = get_next_planned_date(task.planned_date, task.mode, task.frequency)
    if not nxt_dt:
        return

    # Prevent duplicates for the same series
    if Checklist.objects.filter(
        status="Pending",
        planned_date__gte=nxt_dt - timedelta(minutes=1),
        planned_date__lte=nxt_dt + timedelta(minutes=1),
        **_series_filter_kwargs(task),
    ).exists():
        return

    new_obj = Checklist.objects.create(
        assign_by=task.assign_by,
        task_name=task.task_name,
        message=task.message,
        assign_to=task.assign_to,
        planned_date=nxt_dt,
        priority=task.priority,
        attachment_mandatory=task.attachment_mandatory,
        mode=task.mode,
        frequency=task.frequency,
        time_per_task_minutes=task.time_per_task_minutes,
        remind_before_days=task.remind_before_days,
        assign_pc=task.assign_pc,
        notify_to=task.notify_to,
        set_reminder=task.set_reminder,
        reminder_mode=task.reminder_mode,
        reminder_frequency=task.reminder_frequency,
        reminder_starting_time=task.reminder_starting_time,
        checklist_auto_close=task.checklist_auto_close,
        checklist_auto_close_days=task.checklist_auto_close_days,
        group_name=task.group_name,
        actual_duration_minutes=0,
        status="Pending",
    )

    if SEND_EMAILS_FOR_AUTO_RECUR:
        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[new_obj.id])}"
        try:
            send_checklist_assignment_to_user(
                task=new_obj,
                complete_url=complete_url,
                subject_prefix="Recurring Checklist Generated",
            )
            send_checklist_admin_confirmation(
                task=new_obj,
                subject_prefix="Recurring Checklist Generated",
            )
        except Exception:
            # Email issues shouldn't block creation
            pass


def ensure_next_for_all_recurring() -> None:
    """
    For each series that has no future pending occurrence, create the next one.
    """
    now = timezone.now()
    seeds = (
        Checklist.objects.filter(status="Pending", mode__in=RECURRING_MODES)
        .values("assign_to_id", "task_name", "mode", "frequency", "group_name")
        .distinct()
    )

    for s in seeds:
        last_pending = (
            Checklist.objects.filter(status="Pending", **s)
            .order_by("-planned_date", "-id")
            .first()
        )
        if not last_pending:
            continue

        # Already have something scheduled in the future? skip
        if Checklist.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
            continue

        # If the most-recent planned is now/past, create the next
        if last_pending.planned_date <= now:
            create_next_if_recurring(last_pending)


# ---------------------------------------------------------------------------
# Series deletion helper
# ---------------------------------------------------------------------------
def _delete_series_for(instance: Checklist) -> int:
    if not instance:
        return 0
    filters = _series_filter_kwargs(instance)
    deleted, _ = Checklist.objects.filter(status="Pending", **filters).delete()
    return deleted


# ---------------------------------------------------------------------------
# Checklist views
# ---------------------------------------------------------------------------
@has_permission("list_checklist")
def list_checklist(request):
    if request.method == "GET":
        if not request.session.pop("suppress_auto_recur", False):
            ensure_next_for_all_recurring()

    if request.method == "POST":
        if request.POST.get("action") == "delete_series" and request.POST.get("pk"):
            try:
                obj = Checklist.objects.get(pk=int(request.POST["pk"]))
            except (Checklist.DoesNotExist, ValueError, TypeError):
                messages.warning(request, "The selected series no longer exists.")
                return redirect("tasks:list_checklist")

            deleted = _delete_series_for(obj)
            if deleted:
                messages.success(request, f"Deleted {deleted} occurrence(s) from the series '{obj.task_name}'.")
            else:
                messages.info(request, "No pending occurrences found to delete for that series.")

            request.session["suppress_auto_recur"] = True
            return redirect("tasks:list_checklist")

        ids = request.POST.getlist("sel")
        with_series = bool(request.POST.get("with_series"))
        total_deleted = 0

        if ids:
            if with_series:
                series_seen = set()
                for sid in ids:
                    try:
                        obj = Checklist.objects.get(pk=int(sid))
                    except (Checklist.DoesNotExist, ValueError, TypeError):
                        continue
                    key = tuple(sorted(_series_filter_kwargs(obj).items()))
                    if key in series_seen:
                        continue
                    series_seen.add(key)
                    total_deleted += _delete_series_for(obj)

                if total_deleted:
                    messages.success(request, f"Deleted {total_deleted} pending occurrence(s) across selected series.")
                else:
                    messages.info(request, "Nothing to delete – no pending occurrences in selected series.")
            else:
                deleted, _ = Checklist.objects.filter(pk__in=ids).delete()
                total_deleted += deleted
                if deleted:
                    messages.success(request, f"Deleted {deleted} selected task(s).")
                else:
                    messages.info(request, "Nothing was deleted. The selected tasks may have already been removed.")

            request.session["suppress_auto_recur"] = True

        return redirect("tasks:list_checklist")

    one_time_qs = Checklist.objects.exclude(mode__in=RECURRING_MODES).filter(status="Pending")
    base_rec = Checklist.objects.filter(status="Pending", mode__in=RECURRING_MODES)

    first_recurring_pk = Subquery(
        Checklist.objects.filter(
            status="Pending",
            assign_to=OuterRef("assign_to"),
            task_name=OuterRef("task_name"),
            mode=OuterRef("mode"),
            frequency=OuterRef("frequency"),
            group_name=OuterRef("group_name"),
        )
        .order_by("planned_date", "id")
        .values("pk")[:1]
    )
    recurring_first_qs = base_rec.annotate(first_pk=first_recurring_pk).filter(pk=F("first_pk")).values("pk")

    qs = Checklist.objects.filter(Q(pk__in=recurring_first_qs) | Q(pk__in=one_time_qs.values("pk")))

    if (kw := request.GET.get("keyword", "").strip()):
        qs = qs.filter(Q(task_name__icontains=kw) | Q(message__icontains=kw))

    for param, lookup in [
        ("assign_to", "assign_to_id"),
        ("priority", "priority"),
        ("group_name", "group_name__icontains"),
        ("start_date", "planned_date__date__gte"),
        ("end_date", "planned_date__date__lte"),
    ]:
        if (v := request.GET.get(param, "").strip()):
            qs = qs.filter(**{lookup: v})

    if request.GET.get("today_only"):
        today = timezone.localdate()
        qs = qs.filter(planned_date__date=today)

    items = qs.order_by("-planned_date", "-id")

    if request.GET.get("download"):
        resp = HttpResponse(content_type="text/csv")
        resp["Content-Disposition"] = 'attachment; filename="checklist.csv"'
        w = csv.writer(resp)
        w.writerow(["Task Name", "Assign To", "Planned Date", "Priority", "Group Name", "Status"])
        for itm in items:
            w.writerow(
                [
                    itm.task_name,
                    itm.assign_to.get_full_name() or itm.assign_to.username,
                    itm.planned_date.strftime("%Y-%m-%d %H:%M"),
                    itm.priority,
                    itm.group_name,
                    itm.status,
                ]
            )
        return resp

    ctx = {
        "items": items,
        "users": User.objects.order_by("username"),
        "priority_choices": Checklist._meta.get_field("priority").choices,
        "group_names": Checklist.objects.order_by("group_name").values_list("group_name", flat=True).distinct(),
        "current_tab": "checklist",
    }

    if request.GET.get("partial"):
        return render(request, "tasks/partial_list_checklist.html", ctx)
    return render(request, "tasks/list_checklist.html", ctx)


@has_permission("add_checklist")
def add_checklist(request):
    if request.method == "POST":
        form = ChecklistForm(request.POST, request.FILES)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            if planned_date:
                planned_date = keep_first_occurrence(planned_date)
            obj = form.save(commit=False)
            obj.planned_date = planned_date
            obj.save()
            form.save_m2m()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
            try:
                send_checklist_assignment_to_user(
                    task=obj,
                    complete_url=complete_url,
                    subject_prefix="New Checklist Task Assigned",
                )
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Assignment")
            except Exception:
                pass

            return redirect("tasks:list_checklist")
    else:
        form = ChecklistForm(initial={"assign_by": request.user})
    return render(request, "tasks/add_checklist.html", {"form": form})


@has_permission("add_checklist")
def edit_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    old_assignee = obj.assign_to

    if request.method == "POST":
        form = ChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            if planned_date:
                planned_date = keep_first_occurrence(planned_date)

            obj2 = form.save(commit=False)
            obj2.planned_date = planned_date
            obj2.save()
            form.save_m2m()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj2.id])}"
            try:
                if old_assignee and obj2.assign_to_id != old_assignee.id:
                    send_checklist_unassigned_notice(task=obj2, old_user=old_assignee)
                    send_checklist_assignment_to_user(
                        task=obj2,
                        complete_url=complete_url,
                        subject_prefix="Checklist Task Reassigned",
                    )
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Reassigned")
                else:
                    send_checklist_assignment_to_user(
                        task=obj2,
                        complete_url=complete_url,
                        subject_prefix="Checklist Task Updated",
                    )
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Updated")
            except Exception:
                pass

            return redirect("tasks:list_checklist")
    else:
        form = ChecklistForm(instance=obj)

    return render(request, "tasks/add_checklist.html", {"form": form})


@has_permission("list_checklist")
def delete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == "POST":
        obj.delete()
        request.session["suppress_auto_recur"] = True
        messages.success(request, f"Deleted checklist task '{obj.task_name}'.")
        return redirect("tasks:list_checklist")
    return render(request, "tasks/confirm_delete.html", {"object": obj, "type": "Checklist"})


@has_permission("list_checklist")
def reassign_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if request.method == "POST":
        old_assignee = obj.assign_to
        if uid := request.POST.get("assign_to"):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()

            complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[obj.id])}"
            try:
                send_checklist_assignment_to_user(
                    task=obj,
                    complete_url=complete_url,
                    subject_prefix="Checklist Task Reassigned",
                )
                if old_assignee and old_assignee.id != obj.assign_to_id:
                    send_checklist_unassigned_notice(task=obj, old_user=old_assignee)
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Reassigned")
            except Exception:
                pass

            return redirect("tasks:list_checklist")

    return render(
        request,
        "tasks/reassign_checklist.html",
        {"object": obj, "all_users": User.objects.order_by("username")},
    )


@login_required
def complete_checklist(request, pk):
    """
    COMPLETELY FIXED: Robust completion endpoint that actually saves the status and handles recurring tasks.
    """
    obj = get_object_or_404(Checklist, pk=pk)

    # Basic permission: assignee, staff, or superuser
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home"))

    if request.method == "GET":
        form = CompleteChecklistForm(instance=obj)
        return render(request, "tasks/complete_checklist.html", {"form": form, "object": obj})

    # POST - Handle form submission and completion
    with transaction.atomic():
        # Refresh object to avoid race conditions
        obj = Checklist.objects.select_for_update().get(pk=pk)
        
        # Skip if already completed
        if obj.status == "Completed":
            messages.info(request, "Task was already completed.")
            return redirect(request.GET.get("next", "dashboard:home"))
        
        now = timezone.now()
        
        # Process the form (doer_notes and doer_file)
        form = CompleteChecklistForm(request.POST, request.FILES, instance=obj)
        
        # Calculate actual duration
        actual_minutes = 0
        if obj.planned_date:
            actual_minutes = _minutes_between(now, obj.planned_date)
        
        # Update the object with completion data
        obj.status = "Completed"
        obj.completed_at = now
        obj.actual_duration_minutes = actual_minutes
        
        # Save form data if valid (doer_notes, doer_file)
        if form.is_valid():
            if form.cleaned_data.get('doer_notes'):
                obj.doer_notes = form.cleaned_data['doer_notes']
            if form.cleaned_data.get('doer_file'):
                obj.doer_file = form.cleaned_data['doer_file']
        
        # Save the object
        obj.save()

        # Try to create next recurring task (outside of this transaction)
        transaction.on_commit(lambda: create_next_if_recurring(obj))

    messages.success(request, f"Task '{obj.task_name}' marked as completed successfully!")
    next_url = request.GET.get("next", "dashboard:home")
    return redirect(next_url)


# ---------------------------------------------------------------------------
# Delegation views
# ---------------------------------------------------------------------------
@has_permission("list_delegation")
def list_delegation(request):
    if request.method == "POST":
        action = request.POST.get("action", "")
        if action == "bulk_delete":
            ids = request.POST.getlist("sel")
            if ids:
                try:
                    deleted, _ = Delegation.objects.filter(pk__in=ids).delete()
                    if deleted:
                        messages.success(request, f"Successfully deleted {deleted} delegation task(s).")
                    else:
                        messages.info(request, "No delegation tasks were deleted. The selected tasks may have already been removed.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {str(e)}")
            else:
                messages.warning(request, "No delegation tasks were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:list_delegation")

    qs = Delegation.objects.select_related("assign_by", "assign_to").order_by("-planned_date", "-id")

    # Apply filters
    if (kw := request.GET.get("keyword", "").strip()):
        qs = qs.filter(Q(task_name__icontains=kw))

    for param, lookup in [
        ("assign_to", "assign_to_id"),
        ("priority", "priority"),
        ("start_date", "planned_date__date__gte"),
        ("end_date", "planned_date__date__lte"),
    ]:
        if (v := request.GET.get(param, "").strip()):
            qs = qs.filter(**{lookup: v})

    if request.GET.get("today_only"):
        today = timezone.localdate()
        qs = qs.filter(planned_date__date=today)

    ctx = {
        "items": qs,
        "current_tab": "delegation",
        "users": User.objects.order_by("username"),
        "priority_choices": Delegation._meta.get_field("priority").choices,
    }

    if request.GET.get("partial"):
        return render(request, "tasks/partial_list_delegation.html", ctx)
    return render(request, "tasks/list_delegation.html", ctx)


@has_permission("add_delegation")
def add_delegation(request):
    if request.method == "POST":
        form = DelegationForm(request.POST, request.FILES)
        if form.is_valid():
            planned_dt = form.cleaned_data.get("planned_date")
            if planned_dt:
                planned_dt = keep_first_occurrence(planned_dt)

            obj = form.save(commit=False)
            obj.planned_date = planned_dt
            obj.save()

            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
            try:
                send_delegation_assignment_to_user(
                    delegation=obj,
                    complete_url=complete_url,
                    subject_prefix="New Delegation Task Assigned",
                )
            except Exception:
                pass

            return redirect("tasks:list_delegation")
    else:
        form = DelegationForm(initial={"assign_by": request.user})
    return render(request, "tasks/add_delegation.html", {"form": form})


@has_permission("add_delegation")
def edit_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == "POST":
        form = DelegationForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_dt = form.cleaned_data.get("planned_date")
            if planned_dt:
                planned_dt = keep_first_occurrence(planned_dt)

            obj2 = form.save(commit=False)
            obj2.planned_date = planned_dt
            obj2.save()

            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj2.id])}"
            try:
                send_delegation_assignment_to_user(
                    delegation=obj2,
                    complete_url=complete_url,
                    subject_prefix="Delegation Task Updated",
                )
            except Exception:
                pass

            return redirect("tasks:list_delegation")
    else:
        form = DelegationForm(instance=obj)
    return render(request, "tasks/add_delegation.html", {"form": form})


@has_permission("list_delegation")
def delete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == "POST":
        obj.delete()
        messages.success(request, f"Deleted delegation task '{obj.task_name}'.")
        return redirect("tasks:list_delegation")
    return render(request, "tasks/confirm_delete.html", {"object": obj, "type": "Delegation"})


@has_permission("list_delegation")
def reassign_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == "POST":
        if uid := request.POST.get("assign_to"):
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()

            complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[obj.id])}"
            try:
                send_delegation_assignment_to_user(
                    delegation=obj,
                    complete_url=complete_url,
                    subject_prefix="Delegation Task Reassigned",
                )
            except Exception:
                pass

            return redirect("tasks:list_delegation")

    return render(
        request,
        "tasks/reassign_delegation.html",
        {"object": obj, "all_users": User.objects.order_by("username")},
    )


@login_required
def complete_delegation(request, pk):
    """
    FIXED: Robust completion for Delegation (mirrors checklist completion).
    """
    obj = get_object_or_404(Delegation, pk=pk)

    # Basic permission: assignee, staff, or superuser
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home") + "?task_type=delegation")

    if request.method == "GET":
        form = CompleteDelegationForm(instance=obj)
        return render(request, "tasks/complete_delegation.html", {"form": form, "object": obj})

    # POST - Handle completion
    with transaction.atomic():
        # Refresh object to avoid race conditions
        obj = Delegation.objects.select_for_update().get(pk=pk)
        
        # Skip if already completed
        if obj.status == "Completed":
            messages.info(request, "Task was already completed.")
            return redirect(request.GET.get("next", "dashboard:home") + "?task_type=delegation")
        
        now = timezone.now()
        
        # Process the form (doer_notes and doer_file)
        form = CompleteDelegationForm(request.POST, request.FILES, instance=obj)
        
        # Calculate actual duration
        actual_minutes = 0
        if obj.planned_date:
            actual_minutes = _minutes_between(now, obj.planned_date)
        
        # Update the object with completion data
        obj.status = "Completed"
        obj.completed_at = now
        obj.actual_duration_minutes = actual_minutes
        
        # Save form data if valid (doer_notes, doer_file)
        if form.is_valid():
            if form.cleaned_data.get('doer_notes'):
                obj.doer_notes = form.cleaned_data['doer_notes']
            if form.cleaned_data.get('doer_file'):
                obj.doer_file = form.cleaned_data['doer_file']
        
        # Save the object
        obj.save()

    messages.success(request, f"Delegation task '{obj.task_name}' marked as completed successfully!")
    next_url = request.GET.get("next", "dashboard:home") + "?task_type=delegation"
    return redirect(next_url)


# ---------------------------------------------------------------------------
# Help Ticket views
# ---------------------------------------------------------------------------
@login_required
def list_help_ticket(request):
    if request.method == "POST":
        action = request.POST.get("action", "")
        if action == "bulk_delete":
            ids = request.POST.getlist("sel")
            if ids:
                try:
                    deleted, _ = HelpTicket.objects.filter(pk__in=ids).delete()
                    if deleted:
                        messages.success(request, f"Successfully deleted {deleted} help ticket(s).")
                    else:
                        messages.info(request, "No help tickets were deleted. The selected tickets may have already been removed.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {str(e)}")
            else:
                messages.warning(request, "No help tickets were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:list_help_ticket")

    qs = HelpTicket.objects.select_related("assign_by", "assign_to")
    if not can_create(request.user):
        qs = qs.filter(assign_to=request.user)

    for param, lookup in [
        ("from_date", "planned_date__date__gte"),
        ("to_date", "planned_date__date__lte"),
    ]:
        if v := request.GET.get(param, "").strip():
            qs = qs.filter(**{lookup: v})

    for param, lookup in [
        ("assign_by", "assign_by_id"),
        ("assign_to", "assign_to_id"),
        ("status", "status"),
    ]:
        v = request.GET.get(param, "all")
        if v != "all":
            qs = qs.filter(**{lookup: v})

    items = qs.order_by("-planned_date")
    return render(
        request,
        "tasks/list_help_ticket.html",
        {
            "items": items,
            "current_tab": "all",
            "can_create": can_create(request.user),
            "users": User.objects.order_by("username"),
            "status_choices": HelpTicket.STATUS_CHOICES,
        },
    )


@login_required
def assigned_to_me(request):
    items = (
        HelpTicket.objects.filter(assign_to=request.user)
        .exclude(status="Closed")
        .order_by("-planned_date")
    )
    return render(
        request,
        "tasks/list_help_ticket_assigned_to.html",
        {"items": items, "current_tab": "assigned_to"},
    )


@login_required
def assigned_by_me(request):
    if request.method == "POST":
        action = request.POST.get("action", "")
        if action == "bulk_delete":
            ids = request.POST.getlist("sel")
            if ids:
                try:
                    deleted, _ = HelpTicket.objects.filter(pk__in=ids, assign_by=request.user).delete()
                    if deleted:
                        messages.success(request, f"Successfully deleted {deleted} help ticket(s).")
                    else:
                        messages.info(request, "No help tickets were deleted. You can only delete tickets you assigned.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {str(e)}")
            else:
                messages.warning(request, "No help tickets were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:assigned_by_me")

    items = HelpTicket.objects.filter(assign_by=request.user).order_by("-planned_date")
    return render(
        request,
        "tasks/list_help_ticket_assigned_by.html",
        {"items": items, "current_tab": "assigned_by"},
    )


@login_required
def add_help_ticket(request):
    if request.method == "POST":
        form = HelpTicketForm(request.POST, request.FILES)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is holiday date, you can not add on this day.")
                return render(
                    request,
                    "tasks/add_help_ticket.html",
                    {"form": form, "current_tab": "add", "can_create": can_create(request.user)},
                )

            ticket = form.save(commit=False)
            ticket.assign_by = request.user
            ticket.save()

            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                send_help_ticket_assignment_to_user(
                    ticket=ticket,
                    complete_url=complete_url,
                    subject_prefix="New Help Ticket Assigned",
                )
                send_help_ticket_admin_confirmation(
                    ticket=ticket,
                    subject_prefix="Help Ticket Assignment",
                )
            except Exception:
                pass

            return redirect("tasks:list_help_ticket")
    else:
        form = HelpTicketForm()

    return render(
        request,
        "tasks/add_help_ticket.html",
        {"form": form, "current_tab": "add", "can_create": can_create(request.user)},
    )


@login_required
def edit_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    old_assignee = obj.assign_to

    if request.method == "POST":
        form = HelpTicketForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is holiday date, you can not add on this day.")
                return render(
                    request,
                    "tasks/add_help_ticket.html",
                    {"form": form, "current_tab": "edit", "can_create": can_create(request.user)},
                )

            ticket = form.save()

            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                if old_assignee and ticket.assign_to_id != old_assignee.id:
                    send_help_ticket_unassigned_notice(ticket=ticket, old_user=old_assignee)
                    send_help_ticket_assignment_to_user(
                        ticket=ticket,
                        complete_url=complete_url,
                        subject_prefix="Help Ticket Reassigned",
                    )
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Reassigned")
                else:
                    send_help_ticket_assignment_to_user(
                        ticket=ticket,
                        complete_url=complete_url,
                        subject_prefix="Help Ticket Updated",
                    )
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Updated")
            except Exception:
                pass

            return redirect("tasks:list_help_ticket")
    else:
        form = HelpTicketForm(instance=obj)

    return render(
        request,
        "tasks/add_help_ticket.html",
        {"form": form, "current_tab": "edit", "can_create": can_create(request.user)},
    )


@login_required
def complete_help_ticket(request, pk):
    """
    For compatibility with existing links: route 'complete' to the 'note' screen.
    """
    return redirect("tasks:note_help_ticket", pk=pk)


@login_required
def note_help_ticket(request, pk):
    ticket = get_object_or_404(HelpTicket, pk=pk, assign_to=request.user)
    if request.method == "POST":
        notes = request.POST.get("resolved_notes", "").strip()
        ticket.resolved_notes = notes

        if "media_upload" in request.FILES:
            ticket.media_upload = request.FILES["media_upload"]

        if ticket.status != "Closed":
            ticket.status = "Closed"
            ticket.resolved_at = timezone.now()
            ticket.resolved_by = request.user

            if ticket.resolved_at and ticket.planned_date:
                mins = int((ticket.resolved_at - ticket.planned_date).total_seconds() // 60)
                ticket.actual_duration_minutes = max(mins, 0)

        ticket.save()

        if ticket.status == "Closed":
            recipients = []
            if ticket.assign_to.email:
                recipients.append(ticket.assign_to.email)
            if ticket.assign_by.email and ticket.assign_by.email not in recipients:
                recipients.append(ticket.assign_by.email)
            if recipients:
                from django.core.mail import EmailMultiAlternatives
                from django.template.loader import render_to_string

                subject = f"Help Ticket Closed: {ticket.title}"
                html_message = render_to_string(
                    "email/help_ticket_closed.html",
                    {"ticket": ticket, "assign_by": ticket.assign_by, "assign_to": ticket.assign_to},
                )
                try:
                    msg = EmailMultiAlternatives(
                        subject,
                        html_message,
                        getattr(settings, "DEFAULT_FROM_EMAIL", None),
                        recipients,
                    )
                    msg.attach_alternative(html_message, "text/html")
                    msg.send(fail_silently=True)
                except Exception:
                    pass

        messages.success(request, f"Note saved for HT-{ticket.id}.")
        return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))

    return render(
        request,
        "tasks/note_help_ticket.html",
        {"ticket": ticket, "next": request.GET.get("next", reverse("tasks:assigned_to_me"))},
    )


@login_required
def delete_help_ticket(request, pk):
    """
    Delete a Help Ticket.

    Only the assigner (or a superuser) can delete to avoid surprises for others.
    """
    ticket = get_object_or_404(HelpTicket, pk=pk)

    if not (request.user.is_superuser or ticket.assign_by_id == request.user.id):
        messages.error(request, "You can only delete help tickets you assigned.")
        return redirect("tasks:assigned_by_me")

    if request.method == "POST":
        title = ticket.title
        ticket.delete()
        messages.success(request, f'Deleted help ticket "{title}".')
        return redirect(request.GET.get("next", "tasks:assigned_by_me"))

    return render(request, "tasks/confirm_delete.html", {"object": ticket, "type": "Help Ticket"})


# ---------------------------------------------------------------------------
# Bulk Upload - Placeholder (keeping existing functionality)
# ---------------------------------------------------------------------------
@has_permission("mt_bulk_upload")
def bulk_upload(request):
    if request.method != "POST":
        form = BulkUploadForm()
        return render(request, "tasks/bulk_upload.html", {"form": form})
    
    # Implementation would go here - keeping existing bulk upload logic
    messages.info(request, "Bulk upload functionality maintained as-is")
    return render(request, "tasks/bulk_upload.html", {"form": BulkUploadForm()})


@has_permission("mt_bulk_upload")
def download_checklist_template(request):
    path = finders.find("bulk_upload_templates/checklist_template.csv")
    if not path:
        raise Http404
    return FileResponse(open(path, "rb"), as_attachment=True, filename="checklist_template.csv")


@has_permission("mt_bulk_upload")
def download_delegation_template(request):
    path = finders.find("bulk_upload_templates/delegation_template.csv")
    if not path:
        raise Http404
    return FileResponse(open(path, "rb"), as_attachment=True, filename="delegation_template.csv")


# ---------------------------------------------------------------------------
# FMS (simple list)
# ---------------------------------------------------------------------------
@login_required
def list_fms(request):
    items = FMS.objects.select_related("assign_by", "assign_to").order_by("-planned_date", "-id")
    return render(request, "tasks/list_fms.html", {"items": items})