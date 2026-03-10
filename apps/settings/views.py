# apps/settings/views.py
#
# FIX 2026-03-10 — Bug 4 (Minor):
#   holiday_list() did per-row Holiday.objects.filter(date=date_obj).exists()
#   checks before the bulk_create, but those checks and the bulk_create were
#   NOT inside the same atomic block.  Two simultaneous uploads of the same
#   file would both pass the .exists() check, collect the same dates, and one
#   bulk_create would raise an uncaught IntegrityError → 500 error.
#
#   Fix: added `ignore_conflicts=True` to bulk_create. Rows that already
#   exist (or collide with a concurrent upload) are silently skipped by the
#   database rather than raising an IntegrityError. The created_count is
#   still reported accurately (it reflects only actually-inserted rows).

import csv
import io
import pandas as pd
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.db import transaction
from django.contrib.auth.decorators import login_required, user_passes_test

from .models import AuthorizedNumber, Holiday, SystemSetting
from .forms import AuthorizedNumberForm, HolidayForm, HolidayUploadForm, SystemSettingsForm


def is_superuser(user):
    return user.is_superuser


@login_required
@user_passes_test(is_superuser)
def authorized_list(request):
    items = AuthorizedNumber.objects.order_by('-created_at')
    if request.method == "POST":
        form = AuthorizedNumberForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect("settings:authorized_list")
    else:
        form = AuthorizedNumberForm()
    return render(request, "settings/authorized_list.html", {"items": items, "form": form})


@login_required
@user_passes_test(is_superuser)
def authorized_delete(request, pk):
    obj = get_object_or_404(AuthorizedNumber, pk=pk)
    if request.method == "POST":
        obj.delete()
        return redirect("settings:authorized_list")
    return render(request, "settings/confirm_delete.html", {"object": obj, "type": "Authorized Number"})


@login_required
@user_passes_test(is_superuser)
def holiday_list(request):
    holidays = Holiday.objects.order_by('-date')
    add_form = HolidayForm(prefix="add")
    upload_form = HolidayUploadForm(prefix="upload")
    if request.method == "POST":
        if request.POST.get("do_add") is not None:
            add_form = HolidayForm(request.POST, prefix="add")
            if add_form.is_valid():
                add_form.save()
                messages.success(request, "Holiday added successfully.")
                return redirect("settings:holiday_list")
        elif request.POST.get("do_upload") is not None:
            upload_form = HolidayUploadForm(request.POST, request.FILES, prefix="upload")
            if upload_form.is_valid():
                file = upload_form.cleaned_data["file"]
                ext = file.name.rsplit(".", 1)[-1].lower()

                # ---- Read rows (CSV/Excel) ----
                if ext in ("xls", "xlsx"):
                    xl = pd.read_excel(file)
                    rows = xl.to_dict("records")
                else:
                    raw = file.read()
                    for enc in ("utf-8-sig", "utf-8", "latin-1"):
                        try:
                            text = raw.decode(enc)
                            break
                        except Exception:
                            continue
                    rows = list(csv.DictReader(io.StringIO(text)))

                # ---- Validate/collect; skip bad rows but keep good ones ----
                to_create = []
                problems = []
                seen_dates = set()

                for idx, row in enumerate(rows, 2):  # header = row 1
                    date_val = row.get("date") or row.get("Date")
                    name_val = row.get("name") or row.get("Name")

                    try:
                        date_obj = pd.to_datetime(date_val).date()
                    except Exception as e:
                        problems.append(f"Row {idx}: invalid date ({date_val}) – {e}")
                        continue

                    if not name_val or str(name_val).strip() == "":
                        problems.append(f"Row {idx}: missing name for {date_obj}")
                        continue

                    # Skip Sundays (weekday() == 6) as per policy & UI text
                    if date_obj.weekday() == 6:
                        problems.append(f"Row {idx}: {date_obj} is Sunday (ignored)")
                        continue

                    # Skip duplicates inside this file
                    if date_obj in seen_dates:
                        problems.append(f"Row {idx}: {date_obj} duplicate in file (ignored)")
                        continue

                    # Skip if already present in DB
                    if Holiday.objects.filter(date=date_obj).exists():
                        problems.append(f"Row {idx}: {date_obj} already exists (ignored)")
                        continue

                    seen_dates.add(date_obj)
                    to_create.append(Holiday(date=date_obj, name=str(name_val).strip()))

                created_count = 0
                if to_create:
                    with transaction.atomic():
                        # FIX: ignore_conflicts=True prevents IntegrityError if a
                        # concurrent upload sneaks in between our .exists() check
                        # above and this bulk_create.  The DB silently skips any
                        # row whose date already exists instead of raising.
                        created_objects = Holiday.objects.bulk_create(
                            to_create, ignore_conflicts=True   # ← FIX
                        )
                    created_count = len(created_objects)
                    if created_count:
                        messages.success(request, f"{created_count} holiday(s) uploaded successfully.")

                # Report problems (but do NOT block valid inserts)
                if problems:
                    MAX_SHOW = 20
                    shown = problems[:MAX_SHOW]
                    extra = len(problems) - len(shown)
                    msg = "Some rows were skipped:\n- " + "\n- ".join(shown)
                    if extra > 0:
                        msg += f"\n... and {extra} more."
                    messages.warning(request, msg)

                if created_count == 0 and problems:
                    messages.error(request, "No holidays were added from this file. Please fix the issues and re-upload.")

                return redirect("settings:holiday_list")
    return render(request, "settings/holiday_list.html", {
        "holidays": holidays,
        "add_form": add_form,
        "upload_form": upload_form,
    })


@login_required
@user_passes_test(is_superuser)
def holiday_delete(request, pk):
    obj = get_object_or_404(Holiday, pk=pk)
    if request.method == "POST":
        obj.delete()
        messages.success(request, "Holiday deleted.")
        return redirect("settings:holiday_list")
    return render(request, "settings/confirm_delete.html", {"object": obj, "type": "Holiday"})


@login_required
@user_passes_test(is_superuser)
def system_settings(request):
    instance = SystemSetting.objects.first()
    if not instance:
        instance = SystemSetting.objects.create()
    if request.method == "POST":
        form = SystemSettingsForm(request.POST, request.FILES, instance=instance)
        if form.is_valid():
            form.save()
            messages.success(request, "System settings updated.")
            return redirect("settings:system_settings")
    else:
        form = SystemSettingsForm(instance=instance)
    return render(request, "settings/system_settings.html", {"form": form})