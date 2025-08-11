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
                holiday_objs = []
                failed_rows = []
                seen_dates = set()
                for idx, row in enumerate(rows, 2):
                    date_val = row.get("date") or row.get("Date")
                    name_val = row.get("name") or row.get("Name")
                    try:
                        date_obj = pd.to_datetime(date_val).date()
                        if date_obj.weekday() == 6:
                            failed_rows.append(f"Row {idx}: {date_obj} is a Sunday")
                            continue
                        if not name_val:
                            failed_rows.append(f"Row {idx}: missing name")
                            continue
                        if date_obj in seen_dates:
                            failed_rows.append(f"Row {idx}: {date_obj} duplicate in file")
                            continue
                        if Holiday.objects.filter(date=date_obj).exists():
                            failed_rows.append(f"Row {idx}: {date_obj} already exists")
                            continue
                        seen_dates.add(date_obj)
                        holiday_objs.append(Holiday(date=date_obj, name=str(name_val).strip()))
                    except Exception as e:
                        failed_rows.append(f"Row {idx}: Invalid ({date_val}) {str(e)}")
                if failed_rows:
                    messages.error(request, "Upload failed:<br>" + "<br>".join(failed_rows))
                else:
                    with transaction.atomic():
                        Holiday.objects.bulk_create(holiday_objs)
                    messages.success(request, f"{len(holiday_objs)} holiday(s) uploaded successfully.")
                return redirect("settings:holiday_list")
    return render(request, "settings/holiday_list.html", {"holidays": holidays, "add_form": add_form, "upload_form": upload_form})


@login_required
@user_passes_test(is_superuser)
def holiday_delete(request, pk):
    obj = get_object_or_404(Holiday, pk=pk)
    if request.method == "POST":
        obj.delete()
        return redirect("settings:holiday_list")
    return render(request, "settings/confirm_delete.html", {"object": obj, "type": "Holiday"})


@login_required
@user_passes_test(is_superuser)
def system_settings(request):
    settings = SystemSetting.objects.first()
    if not settings:
        settings = SystemSetting.objects.create()
    if request.method == "POST":
        form = SystemSettingsForm(request.POST, request.FILES, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "System settings updated.")
            return redirect("settings:system_settings")
    else:
        form = SystemSettingsForm(instance=settings)
    return render(request, "settings/system_settings.html", {"form": form})
