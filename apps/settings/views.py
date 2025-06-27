import csv, io
import pandas as pd
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, redirect, get_object_or_404
from .models import AuthorizedNumber, Holiday
from .forms import AuthorizedNumberForm, HolidayForm, HolidayUploadForm

def admin_only(user):
    return user.is_superuser

@login_required
@user_passes_test(admin_only)
def authorized_list(request):
    nums = AuthorizedNumber.objects.all().order_by('label')
    form = AuthorizedNumberForm(request.POST or None)
    if request.method == 'POST' and form.is_valid():
        form.save()
        return redirect('settings:authorized_list')
    return render(request, "settings/authorized_list.html", {
        "numbers": nums,
        "form":     form,
    })

@login_required
@user_passes_test(admin_only)
def authorized_delete(request, pk):
    num = get_object_or_404(AuthorizedNumber, pk=pk)
    num.delete()
    return redirect('settings:authorized_list')


@login_required
@user_passes_test(admin_only)
def holiday_list(request):
    holidays = Holiday.objects.all()
    add_form    = HolidayForm(request.POST or None, prefix="add")
    upload_form = HolidayUploadForm(request.POST or None, request.FILES or None, prefix="upl")

    # manual add
    if request.method == 'POST' and 'add-date' in request.POST and add_form.is_valid():
        add_form.save()
        return redirect('settings:holiday_list')

    # file upload
    if request.method == 'POST' and 'upl-file' in request.FILES and upload_form.is_valid():
        f = upload_form.cleaned_data['file']
        ext = f.name.rsplit('.',1)[-1].lower()
        if ext in ('xls','xlsx'):
            df = pd.read_excel(f)
        else:
            raw = f.read().decode('utf-8-sig')
            df = pd.read_csv(io.StringIO(raw))
        # expect columns 'date','name'
        for _, row in df.iterrows():
            try:
                Holiday.objects.update_or_create(
                    date=row['date'],
                    defaults={'name': row['name']}
                )
            except Exception:
                continue
        return redirect('settings:holiday_list')

    return render(request, "settings/holiday_list.html", {
        "holidays":    holidays,
        "add_form":    add_form,
        "upload_form": upload_form,
    })

@login_required
@user_passes_test(admin_only)
def holiday_delete(request, pk):
    h = get_object_or_404(Holiday, pk=pk)
    h.delete()
    return redirect('settings:holiday_list')
