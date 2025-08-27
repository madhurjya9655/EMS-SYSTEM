from __future__ import annotations

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def backfill_datetimes(apps, schema_editor):
    from datetime import datetime, time, timedelta
    from django.utils import timezone

    try:
        from zoneinfo import ZoneInfo
        ist = ZoneInfo("Asia/Kolkata")
    except Exception:  # pragma: no cover
        ist = None

    LeaveRequest = apps.get_model("leave", "LeaveRequest")

    status_map = {
        "PM": "PENDING",
        "PH": "PENDING",
        "A": "APPROVED",
        "R": "REJECTED",
        "Pending Manager": "PENDING",
        "Pending HR": "PENDING",
        "Approved": "APPROVED",
        "Rejected": "REJECTED",
    }

    qs = LeaveRequest.objects.all()
    for lr in qs.iterator():
        # Normalize status to new TextChoices values
        old = getattr(lr, "status", None)
        new = status_map.get(old, None)
        if new and new != old:
            lr.status = new

        s = getattr(lr, "start_at", None)
        e = getattr(lr, "end_at", None)

        if not s or not e:
            if getattr(lr, "start_date", None) and getattr(lr, "end_date", None):
                sdt = datetime.combine(lr.start_date, time(9, 0, 0))
                edt = datetime.combine(lr.end_date, time(18, 0, 0))
            else:
                base = getattr(lr, "applied_at", None) or timezone.now()
                if timezone.is_naive(base):
                    try:
                        base = timezone.make_aware(base, ist)
                    except Exception:
                        base = timezone.make_aware(base)
                sdt = base
                edt = base + timedelta(hours=9)

            # Make aware in IST
            if ist:
                if timezone.is_naive(sdt):
                    sdt = timezone.make_aware(sdt, ist)
                if timezone.is_naive(edt):
                    edt = timezone.make_aware(edt, ist)
            else:
                sdt = timezone.make_aware(sdt)
                edt = timezone.make_aware(edt)

            lr.start_at = sdt
            lr.end_at = edt

        # Minimal snapshot safety (don’t override if present)
        lr.employee_name = lr.employee_name or (getattr(lr.employee, "get_full_name", lambda: "")() or lr.employee.username)
        lr.employee_email = lr.employee_email or (lr.employee.email or "")
        lr.employee_designation = lr.employee_designation or ""

        lr.save(
            update_fields=[
                "status",
                "start_at",
                "end_at",
                "employee_name",
                "employee_email",
                "employee_designation",
            ]
        )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("leave", "0001_initial"),
    ]

    operations = [
        # Add new columns as nullable first
        migrations.AddField(
            model_name="leaverequest",
            name="start_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="end_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="is_half_day",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="attachment",
            field=models.FileField(
                upload_to="apps.leave.models.leave_attachment_upload_to", null=True, blank=True
            ),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="manager",
            field=models.ForeignKey(
                to=settings.AUTH_USER_MODEL,
                on_delete=django.db.models.deletion.SET_NULL,
                null=True,
                blank=True,
                related_name="managed_leave_requests",
            ),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="approver",
            field=models.ForeignKey(
                to=settings.AUTH_USER_MODEL,
                on_delete=django.db.models.deletion.SET_NULL,
                null=True,
                blank=True,
                related_name="approved_leaves",
            ),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="decided_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="decision_comment",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="employee_name",
            field=models.CharField(max_length=150, blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="employee_email",
            field=models.EmailField(max_length=254, blank=True),
        ),
        migrations.AddField(
            model_name="leaverequest",
            name="employee_designation",
            field=models.CharField(max_length=150, blank=True),
        ),

        # Optional: make LeaveType.name unique to match new model
        migrations.AlterField(
            model_name="leavetype",
            name="name",
            field=models.CharField(max_length=50, unique=True),
        ),

        # Backfill data safely
        migrations.RunPython(backfill_datetimes, noop),

        # Now enforce NOT NULL on datetimes
        migrations.AlterField(
            model_name="leaverequest",
            name="start_at",
            field=models.DateTimeField(null=False, blank=False),
        ),
        migrations.AlterField(
            model_name="leaverequest",
            name="end_at",
            field=models.DateTimeField(null=False, blank=False),
        ),
    ]
