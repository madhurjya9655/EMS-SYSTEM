# apps/leave/migrations/0005_start_end_and_mapping_nullable.py
from django.db import migrations, models
from django.conf import settings
from django.db.models import deletion
from django.utils import timezone
import datetime
import pytz


def backfill_start_end(apps, schema_editor):
    LeaveRequest = apps.get_model('leave', 'LeaveRequest')
    tz = pytz.timezone('Asia/Kolkata')
    now = timezone.now()

    # For any rows where start_at/end_at are NULL, fill them from start_date/end_date.
    # If start_date/end_date are also missing, fall back to "now" to satisfy NOT NULL.
    qs = LeaveRequest.objects.all().only('id', 'start_at', 'end_at', 'start_date', 'end_date')
    for lr in qs:
        need_start = lr.start_at is None
        need_end = lr.end_at is None
        if not (need_start or need_end):
            continue

        if lr.start_date and lr.end_date:
            sdt_naive = datetime.datetime.combine(lr.start_date, datetime.time(0, 0, 0))
            edt_naive = datetime.datetime.combine(lr.end_date, datetime.time(23, 59, 59))
            sdt = tz.localize(sdt_naive)
            # inclusive end → push to last microsecond of day
            edt = tz.localize(edt_naive) + datetime.timedelta(microseconds=999_999)
        else:
            # Fallback to avoid nulls if old data is incomplete
            sdt = now
            edt = now + datetime.timedelta(hours=1)

        if need_start:
            lr.start_at = sdt
        if need_end:
            lr.end_at = edt
        lr.save(update_fields=['start_at', 'end_at'])


class Migration(migrations.Migration):

    dependencies = [
        ('leave', '0004_merge_20250908_1202'),
    ]

    operations = [
        # 1) Make ApproverMapping FKs nullable + SET_NULL so deletes don’t fail
        migrations.AlterField(
            model_name='approvermapping',
            name='reporting_person',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=deletion.SET_NULL,
                related_name='reports_for_approval',
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AlterField(
            model_name='approvermapping',
            name='cc_person',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=deletion.SET_NULL,
                related_name='cc_for_approval',
                to=settings.AUTH_USER_MODEL,
            ),
        ),

        # 2) Add start_at/end_at as NULLABLE first (so we can backfill)
        migrations.AddField(
            model_name='leaverequest',
            name='start_at',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='leaverequest',
            name='end_at',
            field=models.DateTimeField(null=True),
        ),

        # 3) Backfill from start_date/end_date (or now as fallback)
        migrations.RunPython(backfill_start_end, migrations.RunPython.noop),

        # 4) Enforce NOT NULL on start_at/end_at after backfill
        migrations.AlterField(
            model_name='leaverequest',
            name='start_at',
            field=models.DateTimeField(null=False),
        ),
        migrations.AlterField(
            model_name='leaverequest',
            name='end_at',
            field=models.DateTimeField(null=False),
        ),
    ]
