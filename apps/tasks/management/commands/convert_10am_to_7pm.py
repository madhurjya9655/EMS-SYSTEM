# apps/tasks/management/commands/convert_10am_to_7pm.py
from __future__ import annotations

from typing import List
import pytz

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction

from apps.tasks.models import Checklist
from apps.tasks.recurrence import RECURRING_MODES

IST = pytz.timezone("Asia/Kolkata")
FROM_HOUR = 10   # 10:00 AM IST
TO_HOUR = 19     # 7:00 PM IST


def _as_ist(dt):
    """
    Return dt as an aware IST datetime.
    If dt is naive, assume it was intended as IST wall-clock.
    """
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return dt.astimezone(IST)
    return IST.localize(dt)


def _is_ist_10am(dt, tolerance_sec: int = 59) -> bool:
    ist = _as_ist(dt)
    secs = ist.hour * 3600 + ist.minute * 60 + ist.second
    target = FROM_HOUR * 3600  # 10:00:00
    return abs(secs - target) <= max(0, int(tolerance_sec))


class Command(BaseCommand):
    help = (
        "Shift EXISTING recurring Checklist rows set at 10:00 IST to 19:00 IST. "
        "Only affects status=Pending items with a valid recurrence."
    )

    def add_arguments(self, parser):
        parser.add_argument("--commit", action="store_true", help="Write the changes.")
        parser.add_argument("--dry-run", action="store_true", help="Only show what would change.")
        parser.add_argument("--user-id", type=int, help="Limit to a specific assignee.")
        parser.add_argument("--tolerance", type=int, default=59,
                            help="Seconds tolerance around 10:00 (default 59).")

    def handle(self, *args, **opts):
        commit = bool(opts.get("commit"))
        dry_run = bool(opts.get("dry_run")) or not commit
        tol = int(opts.get("tolerance") or 59)
        user_id = opts.get("user_id")

        filters = {
            "status": "Pending",
            "mode__in": RECURRING_MODES,
            "frequency__gte": 1,
        }
        if user_id:
            filters["assign_to_id"] = user_id

        qs = (
            Checklist.objects.filter(**filters)
            .only("id", "planned_date", "task_name", "assign_to", "mode", "frequency")
            .order_by("planned_date", "id")
        )

        tz = timezone.get_current_timezone()
        to_update: List[Checklist] = []
        scanned = 0
        matched = 0
        sample_ids: List[int] = []

        for obj in qs.iterator(chunk_size=500):
            scanned += 1
            if _is_ist_10am(obj.planned_date, tol):
                matched += 1
                ist = _as_ist(obj.planned_date)
                new_ist = ist.replace(hour=TO_HOUR, minute=0, second=0, microsecond=0)
                obj.planned_date = new_ist.astimezone(tz)
                to_update.append(obj)
                if len(sample_ids) < 10:
                    sample_ids.append(obj.id)

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY-RUN — no data written"))
            self.stdout.write(f"Scanned: {scanned:,} | Matches (10:00±{tol}s): {matched:,}")
            if sample_ids:
                self.stdout.write(f"Sample IDs to change: {', '.join(map(str, sample_ids))}")
            return

        if not to_update:
            self.stdout.write("Nothing to update.")
            return

        with transaction.atomic():
            Checklist.objects.bulk_update(to_update, ["planned_date"], batch_size=500)

        self.stdout.write(self.style.SUCCESS(
            f"Done. Scanned {scanned:,}, updated {len(to_update):,} to 19:00 IST."
        ))
        if sample_ids:
            self.stdout.write(f"Sample updated IDs: {', '.join(map(str, sample_ids))}")
