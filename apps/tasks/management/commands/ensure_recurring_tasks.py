import pytz
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
from django.conf import settings
import logging

from apps.tasks.models import Checklist
from apps.settings.models import Holiday

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
TEN_AM = time(10, 0, 0)
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]


def is_working_day(d):
    if hasattr(d, "date"):
        d = d.date()
    return d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()


def next_working_day(d):
    if hasattr(d, "date"):
        d = d.date()
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def extract_ist_wallclock(dt):
    if timezone.is_naive(dt):
        dt = IST.localize(dt)
    dt_ist = dt.astimezone(IST)
    return dt_ist.date(), time(dt_ist.hour, dt_ist.minute, dt_ist.second, dt_ist.microsecond)


def ist_wallclock_to_project_tz(d, t):
    ist_dt = IST.localize(datetime.combine(d, t))
    return ist_dt.astimezone(timezone.get_current_timezone())


def get_next_planned_datetime(prev_dt, mode, freq):
    if not prev_dt or mode not in RECURRING_MODES:
        return None

    base_date_ist, _ = extract_ist_wallclock(prev_dt)
    seed_ist = IST.localize(datetime.combine(base_date_ist, TEN_AM))

    step = max(int(freq or 1), 1)
    if mode == "Daily":
        next_ist = seed_ist + relativedelta(days=step)
    elif mode == "Weekly":
        next_ist = seed_ist + relativedelta(weeks=step)
    elif mode == "Monthly":
        next_ist = seed_ist + relativedelta(months=step)
    else:
        next_ist = seed_ist + relativedelta(years=step)

    next_date = next_working_day(next_ist.date())
    return ist_wallclock_to_project_tz(next_date, TEN_AM)


class Command(BaseCommand):
    help = 'Ensure all recurring checklist series have future pending tasks'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating tasks',
        )
        parser.add_argument(
            '--user-id',
            type=int,
            help='Only process recurring tasks for specific user ID',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        user_id = options['user_id']
        
        now = timezone.now()
        created_count = 0
        
        self.stdout.write(f"Checking recurring checklist series at {now}")
        
        filters = {'mode__in': RECURRING_MODES}
        if user_id:
            filters['assign_to_id'] = user_id
            
        seeds = (
            Checklist.objects.filter(**filters)
            .values('assign_to_id', 'task_name', 'mode', 'frequency', 'group_name')
            .distinct()
        )

        total_series = seeds.count()
        self.stdout.write(f"Found {total_series} recurring series to check")

        for i, s in enumerate(seeds, 1):
            self.stdout.write(f"Processing series {i}/{total_series}: {s['task_name']}")
            
            try:
                last = (
                    Checklist.objects
                    .filter(**s)
                    .order_by('-planned_date', '-id')
                    .first()
                )
                if not last:
                    self.stdout.write("  No tasks found for series, skipping")
                    continue

                future_exists = Checklist.objects.filter(
                    status='Pending', 
                    planned_date__gt=now, 
                    **s
                ).exists()
                
                if future_exists:
                    self.stdout.write("  Future task already exists, skipping")
                    continue

                next_planned = get_next_planned_datetime(last.planned_date, last.mode, last.frequency)
                if not next_planned:
                    self.stdout.write("  Could not calculate next date, skipping")
                    continue

                safety = 0
                while next_planned and next_planned <= now and safety < 730:
                    next_planned = get_next_planned_datetime(next_planned, last.mode, last.frequency)
                    safety += 1
                if not next_planned:
                    self.stdout.write("  Could not advance to future date, skipping")
                    continue

                is_dupe = Checklist.objects.filter(
                    assign_to_id=s['assign_to_id'],
                    task_name=s['task_name'],
                    mode=s['mode'],
                    frequency=s['frequency'],
                    group_name=s['group_name'],
                    planned_date__gte=next_planned - timedelta(minutes=1),
                    planned_date__lt=next_planned + timedelta(minutes=1),
                    status='Pending',
                ).exists()
                if is_dupe:
                    self.stdout.write("  Duplicate would be created, skipping")
                    continue

                if dry_run:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  [DRY RUN] Would create next occurrence for {next_planned}"
                        )
                    )
                else:
                    with transaction.atomic():
                        new_obj = Checklist.objects.create(
                            assign_by=last.assign_by,
                            task_name=last.task_name,
                            message=last.message,
                            assign_to=last.assign_to,
                            planned_date=next_planned,
                            priority=last.priority,
                            attachment_mandatory=last.attachment_mandatory,
                            mode=last.mode,
                            frequency=last.frequency,
                            time_per_task_minutes=last.time_per_task_minutes,
                            remind_before_days=last.remind_before_days,
                            assign_pc=last.assign_pc,
                            notify_to=last.notify_to,
                            set_reminder=last.set_reminder,
                            reminder_mode=last.reminder_mode,
                            reminder_frequency=last.reminder_frequency,
                            reminder_starting_time=last.reminder_starting_time,
                            checklist_auto_close=last.checklist_auto_close,
                            checklist_auto_close_days=last.checklist_auto_close_days,
                            group_name=getattr(last, 'group_name', None),
                            actual_duration_minutes=0,
                            status='Pending',
                        )
                        
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  Created next occurrence ID {new_obj.id} for {next_planned}"
                        )
                    )
                    created_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"  Error processing series: {e}")
                )
                logger.error(f"Error in ensure_recurring_tasks for series {s}: {e}")
                continue

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(f"\n[DRY RUN] Would have created {created_count} recurring tasks")
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"\nSuccessfully created {created_count} recurring tasks")
            )
            
        return f"Processed {total_series} series, created {created_count} tasks"