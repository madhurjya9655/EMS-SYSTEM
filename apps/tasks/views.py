# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\views.py
# OPTIMIZED VERSION FOR ULTRA-FAST BULK UPLOADS

import csv
import pytz
import time
import logging
from datetime import datetime, timedelta, date, time as dt_time
from dateutil.relativedelta import relativedelta
import pandas as pd
from io import TextIOWrapper
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.db import transaction, OperationalError, connections
from django.db.models import Q, F, Subquery, OuterRef
from django.http import FileResponse, Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.core.cache import cache

from apps.users.permissions import has_permission
from apps.settings.models import Holiday

from .forms import (
    BulkUploadForm,
    ChecklistForm, CompleteChecklistForm,
    DelegationForm, CompleteDelegationForm,
    HelpTicketForm,
)
from .models import Checklist, Delegation, FMS, HelpTicket
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
from .recurrence import get_next_planned_date

logger = logging.getLogger(__name__)
User = get_user_model()

# Global locks for thread safety
user_cache_lock = Lock()
db_operation_lock = Lock()

can_create = lambda u: u.is_superuser or u.groups.filter(name__in=["Admin", "Manager", "EA", "CEO"]).exists()

site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")

IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

# Performance settings
BULK_BATCH_SIZE = getattr(settings, "BULK_UPLOAD_BATCH_SIZE", 20)
EMAIL_BATCH_SIZE = getattr(settings, "EMAIL_BATCH_SIZE", 10)
EMAIL_SEND_DELAY = getattr(settings, "EMAIL_SEND_DELAY", 0.1)


def retry_db_operation(max_retries=3, delay=0.1):
    """Decorator to retry database operations on lock errors"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                        continue
                    raise
            return func(*args, **kwargs)
        return wrapper
    return decorator


class UserCache:
    """Thread-safe user caching for bulk operations"""
    def __init__(self):
        self._cache = {}
        self._lock = Lock()
    
    def get_user(self, username):
        with self._lock:
            if username not in self._cache:
                try:
                    user = User.objects.get(username=username, is_active=True)
                    self._cache[username] = user
                except User.DoesNotExist:
                    self._cache[username] = None
            return self._cache[username]
    
    def clear(self):
        with self._lock:
            self._cache.clear()

# Global user cache instance
user_cache = UserCache()


def _minutes_between(now_dt: datetime, planned_dt: datetime) -> int:
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


def is_working_day(d: date) -> bool:
    """Check if date is working day (not Sunday and not holiday) - with caching"""
    cache_key = f"is_working_day_{d.isoformat()}"
    result = cache.get(cache_key)
    if result is None:
        result = d.weekday() != 6 and not Holiday.objects.filter(date=d).exists()
        cache.set(cache_key, result, 86400)  # Cache for 24 hours
    return result


def next_working_day(d: date) -> date:
    """Find next working day from given date"""
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


def preserve_first_occurrence_time(planned_dt: datetime) -> datetime:
    """
    For FIRST occurrence (manual add or bulk upload):
    - Preserve the exact datetime as given by user
    - If naive, interpret as IST and make timezone-aware
    - If falls on holiday/Sunday, shift to next working day but keep the TIME
    """
    if not planned_dt:
        return planned_dt
    
    # Make timezone-aware if naive (interpret as IST)
    if timezone.is_naive(planned_dt):
        planned_dt = IST.localize(planned_dt)
    
    # Convert to IST for date checking
    planned_ist = planned_dt.astimezone(IST)
    planned_date = planned_ist.date()
    planned_time = planned_ist.time()
    
    # If it's a working day, return as-is
    if is_working_day(planned_date):
        return planned_dt
    
    # Find next working day and preserve the time
    next_work_date = next_working_day(planned_date)
    next_work_dt = IST.localize(datetime.combine(next_work_date, planned_time))
    
    # Convert back to project timezone
    return next_work_dt.astimezone(timezone.get_current_timezone())


def schedule_recurring_at_10am(planned_dt: datetime) -> datetime:
    """
    For RECURRING occurrences (after first):
    - Always schedule at 10:00 AM IST
    - Skip Sundays and holidays
    """
    if not planned_dt:
        return planned_dt
    
    # Get the date in IST
    if timezone.is_naive(planned_dt):
        planned_dt = timezone.make_aware(planned_dt)
    
    planned_ist = planned_dt.astimezone(IST)
    planned_date = planned_ist.date()
    
    # Find next working day if needed
    if not is_working_day(planned_date):
        planned_date = next_working_day(planned_date)
    
    # Set to 10:00 AM IST
    recur_dt = IST.localize(datetime.combine(planned_date, dt_time(ASSIGN_HOUR, ASSIGN_MINUTE)))
    
    # Convert back to project timezone
    return recur_dt.astimezone(timezone.get_current_timezone())


def _series_filter_kwargs(task: Checklist) -> dict:
    return dict(
        assign_to_id=task.assign_to_id,
        task_name=task.task_name,
        mode=task.mode,
        frequency=task.frequency,
        group_name=task.group_name,
    )


@retry_db_operation(max_retries=3, delay=0.2)
def create_next_if_recurring(task: Checklist) -> None:
    """Create next recurring occurrence at 10:00 AM IST on working days"""
    if (task.mode or "") not in RECURRING_MODES:
        return
    
    nxt_dt = get_next_planned_date(task.planned_date, task.mode, task.frequency)
    if not nxt_dt:
        return
    
    # Schedule recurring at 10:00 AM IST on working days
    nxt_dt = schedule_recurring_at_10am(nxt_dt)
    
    # Use shorter atomic block just for the creation
    with transaction.atomic():
        # Check for existing task with select_for_update to prevent race conditions
        if Checklist.objects.select_for_update().filter(
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
    
    # Send emails outside of transaction
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
        except Exception as e:
            logger.error(f"Failed to send recurring task emails: {str(e)}")


def ensure_next_for_all_recurring() -> None:
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
        if Checklist.objects.filter(status="Pending", planned_date__gt=now, **s).exists():
            continue
        if last_pending.planned_date <= now:
            create_next_if_recurring(last_pending)


def _delete_series_for(instance: Checklist) -> int:
    if not instance:
        return 0
    filters = _series_filter_kwargs(instance)
    deleted, _ = Checklist.objects.filter(status="Pending", **filters).delete()
    return deleted


# ===== ULTRA-FAST BULK UPLOAD HELPER FUNCTIONS =====

def parse_datetime_flexible(date_str):
    """Parse various datetime formats flexibly - OPTIMIZED"""
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    # Cache parsed dates for better performance
    cache_key = f"parsed_date_{hash(date_str)}"
    cached_result = cache.get(cache_key)
    if cached_result is not None:
        return cached_result
    
    # Common datetime formats to try
    formats = [
        "%Y-%m-%d %H:%M",      # 2025-08-16 17:00
        "%Y-%m-%d %H:%M:%S",   # 2025-08-16 17:00:00
        "%m/%d/%Y %H:%M",      # 8/16/2025 17:00
        "%d/%m/%Y %H:%M",      # 16/8/2025 17:00
        "%Y-%m-%d",            # 2025-08-16 (will add default time)
        "%m/%d/%Y",            # 8/16/2025 (will add default time)
        "%d/%m/%Y",            # 16/8/2025 (will add default time)
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # If date-only format, add default time (preserve user intent)
            if fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
                dt = dt.replace(hour=10, minute=0)  # Default to 10:00 AM
            cache.set(cache_key, dt, 3600)  # Cache for 1 hour
            return dt
        except ValueError:
            continue
    
    # Try pandas parsing as fallback
    try:
        dt = pd.to_datetime(date_str)
        if pd.isna(dt):
            cache.set(cache_key, None, 3600)
            return None
        result = dt.to_pydatetime()
        cache.set(cache_key, result, 3600)
        return result
    except:
        cache.set(cache_key, None, 3600)
        return None


def parse_csv_or_excel_optimized(file):
    """Parse uploaded CSV or Excel file and return DataFrame - ULTRA FAST"""
    file.seek(0)
    
    if file.name.endswith('.csv'):
        # Use faster CSV reading with optimizations
        return pd.read_csv(
            file,
            dtype=str,  # Read everything as string initially
            na_filter=False,  # Don't convert to NaN
            engine='c',  # Use faster C engine
        )
    elif file.name.endswith(('.xlsx', '.xls')):
        # Use faster Excel reading
        return pd.read_excel(
            file,
            dtype=str,  # Read everything as string initially
            na_filter=False,  # Don't convert to NaN
            engine='openpyxl' if file.name.endswith('.xlsx') else 'xlrd',
        )
    else:
        raise ValueError("Unsupported file format")


def validate_and_prepare_data(df, task_type="checklist"):
    """Pre-validate and prepare data for ultra-fast processing"""
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    # Required columns based on task type
    if task_type == "checklist":
        required_cols = ['Task Name', 'Assign To', 'Planned Date']
    else:  # delegation
        required_cols = ['Task Name', 'Assign To', 'Planned Date']
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return None, [f"Missing required columns: {', '.join(missing_cols)}"]
    
    # Pre-filter empty rows
    df = df[df['Task Name'].str.strip().astype(bool)]
    
    if len(df) == 0:
        return None, ["No valid rows found in the file"]
    
    # Pre-validate users in bulk
    unique_usernames = set()
    for col in ['Assign To', 'Assign PC', 'Notify To', 'Auditor']:
        if col in df.columns:
            unique_usernames.update(df[col].dropna().str.strip().unique())
    
    # Bulk load all users into cache
    valid_users = User.objects.filter(username__in=unique_usernames, is_active=True)
    user_lookup = {user.username: user for user in valid_users}
    
    # Update user cache
    with user_cache_lock:
        user_cache._cache.update(user_lookup)
    
    return df, []


@retry_db_operation(max_retries=5, delay=0.1)
def process_checklist_batch_ultra_fast(batch_df, assign_by_user, start_idx):
    """Ultra-fast checklist batch processing with bulk operations"""
    task_objects = []
    errors = []
    
    for idx, row in batch_df.iterrows():
        try:
            # Skip empty rows
            if not str(row.get('Task Name', '')).strip():
                continue
            
            # Parse required fields
            task_name = str(row['Task Name']).strip()
            assign_to_username = str(row['Assign To']).strip()
            
            # Get user from cache
            assign_to = user_cache.get_user(assign_to_username)
            if not assign_to:
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found")
                continue
            
            # Parse planned date
            planned_dt = parse_datetime_flexible(row['Planned Date'])
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid planned date format")
                continue
            
            planned_dt = preserve_first_occurrence_time(planned_dt)
            
            # Parse optional fields efficiently
            message = str(row.get('Message', '')).strip() if row.get('Message') else ''
            priority = str(row.get('Priority', 'Low')).strip()
            if priority not in ['Low', 'Medium', 'High']:
                priority = 'Low'
            
            mode = str(row.get('Mode', '')).strip() if row.get('Mode') else ''
            if mode not in RECURRING_MODES:
                mode = ''
            
            frequency = 1
            if row.get('Frequency'):
                try:
                    frequency = max(1, int(float(str(row.get('Frequency')))))
                except (ValueError, TypeError):
                    frequency = 1
            
            time_per_task = 0
            if row.get('Time per Task (minutes)'):
                try:
                    time_per_task = max(0, int(float(str(row.get('Time per Task (minutes)')))))
                except (ValueError, TypeError):
                    time_per_task = 0
            
            remind_before_days = 0
            if row.get('Reminder Before Days'):
                try:
                    remind_before_days = max(0, int(float(str(row.get('Reminder Before Days')))))
                except (ValueError, TypeError):
                    remind_before_days = 0
            
            # Handle optional user fields
            assign_pc = user_cache.get_user(str(row.get('Assign PC', '')).strip()) if row.get('Assign PC') else None
            notify_to = user_cache.get_user(str(row.get('Notify To', '')).strip()) if row.get('Notify To') else None
            auditor = user_cache.get_user(str(row.get('Auditor', '')).strip()) if row.get('Auditor') else None
            
            group_name = str(row.get('Group Name', '')).strip() if row.get('Group Name') else ''
            
            # Handle reminder fields
            set_reminder = str(row.get('Set Reminder', '')).strip().lower() in ['yes', 'true', '1', 'y']
            reminder_mode = ''
            if set_reminder and row.get('Reminder Mode'):
                reminder_mode = str(row.get('Reminder Mode')).strip()
                if reminder_mode not in RECURRING_MODES:
                    reminder_mode = 'Daily'
            
            reminder_frequency = 1
            if set_reminder and row.get('Reminder Frequency'):
                try:
                    reminder_frequency = max(1, int(float(str(row.get('Reminder Frequency')))))
                except (ValueError, TypeError):
                    reminder_frequency = 1
            
            reminder_starting_time = None
            if row.get('Reminder Starting Time'):
                time_str = str(row.get('Reminder Starting Time')).strip()
                try:
                    reminder_starting_time = datetime.strptime(time_str, '%H:%M').time()
                except ValueError:
                    pass
            
            # Handle auto close
            checklist_auto_close = str(row.get('Checklist Auto Close', '')).strip().lower() in ['yes', 'true', '1', 'y']
            checklist_auto_close_days = 0
            if checklist_auto_close and row.get('Checklist Auto Close Days'):
                try:
                    checklist_auto_close_days = max(0, int(float(str(row.get('Checklist Auto Close Days')))))
                except (ValueError, TypeError):
                    checklist_auto_close_days = 0
            
            # Create checklist object (don't save yet)
            checklist = Checklist(
                assign_by=assign_by_user,
                task_name=task_name,
                message=message,
                assign_to=assign_to,
                planned_date=planned_dt,
                priority=priority,
                attachment_mandatory=False,
                mode=mode,
                frequency=frequency if mode else None,
                time_per_task_minutes=time_per_task,
                remind_before_days=remind_before_days,
                assign_pc=assign_pc,
                group_name=group_name,
                notify_to=notify_to,
                auditor=auditor,
                set_reminder=set_reminder,
                reminder_mode=reminder_mode if set_reminder else None,
                reminder_frequency=reminder_frequency if set_reminder else None,
                reminder_starting_time=reminder_starting_time,
                checklist_auto_close=checklist_auto_close,
                checklist_auto_close_days=checklist_auto_close_days,
                actual_duration_minutes=0,
                status="Pending",
            )
            
            task_objects.append(checklist)
            
        except Exception as e:
            errors.append(f"Row {idx+1}: {str(e)}")
    
    # Bulk create all objects at once
    created_tasks = []
    if task_objects:
        with transaction.atomic():
            created_tasks = Checklist.objects.bulk_create(task_objects, batch_size=50)
    
    return created_tasks, errors


@retry_db_operation(max_retries=5, delay=0.1)
def process_delegation_batch_ultra_fast(batch_df, assign_by_user, start_idx):
    """Ultra-fast delegation batch processing with bulk operations"""
    task_objects = []
    errors = []
    
    for idx, row in batch_df.iterrows():
        try:
            # Skip empty rows
            if not str(row.get('Task Name', '')).strip():
                continue
            
            # Parse required fields
            task_name = str(row['Task Name']).strip()
            assign_to_username = str(row['Assign To']).strip()
            
            # Get user from cache
            assign_to = user_cache.get_user(assign_to_username)
            if not assign_to:
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found")
                continue
            
            # Parse planned date
            planned_dt = parse_datetime_flexible(row['Planned Date'])
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid planned date format")
                continue
            
            planned_dt = preserve_first_occurrence_time(planned_dt)
            
            # Parse optional fields
            priority = str(row.get('Priority', 'Low')).strip()
            if priority not in ['Low', 'Medium', 'High']:
                priority = 'Low'
            
            mode = str(row.get('Mode', '')).strip() if row.get('Mode') else ''
            if mode not in RECURRING_MODES:
                mode = ''
            
            frequency = 1
            if row.get('Frequency'):
                try:
                    frequency = max(1, int(float(str(row.get('Frequency')))))
                except (ValueError, TypeError):
                    frequency = 1
            
            time_per_task = 0
            if row.get('Time per Task (minutes)'):
                try:
                    time_per_task = max(0, int(float(str(row.get('Time per Task (minutes)')))))
                except (ValueError, TypeError):
                    time_per_task = 0
            
            # Create delegation object (don't save yet)
            delegation = Delegation(
                assign_by=assign_by_user,
                task_name=task_name,
                assign_to=assign_to,
                planned_date=planned_dt,
                priority=priority,
                attachment_mandatory=False,
                mode=mode,
                frequency=frequency if mode else None,
                time_per_task_minutes=time_per_task,
                actual_duration_minutes=0,
                status="Pending",
            )
            
            task_objects.append(delegation)
            
        except Exception as e:
            errors.append(f"Row {idx+1}: {str(e)}")
    
    # Bulk create all objects at once
    created_tasks = []
    if task_objects:
        with transaction.atomic():
            created_tasks = Delegation.objects.bulk_create(task_objects, batch_size=50)
    
    return created_tasks, errors


def process_checklist_bulk_upload_ultra_fast(file, assign_by_user):
    """Ultra-fast checklist bulk upload with optimizations"""
    try:
        df = parse_csv_or_excel_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {str(e)}"]
    
    # Pre-validate and prepare data
    df, validation_errors = validate_and_prepare_data(df, "checklist")
    if validation_errors:
        return [], validation_errors
    
    created_tasks = []
    errors = []
    
    # Clear user cache before processing
    user_cache.clear()
    
    # Process in optimized batches
    total_rows = len(df)
    batch_size = min(BULK_BATCH_SIZE, 50)  # Optimal batch size
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        
        batch_tasks, batch_errors = process_checklist_batch_ultra_fast(batch_df, assign_by_user, start_idx)
        created_tasks.extend(batch_tasks)
        errors.extend(batch_errors)
    
    return created_tasks, errors


def process_delegation_bulk_upload_ultra_fast(file, assign_by_user):
    """Ultra-fast delegation bulk upload with optimizations"""
    try:
        df = parse_csv_or_excel_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {str(e)}"]
    
    # Pre-validate and prepare data
    df, validation_errors = validate_and_prepare_data(df, "delegation")
    if validation_errors:
        return [], validation_errors
    
    created_tasks = []
    errors = []
    
    # Clear user cache before processing
    user_cache.clear()
    
    # Process in optimized batches
    total_rows = len(df)
    batch_size = min(BULK_BATCH_SIZE, 50)  # Optimal batch size
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        
        batch_tasks, batch_errors = process_delegation_batch_ultra_fast(batch_df, assign_by_user, start_idx)
        created_tasks.extend(batch_tasks)
        errors.extend(batch_errors)
    
    return created_tasks, errors


def send_bulk_emails_optimized(created_tasks, task_type="Checklist"):
    """Send emails in optimized batches with threading"""
    if not created_tasks:
        return
    
    def send_email_batch(task_batch):
        for task in task_batch:
            try:
                if task_type == "Checklist":
                    complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[task.id])}"
                    send_checklist_assignment_to_user(
                        task=task,
                        complete_url=complete_url,
                        subject_prefix="Bulk Upload Checklist Assigned",
                    )
                elif task_type == "Delegation":
                    complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[task.id])}"
                    send_delegation_assignment_to_user(
                        delegation=task,
                        complete_url=complete_url,
                        subject_prefix="Bulk Upload Delegation Assigned",
                    )
            except Exception as e:
                logger.error(f"Failed to send email for task {task.id}: {str(e)}")
    
    # Send emails in batches using threading
    email_batch_size = min(EMAIL_BATCH_SIZE, 10)
    with ThreadPoolExecutor(max_workers=3) as executor:
        for i in range(0, len(created_tasks), email_batch_size):
            batch = created_tasks[i:i + email_batch_size]
            executor.submit(send_email_batch, batch)
            time.sleep(EMAIL_SEND_DELAY)


# ===== VIEW FUNCTIONS =====

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
    items = qs.select_related("assign_by", "assign_to").order_by("-planned_date", "-id")
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
            # For manual add, preserve exact datetime as entered
            planned_date = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
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
            # For manual edit, preserve exact datetime as entered
            planned_date = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
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
    obj = get_object_or_404(Checklist, pk=pk)
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home"))
    
    if request.method == "GET":
        form = CompleteChecklistForm(instance=obj)
        return render(request, "tasks/complete_checklist.html", {"form": form, "object": obj})
    
    # Complete the task with retry logic
    @retry_db_operation(max_retries=3, delay=0.1)
    def complete_task():
        with transaction.atomic():
            obj = Checklist.objects.select_for_update().get(pk=pk)
            form = CompleteChecklistForm(request.POST, request.FILES, instance=obj)
            if obj.attachment_mandatory and not request.FILES.get("doer_file") and not obj.doer_file:
                form.add_error("doer_file", "Attachment is required for this task.")
            if not form.is_valid():
                return form, obj
            
            now = timezone.now()
            actual_minutes = _minutes_between(now, obj.planned_date) if obj.planned_date else 0
            inst = form.save(commit=False)
            inst.status = "Completed"
            inst.completed_at = now
            inst.actual_duration_minutes = actual_minutes
            inst.save()
            return None, inst
    
    try:
        form_result, completed_task = complete_task()
        if form_result:  # Form validation error
            return render(request, "tasks/complete_checklist.html", {"form": form_result, "object": obj})
        
        # Create next recurring task outside of the completion transaction
        try:
            create_next_if_recurring(completed_task)
        except Exception as e:
            logger.error(f"Failed to create next recurring task: {str(e)}")
        
        messages.success(request, f"Task '{completed_task.task_name}' marked as completed successfully!")
        return redirect(request.GET.get("next", "dashboard:home"))
        
    except Exception as e:
        logger.error(f"Error completing task {pk}: {str(e)}")
        messages.error(request, "An error occurred while completing the task. Please try again.")
        return redirect(request.GET.get("next", "dashboard:home"))


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
    qs = Delegation.objects.filter(status="Pending").select_related("assign_by", "assign_to").order_by("-planned_date", "-id")
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
    status_param = request.GET.get("status", "").strip()
    if status_param == "all":
        qs = Delegation.objects.all().select_related("assign_by", "assign_to")
    elif status_param and status_param != "Pending":
        qs = Delegation.objects.filter(status=status_param).select_related("assign_by", "assign_to")
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
            planned_dt = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            obj = form.save(commit=False)
            obj.planned_date = planned_dt
            obj.save()
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
            planned_dt = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            obj2 = form.save(commit=False)
            obj2.planned_date = planned_dt
            obj2.save()
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
            return redirect("tasks:list_delegation")
    return render(
        request,
        "tasks/reassign_delegation.html",
        {"object": obj, "all_users": User.objects.order_by("username")},
    )


@login_required
def complete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home") + "?task_type=delegation")
    if request.method == "GET":
        form = CompleteDelegationForm(instance=obj)
        return render(request, "tasks/complete_delegation.html", {"form": form, "object": obj})
    with transaction.atomic():
        obj = Delegation.objects.select_for_update().get(pk=pk)
        form = CompleteDelegationForm(request.POST, request.FILES, instance=obj)
        if obj.attachment_mandatory and not request.FILES.get("doer_file") and not obj.doer_file:
            form.add_error("doer_file", "Attachment is required for this task.")
        if not form.is_valid():
            return render(request, "tasks/complete_delegation.html", {"form": form, "object": obj})
        now = timezone.now()
        actual_minutes = _minutes_between(now, obj.planned_date) if obj.planned_date else 0
        inst = form.save(commit=False)
        inst.status = "Completed"
        inst.completed_at = now
        inst.actual_duration_minutes = actual_minutes
        inst.save()
    messages.success(request, f"Delegation task '{obj.task_name}' marked as completed successfully!")
    return redirect(request.GET.get("next", "dashboard:home") + "?task_type=delegation")


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
    qs = HelpTicket.objects.select_related("assign_by", "assign_to").exclude(status="Closed")
    if not can_create(request.user):
        qs = qs.filter(assign_to=request.user)
    for param, lookup in [
        ("from_date", "planned_date__date__gte"),
        ("to_date", "planned_date__date__lte"),
    ]:
        if v := request.GET.get(param, "").strip():
            qs = qs.filter(**{lookup: v})
    v_assign_by = request.GET.get("assign_by", "all")
    v_assign_to = request.GET.get("assign_to", "all")
    v_status = request.GET.get("status", "open")
    if v_assign_by != "all":
        qs = qs.filter(assign_by_id=v_assign_by)
    if v_assign_to != "all":
        qs = qs.filter(assign_to_id=v_assign_to)
    if v_status and v_status != "all":
        if v_status == "open":
            qs = qs.exclude(status="Closed")
        else:
            qs = qs.filter(status=v_status)
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
        .select_related("assign_by", "assign_to")
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
                        messages.success(request, f"Successfully deleted {deleted} help tickets(s).")
                    else:
                        messages.info(request, "No help tickets were deleted. You can only delete tickets you assigned.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {str(e)}")
            else:
                messages.warning(request, "No help tickets were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:assigned_by_me")
    items = HelpTicket.objects.filter(assign_by=request.user).select_related("assign_by", "assign_to").order_by("-planned_date")
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


# ===== ULTRA-FAST BULK UPLOAD VIEW =====

# Updated bulk_upload function in views.py - Replace the existing bulk_upload function

@has_permission("mt_bulk_upload")
def bulk_upload(request):
    if request.method != "POST":
        form = BulkUploadForm()
        return render(request, "tasks/bulk_upload.html", {"form": form})
    
    form = BulkUploadForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "tasks/bulk_upload.html", {"form": form})
    
    form_type = form.cleaned_data['form_type']
    csv_file = form.cleaned_data['csv_file']
    
    # Start timing for performance measurement
    start_time = time.time()
    
    try:
        if form_type == 'checklist':
            created_tasks, errors = process_checklist_bulk_upload_ultra_fast(csv_file, request.user)
            task_type_name = "Checklist"
        elif form_type == 'delegation':
            created_tasks, errors = process_delegation_bulk_upload_ultra_fast(csv_file, request.user)
            task_type_name = "Delegation"
        else:
            messages.error(request, "Invalid form type selected.")
            return render(request, "tasks/bulk_upload.html", {"form": form})
        
        # Send emails asynchronously in optimized batches
        if created_tasks:
            send_bulk_emails_optimized(created_tasks, task_type_name)
        
        # Show results with performance metrics
        processing_time = round(time.time() - start_time, 2)
        
        if created_tasks:
            messages.success(
                request, 
                f"Successfully uploaded {len(created_tasks)} {task_type_name} task(s) in {processing_time}s"
            )
            
            # Send clean admin summary email
            if created_tasks:
                summary_rows = []
                for task in created_tasks:
                    # Generate proper complete URL for each task
                    if task_type_name == "Checklist":
                        complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[task.id])}"
                    else:
                        complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[task.id])}"
                    
                    summary_rows.append({
                        "Task Name": task.task_name,
                        "Assign To": task.assign_to.get_full_name() or task.assign_to.username,
                        "Planned Date": task.planned_date.strftime("%Y-%m-%d %H:%M") if task.planned_date else "N/A",
                        "Priority": task.priority,
                        "complete_url": complete_url,
                    })
                
                try:
                    send_admin_bulk_summary(
                        title=f"Bulk Upload Complete: {len(created_tasks)} {task_type_name} Tasks Created",
                        rows=summary_rows
                    )
                except Exception as e:
                    logger.error(f"Failed to send admin summary: {str(e)}")
        
        if errors:
            for error in errors:
                messages.error(request, error)
        
        if not created_tasks and not errors:
            messages.warning(request, "No tasks were created. Please check your file format.")
            
    except Exception as e:
        processing_time = round(time.time() - start_time, 2)
        logger.error(f"Bulk upload error after {processing_time}s: {str(e)}")
        messages.error(request, f"An error occurred during bulk upload: {str(e)}")
    
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


@login_required
def list_fms(request):
    items = FMS.objects.select_related("assign_by", "assign_to").order_by("-planned_date", "-id")
    return render(request, "tasks/list_fms.html", {"items": items})