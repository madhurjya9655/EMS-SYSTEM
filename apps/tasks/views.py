import csv
import logging
import pytz
import re
import time  # stdlib time module (we alias datetime.time as dt_time below)
import unicodedata
from typing import Optional
from datetime import datetime, timedelta, date, time as dt_time
from functools import wraps
from threading import Lock, Thread

import pandas as pd
from dateutil.relativedelta import relativedelta  # (kept if used by other helpers)

from django.apps import apps
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.db import transaction, OperationalError, connection, close_old_connections
from django.db.models import Q, Sum
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from apps.users.permissions import has_permission
from apps.settings.models import Holiday

# LeaveRequest/Leave* may cause circular imports at import-time in some setups.
# Make this import tolerant and fall back to None; callsites already guard via getattr/apps.get_model.
try:
    from apps.leave.models import LeaveHandover, LeaveStatus, LeaveRequest  # noqa: F401
except Exception:  # pragma: no cover
    LeaveHandover = LeaveStatus = LeaveRequest = None  # type: ignore

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
from .recurrence_utils import preserve_first_occurrence_time

# ✅ Central leave-blocking (date- & time-aware)
from apps.tasks.utils.blocking import (
    is_user_blocked,         # date-level, 10:00 IST anchor (for CL/DL visibility)
    is_user_blocked_at,      # time-level, exact instant in IST (for Help Tickets / "right now")
)

logger = logging.getLogger(__name__)
User = get_user_model()

IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0

SEND_EMAILS_FOR_AUTO_RECUR = getattr(settings, "SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_RECUR_EMAILS_ONLY_AT_10AM = getattr(settings, "SEND_RECUR_EMAILS_ONLY_AT_10AM", True)
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

BULK_BATCH_SIZE = 500

site_url = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")


def _background(target, /, *args, **kwargs):
    thread_name = kwargs.pop("thread_name", "bulk-bg")

    def _runner():
        try:
            close_old_connections()
            target(*args, **kwargs)
        except Exception as e:
            logger.exception("Background task failed: %s", e)
        finally:
            try:
                close_old_connections()
            except Exception:
                pass

    Thread(target=_runner, daemon=True, name=thread_name).start()


def _safe_console_text(s: object) -> str:
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    try:
        return text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    except Exception:
        return text


def clean_unicode_string(text):
    if not text:
        return text
    text = str(text).replace("\x96", "-").replace("\u2013", "-").replace("\u2014", "-")
    return unicodedata.normalize("NFKD", text)


def robust_db_operation(max_retries=3, base_delay=0.05):
    """
    Decorator to retry DB ops that can briefly fail with 'database is locked' (SQLite etc).
    """
    def deco(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            last = None
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except OperationalError as e:
                    txt = str(e).lower()
                    last = e
                    if "locked" in txt and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning("DB locked; retrying %s/%s in %.3fs", attempt + 1, max_retries, delay)
                        time.sleep(delay)
                        continue
                    else:
                        # Not a lock, or out of retries -> re-raise
                        raise
            if last is not None:
                raise last
            return None
        return inner
    return deco


def optimal_batch_size() -> int:
    try:
        if connection.vendor == "sqlite":
            return 250
        return min(BULK_BATCH_SIZE, 500)
    except Exception:
        return 250


def _minutes_between(now_dt: datetime, planned_dt: datetime) -> int:
    if not planned_dt:
        return 0
    try:
        now_dt = timezone.localtime(now_dt)
        planned_dt = timezone.localtime(planned_dt)
    except Exception:
        pass
    return max(int((now_dt - planned_dt).total_seconds() // 60), 0)


def is_working_day(d: date) -> bool:
    # Sunday (weekday=6) is off
    if d.weekday() == 6:
        return False
    return not Holiday.objects.filter(date=d).exists()


def next_working_day(d: date) -> date:
    while not is_working_day(d):
        d += timedelta(days=1)
    return d


# Next working day that also skips the assignee's leave window (for Checklist/Delegation only)
def next_working_day_skip_leaves(assign_to: User, d: date) -> date:
    # FIX: guard against projects where LeaveRequest doesn't expose helper method
    is_blocked = getattr(LeaveRequest, "is_user_blocked_on", None)
    for _ in range(0, 120):
        if is_working_day(d) and (not is_blocked or not is_blocked(assign_to, d)):
            return d
        d += timedelta(days=1)
    return d


def day_bounds(d: date):
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(d, dt_time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def span_bounds(d_from: date, d_to_inclusive: date):
    start, _ = day_bounds(d_from)
    _, end = day_bounds(d_to_inclusive)
    return start, end


def _ist_date(dt: datetime) -> Optional[date]:
    if not dt:
        return None
    if timezone.is_aware(dt):
        return dt.astimezone(IST).date()
    # FIX: don't localize an already-aware dt; handled above
    return IST.localize(dt).date()


def _normalize_task_type(val) -> Optional[str]:
    if val is None:
        return None
    try:
        ival = int(val)
        mapping = {1: "checklist", 2: "delegation", 3: "help_ticket"}
        if ival in mapping:
            return mapping[ival]
    except Exception:
        pass
    s = str(val).strip().lower().replace(" ", "").replace("_", "")
    if s in ("checklist", "delegation", "helpticket"):
        return "help_ticket" if s == "helpticket" else s
    return None


def _get_handover_tasks_for_user(user, today_date):
    try:
        LeaveHandover = apps.get_model("leave", "LeaveHandover")
        if not LeaveHandover:
            return {'checklist': [], 'delegation': [], 'help_ticket': []}

        active_handovers = (
            LeaveHandover.objects
            .filter(
                new_assignee=user,
                is_active=True,
                effective_start_date__lte=today_date,
                effective_end_date__gte=today_date,
                leave_request__status='APPROVED',
            )
            .select_related('leave_request')
        )

        handover_tasks = {'checklist': [], 'delegation': [], 'help_ticket': []}
        for handover in active_handovers:
            task_type_key = str(getattr(handover, "task_type", "")).lower().replace('_', '')
            if task_type_key == 'helpticket':
                task_type_key = 'help_ticket'
            if task_type_key in handover_tasks:
                handover_tasks[task_type_key].append(handover.original_task_id)
        return handover_tasks
    except Exception as e:
        logger.error("Error getting handover tasks: %s", e)
        return {'checklist': [], 'delegation': [], 'help_ticket': []}


def _get_handover_rows_for_user(user, today_date):
    LeaveHandover = apps.get_model("leave", "LeaveHandover")
    if not LeaveHandover:
        return {'checklist': [], 'delegation': [], 'help_ticket': []}

    rows = {'checklist': [], 'delegation': [], 'help_ticket': []}
    handovers = (
        LeaveHandover.objects
        .filter(
            new_assignee=user,
            is_active=True,
            effective_start_date__lte=today_date,
            effective_end_date__gte=today_date,
            leave_request__status="APPROVED",
        )
        .select_related("leave_request", "original_assignee", "new_assignee")
        .order_by("id")
    )

    ids = {'checklist': [], 'delegation': [], 'help_ticket': []}
    hlist = {'checklist': [], 'delegation': [], 'help_ticket': []}
    for ho in handovers:
        key = _normalize_task_type(getattr(ho, "task_type", None))
        if key:
            ids[key].append(ho.original_task_id)
            hlist[key].append(ho)

    tasks_map = {'checklist': {}, 'delegation': {}, 'help_ticket': {}}
    if ids['checklist']:
        for t in Checklist.objects.filter(id__in=ids['checklist']).select_related("assign_by", "assign_to"):
            tasks_map['checklist'][t.id] = t
    if ids['delegation']:
        for t in Delegation.objects.filter(id__in=ids['delegation']).select_related("assign_by", "assign_to"):
            tasks_map['delegation'][t.id] = t
    if ids['help_ticket']:
        for t in HelpTicket.objects.filter(id__in=ids['help_ticket']).select_related("assign_by", "assign_to"):
            tasks_map['help_ticket'][t.id] = t

    for key in ('checklist', 'delegation', 'help_ticket'):
        for ho in hlist[key]:
            task = tasks_map[key].get(ho.original_task_id)
            if not task:
                continue
            setattr(ho, "is_currently_active", True)
            rows[key].append({
                "task": task,
                "handover": ho,
                "original_assignee": getattr(ho, "original_assignee", None),
                "leave_request": getattr(ho, "leave_request", None),
                "handover_message": getattr(ho, "message", "") or "",
                "task_url": None,
            })

    return rows


def _should_show_checklist(task_dt: datetime, now_ist: datetime) -> bool:
    if not task_dt:
        return False
    dt_ist = task_dt.astimezone(IST) if timezone.is_aware(task_dt) else IST.localize(task_dt)
    task_date = dt_ist.date()
    today = now_ist.date()
    if task_date < today:
        return True
    if task_date > today:
        return False
    anchor_10am = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return now_ist >= anchor_10am


# Use same 10AM gate for Delegation (today/past), Help Tickets are immediate
def _should_show_today_or_past(task_dt: datetime, now_ist: datetime) -> bool:
    if not task_dt:
        return False
    dt_ist = task_dt.astimezone(IST) if timezone.is_aware(task_dt) else IST.localize(task_dt)
    d = dt_ist.date()
    today = now_ist.date()
    if d < today:
        return True
    if d > today:
        return False
    anchor_10am = now_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    return now_ist >= anchor_10am


def parse_datetime_flexible(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.to_pydatetime()
    s = str(value).strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M",
        "%d-%m-%Y %H:%M", "%d.%m.%Y %H:%M",
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if f in {"%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y"}:
                dt = dt.replace(hour=19, minute=0, second=0, microsecond=0)
            return dt
        except ValueError:
            continue
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(s, dayfirst=False, errors="coerce")
        if pd.isna(dt):
            return None
        py = dt.to_pydatetime()
        if py.hour == 0 and py.minute == 0 and (re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$", s) is not None):
            py = py.replace(hour=19, minute=0, second=0, microsecond=0)
        return py
    except Exception:
        return None


_SYN_MODE = {
    "day": "Daily", "daily": "Daily",
    "week": "Weekly", "weekly": "Weekly",
    "month": "Monthly", "monthly": "Monthly",
    "year": "Yearly", "yearly": "Yearly",
}
_RECURRENCE_RE = re.compile(r"(?i)\b(?:every|evry)?\s*(\d+)?\s*(day|daily|week|weekly|month|monthly|year|yearly)\b")


def _clean_str(val):
    return clean_unicode_string("" if val is None else str(val).strip())


def parse_mode_frequency_from_row(row):
    raw_mode = _clean_str(row.get("Mode"))
    raw_freq = _clean_str(row.get("Frequency"))
    if raw_mode:
        mode = _SYN_MODE.get(raw_mode.lower(), raw_mode.title())
        if mode not in RECURRING_MODES:
            mode = ""
        freq = None
        if raw_freq:
            try:
                freq = max(1, int(float(raw_freq)))
            except Exception:
                m = _RECURRENCE_RE.search(raw_freq)
                if m:
                    n = m.group(1)
                    freq = max(1, int(n)) if n else 1
        if mode:
            return mode, (freq or 1)

    for key in ["Recurrence", "Repeat", "Frequency", "Every"]:
        text = _clean_str(row.get(key))
        if not text:
            continue
        m = _RECURRENCE_RE.search(text)
        if m:
            n = m.group(1)
            unit = m.group(2).lower()
            mode = _SYN_MODE.get(unit, "")
            if mode in RECURRING_MODES:
                return mode, (max(1, int(n)) if n else 1)
        unit = text.lower()
        if unit in _SYN_MODE:
            mode = _SYN_MODE[unit]
            return mode, 1
    return "", None


def parse_bool(val) -> bool:
    return _clean_str(val).lower() in {"1", "true", "yes", "y", "on"}


def parse_int(val, default=0) -> int:
    s = _clean_str(val)
    if not s:
        return default
    m = re.search(r"(-?\d+)", s)
    if not m:
        return default
    try:
        return int(float(m.group(1)))
    except Exception:
        return default


def parse_excel_file_optimized(file):
    file.seek(0)
    name = (file.name or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str, na_filter=False, engine="c", encoding="utf-8-sig", skipinitialspace=True)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(
            file,
            dtype=str,
            na_filter=False,
            engine="openpyxl" if name.endswith(".xlsx") else "xlrd",
            keep_default_na=False,
        )
    raise ValueError("Unsupported file format. Please upload .xlsx, .xls or .csv")


def validate_and_prepare_excel_data(df, task_type="checklist"):
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("\n", " ").str.replace("\r", " ")
    )
    cmap = {
        "task name": "Task Name", "taskname": "Task Name", "task_name": "Task Name",
        "assign to": "Assign To", "assignto": "Assign To", "assign_to": "Assign To", "assigned to": "Assign To",
        "planned date": "Planned Date", "planned_date": "Planned Date", "due date": "Planned Date", "date": "Planned Date",
        "reminder before days": "Remind Before Days", "remind before days": "Remind Before Days",
        "remind days": "Remind Before Days", "remind before": "Remind Before Days",
        "time per task (minutes)": "Time per Task (minutes)", "time per task": "Time per Task (minutes)",
        "mode": "Mode", "frequency": "Frequency", "recurrence": "Recurrence", "repeat": "Recurrence", "every": "Recurrence",
    }
    df.columns = [cmap.get(col.lower(), col) for col in df.columns]

    required = ["Task Name", "Assign To", "Planned Date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return None, [f"Missing required columns: {', '.join(missing)}. Available: {', '.join(df.columns)}"]

    df = df.replace("", pd.NA).dropna(subset=["Task Name"])
    df = df[df["Task Name"].astype(str).str.strip().astype(bool)]
    if len(df) == 0:
        return None, ["No valid rows found in the file"]

    usernames = set()
    for col in ["Assign To", "Assign PC", "Notify To", "Auditor"]:
        if col in df.columns:
            ser = df[col].astype(str).str.strip()
            ser = ser[(ser != "") & ser.notna()]
            usernames.update(ser.unique().tolist())
    user_cache.preload_usernames(list(usernames))

    return df, []


class UserCache:
    def __init__(self):
        self._cache = {}
        self._lock = Lock()

    def get_user(self, username_or_email):
        key = (username_or_email or "").strip()
        if not key:
            return None
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        u = None
        try:
            u = User.objects.get(username=key, is_active=True)
        except User.DoesNotExist:
            try:
                u = User.objects.get(email__iexact=key, is_active=True)
            except User.DoesNotExist:
                u = None
        with self._lock:
            self._cache[key] = u
        return u

    def preload_usernames(self, usernames):
        if not usernames:
            return
        qs = User.objects.filter(is_active=True).filter(Q(username__in=usernames) | Q(email__in=usernames))
        with self._lock:
            for u in qs:
                self._cache[u.username] = u
                if u.email:
                    self._cache[u.email] = u

    def clear(self):
        with self._lock:
            self._cache.clear()


user_cache = UserCache()
can_create = lambda u: u.is_superuser or u.groups.filter(name__in=["Admin", "Manager", "EA", "CEO"]).exists()


@robust_db_operation()
def process_checklist_batch_excel_ultra_optimized(batch_df, assign_by_user, start_idx):
    task_objects, errors = [], []
    for idx, row in batch_df.iterrows():
        try:
            task_name = _clean_str(row.get("Task Name"))
            if not task_name:
                errors.append(f"Row {idx+1}: Missing 'Task Name'")
                continue

            assign_to_username = _clean_str(row.get("Assign To"))
            if not assign_to_username:
                errors.append(f"Row {idx+1}: Missing 'Assign To'")
                continue
            assign_to = user_cache.get_user(assign_to_username)
            if not assign_to:
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found or inactive")
                continue

            planned_dt = parse_datetime_flexible(row.get("Planned Date"))
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid or missing planned date")
                continue
            planned_dt = preserve_first_occurrence_time(planned_dt)

            # Enforce holiday/leave blocking for bulk uploads (shift forward)
            planned_ist_date = (planned_dt if timezone.is_aware(planned_dt) else IST.localize(planned_dt)).astimezone(IST).date()
            safe_date = next_working_day_skip_leaves(assign_to, planned_ist_date)
            if safe_date != planned_ist_date:
                planned_dt = IST.localize(datetime.combine(safe_date, dt_time(19, 0))).astimezone(
                    timezone.get_current_timezone()
                )

            message = _clean_str(row.get("Message"))
            priority = (_clean_str(row.get("Priority")) or "Low").title()
            if priority not in ["Low", "Medium", "High"]:
                priority = "Low"

            mode, frequency = parse_mode_frequency_from_row(row)
            time_per_task = parse_int(row.get("Time per Task (minutes)"), default=0)
            remind_before_days = parse_int(
                row.get("Remind Before Days") or row.get("Reminder Before Days") or
                row.get("Remind days") or row.get("Remind Before"), default=0
            )
            if remind_before_days < 0:
                remind_before_days = 0

            assign_pc = user_cache.get_user(_clean_str(row.get("Assign PC")))
            notify_to = user_cache.get_user(_clean_str(row.get("Notify To")))
            auditor = user_cache.get_user(_clean_str(row.get("Auditor")))
            group_name = _clean_str(row.get("Group Name"))

            set_reminder = parse_bool(row.get("Set Reminder"))
            reminder_mode = None
            reminder_frequency = None
            reminder_starting_time = None
            if set_reminder:
                rmode = _clean_str(row.get("Reminder Mode"))
                rmode = _SYN_MODE.get(rmode.lower(), rmode.title()) if rmode else "Daily"
                reminder_mode = rmode if rmode in RECURRING_MODES else "Daily"
                reminder_frequency = parse_int(row.get("Reminder Frequency"), default=1)
                tval = row.get("Reminder Starting Time")
                if tval:
                    ts = _clean_str(tval)
                    try:
                        if ":" in ts:
                            reminder_starting_time = datetime.strptime(ts, "%H:%M").time()
                        else:
                            f = float(ts)
                            h = int(f * 24) % 24
                            m = int(round(f * 24 * 60)) % 60
                            reminder_starting_time = dt_time(h, m)
                    except Exception:
                        reminder_starting_time = None

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
                reminder_mode=reminder_mode,
                reminder_frequency=reminder_frequency,
                reminder_starting_time=reminder_starting_time,
                checklist_auto_close=parse_bool(row.get("Checklist Auto Close")),
                checklist_auto_close_days=parse_int(row.get("Checklist Auto Close Days"), default=0),
                actual_duration_minutes=0,
                status="Pending",
            )
            task_objects.append(checklist)
        except Exception as e:
            errors.append(f"Row {idx+1}: {str(e)}")

    created = []
    if task_objects:
        try:
            with transaction.atomic():
                bs = min(len(task_objects), optimal_batch_size())
                created = Checklist.objects.bulk_create(task_objects, batch_size=bs, ignore_conflicts=False)
        except Exception as e:
            logger.error("bulk_create failed; falling back: %s", e)
            for i in range(0, len(task_objects), 50):
                batch = task_objects[i:i+50]
                try:
                    created.extend(Checklist.objects.bulk_create(batch, batch_size=50))
                except Exception:
                    for obj in batch:
                        try:
                            obj.save()
                            created.append(obj)
                        except Exception as save_err:
                            errors.append(f"Failed to save '{clean_unicode_string(obj.task_name)}': {save_err}")
    return created, errors


@robust_db_operation()
def process_delegation_batch_excel_ultra_optimized(batch_df, assign_by_user, start_idx):
    task_objects, errors = [], []
    for idx, row in batch_df.iterrows():
        try:
            task_name = _clean_str(row.get("Task Name"))
            if not task_name:
                errors.append(f"Row {idx+1}: Missing 'Task Name'")
                continue

            assign_to_username = _clean_str(row.get("Assign To"))
            if not assign_to_username:
                errors.append(f"Row {idx+1}: Missing 'Assign To'")
                continue
            assign_to = user_cache.get_user(assign_to_username)
            if not assign_to:
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found or inactive")
                continue

            planned_dt = parse_datetime_flexible(row.get("Planned Date"))
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid or missing planned date")
                continue
            planned_dt = preserve_first_occurrence_time(planned_dt)

            # Shift to next safe day (holiday + leave)
            planned_ist_date = (planned_dt if timezone.is_aware(planned_dt) else IST.localize(planned_dt)).astimezone(IST).date()
            safe_date = next_working_day_skip_leaves(assign_to, planned_ist_date)
            if safe_date != planned_ist_date:
                planned_dt = IST.localize(datetime.combine(safe_date, dt_time(19, 0))).astimezone(
                    timezone.get_current_timezone()
                )

            priority = (_clean_str(row.get("Priority")) or "Low").title()
            if priority not in ["Low", "Medium", "High"]:
                priority = "Low"

            time_per_task = parse_int(row.get("Time per Task (minutes)"), default=0)

            delegation = Delegation(
                assign_by=assign_by_user,
                task_name=task_name,
                assign_to=assign_to,
                planned_date=planned_dt,
                priority=priority,
                attachment_mandatory=False,
                mode=None,
                frequency=None,
                time_per_task_minutes=time_per_task,
                actual_duration_minutes=0,
                status="Pending",
            )
            task_objects.append(delegation)
        except Exception as e:
            errors.append(f"Row {idx+1}: {str(e)}")

    created = []
    if task_objects:
        try:
            with transaction.atomic():
                bs = min(len(task_objects), optimal_batch_size())
                created = Delegation.objects.bulk_create(task_objects, batch_size=bs)
        except Exception as e:
            logger.error("Delegation bulk_create fallback: %s", e)
            for i in range(0, len(task_objects), 50):
                batch = task_objects[i:i+50]
                try:
                    created.extend(Delegation.objects.bulk_create(batch, batch_size=50))
                except Exception:
                    for obj in batch:
                        try:
                            obj.save()
                            created.append(obj)
                        except Exception as save_err:
                            errors.append(f"Failed to save delegation '{clean_unicode_string(obj.task_name)}': {save_err}")
    return created, errors


def process_checklist_bulk_upload_excel_friendly(file, assign_by_user):
    try:
        df = parse_excel_file_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {e}. Please upload .xlsx, .xls or .csv"]

    df, v_errors = validate_and_prepare_excel_data(df, "checklist")
    if v_errors:
        return [], v_errors

    created, errors = [], []
    user_cache.clear()

    total = len(df)
    bs = optimal_batch_size()
    for start_idx in range(0, total, bs):
        end_idx = min(start_idx + bs, total)
        batch_df = df.iloc[start_idx:end_idx]
        batch_created, batch_errors = process_checklist_batch_excel_ultra_optimized(
            batch_df, assign_by_user, start_idx
        )
        created.extend(batch_created)
        errors.extend(batch_errors)
        if connection.vendor == "sqlite":
            time.sleep(0.01)
    return created, errors


def process_delegation_bulk_upload_excel_friendly(file, assign_by_user):
    try:
        df = parse_excel_file_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {e}. Please upload .xlsx, .xls or .csv"]

    df, v_errors = validate_and_prepare_excel_data(df, "delegation")
    if v_errors:
        return [], v_errors

    created, errors = [], []
    user_cache.clear()

    total = len(df)
    bs = optimal_batch_size()
    for start_idx in range(0, total, bs):
        end_idx = min(start_idx + bs, total)
        batch_df = df.iloc[start_idx:end_idx]
        batch_created, batch_errors = process_delegation_batch_excel_ultra_optimized(
            batch_df, assign_by_user, start_idx
        )
        created.extend(batch_created)
        errors.extend(batch_errors)
        if connection.vendor == "sqlite":
            time.sleep(0.01)
    return created, errors


def _send_bulk_emails_by_ids(task_ids, *, task_type: str):
    """
    NOTE: We now respect SEND_RECUR_EMAILS_ONLY_AT_10AM.
    If True (default), we DO NOT send immediate assignee emails here; 10AM schedulers/signals will handle it.
    """
    if SEND_RECUR_EMAILS_ONLY_AT_10AM:
        return  # skip immediate fan-out; rely on 10 AM flow

    Model = Checklist if task_type == "Checklist" else Delegation
    CHUNK = 100
    for i in range(0, len(task_ids), CHUNK):
        ids_chunk = task_ids[i:i + CHUNK]
        qs = (
            Model.objects.filter(id__in=ids_chunk)
            .select_related("assign_by", "assign_to")
            .order_by("id")
        )
        for task in qs:
            try:
                if task_type == "Checklist":
                    complete_url = f"{site_url}{reverse('tasks:complete_checklist', args=[task.id])}"
                    send_checklist_assignment_to_user(
                        task=task,
                        complete_url=complete_url,
                        subject_prefix=f"Today’s Checklist – {task.task_name}",
                    )
                else:
                    complete_url = f"{site_url}{reverse('tasks:complete_delegation', args=[task.id])}"
                    send_delegation_assignment_to_user(
                        delegation=task,
                        complete_url=complete_url,
                        subject_prefix=f"Today’s Delegation – {task.task_name} (due 7 PM)",
                    )
            except Exception as e:
                logger.error("Failed to send email for %s %s: %s", task_type, getattr(task, "id", "?"), e)


def kick_off_bulk_emails_async(created_tasks, task_type="Checklist"):
    if not created_tasks:
        return
    task_ids = [t.id for t in created_tasks if getattr(t, "id", None)]
    if not task_ids:
        return
    _background(_send_bulk_emails_by_ids, task_ids, task_type=task_type, thread_name="bulk-emails")


def send_admin_bulk_summary_async(*, title: str, rows, exclude_assigner_email: Optional[str] = None):
    def _safe_call():
        try:
            try:
                send_admin_bulk_summary(title=title, rows=rows, exclude_assigner_email=exclude_assigner_email)
            except TypeError:
                send_admin_bulk_summary(title=title, rows=rows)
        except Exception as e:
            logger.error("Admin summary failed: %s", e)

    _background(_safe_call, thread_name="bulk-admin-summary")


@has_permission("list_checklist")
def list_checklist(request):
    if request.method == "POST":
        if request.POST.get("action") == "delete_series" and request.POST.get("pk"):
            try:
                obj = Checklist.objects.get(pk=int(request.POST["pk"]))
            except (Checklist.DoesNotExist, ValueError, TypeError):
                messages.warning(request, "The selected series no longer exists.")
                return redirect("tasks:list_checklist")
            filters = dict(
                assign_to_id=obj.assign_to_id,
                task_name=obj.task_name,
                mode=obj.mode,
                frequency=obj.frequency,
                group_name=getattr(obj, "group_name", None),
            )
            deleted, _ = Checklist.objects.filter(status="Pending", **filters).delete()
            messages.success(request, f"Deleted {deleted} pending occurrence(s) from the series '{obj.task_name}'.")
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
                    key = (obj.assign_to_id, obj.task_name, obj.mode, obj.frequency, obj.group_name)
                    if key in series_seen:
                        continue
                    series_seen.add(key)
                    deleted, _ = Checklist.objects.filter(status="Pending", **{
                        "assign_to_id": obj.assign_to_id,
                        "task_name": obj.task_name,
                        "mode": obj.mode,
                        "frequency": obj.frequency,
                        "group_name": obj.group_name,
                    }).delete()
                    total_deleted += deleted
                messages.success(request, f"Deleted {total_deleted} pending occurrence(s) across selected series.")
            else:
                deleted, _ = Checklist.objects.filter(pk__in=ids).delete()
                total_deleted += deleted
                if deleted:
                    messages.success(request, f"Deleted {deleted} selected task(s).")
                else:
                    messages.info(request, "Nothing was deleted.")
            request.session["suppress_auto_recur"] = True
        return redirect("tasks:list_checklist")

    qs = Checklist.objects.filter(status="Pending").select_related("assign_by", "assign_to")

    kw = request.GET.get("keyword", "").strip()
    if kw:
        qs = qs.filter(Q(task_name__icontains=kw) | Q(message__icontains=kw))

    if request.GET.get("assign_to", "").strip():
        qs = qs.filter(assign_to_id=request.GET.get("assign_to").strip())

    if request.GET.get("priority", "").strip():
        qs = qs.filter(priority=request.GET.get("priority").strip())

    if request.GET.get("group_name", "").strip():
        qs = qs.filter(group_name__icontains=request.GET.get("group_name").strip())

    if request.GET.get("start_date", "").strip():
        qs = qs.filter(planned_date__date__gte=request.GET.get("start_date").strip())

    if request.GET.get("end_date", "").strip():
        qs = qs.filter(planned_date__date__lte=request.GET.get("end_date").strip())

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
            w.writerow([
                clean_unicode_string(itm.task_name),
                itm.assign_to.get_full_name() or itm.assign_to.username,
                itm.planned_date.strftime("%Y-%m-%d %H:%M") if itm.planned_date else "",
                itm.priority,
                itm.group_name,
                itm.status,
            ])
        return resp

    ctx = {
        "items": items,
        "users": User.objects.filter(is_active=True).order_by("username"),
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
            planned_date = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            assignee = form.cleaned_data.get("assign_to")

            # hard-block holidays and leave (date-level @ 10:00 IST)
            ist_day = planned_date.astimezone(IST).date() if planned_date else None
            if ist_day and not is_working_day(ist_day):
                messages.error(request, "This day is holiday")
                return render(request, "tasks/add_checklist.html", {"form": form})
            if assignee and ist_day and getattr(LeaveRequest, "is_user_blocked_on", lambda *a, **k: False)(assignee, ist_day):
                messages.error(request, "Assignee is on leave during this period.")
                return render(request, "tasks/add_checklist.html", {"form": form})

            obj = form.save(commit=False)
            obj.planned_date = planned_date
            obj.save()
            form.save_m2m()

            # DO NOT send assignee email immediately; 10 AM scheduler will do it.
            try:
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Assignment")
            except Exception as e:
                logger.error("Admin confirmation email failed: %s", e)

            messages.success(request, f"Checklist task '{obj.task_name}' created and will notify the assignee at 10:00 AM on the due day.")
            return redirect("tasks:list_checklist")
    else:
        form = ChecklistForm(initial({"assign_by": request.user}))
    return render(request, "tasks/add_checklist.html", {"form": form})


@has_permission("add_checklist")
def edit_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    old_assignee = obj.assign_to
    if request.method == "POST":
        form = ChecklistForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            assignee = form.cleaned_data.get("assign_to")

            # block rescheduling to holidays / leave
            ist_day = planned_date.astimezone(IST).date() if planned_date else None
            if ist_day and not is_working_day(ist_day):
                messages.error(request, "This day is holiday")
                return render(request, "tasks/add_checklist.html", {"form": form})
            if assignee and ist_day and getattr(LeaveRequest, "is_user_blocked_on", lambda *a, **k: False)(assignee, ist_day):
                messages.error(request, "Assignee is on leave during this period.")
                return render(request, "tasks/add_checklist.html", {"form": form})

            obj2 = form.save(commit=False)
            obj2.planned_date = planned_date
            obj2.save()
            form.save_m2m()

            # Do not email assignee immediately; only admin confirmations + old-assignee notice if changed
            try:
                if old_assignee and obj2.assign_to_id != old_assignee.id:
                    send_checklist_unassigned_notice(task=obj2, old_user=old_assignee)
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Reassigned")
                else:
                    send_checklist_admin_confirmation(task=obj2, subject_prefix="Checklist Task Updated")
            except Exception as e:
                logger.error("Update emails failed: %s", e)

            messages.success(request, f"Checklist task '{obj2.task_name}' updated successfully! Assignee will be notified at 10:00 AM on the due day.")
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
        uid = request.POST.get("assign_to")
        if uid:
            obj.assign_to = User.objects.get(pk=uid)
            obj.save()

            # No immediate assignee email; keep old-assignee notice + admin confirmation
            try:
                if old_assignee and old_assignee.id != obj.assign_to_id:
                    send_checklist_unassigned_notice(task=obj, old_user=old_assignee)
                send_checklist_admin_confirmation(task=obj, subject_prefix="Checklist Task Reassigned")
            except Exception as e:
                logger.error("Reassignment emails failed: %s", e)

            messages.success(request, f"Task reassigned to {obj.assign_to.get_full_name() or obj.assign_to.username} (assignee will be notified at 10:00 AM on the due day).")
            return redirect("tasks:list_checklist")
    return render(request, "tasks/reassign_checklist.html", {"object": obj, "all_users": User.objects.filter(is_active=True).order_by("username")})


@login_required
def complete_checklist(request, pk):
    obj = get_object_or_404(Checklist, pk=pk)
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home"))

    if request.method == "GET":
        form = CompleteChecklistForm(instance=obj)
        return render(request, "tasks/complete_checklist.html", {"form": form, "object": obj})

    def _complete_once():
        with transaction.atomic():
            current = Checklist.objects.select_for_update(nowait=True).get(pk=pk)
            form = CompleteChecklistForm(request.POST, request.FILES, instance=current)
            if current.attachment_mandatory and not request.FILES.get("doer_file") and not current.doer_file:
                form.add_error("doer_file", "Attachment is required for this task.")
            if not form.is_valid():
                return form, None
            now = timezone.now()
            actual_minutes = _minutes_between(now, current.planned_date) if current.planned_date else 0
            inst = form.save(commit=False)
            inst.status = "Completed"
            inst.completed_at = now
            inst.actual_duration_minutes = actual_minutes
            inst.save()
            return None, inst

    for attempt in range(3):
        try:
            invalid_form, completed = _complete_once()
            if invalid_form:
                return render(request, "tasks/complete_checklist.html", {"form": invalid_form, "object": obj})
            break
        except OperationalError as e:
            if "locked" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (2 ** attempt))
                continue
            logger.error("complete_checklist failed after retries: %s", e)
            messages.error(request, "The task completion is taking longer than expected. Please try again.")
            return redirect(request.GET.get("next", "dashboard:home"))
        except Exception as e:
            logger.error("Unexpected completion error: %s", e)
            messages.error(request, "An unexpected error occurred. Please try again.")
            return redirect(request.GET.get("next", "dashboard:home"))

    messages.success(request, f"✅ Task '{completed.task_name}' completed successfully!")
    return redirect(request.GET.get("next", "dashboard:home"))


@has_permission("add_delegation")
def add_delegation(request):
    if request.method == "POST":
        form = DelegationForm(request.POST, request.FILES)
        if form.is_valid():
            planned_dt = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            assignee = form.cleaned_data.get("assign_to")

            # hard-block holidays and leave
            ist_day = planned_dt.astimezone(IST).date() if planned_dt else None
            if ist_day and not is_working_day(ist_day):
                messages.error(request, "This day is holiday")
                return render(request, "tasks/add_delegation.html", {"form": form})
            if assignee and ist_day and getattr(LeaveRequest, "is_user_blocked_on", lambda *a, **k: False)(assignee, ist_day):
                messages.error(request, "Assignee is on leave during this period.")
                return render(request, "tasks/add_delegation.html", {"form": form})

            obj = form.save(commit=False)
            obj.planned_date = planned_dt
            obj.mode = None
            obj.frequency = None
            obj.save()

            # DO NOT email assignee immediately; signals handle immediate + 10:00 reminder logic.
            messages.success(request, f"Delegation task '{obj.task_name}' created. Assignee will be notified appropriately.")
            return redirect("tasks:list_delegation")
    else:
        form = DelegationForm(initial({"assign_by": request.user}))
    return render(request, "tasks/add_delegation.html", {"form": form})


@has_permission("list_delegation")
def list_delegation(request):
    """
    Delegation list with persistent filters.
    """
    if request.method == "POST":
        action = request.POST.get("action", "")
        if action == "bulk_delete":
            ids = request.POST.getlist("sel")
            return_url = request.POST.get("return_url") or reverse("tasks:list_delegation")
            if ids:
                try:
                    deleted, _ = Delegation.objects.filter(pk__in=ids).delete()
                    if deleted:
                        messages.success(request, f"Successfully deleted {deleted} delegation task(s).")
                    else:
                        messages.info(request, "No delegation tasks were deleted.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {e}")
            else:
                messages.warning(request, "No delegation tasks were selected for deletion.")
            return redirect(return_url)
        else:
            messages.warning(request, "Invalid action specified.")
            return redirect("tasks:list_delegation")

    # Start from ALL, then apply filters. Default behavior keeps Pending unless status filter says otherwise.
    base_qs = Delegation.objects.select_related("assign_by", "assign_to")
    status_param = (request.GET.get("status") or "").strip()

    if not status_param or status_param == "Pending":
        qs = base_qs.filter(status="Pending")
    elif status_param == "all":
        qs = base_qs
    else:
        qs = base_qs.filter(status=status_param)

    assign_by_id = (request.GET.get("assign_by") or "").strip()
    assign_to_id = (request.GET.get("assign_to") or "").strip()
    if assign_by_id:
        qs = qs.filter(assign_by_id=assign_by_id)
    if assign_to_id:
        qs = qs.filter(assign_to_id=assign_to_id)

    priority_val = (request.GET.get("priority") or "").strip()
    if priority_val:
        qs = qs.filter(priority=priority_val)

    if (request.GET.get("start_date") or "").strip():
        qs = qs.filter(planned_date__date__gte=request.GET.get("start_date").strip())
    if (request.GET.get("end_date") or "").strip():
        qs = qs.filter(planned_date__date__lte=request.GET.get("end_date").strip())

    if request.GET.get("today_only"):
        today = timezone.localdate()
        qs = qs.filter(planned_date__date=today)

    agg = qs.aggregate(
        assign_time=Sum("time_per_task_minutes"),
        actual_time=Sum("actual_duration_minutes"),
    )
    assign_time = agg.get("assign_time") or 0
    actual_time = agg.get("actual_time") or 0

    qs = qs.order_by("-planned_date", "-id")

    ctx = {
        "items": qs,
        "current_tab": "delegation",
        "users": User.objects.filter(is_active=True).order_by("username"),
        "priority_choices": Delegation._meta.get_field("priority").choices,
        "assign_time": assign_time,
        "actual_time": actual_time,
    }
    if request.GET.get("partial"):
        return render(request, "tasks/partial_list_delegation.html", ctx)
    return render(request, "tasks/list_delegation.html", ctx)


@has_permission("add_delegation")
def edit_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == "POST":
        form = DelegationForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_dt = preserve_first_occurrence_time(form.cleaned_data.get("planned_date"))
            assignee = form.cleaned_data.get("assign_to")

            ist_day = planned_dt.astimezone(IST).date() if planned_dt else None
            if ist_day and not is_working_day(ist_day):
                messages.error(request, "This day is holiday")
                return render(request, "tasks/add_delegation.html", {"form": form})
            if assignee and ist_day and getattr(LeaveRequest, "is_user_blocked_on", lambda *a, **k: False)(assignee, ist_day):
                messages.error(request, "Assignee is on leave during this period.")
                return render(request, "tasks/add_delegation.html", {"form": form})

            obj2 = form.save(commit=False)
            obj2.planned_date = planned_dt
            obj2.mode = None
            obj2.frequency = None
            obj2.save()
            messages.success(request, f"Delegation task '{obj2.task_name}' updated successfully! Assignee will be notified at 10:00 AM on the due day.")
            return redirect(request.GET.get("next", reverse("tasks:list_delegation")))
    else:
        form = DelegationForm(instance=obj)
    return render(request, "tasks/add_delegation.html", {"form": form})


@has_permission("list_delegation")
def delete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if request.method == "POST":
        obj.delete()
        messages.success(request, f"Deleted delegation task '{obj.task_name}'.")
        return redirect(request.GET.get("next", reverse("tasks:list_delegation")))
    return render(request, "tasks/confirm_delete.html", {"object": obj, "type": "Delegation"})


@login_required
def complete_delegation(request, pk):
    obj = get_object_or_404(Delegation, pk=pk)
    if obj.assign_to_id and obj.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this task.")
        return redirect(request.GET.get("next", "dashboard:home"))

    if request.method == "GET":
        form = CompleteDelegationForm(instance=obj)
        return render(request, "tasks/complete_delegation.html", {"form": form, "object": obj})

    def _complete_once():
        with transaction.atomic():
            current = Delegation.objects.select_for_update(nowait=True).get(pk=pk)
            form = CompleteDelegationForm(request.POST, request.FILES, instance=current)
            if current.attachment_mandatory and not request.FILES.get("doer_file") and not current.doer_file:
                form.add_error("doer_file", "Attachment is required for this task.")
            if not form.is_valid():
                return form, None
            now = timezone.now()
            actual_minutes = _minutes_between(now, current.planned_date) if current.planned_date else 0
            inst = form.save(commit=False)
            inst.status = "Completed"
            inst.completed_at = now
            inst.actual_duration_minutes = actual_minutes
            inst.save()
            return None, inst

    for attempt in range(3):
        try:
            invalid_form, completed = _complete_once()
            if invalid_form:
                return render(request, "tasks/complete_delegation.html", {"form": invalid_form, "object": obj})
            break
        except OperationalError as e:
            if "locked" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (2 ** attempt))
                continue
            logger.error("complete_delegation failed after retries: %s", e)
            messages.error(request, "The task completion is taking longer than expected. Please try again.")
            return redirect(request.GET.get("next", "dashboard:home"))
        except Exception as e:
            logger.error("Unexpected completion error (delegation): %s", e)
            messages.error(request, "An unexpected error occurred. Please try again.")
            return redirect(request.GET.get("next", "dashboard:home"))

    messages.success(request, f"✅ Delegation '{completed.task_name}' completed successfully!")
    return redirect(request.GET.get("next", "dashboard:home"))


@login_required
def add_help_ticket(request):
    if request.method == "POST":
        form = HelpTicketForm(request.POST, request.FILES)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            assignee = form.cleaned_data.get("assign_to")

            # Block creation on holidays / Sundays ONLY (as before) …
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is a holiday; you cannot add on this day.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "add", "can_create": can_create(request.user)})

            # …and CRITICALLY block if the assignee is ON LEAVE at the time of assignment
            now_ist = timezone.now().astimezone(IST)
            if assignee and is_user_blocked_at(assignee, now_ist):
                messages.error(request, "Assignee is currently on leave. Please reassign or create after the leave window.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "add", "can_create": can_create(request.user)})

            # Also block if the planned timestamp itself lies within the leave window
            if assignee and planned_date and is_user_blocked_at(assignee, planned_date.astimezone(IST)):
                messages.error(request, "Planned time falls within assignee’s leave window. Choose another time or reassign.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "add", "can_create": can_create(request.user)})

            ticket = form.save(commit=False)
            ticket.assign_by = request.user
            ticket.save()

            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                # IMPORTANT: DO NOT send the assignee mail here (signal handles create).
                # Still send admin confirmation.
                send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Assignment")
            except Exception as e:
                logger.error("Help-ticket assignment/admin emails failed: %s", e)

            messages.success(request, f"Help ticket '{ticket.title}' created and assigned successfully!")
            return redirect("tasks:list_help_ticket")
    else:
        form = HelpTicketForm()
    return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "add", "can_create": can_create(request.user)})


@login_required
def edit_help_ticket(request, pk):
    obj = get_object_or_404(HelpTicket, pk=pk)
    old_assignee = obj.assign_to
    if request.method == "POST":
        form = HelpTicketForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            planned_date = form.cleaned_data.get("planned_date")
            planned_date_local = planned_date.astimezone(IST).date() if planned_date else None
            new_assignee = form.cleaned_data.get("assign_to")

            # Block moving onto holidays/Sundays ONLY
            if planned_date_local and not is_working_day(planned_date_local):
                messages.error(request, "This is a holiday; you cannot add on this day.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "edit", "can_create": can_create(request.user)})

            # Leave blocking: if changing assignee/time, prevent assigning into an active leave window
            now_ist = timezone.now().astimezone(IST)
            if new_assignee and is_user_blocked_at(new_assignee, now_ist):
                messages.error(request, "Assignee is currently on leave. Please reassign or update after the leave window.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "edit", "can_create": can_create(request.user)})

            if new_assignee and planned_date and is_user_blocked_at(new_assignee, planned_date.astimezone(IST)):
                messages.error(request, "Planned time falls within assignee’s leave window. Choose another time or reassign.")
                return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "edit", "can_create": can_create(request.user)})

            ticket = form.save()
            complete_url = f"{site_url}{reverse('tasks:note_help_ticket', args=[ticket.id])}"
            try:
                if old_assignee and ticket.assign_to_id != old_assignee.id:
                    send_help_ticket_unassigned_notice(ticket=ticket, old_user=old_assignee)
                    send_help_ticket_assignment_to_user(ticket=ticket, complete_url=complete_url, subject_prefix="Help Ticket Reassigned")
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Reassigned")
                else:
                    # On edit (same assignee), notify assignee + admin
                    send_help_ticket_assignment_to_user(ticket=ticket, complete_url=complete_url, subject_prefix="Help Ticket Updated")
                    send_help_ticket_admin_confirmation(ticket=ticket, subject_prefix="Help Ticket Updated")
            except Exception as e:
                logger.error("Help-ticket update emails failed: %s", e)

            messages.success(request, f"Help ticket '{ticket.title}' updated successfully!")
            return redirect("tasks:list_help_ticket")
    else:
        form = HelpTicketForm(instance=obj)
    return render(request, "tasks/add_help_ticket.html", {"form": form, "current_tab": "edit", "can_create": can_create(request.user)})


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
                        messages.info(request, "No help tickets were deleted.")
                except Exception as e:
                    messages.error(request, f"Error during bulk delete: {e}")
            else:
                messages.warning(request, "No help tickets were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:list_help_ticket")

    qs = HelpTicket.objects.select_related("assign_by", "assign_to").exclude(status="Closed")
    if not can_create(request.user):
        qs = qs.filter(assign_to=request.user)

    if request.GET.get("from_date", "").strip():
        qs = qs.filter(planned_date__date__gte=request.GET.get("from_date").strip())
    if request.GET.get("to_date", "").strip():
        qs = qs.filter(planned_date__date__lte=request.GET.get("to_date").strip())

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
            "users": User.objects.filter(is_active=True).order_by("username"),
            # FIX: be tolerant if STATUS_CHOICES is absent on model in some envs
            "status_choices": getattr(HelpTicket, "STATUS_CHOICES", (("Open", "Open"), ("Closed", "Closed"))),
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

    # Leave-blocking for Help Tickets on personal "assigned to me" view:
    # If user is on leave RIGHT NOW, hide all items.
    if is_user_blocked_at(request.user, timezone.now().astimezone(IST)):
        items = HelpTicket.objects.none()

    return render(request, "tasks/list_help_ticket_assigned_to.html", {"items": items, "current_tab": "assigned_to"})


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
                    messages.error(request, f"Error during bulk delete: {e}")
            else:
                messages.warning(request, "No help tickets were selected for deletion.")
        else:
            messages.warning(request, "Invalid action specified.")
        return redirect("tasks:assigned_by_me")

    items = (
        HelpTicket.objects
        .filter(assign_by=request.user)
        .select_related("assign_by", "assign_to")
        .order_by("-planned_date")
    )

    for t in items:
        t.assign_by_display = (t.assign_by.get_full_name() or t.assign_by.username) if t.assign_by else "-"
        t.assign_to_display = (t.assign_to.get_full_name() or t.assign_to.username) if t.assign_to else "-"

    return render(
        request,
        "tasks/list_help_ticket_assigned_by.html",
        {
            "items": items,
            "current_tab": "assigned_by",
        },
    )


@login_required
def complete_help_ticket(request, pk):
    ticket = get_object_or_404(HelpTicket.objects.select_related("assign_by", "assign_to"), pk=pk)

    if ticket.assign_to_id and ticket.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this help ticket.")
        return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))

    do_immediate = (request.method == "POST") or (request.GET.get("immediate") == "1")

    if do_immediate:
        now = timezone.now()
        ticket.status = "Closed"
        ticket.resolved_at = now
        ticket.resolved_by = request.user
        if ticket.planned_date:
            mins = int((now - ticket.planned_date).total_seconds() // 60)
            ticket.actual_duration_minutes = max(mins, 0)
        ticket.save()

        messages.success(request, f"Help ticket '{ticket.title}' marked as completed.")
        return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))

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

        messages.success(request, f"Note saved for HT-{ticket.id}.")
        return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))
    # FIX: render a dedicated note template instead of the list (avoids context assumptions)
    return render(
        request,
        "tasks/note_help_ticket.html",
        {"ticket": ticket, "next": request.GET.get("next", reverse("tasks:assigned_to_me"))}
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


@has_permission("mt_bulk_upload")
def bulk_upload(request):
    if request.method != "POST":
        return render(request, "tasks/bulk_upload.html", {"form": BulkUploadForm()})

    form = BulkUploadForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "tasks/bulk_upload.html", {"form": form})

    form_type = form.cleaned_data["form_type"]
    csv_file = form.cleaned_data["csv_file"]
    start_time = time.time()

    try:
        if form_type == "checklist":
            created_tasks, errors = process_checklist_bulk_upload_excel_friendly(csv_file, request.user)
            task_type_name = "Checklist"
        elif form_type == "delegation":
            created_tasks, errors = process_delegation_bulk_upload_excel_friendly(csv_file, request.user)
            task_type_name = "Delegation"
        else:
            messages.error(request, "Invalid form type selected.")
            return redirect("tasks:bulk_upload")

        processing_time = round(time.time() - start_time, 2)
        count_created = len(created_tasks)

        if created_tasks:
            messages.success(
                request,
                f"Bulk Upload Complete: Created {count_created} {task_type_name} task(s) in {processing_time}s. "
                f"Assignees will be notified at 10:00 AM on the due day."
            )

            kick_off_bulk_emails_async(created_tasks, task_type_name)

            try:
                preview = []
                for t in created_tasks[:10]:
                    complete_url = (
                        f"{site_url}{reverse('tasks:complete_checklist', args=[t.id])}"
                        if task_type_name == "Checklist"
                        else f"{site_url}{reverse('tasks:complete_delegation', args=[t.id])}"
                    )
                    preview.append({
                        "Task Name": t.task_name,
                        "Assign To": t.assign_to.get_full_name() or t.assign_to.username,
                        "Planned Date": t.planned_date.strftime("%Y-%m-%d %H:%M") if t.planned_date else "N/A",
                        "Priority": t.priority,
                        "complete_url": complete_url,
                    })
                title = f"Bulk Upload: {count_created} {task_type_name} Tasks Created"
                send_admin_bulk_summary_async(
                    title=title,
                    rows=preview,
                    exclude_assigner_email=(request.user.email or None),
                )
            except Exception as e:
                logger.error("Admin summary schedule failed: %s", e)

        if errors:
            for err in errors[:15]:
                messages.error(request, err)
            if len(errors) > 15:
                messages.warning(request, f"... and {len(errors) - 15} more error(s). Check logs for details.")

        if not created_tasks and not errors:
            messages.warning(request, "No tasks were created. Please check your file format and data.")

    except Exception as e:
        messages.error(request, f"An error occurred during bulk upload: {e}")
        logger.error("Bulk upload error: %s", e)

    return redirect("tasks:bulk_upload")


@login_required
def list_fms(request):
    items = FMS.objects.select_related("assign_by", "assign_to").order_by("-planned_date", "-id")
    return render(request, "tasks/list_fms.html", {"items": items})


@login_required
def checklist_details(request, pk: int):
    obj = get_object_or_404(Checklist.objects.select_related("assign_by", "assign_to"), pk=pk)
    return render(request, "tasks/partials/checklist_detail.html", {"obj": obj})


@login_required
def delegation_details(request, pk: int):
    obj = get_object_or_404(Delegation.objects.select_related("assign_by", "assign_to"), pk=pk)
    return render(request, "tasks/partials/delegation_detail.html", {"obj": obj})


@login_required
def help_ticket_details(request, pk: int):
    obj = get_object_or_404(HelpTicket.objects.select_related("assign_by", "assign_to"), pk=pk)
    return render(request, "tasks/partials/help_ticket_detail.html", {"obj": obj})


@login_required
def help_ticket_detail(request, pk: int):
    return help_ticket_details(request, pk)


@login_required
def close_help_ticket(request, pk: int):
    ticket = get_object_or_404(HelpTicket.objects.select_related("assign_by", "assign_to"), pk=pk)

    if ticket.assign_to_id and ticket.assign_to_id != request.user.id and not (request.user.is_staff or request.user.is_superuser):
        messages.error(request, "You are not the assignee of this help ticket.")
        return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))

    now = timezone.now()
    ticket.status = "Closed"
    ticket.resolved_at = now
    ticket.resolved_by = request.user
    if ticket.planned_date:
        mins = int((now - ticket.planned_date).total_seconds() // 60)
        ticket.actual_duration_minutes = max(mins, 0)
    ticket.save()

    messages.success(request, f"Help ticket '{ticket.title}' marked as completed.")
    return redirect(request.GET.get("next", reverse("tasks:assigned_to_me")))


# --------------- DASHBOARD (visibility rules + leave gating) ----------------

@login_required
def dashboard_home(request):
    now_ist = timezone.now().astimezone(IST)
    today_ist = now_ist.date()

    logger.info(_safe_console_text(f"Dashboard accessed by {request.user.username} at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}"))

    # Weekly scorecards (Mon..Sun)
    start_current = today_ist - timedelta(days=today_ist.weekday())
    start_prev = start_current - timedelta(days=7)
    end_prev = start_current - timedelta(days=1)

    # Scorecards
    try:
        curr_chk = Checklist.objects.filter(assign_to=request.user, planned_date__date__gte=start_current, planned_date__date__lte=today_ist, status='Completed').count()
        prev_chk = Checklist.objects.filter(assign_to=request.user, planned_date__date__gte=start_prev, planned_date__date__lte=end_prev, status='Completed').count()
        curr_del = Delegation.objects.filter(assign_to=request.user, planned_date__date__gte=start_current, planned_date__date__lte=today_ist, status='Completed').count()
        prev_del = Delegation.objects.filter(assign_to=request.user, planned_date__date__gte=start_prev, planned_date__date__lte=end_prev, status='Completed').count()
        curr_help = HelpTicket.objects.filter(assign_to=request.user, planned_date__date__gte=start_current, planned_date__date__lte=today_ist, status='Closed').count()
        prev_help = HelpTicket.objects.filter(assign_to=request.user, planned_date__date__gte=start_prev, planned_date__date__lte=end_prev, status='Closed').count()
    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating weekly scores: {e}"))
        curr_chk = prev_chk = curr_del = prev_del = curr_help = prev_help = 0

    week_score = {
        'checklist':   {'previous': prev_chk,   'current': curr_chk},
        'delegation':  {'previous': prev_del,   'current': curr_del},
        'help_ticket': {'previous': prev_help,  'current': curr_help},
    }

    # Pending counts
    try:
        pending_tasks = {
            'checklist':   Checklist.objects.filter(assign_to=request.user, status='Pending').count(),
            'delegation':  Delegation.objects.filter(assign_to=request.user, status='Pending').count(),
            'help_ticket': HelpTicket.objects.filter(assign_to=request.user).exclude(status='Closed').count(),
        }
    except Exception as e:
        logger.error(_safe_console_text(f"Error calculating pending counts: {e}"))
        pending_tasks = {'checklist': 0, 'delegation': 0, 'help_ticket': 0}

    selected = request.GET.get('task_type')
    today_only = (request.GET.get('today') == '1' or request.GET.get('today_only') == '1')

    handed_over = _get_handover_rows_for_user(request.user, today_ist)

    try:
        # -------------------- Checklists --------------------
        if today_only:
            base_checklists = list(
                Checklist.objects
                .filter(assign_to=request.user, status='Pending', planned_date__date=today_ist)
                .select_related('assign_by')
                .order_by('planned_date')
            )
        else:
            base_checklists = list(
                Checklist.objects
                .filter(assign_to=request.user, status='Pending')
                .select_related('assign_by')
                .order_by('planned_date')
            )

        ho_ids_checklist = [row["task"].id for row in handed_over['checklist']]
        if ho_ids_checklist:
            ho_checklists = list(
                Checklist.objects
                .filter(id__in=ho_ids_checklist, status='Pending')
                .select_related('assign_by')
                .order_by('planned_date')
            )
            for t in ho_checklists:
                t.is_handover = True
            all_checklists = base_checklists + ho_checklists
        else:
            all_checklists = base_checklists

        if today_only:
            checklist_qs = [
                c for c in all_checklists
                if (_ist_date(c.planned_date) == today_ist) and
                   (getattr(c, 'is_handover', False) or _should_show_checklist(c.planned_date, now_ist))
            ]
        else:
            checklist_qs = [
                c for c in all_checklists
                if getattr(c, 'is_handover', False) or _should_show_checklist(c.planned_date, now_ist)
            ]

        # Leave-filter for Checklist: hide any date that is blocked (10:00 IST anchor)
        try:
            checklist_qs = [c for c in checklist_qs if not is_user_blocked(request.user, _ist_date(c.planned_date))]
        except Exception:
            pass

        # ------------------- Delegations -------------------
        if today_only:
            base_delegations = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending',
                    planned_date__date=today_ist,
                ).select_related('assign_by').order_by('planned_date')
            )
        else:
            base_delegations = list(
                Delegation.objects.filter(
                    assign_to=request.user, status='Pending'
                ).select_related('assign_by').order_by('planned_date')
            )

        ho_ids_delegation = [row["task"].id for row in handed_over['delegation']]
        if ho_ids_delegation:
            ho_delegations = list(
                Delegation.objects.filter(
                    id__in=ho_ids_delegation, status='Pending',
                ).select_related('assign_by').order_by('planned_date')
            )
            for t in ho_delegations:
                t.is_handover = True
            delegation_qs = base_delegations + ho_delegations
        else:
            delegation_qs = base_delegations

        if not today_only:
            delegation_qs = [d for d in delegation_qs if getattr(d, 'is_handover', False) or _should_show_today_or_past(d.planned_date, now_ist)]
        else:
            delegation_qs = [d for d in delegation_qs if _ist_date(d.planned_date) == today_ist and (getattr(d, 'is_handover', False) or _should_show_today_or_past(d.planned_date, now_ist))]

        # Leave-filter for Delegation: hide any date that is blocked (10:00 IST anchor)
        try:
            delegation_qs = [d for d in delegation_qs if not is_user_blocked(request.user, _ist_date(d.planned_date))]
        except Exception:
            pass

        # ------------------- Help Tickets -------------------
        if today_only:
            base_help = list(
                HelpTicket.objects.filter(
                    assign_to=request.user,
                    planned_date__date=today_ist,
                ).exclude(status='Closed').select_related('assign_by').order_by('planned_date')
            )
        else:
            base_help = list(
                HelpTicket.objects.filter(
                    assign_to=request.user
                ).exclude(status='Closed').select_related('assign_by').order_by('planned_date')
            )

        ho_ids_help = [row["task"].id for row in handed_over['help_ticket']]
        if ho_ids_help:
            ho_help = list(
                HelpTicket.objects.filter(
                    id__in=ho_ids_help
                ).exclude(status='Closed').select_related('assign_by').order_by('planned_date')
            )
            for t in ho_help:
                t.is_handover = True
            help_ticket_qs = base_help + ho_help
        else:
            help_ticket_qs = base_help

        if not today_only:
            help_ticket_qs = [h for h in help_ticket_qs if getattr(h, 'is_handover', False) or (_ist_date(h.planned_date) and _ist_date(h.planned_date) <= today_ist)]
        else:
            help_ticket_qs = [h for h in help_ticket_qs if _ist_date(h.planned_date) == today_ist]

        # Leave-filter for Help Tickets: if user is on leave RIGHT NOW, hide all
        try:
            if is_user_blocked_at(request.user, now_ist):
                help_ticket_qs = []
        except Exception:
            pass

        logger.info(_safe_console_text(
            f"Dashboard filtering for {request.user.username}: today_only={today_only} | "
            f"checklists={len(checklist_qs)} delegations={len(delegation_qs)} help_tickets={len(help_ticket_qs)} | "
            f"handed_over: CL={len(handed_over['checklist'])} DL={len(handed_over['delegation'])} HT={len(handed_over['help_ticket'])}"
        ))
    except Exception as e:
        logger.error(_safe_console_text(f"Error querying task lists: {e}"))
        checklist_qs = []
        delegation_qs = []
        help_ticket_qs = []

    # ✅ FINAL LEAVE GUARD (FIXED):
    # If the user is on leave today, hide ONLY today's CL/DL (respecting half-day by 10:00 gate elsewhere).
    # DO NOT hide overdue items (they must remain visible).
    try:
        if is_user_blocked(request.user, today_ist):
            checklist_qs = [c for c in checklist_qs if _ist_date(c.planned_date) and _ist_date(c.planned_date) < today_ist]
            delegation_qs = [d for d in delegation_qs if _ist_date(d.planned_date) and _ist_date(d.planned_date) < today_ist]
    except Exception:
        pass

    tasks = checklist_qs if request.GET.get('task_type') not in ('delegation', 'help_ticket') else (
        delegation_qs if request.GET.get('task_type') == 'delegation' else help_ticket_qs
    )

    if tasks:
        for i, task in enumerate(tasks[:3], start=1):
            tdt = task.planned_date.astimezone(IST) if task.planned_date else None
            handover_info = " (HANDOVER)" if getattr(task, 'is_handover', False) else ""
            logger.info(_safe_console_text(
                f"  - Sample task {i}: '{getattr(task, 'task_name', getattr(task, 'title', ''))}' "
                f"planned {tdt.strftime('%Y-%m-%d %H:%M IST') if tdt else 'No date'}{handover_info}"
            ))

    return render(request, 'dashboard/dashboard.html', {
        'week_score':    week_score,
        'pending_tasks': pending_tasks,
        'tasks':         tasks,
        'selected':      request.GET.get('task_type'),
        'prev_time':     "00:00",
        'curr_time':     "00:00",
        'today_only':    today_only,
        'handed_over':   handed_over,
    })
