from __future__ import annotations

import logging
import re
import time
import unicodedata
from datetime import datetime, time as dt_time
from threading import Thread, Lock
from typing import List, Tuple

import pandas as pd
import pytz

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection, transaction, close_old_connections
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone

from .models import Checklist, Delegation
from .utils import (
    send_checklist_assignment_to_user,
    send_delegation_assignment_to_user,
    send_admin_bulk_summary,
)
from .recurrence import preserve_first_occurrence_time

logger = logging.getLogger(__name__)
User = get_user_model()

# -----------------------------
# Constants
# -----------------------------
IST = pytz.timezone("Asia/Kolkata")
ASSIGN_HOUR = 10
ASSIGN_MINUTE = 0
RECURRING_MODES = ["Daily", "Weekly", "Monthly", "Yearly"]

BULK_BATCH_SIZE = 500
SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")


# -----------------------------
# Background runner (post-response safe)
# -----------------------------
def _background(target, /, *args, **kwargs):
    """
    Run `target(*args, **kwargs)` in a detached daemon thread.
    Ensures DB connections are fresh for the new thread.
    """
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

    t = Thread(target=_runner, daemon=True, name=kwargs.pop("thread_name", "bulk-upload-bg"))
    t.start()


# -----------------------------
# Encoding-safe helpers
# -----------------------------
def clean_unicode_string(text):
    if text is None:
        return ""
    text = str(text).replace("\x96", "-").replace("\u2013", "-").replace("\u2014", "-")
    return unicodedata.normalize("NFKD", text).strip()


def _clean_str(val):  # local alias
    return clean_unicode_string("" if val is None else val)


# -----------------------------
# Parser helpers
# -----------------------------
def parse_datetime_flexible(value):
    """
    Accept common date & date-time forms. If it's a pure date, default time to 10:00.
    If Excel timestamp (pandas Timestamp) comes at 00:00, we *do not* coerce unless it's a pure date.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
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
                dt = dt.replace(hour=ASSIGN_HOUR, minute=ASSIGN_MINUTE, second=0, microsecond=0)
            return dt
        except ValueError:
            continue

    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(s, dayfirst=False, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


_SYN_MODE = {
    "day": "Daily", "daily": "Daily",
    "week": "Weekly", "weekly": "Weekly",
    "month": "Monthly", "monthly": "Monthly",
    "year": "Yearly", "yearly": "Yearly",
}
_RECURRENCE_RE = re.compile(r"(?i)\b(?:every|evry)?\s*(\d+)?\s*(day|daily|week|weekly|month|monthly|year|yearly)\b")


def parse_mode_frequency_from_row(row) -> Tuple[str, int | None]:
    """Parse recurrence info if present, else return ('', None)."""
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
        txt = _clean_str(row.get(key))
        if not txt:
            continue
        m = _RECURRENCE_RE.search(txt)
        if m:
            n = m.group(1)
            unit = m.group(2).lower()
            mode = _SYN_MODE.get(unit, "")
            if mode in RECURRING_MODES:
                return mode, (max(1, int(n)) if n else 1)
        unit = txt.lower()
        if unit in _SYN_MODE:
            return _SYN_MODE[unit], 1
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


def parse_excel_file_optimized(file) -> pd.DataFrame:
    file.seek(0)
    name = (getattr(file, "name", "") or "").lower()
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
    raise ValueError("Unsupported file format. Please upload .xlsx, .xls, or .csv")


def validate_and_prepare_excel_data(df: pd.DataFrame) -> Tuple[pd.DataFrame | None, List[str]]:
    # Normalize headers
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

    # Drop empties
    df = df.replace("", pd.NA).dropna(subset=["Task Name"])
    df = df[df["Task Name"].astype(str).str.strip().astype(bool)]
    if len(df) == 0:
        return None, ["No valid rows found in the file"]

    # Warm user cache for better performance
    usernames = set()
    for col in ["Assign To", "Assign PC", "Notify To", "Auditor"]:
        if col in df.columns:
            ser = df[col].astype(str).str.strip()
            ser = ser[(ser != "") & ser.notna()]
            usernames.update(ser.unique().tolist())
    _user_cache.preload_usernames(list(usernames))

    return df, []


# -----------------------------
# User cache to speed up lookups
# -----------------------------
class _UserCache:
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


_user_cache = _UserCache()


# -----------------------------
# DB helpers
# -----------------------------
def _optimal_batch_size() -> int:
    try:
        if connection.vendor == "sqlite":
            return 250
        return min(BULK_BATCH_SIZE, 500)
    except Exception:
        return 250


# -----------------------------
# Core row -> model builders
# -----------------------------
def _build_checklist_from_row(row, assign_by_user):
    task_name = _clean_str(row.get("Task Name"))
    assign_to_username = _clean_str(row.get("Assign To"))
    planned_dt = parse_datetime_flexible(row.get("Planned Date"))
    planned_dt = preserve_first_occurrence_time(planned_dt) if planned_dt else None

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

    obj = Checklist(
        assign_by=assign_by_user,
        task_name=task_name,
        message=_clean_str(row.get("Message")),
        assign_to=_user_cache.get_user(assign_to_username),
        planned_date=planned_dt,
        priority=priority,
        attachment_mandatory=False,
        mode=mode,
        frequency=frequency if mode else None,
        time_per_task_minutes=time_per_task,
        remind_before_days=remind_before_days,
        assign_pc=_user_cache.get_user(_clean_str(row.get("Assign PC"))),
        group_name=_clean_str(row.get("Group Name")),
        notify_to=_user_cache.get_user(_clean_str(row.get("Notify To"))),
        auditor=_user_cache.get_user(_clean_str(row.get("Auditor"))),
        set_reminder=parse_bool(row.get("Set Reminder")),
        reminder_mode=_SYN_MODE.get(_clean_str(row.get("Reminder Mode")).lower(), _clean_str(row.get("Reminder Mode")).title()) if parse_bool(row.get("Set Reminder")) else None,
        reminder_frequency=parse_int(row.get("Reminder Frequency"), default=1) if parse_bool(row.get("Set Reminder")) else None,
        reminder_starting_time=_parse_time_flexible(row.get("Reminder Starting Time")) if parse_bool(row.get("Set Reminder")) else None,
        checklist_auto_close=parse_bool(row.get("Checklist Auto Close")),
        checklist_auto_close_days=parse_int(row.get("Checklist Auto Close Days"), default=0),
        actual_duration_minutes=0,
        status="Pending",
    )
    return obj


def _build_delegation_from_row(row, assign_by_user):
    task_name = _clean_str(row.get("Task Name"))
    assign_to_username = _clean_str(row.get("Assign To"))
    planned_dt = parse_datetime_flexible(row.get("Planned Date"))
    planned_dt = preserve_first_occurrence_time(planned_dt) if planned_dt else None

    priority = (_clean_str(row.get("Priority")) or "Low").title()
    if priority not in ["Low", "Medium", "High"]:
        priority = "Low"

    time_per_task = parse_int(row.get("Time per Task (minutes)"), default=0)

    obj = Delegation(
        assign_by=assign_by_user,
        task_name=task_name,
        assign_to=_user_cache.get_user(assign_to_username),
        planned_date=planned_dt,
        priority=priority,
        attachment_mandatory=False,
        # one-time only:
        mode=None,
        frequency=None,
        time_per_task_minutes=time_per_task,
        actual_duration_minutes=0,
        status="Pending",
    )
    return obj


def _parse_time_flexible(val):
    s = _clean_str(val)
    if not s:
        return None
    try:
        if ":" in s:
            return datetime.strptime(s, "%H:%M").time()
        # Excel fraction of day
        f = float(s)
        h = int(f * 24) % 24
        m = int(round(f * 24 * 60)) % 60
        return dt_time(h, m)
    except Exception:
        return None


# -----------------------------
# Public APIs
# -----------------------------
def process_checklist_bulk_upload(file, assign_by_user, *, send_emails: bool = True):
    """
    Parse file → create Checklist tasks → send assignee emails in background.
    Returns: (created_objects, errors)
    """
    try:
        df = parse_excel_file_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {e}. Please upload .xlsx, .xls or .csv"]

    df, v_errors = validate_and_prepare_excel_data(df)
    if v_errors:
        return [], v_errors

    created: List[Checklist] = []
    errors: List[str] = []
    _user_cache.clear()

    total = len(df)
    bs = _optimal_batch_size()

    for start_idx in range(0, total, bs):
        end_idx = min(start_idx + bs, total)
        batch_df = df.iloc[start_idx:end_idx]
        objs, batch_errors = _create_checklist_batch(batch_df, assign_by_user)
        created.extend(objs)
        errors.extend(batch_errors)
        if connection.vendor == "sqlite":
            time.sleep(0.01)  # allow locks to settle

    if created and send_emails:
        _kick_off_bulk_emails_async([o.id for o in created if getattr(o, "id", None)], task_type="Checklist")
        _send_admin_preview_async(
            f"✅ Bulk Upload: {len(created)} Checklist Tasks Created",
            created[:10],
            exclude_assigner_email=(assign_by_user.email or None),
        )

    return created, errors


def process_delegation_bulk_upload(file, assign_by_user, *, send_emails: bool = True):
    """
    Parse file → create Delegation tasks → send assignee emails in background.
    Returns: (created_objects, errors)
    """
    try:
        df = parse_excel_file_optimized(file)
    except Exception as e:
        return [], [f"Error reading file: {e}. Please upload .xlsx, .xls or .csv"]

    df, v_errors = validate_and_prepare_excel_data(df)
    if v_errors:
        return [], v_errors

    created: List[Delegation] = []
    errors: List[str] = []
    _user_cache.clear()

    total = len(df)
    bs = _optimal_batch_size()

    for start_idx in range(0, total, bs):
        end_idx = min(start_idx + bs, total)
        batch_df = df.iloc[start_idx:end_idx]
        objs, batch_errors = _create_delegation_batch(batch_df, assign_by_user)
        created.extend(objs)
        errors.extend(batch_errors)
        if connection.vendor == "sqlite":
            time.sleep(0.01)

    if created and send_emails:
        _kick_off_bulk_emails_async([o.id for o in created if getattr(o, "id", None)], task_type="Delegation")
        _send_admin_preview_async(
            f"✅ Bulk Upload: {len(created)} Delegation Tasks Created",
            created[:10],
            exclude_assigner_email=(assign_by_user.email or None),
        )

    return created, errors


# -----------------------------
# Internal batch creators
# -----------------------------
def _create_checklist_batch(batch_df, assign_by_user):
    objs, errors = [], []
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
            if not _user_cache.get_user(assign_to_username):
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found or inactive")
                continue

            planned_dt = parse_datetime_flexible(row.get("Planned Date"))
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid or missing planned date")
                continue

            obj = _build_checklist_from_row(row, assign_by_user)
            objs.append(obj)
        except Exception as e:
            errors.append(f"Row {idx+1}: {e}")

    created = []
    if objs:
        try:
            with transaction.atomic():
                created = Checklist.objects.bulk_create(objs, batch_size=min(len(objs), _optimal_batch_size()))
        except Exception as e:
            logger.error("Checklist bulk_create failed; attempting partial fallback: %s", e)
            # partial fallback
            for i in range(0, len(objs), 50):
                chunk = objs[i:i+50]
                try:
                    created.extend(Checklist.objects.bulk_create(chunk, batch_size=50))
                except Exception:
                    for o in chunk:
                        try:
                            o.save()
                            created.append(o)
                        except Exception as save_err:
                            errors.append(f"Failed to save '{clean_unicode_string(o.task_name)}': {save_err}")
    return created, errors


def _create_delegation_batch(batch_df, assign_by_user):
    objs, errors = [], []
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
            if not _user_cache.get_user(assign_to_username):
                errors.append(f"Row {idx+1}: User '{assign_to_username}' not found or inactive")
                continue

            planned_dt = parse_datetime_flexible(row.get("Planned Date"))
            if not planned_dt:
                errors.append(f"Row {idx+1}: Invalid or missing planned date")
                continue

            obj = _build_delegation_from_row(row, assign_by_user)
            objs.append(obj)
        except Exception as e:
            errors.append(f"Row {idx+1}: {e}")

    created = []
    if objs:
        try:
            with transaction.atomic():
                created = Delegation.objects.bulk_create(objs, batch_size=min(len(objs), _optimal_batch_size()))
        except Exception as e:
            logger.error("Delegation bulk_create failed; attempting partial fallback: %s", e)
            for i in range(0, len(objs), 50):
                chunk = objs[i:i+50]
                try:
                    created.extend(Delegation.objects.bulk_create(chunk, batch_size=50))
                except Exception:
                    for o in chunk:
                        try:
                            o.save()
                            created.append(o)
                        except Exception as save_err:
                            errors.append(f"Failed to save delegation '{clean_unicode_string(o.task_name)}': {save_err}")
    return created, errors


# -----------------------------
# Email helpers (async)
# -----------------------------
def _send_bulk_emails(task_ids: List[int], *, task_type: str):
    """
    Runs in a background thread. Refetches tasks and sends emails to ASSIGNEES ONLY.
    If an assignee has no email, sending is silently skipped (logged inside mailer).
    """
    Model = Checklist if task_type == "Checklist" else Delegation
    CHUNK = 100
    for i in range(0, len(task_ids), CHUNK):
        ids_chunk = task_ids[i:i+CHUNK]
        qs = (
            Model.objects.filter(id__in=ids_chunk)
            .select_related("assign_by", "assign_to")
            .order_by("id")
        )
        for task in qs:
            try:
                if task_type == "Checklist":
                    url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[task.id])}"
                    send_checklist_assignment_to_user(
                        task=task,
                        complete_url=url,
                        subject_prefix="Bulk Upload - Checklist Assigned",
                    )
                else:
                    url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[task.id])}"
                    send_delegation_assignment_to_user(
                        delegation=task,
                        complete_url=url,
                        subject_prefix="Bulk Upload - Delegation Assigned",
                    )
            except Exception as e:
                logger.error("Failed to send %s bulk email for id=%s: %s", task_type, getattr(task, "id", "?"), e)


def _kick_off_bulk_emails_async(task_ids: List[int], *, task_type: str):
    if not task_ids:
        return
    _background(_send_bulk_emails, task_ids, task_type=task_type, thread_name="bulk-upload-emails")


def _send_admin_preview_async(
    title: str,
    created_subset: List[Checklist | Delegation],
    *,
    exclude_assigner_email: str | None = None,
):
    """
    Sends a small preview table to admins/superusers; can optionally exclude the uploader.
    """
    def _runner():
        try:
            rows = []
            for t in created_subset:
                if isinstance(t, Checklist):
                    complete_url = f"{SITE_URL}{reverse('tasks:complete_checklist', args=[t.id])}"
                    assignee = t.assign_to.get_full_name() or t.assign_to.username
                    rows.append({
                        "Task Name": t.task_name,
                        "Assign To": assignee,
                        "Planned Date": t.planned_date.strftime("%Y-%m-%d %H:%M") if t.planned_date else "N/A",
                        "Priority": t.priority,
                        "complete_url": complete_url,
                    })
                else:
                    complete_url = f"{SITE_URL}{reverse('tasks:complete_delegation', args=[t.id])}"
                    assignee = t.assign_to.get_full_name() or t.assign_to.username
                    rows.append({
                        "Task Name": t.task_name,
                        "Assign To": assignee,
                        "Planned Date": t.planned_date.strftime("%Y-%m-%d %H:%M") if t.planned_date else "N/A",
                        "Priority": t.priority,
                        "complete_url": complete_url,
                    })
            # utils should ignore the uploader if an email is provided
            send_admin_bulk_summary(title=title, rows=rows, exclude_assigner_email=exclude_assigner_email)
        except Exception as e:
            logger.error("Admin bulk summary failed: %s", e)

    _background(_runner, thread_name="bulk-upload-admin-summary")


__all__ = [
    "process_checklist_bulk_upload",
    "process_delegation_bulk_upload",
    # lower-level helpers (optional)
    "parse_excel_file_optimized",
    "validate_and_prepare_excel_data",
    "parse_datetime_flexible",
    "parse_mode_frequency_from_row",
    "parse_bool",
    "parse_int",
]
