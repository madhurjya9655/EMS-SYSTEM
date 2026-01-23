from __future__ import annotations

import io
import json
import logging
import mimetypes
import os
import random
import threading
import time
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Callable, Any, Tuple

from django.conf import settings
from django.core.cache import cache
from django.urls import NoReverseMatch, reverse
from django.db import transaction  # on_commit after DB save

from apps.reimbursement.models import ReimbursementLine  # for INCLUDED-only filter

logger = logging.getLogger(__name__)

__all__ = [
    "SPREADSHEET_ID",
    "TAB_MAIN",
    "TAB_CHANGELOG",
    "TAB_SCHEMA",
    "ensure_spreadsheet_structure",
    "sync_request",
    "build_row",   # back-compat alias
    "build_rows",
    "reset_main_data",  # utility to clear existing rows below the header
    "bulk_resync_all_requests",  # NEW: quota-safe full export
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Prefer env override first (Render), then settings, then safe dummy
SPREADSHEET_ID = (
    os.environ.get("REIMBURSEMENT_SHEET_ID")
    or getattr(settings, "REIMBURSEMENT_SHEET_ID", None)
    or getattr(settings, "REIMBURSEMENT_SHEET_ID".lower(), None)  # legacy
    or "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A"
)

TAB_MAIN      = "Reimbursements"
TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA    = "Schema"
TAB_META      = "_Meta"

# Schema version (bump when changing columns/behavior)
SYNC_VERSION = 8

# Structure checks throttle
STRUCTURE_TTL_SECONDS = int(getattr(settings, "REIMBURSEMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600))  # 10 min default

# Quota budgets (per process), defaults keep headroom under Google's 60 writes/min/user limit
READS_PER_MINUTE_BUDGET  = int(getattr(settings, "REIMBURSEMENT_SHEETS_READS_PER_MINUTE", 48))
WRITES_PER_MINUTE_BUDGET = int(getattr(settings, "REIMBURSEMENT_SHEETS_WRITES_PER_MINUTE", 30))

# ---------------------------------------------------------------------------
# ONE ROW PER BILL — business-visible columns (RowKey is hidden)
# ---------------------------------------------------------------------------

HEADER = [
    "RowKey",                   # hidden (internal upsert key)
    "Req ID",
    "Employee Name",
    "date of Bill",
    "Category",
    "Description of Bill",
    "Amount",
    "Receipt",
    "Gst Type",
    "Bill Status",
    "Finance Verifier",
    "Manager Verifier",
    "Bill Submission Time",
    "Finance Verified Time",
    "Manager Approved Time",
    "Bill Paid At (time)",
    "Payment reff",
]

CHANGELOG_HEADER = ["TimestampUTC","Event","RowKey","OldStatus","NewStatus","RowNum","Actor","Result"]
SCHEMA_HEADER    = ["Version","HeaderJSON","Active","RecordedAtUTC","Note"]

# ---------------------------------------------------------------------------
# Google client helpers
# ---------------------------------------------------------------------------

_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS  = False

def _excel_col(n: int) -> str:
    out=[]
    while n:
        n,r=divmod(n-1,26); out.append(chr(65+r))
    return "".join(reversed(out))

def _header_end_col() -> str:
    return _excel_col(len(HEADER))

def _iso(dt):
    """
    Return UTC ISO8601 with seconds for either a `datetime` or a `date`.
    - If `date`, coerce to midnight UTC.
    - If naive datetime, assume UTC.
    """
    if not dt:
        return ""
    try:
        if isinstance(dt, datetime):
            d = dt
        elif isinstance(dt, date):
            d = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        else:
            return str(dt)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        else:
            d = d.astimezone(timezone.utc)
        return d.isoformat(timespec="seconds")
    except Exception:
        return ""

def _site_url() -> str:
    return (getattr(settings, "SITE_URL", "").rstrip("/")) or "http://127.0.0.1:8000"

def _detail_url(req_id: int) -> str:
    base = _site_url()
    try:
        return f"{base}{reverse('admin:reimbursement_reimbursementrequest_change', args=[req_id])}"
    except NoReverseMatch:
        pass
    tmpl = getattr(settings, "REIMBURSEMENT_DETAIL_URL_TEMPLATE", None) or os.environ.get("REIMBURSEMENT_DETAIL_URL_TEMPLATE")
    if tmpl:
        path = tmpl.format(id=req_id, pk=req_id)
        if not path.startswith("/"):
            path = "/" + path
        return f"{base}{path}"
    return base + "/"

def _google_available() -> bool:
    global _WARNED_MISSING_GOOGLE, _WARNED_MISSING_CREDS
    try:
        import googleapiclient.discovery  # noqa
        import google.oauth2.service_account  # noqa
        import googleapiclient.http  # noqa
    except Exception:
        if not _WARNED_MISSING_GOOGLE:
            logger.warning(
                "Google sync disabled: install deps -> pip install google-api-python-client google-auth google-auth-httplib2"
            )
            _WARNED_MISSING_GOOGLE = True
        return False

    if not (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
        or getattr(settings, "GOOGLE_SERVICE_ACCOUNT_FILE", None)
    ):
        if not _WARNED_MISSING_CREDS:
            logger.warning(
                "Google sync disabled: credentials missing. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE."
            )
            _WARNED_MISSING_CREDS = True
        return False
    return True

def _credentials():
    from google.oauth2 import service_account
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return service_account.Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    file_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or getattr(settings, "GOOGLE_SERVICE_ACCOUNT_FILE", None)
    if file_path:
        return service_account.Credentials.from_service_account_file(file_path, scopes=scopes)
    raise RuntimeError("Google credentials not found")

def _svc_sheets():
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=_credentials(), cache_discovery=False)

def _svc_drive():
    from googleapiclient.discovery import build
    return build("drive", "v3", credentials=_credentials(), cache_discovery=False)

# ---------------------------------------------------------------------------
# Backoff + per-process token buckets for reads & writes
# ---------------------------------------------------------------------------

_rw_lock = threading.Lock()
_read_next_refill  = time.monotonic()
_write_next_refill = time.monotonic()
_read_tokens  = READS_PER_MINUTE_BUDGET
_write_tokens = WRITES_PER_MINUTE_BUDGET

def _consume_tokens(kind: str, n: int = 1):
    """
    kind: 'read' or 'write'
    """
    global _read_tokens, _write_tokens, _read_next_refill, _write_next_refill
    with _rw_lock:
        now = time.monotonic()
        # refill buckets per minute
        if now >= _read_next_refill:
            _read_tokens = READS_PER_MINUTE_BUDGET
            _read_next_refill = now + 60.0
        if now >= _write_next_refill:
            _write_tokens = WRITES_PER_MINUTE_BUDGET
            _write_next_refill = now + 60.0

        if kind == "read":
            bucket, next_refill = _read_tokens, _read_next_refill
        else:
            bucket, next_refill = _write_tokens, _write_next_refill

        if bucket < n:
            sleep_for = max(0.05, next_refill - now + 0.01)
            time.sleep(sleep_for)
            now2 = time.monotonic()
            if kind == "read":
                if now2 >= _read_next_refill:
                    _read_tokens = READS_PER_MINUTE_BUDGET
                    _read_next_refill = now2 + 60.0
                _read_tokens = max(0, _read_tokens - n)
            else:
                if now2 >= _write_next_refill:
                    _write_tokens = WRITES_PER_MINUTE_BUDGET
                    _write_next_refill = now2 + 60.0
                _write_tokens = max(0, _write_tokens - n)
            return

        if kind == "read":
            _read_tokens = max(0, _read_tokens - n)
        else:
            _write_tokens = max(0, _write_tokens - n)

def _with_backoff(label: str, kind: str, fn: Callable[[], Any]) -> Any:
    """
    kind: 'read' or 'write'
    """
    _consume_tokens(kind)
    delays = [0.2, 0.5, 1.0, 2.0, 4.0]
    last_exc = None
    for i, d in enumerate(delays, start=1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            code = getattr(getattr(e, "resp", None), "status", None)
            if code not in (429, 500, 502, 503, 504):
                raise
            if i == len(delays):
                break
            sleep_for = d + random.uniform(0.0, 0.2)
            logger.info("Retrying %s after %s (attempt %s/%s)", label, e, i, len(delays))
            time.sleep(sleep_for)
    raise last_exc

# ---------------------------------------------------------------------------
# Spreadsheet structure + formatting
# ---------------------------------------------------------------------------

_meta_lock = threading.Lock()
_meta_cache: Dict[str, Any] = {}

def _spreadsheets_get() -> dict:
    def _call():
        return _svc_sheets().spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    cache_key = f"sheets.meta.{SPREADSHEET_ID}"
    with _meta_lock:
        meta = _meta_cache.get(cache_key)
        if meta:
            return meta
        meta = _with_backoff("spreadsheets.get", "read", _call)
        _meta_cache[cache_key] = meta
        return meta

def _get_sheet_map_from_meta(meta: dict) -> Dict[str, int]:
    out: Dict[str,int] = {}
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        out[props.get("title")] = props.get("sheetId")
    return out

def _batch_update(requests: list) -> None:
    if not requests:
        return
    def _call():
        return _svc_sheets().spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
        ).execute()
    _with_backoff("spreadsheets.batchUpdate", "write", _call)

def _values_batch_get(ranges: List[str]) -> List[List[List[str]]]:
    def _call():
        return _svc_sheets().spreadsheets().values().batchGet(
            spreadsheetId=SPREADSHEET_ID, ranges=ranges
        ).execute()
    resp = _with_backoff("values.batchGet", "read", _call)
    return [x.get("values", [[]]) for x in resp.get("valueRanges", [])]

def _values_update(range_: str, values: List[List[Any]]) -> None:
    def _call():
        return _svc_sheets().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    _with_backoff("values.update", "write", _call)

def _values_append(range_: str, values: List[List[Any]], user_entered: bool = False) -> dict:
    def _call():
        return _svc_sheets().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="USER_ENTERED" if user_entered else "RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    return _with_backoff("values.append", "write", _call)

def _values_batch_update(data_blocks: List[Dict[str, Any]], input_option: str = "USER_ENTERED") -> None:
    if not data_blocks:
        return
    def _call():
        return _svc_sheets().spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": input_option, "data": data_blocks},
        ).execute()
    _with_backoff("values.batchUpdate", "write", _call)

def _friendly_format_main(sheet_id: int) -> None:
    end_col = len(HEADER)
    requests = []

    # Freeze header
    requests.append({
        "updateSheetProperties":{
            "properties":{"sheetId":sheet_id,"gridProperties":{"frozenRowCount":1}},
            "fields":"gridProperties.frozenRowCount"
        }
    })

    # Hide column A (RowKey)
    requests.append({
        "updateDimensionProperties":{
            "range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":0,"endIndex":1},
            "properties":{"hiddenByUser": True},
            "fields":"hiddenByUser"
        }
    })

    # Basic filter on all columns
    requests.append({"setBasicFilter":{"filter":{"range":{
        "sheetId":sheet_id,"startRowIndex":0,"endRowIndex":1_000_000,
        "startColumnIndex":0,"endColumnIndex":end_col
    }}}})

    # Column widths (B..Q visible columns)
    widths = {
        2:90,   # Req ID
        3:200,  # Employee Name
        4:130,  # date of Bill
        5:120,  # Category
        6:260,  # Description
        7:110,  # Amount
        8:120,  # Receipt
        9:100,  # GST
        10:140, # Bill Status
        11:170, # Finance Verifier
        12:170, # Manager Verifier
        13:170, # Submission
        14:170, # Finance Verified
        15:170, # Manager Approved
        16:170, # Paid At
        17:160, # Payment ref
    }
    for idx, px in widths.items():
        requests.append({"updateDimensionProperties":{
            "range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":idx-1,"endIndex":idx},
            "properties":{"pixelSize":px},"fields":"pixelSize"
        }})

    # Wrap long text columns
    for col in [6]:  # Description
        requests.append({"repeatCell":{"range":{
            "sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":col-1,"endColumnIndex":col
        },"cell":{"userEnteredFormat":{"wrapStrategy":"WRAP"}},"fields":"userEnteredFormat.wrapStrategy"}})

    # Amount as number
    requests.append({"repeatCell":{"range":{
        "sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":7-1,"endColumnIndex":7
    },"cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"#,##0.00"}}},"fields":"userEnteredFormat.numberFormat"}})

    # Datetime formatting for date/time columns
    for col in [4,13,14,15,16]:
        requests.append({"repeatCell":{"range":{
            "sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":col-1,"endColumnIndex":col
        },"cell":{"userEnteredFormat":{"numberFormat":{"type":"DATE_TIME","pattern":"yyyy-mm-dd hh:mm:ss"}}},"fields":"userEnteredFormat.numberFormat"}})

    try:
        _batch_update(requests)
    except Exception as e:
        logger.info("Non-fatal formatting skip: %s", e)

_structure_lock = threading.Lock()
_last_ensured_ts: float | None = None

def _structure_cache_key() -> str:
    return f"reimb.sheets.structure.{SPREADSHEET_ID}.v{SYNC_VERSION}"

def ensure_spreadsheet_structure() -> None:
    if not _google_available():
        return

    global _last_ensured_ts
    now = time.monotonic()

    ck = _structure_cache_key()
    cached = cache.get(ck)
    if cached:
        return

    with _structure_lock:
        if _last_ensured_ts and (now - _last_ensured_ts) < STRUCTURE_TTL_SECONDS:
            cache.set(ck, True, timeout=STRUCTURE_TTL_SECONDS)
            return

        meta = _spreadsheets_get()
        existing = _get_sheet_map_from_meta(meta)

        requests=[]
        for title in [TAB_MAIN, TAB_CHANGELOG, TAB_SCHEMA]:
            if title not in existing:
                requests.append({"addSheet":{"properties":{"title":title,"gridProperties":{"rowCount":2000,"columnCount":50}}}})
        if requests:
            _batch_update(requests)
            meta = _spreadsheets_get()
            existing = _get_sheet_map_from_meta(meta)

        # Hide non-main tabs
        requests=[]
        for title in [TAB_CHANGELOG, TAB_SCHEMA]:
            sid = existing.get(title)
            if sid is not None:
                requests.append({"updateSheetProperties":{"properties":{"sheetId":sid,"hidden":True},"fields":"hidden"}})
        _batch_update(requests)

        main_id = existing.get(TAB_MAIN)
        if main_id is not None:
            _friendly_format_main(main_id)

        ranges = [f"{TAB_MAIN}!1:1", f"{TAB_CHANGELOG}!1:1", f"{TAB_SCHEMA}!1:1"]
        values_list = _values_batch_get(ranges)

        if (values_list[0][0] if values_list and values_list[0] else []) != HEADER:
            _values_update(f"{TAB_MAIN}!1:1", [HEADER])

        if (values_list[1][0] if len(values_list) > 1 and values_list[1] else []) != CHANGELOG_HEADER:
            _values_update(f"{TAB_CHANGELOG}!1:1", [CHANGELOG_HEADER])

        if (values_list[2][0] if len(values_list) > 2 and values_list[2] else []) != SCHEMA_HEADER:
            _values_update(f"{TAB_SCHEMA}!1:1", [SCHEMA_HEADER])

        hb=[SYNC_VERSION, json.dumps(HEADER, ensure_ascii=False), True, _iso(datetime.now(timezone.utc)), "bootstrap/update"]
        try:
            _values_append(f"{TAB_SCHEMA}!A:E", [hb], user_entered=False)
        except Exception:
            pass

        _last_ensured_ts = time.monotonic()
        cache.set(ck, True, timeout=STRUCTURE_TTL_SECONDS)

# ---------------------------------------------------------------------------
# Drive helpers (upload receipts, return shareable link)
# ---------------------------------------------------------------------------

def _drive_folder_id() -> Optional[str]:
    return os.environ.get("REIMBURSEMENT_DRIVE_FOLDER_ID") or getattr(settings, "REIMBURSEMENT_DRIVE_FOLDER_ID", None)

def _drive_share_anyone() -> bool:
    return (os.environ.get("REIMBURSEMENT_DRIVE_LINK_SHARING") or getattr(settings, "REIMBURSEMENT_DRIVE_LINK_SHARING", "anyone")).lower() == "anyone"

def _drive_domain() -> Optional[str]:
    return os.environ.get("REIMBURSEMENT_DRIVE_DOMAIN") or getattr(settings, "REIMBURSEMENT_DRIVE_DOMAIN", None)

def _drive_find_file_by_name(name: str, parent: str) -> Optional[str]:
    def _call():
        svc = _svc_drive()
        safe_name = name.replace("'", "\\'")
        q = f"name = '{safe_name}' and '{parent}' in parents and trashed = false"
        return svc.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
    try:
        resp = _with_backoff("drive.files.list", "read", _call)
        items = resp.get("files", [])
        return items[0]["id"] if items else None
    except Exception:
        return None

def _drive_ensure_permission(file_id: str) -> None:
    def _call(body: dict):
        svc = _svc_drive()
        return svc.permissions().create(fileId=file_id, body=body, fields="id").execute()
    try:
        if _drive_share_anyone():
            body = {"type": "anyone", "role": "reader"}
        else:
            domain = _drive_domain()
            if not domain:
                body = {"type": "anyone", "role": "reader"}
            else:
                body = {"type": "domain", "role": "reader", "domain": domain, "allowFileDiscovery": False}
        _with_backoff("drive.permissions.create", "write", lambda: _call(body))
    except Exception as e:
        logger.info("Drive permission set failed for %s: %s", file_id, e)

def _drive_upload_bytes(name: str, data: bytes, parent: str, mime: Optional[str]) -> Optional[str]:
    from googleapiclient.http import MediaIoBaseUpload
    def _call():
        svc = _svc_drive()
        media = MediaIoBaseUpload(io.BytesIO(data), mimetypes.guess_type(name)[0] or "application/octet-stream", resumable=False)
        body = {"name": name, "parents": [parent]}
        return svc.files().create(body=body, media_body=media, fields="id").execute()
    try:
        file = _with_backoff("drive.files.create", "write", _call)
        fid = file.get("id")
        if fid:
            _drive_ensure_permission(fid)
        return fid
    except Exception as e:
        logger.info("Drive upload failed for %s: %s", name, e)
        return None

def _receipt_drive_filename(req_id: int, line_id: int, original_name: str) -> str:
    base = os.path.basename(original_name or "") or "receipt"
    return f"reimb_{req_id}_line_{line_id}_{base}"

def _drive_link(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view?usp=drivesdk"

# ---------------------------------------------------------------------------
# Helpers for building rows (bill-wise)
# ---------------------------------------------------------------------------

def _collect_receipt_link_for_line(req, line) -> str:
    # Try Google Drive
    try:
        if _google_available() and _drive_folder_id():
            f = getattr(line, "receipt_file", None) or getattr(line, "expense_item", None) and getattr(line.expense_item, "receipt_file", None)
            if f:
                existing_id = _drive_find_file_by_name(
                    _receipt_drive_filename(req.id, line.id, getattr(f, "name", "receipt")),
                    _drive_folder_id(),
                )
                if existing_id:
                    return _drive_link(existing_id)
                with f.open("rb") as fh:
                    data = fh.read()
                fid = _drive_upload_bytes(
                    _receipt_drive_filename(req.id, line.id, getattr(f, "name", "receipt")),
                    data,
                    _drive_folder_id(),
                    mimetypes.guess_type(getattr(f, "name", ""))[0],
                )
                if fid:
                    return _drive_link(fid)
    except Exception as e:
        logger.info("Drive link for req=%s line=%s skipped: %s", getattr(req, "id", None), getattr(line, "id", None), e)

    # Storage URL
    f = getattr(line, "receipt_file", None) or getattr(line, "expense_item", None) and getattr(line.expense_item, "receipt_file", None)
    if f and getattr(f, "url", None):
        try:
            return f.url
        except Exception:
            pass

    # EMS secured route (if available)
    try:
        return _site_url() + reverse("reimbursement:receipt_line", args=[line.id])
    except Exception:
        return ""

def _employee_name(user) -> str:
    try:
        full = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        return full or (user.username or f"User #{getattr(user,'id','')}")
    except Exception:
        return f"User #{getattr(user,'id','')}"

def _row_key(req_id: int, line_id: int) -> str:
    return f"{req_id}-{line_id}"

# ---------------------------------------------------------------------------
# Row building (BILL-WISE) — exact business columns order
# ---------------------------------------------------------------------------

def build_rows(req) -> List[List[Any]]:
    """
    Build one row per INCLUDED bill (ReimbursementLine).
    Columns match business spec; RowKey (hidden col A) keeps upserts stable.
    """
    rows: List[List[Any]] = []
    employee = getattr(req, "created_by", None)

    # INCLUDED lines only
    lines_qs = req.lines.select_related("expense_item").filter(
        status=ReimbursementLine.Status.INCLUDED
    )

    for line in lines_qs:
        item = line.expense_item

        amount = float(getattr(line, "amount", None) or getattr(item, "amount", 0) or 0)

        # Category label
        try:
            category = item.get_category_display()
        except Exception:
            category = getattr(item, "category", "") or ""

        # GST label
        try:
            gst = item.get_gst_type_display()
        except Exception:
            gst = getattr(item, "gst_type", "") or ""

        # Receipt hyperlink
        link = _collect_receipt_link_for_line(req, line)
        receipt_cell = f'=HYPERLINK("{link}","View")' if link else ""

        # Bill status label
        bill_status = getattr(line, "bill_status", "") or ""
        try:
            bill_status = line.get_bill_status_display()
        except Exception:
            pass

        finance_verifier = _employee_name(getattr(req, "verified_by", None)) if getattr(req, "verified_by_id", None) else ""
        manager_verifier = _employee_name(getattr(req, "manager", None)) if getattr(req, "manager_id", None) else ""

        # timestamps
        date_of_bill = getattr(item, "date", None)
        submitted_at = getattr(req, "submitted_at", None)
        verified_at = getattr(req, "verified_at", None)
        manager_approved_at = getattr(req, "manager_decided_at", None) if (str(getattr(req, "manager_decision", "")).lower() == "approved") else None
        paid_at = getattr(line, "paid_at", None) or getattr(req, "paid_at", None)

        payment_ref = (
            (getattr(line, "payment_reference", None) or "")
            or (getattr(req, "finance_payment_reference", None) or "")
        )

        # RowKey keeps upsert idempotent
        rowkey = _row_key(req.id, line.id)

        row = [
            rowkey,                         # RowKey (hidden)
            req.id,                         # Req ID
            _employee_name(employee),       # Employee Name
            _iso(date_of_bill),             # date of Bill
            category,                       # Category
            (line.description or getattr(item, "description", "") or ""),  # Description of Bill
            amount,                         # Amount
            receipt_cell,                   # Receipt (HYPERLINK)
            gst,                            # Gst Type
            bill_status,                    # Bill Status
            finance_verifier,               # Finance Verifier
            manager_verifier,               # Manager Verifier
            _iso(submitted_at),             # Bill Submission Time
            _iso(verified_at),              # Finance Verified Time
            _iso(manager_approved_at),      # Manager Approved Time
            _iso(paid_at),                  # Bill Paid At (time)
            payment_ref,                    # Payment reff
        ]
        rows.append(row)

    return rows

# Backward-compatible alias
def build_row(req) -> List[List[Any]]:
    return build_rows(req)

# ---------------------------------------------------------------------------
# Upsert helpers (bill-wise; key=A: RowKey)
# ---------------------------------------------------------------------------

def _index_by_rowkey() -> Dict[str,int]:
    """Map RowKey -> row number (reads A2:A once)."""
    def _call():
        return _svc_sheets().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!A2:A"
        ).execute()
    resp = _with_backoff("values.get A2:A", "read", _call)
    idx: Dict[str,int] = {}
    for i,v in enumerate(resp.get("values", []), start=2):
        if v:
            idx[str(v[0])] = i
    return idx

def _upsert_rows_batch(rows: List[List[Any]]) -> Dict[str, int]:
    """
    Performance path (per-request):
    - Read the index ONCE
    - Batch update existing rows via values.batchUpdate
    - Append all new rows in ONE append call
    Returns: rowkey -> rownum for written rows (best-effort for appended).
    """
    if not rows:
        return {}

    end_col = _header_end_col()
    idx = _index_by_rowkey()  # single read
    updates: List[Dict[str, Any]] = []
    to_append: List[List[Any]] = []
    written_map: Dict[str, int] = {}

    for row in rows:
        rowkey = str(row[0])
        rn = idx.get(rowkey)
        if rn:
            updates.append({
                "range": f"{TAB_MAIN}!A{rn}:{end_col}{rn}",
                "values": [row],
            })
            written_map[rowkey] = rn
        else:
            to_append.append(row)

    # Batch update existing rows
    if updates:
        _values_batch_update(updates, input_option="USER_ENTERED")

    # Append new rows in one shot
    if to_append:
        resp = _values_append(f"{TAB_MAIN}!A:{end_col}", to_append, user_entered=True)
        try:
            rng = resp.get("updates", {}).get("updatedRange", "")
            first_rn = int(rng.split("!")[1].split(":")[0][1:])
        except Exception:
            first_rn = 0
        if first_rn:
            for i, row in enumerate(to_append):
                written_map[str(row[0])] = first_rn + i

    return written_map

def append_changelog(event: str, rowkey: str, old: str, new: str, rownum: int, actor: str = "", result: str = "ok", err: str = "") -> None:
    try:
        _values_append(
            f"{TAB_CHANGELOG}!A:H",
            [[_iso(datetime.now(timezone.utc)), event, rowkey, old or "", new or "", rownum, actor or "", f"{result}: {err}" if err else result]],
            user_entered=False,
        )
    except Exception as e:
        logger.info("Changelog append skipped: %s", e)

# ---------------------------------------------------------------------------
# Public entry: sync a single request (exports one row per bill)
# ---------------------------------------------------------------------------

def _sync_request_impl(req_id: int) -> None:
    if not _google_available():
        return
    from apps.reimbursement.models import ReimbursementRequest  # lazy import

    try:
        req = (
            ReimbursementRequest.objects.select_related("created_by", "manager", "management", "verified_by")
            .prefetch_related("lines__expense_item")
            .get(pk=req_id)
        )
    except Exception:
        return

    ensure_spreadsheet_structure()

    rows = build_rows(req)

    try:
        written = _upsert_rows_batch(rows)
    except Exception as e:
        logger.exception("Google Sheets batch upsert failed for req=%s: %s", req_id, e)
        written = {}

    prev_status = getattr(req, "status", "") or ""
    for row in rows:
        rk = str(row[0])
        rn = int(written.get(rk, 0))
        try:
            append_changelog("upsert", rk, prev_status, req.status, rn, "", "ok")
        except Exception:
            pass

def sync_request(req) -> None:
    """
    Export-only. No status mutations or audit writes.
    Non-blocking for the web request:
      - Debounced per request via cache
      - Enqueued after DB commit using transaction.on_commit
      - Runs in a daemon thread
    """
    if req is None:
        return
    try:
        req_id = int(getattr(req, "id", 0)) or 0
    except Exception:
        req_id = 0
    if not req_id:
        return
    if not _google_available():
        return

    lock_key = f"reimb.sheets.sync.lock.{req_id}"
    if not cache.add(lock_key, True, timeout=30):
        return

    def _kick():
        try:
            t = threading.Thread(target=_sync_request_impl, args=(req_id,), daemon=True)
            t.start()
        except Exception:
            cache.delete(lock_key)

    try:
        transaction.on_commit(_kick)
    except Exception:
        _kick()

# ---------------------------------------------------------------------------
# Maintenance helper: clear all data rows under header
# ---------------------------------------------------------------------------

def reset_main_data() -> None:
    """Clears all rows below header in TAB_MAIN (keeps header)."""
    if not _google_available():
        return
    end_col = _header_end_col()
    def _call():
        return _svc_sheets().spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A2:{end_col}"
        ).execute()
    try:
        _with_backoff("values.clear", "write", _call)
        logger.info("Cleared main sheet data under header.")
    except Exception as e:
        logger.exception("Failed to clear main sheet: %s", e)

# ---------------------------------------------------------------------------
# NEW: Bulk re-sync (quota-safe)
# ---------------------------------------------------------------------------

def _collect_all_rows() -> Tuple[List[List[Any]], Dict[str, List[Any]]]:
    """
    Build rows for all ReimbursementRequests and return:
      - all_rows: list of rows
      - rows_by_rowkey: dict RowKey -> row
    """
    from apps.reimbursement.models import ReimbursementRequest
    all_rows: List[List[Any]] = []
    rows_by_rowkey: Dict[str, List[Any]] = {}

    qs = ReimbursementRequest.objects.select_related("created_by", "manager", "management", "verified_by") \
                                     .prefetch_related("lines__expense_item") \
                                     .only("id")
    for req in qs.iterator():
        rows = build_rows(req)
        all_rows.extend(rows)
        for r in rows:
            rows_by_rowkey[str(r[0])] = r
    return all_rows, rows_by_rowkey

def _chunked(seq: List[Any], size: int) -> List[List[Any]]:
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def bulk_resync_all_requests(
    update_chunk_size: int = 500,
    append_chunk_size: int = 500,
    sleep_between_append_chunks_sec: int = 65,
    disable_changelog: bool = True,
) -> None:
    """
    Quota-safe full export:
      1) Ensure structure.
      2) Read main sheet RowKey index once.
      3) Compute updates vs. appends for ALL rows.
      4) values.batchUpdate in chunks for updates (1 write call per chunk).
      5) values.append in chunks for appends (1 write call per chunk) with a pause between chunks.
      6) Optionally skip ChangeLog writes during bulk to preserve write quota.
    """
    if not _google_available():
        return

    ensure_spreadsheet_structure()

    # Build all rows across all requests
    all_rows, rows_by_rowkey = _collect_all_rows()
    if not all_rows:
        logger.info("bulk_resync_all_requests: no rows to write.")
        return

    # Existing index on the sheet
    idx = _index_by_rowkey()

    end_col = _header_end_col()
    updates_blocks: List[Dict[str, Any]] = []
    appends_rows: List[List[Any]] = []

    for rowkey, row in rows_by_rowkey.items():
        rn = idx.get(rowkey)
        if rn:
            updates_blocks.append({"range": f"{TAB_MAIN}!A{rn}:{end_col}{rn}", "values": [row]})
        else:
            appends_rows.append(row)

    # 1) Updates in chunks (batchUpdate)
    if updates_blocks:
        for chunk in _chunked(updates_blocks, update_chunk_size):
            _values_batch_update(chunk, input_option="USER_ENTERED")
        logger.info("bulk_resync_all_requests: updated %s existing rows.", len(updates_blocks))

    # 2) Appends in chunks with a pause to satisfy write quota
    if appends_rows:
        first_chunk = True
        for chunk in _chunked(appends_rows, append_chunk_size):
            if not first_chunk:
                # pause between append chunks to avoid 60 writes/min/user
                time.sleep(max(1, sleep_between_append_chunks_sec))
            _values_append(f"{TAB_MAIN}!A:{end_col}", chunk, user_entered=True)
            first_chunk = False
        logger.info("bulk_resync_all_requests: appended %s new rows.", len(appends_rows))

    # 3) Changelog (optional; skipped by default to save write quota)
    if not disable_changelog:
        now = _iso(datetime.now(timezone.utc))
        rows = [[now, "bulk_resync", "", "", "", 0, "", "ok"]]
        try:
            _values_append(f"{TAB_CHANGELOG}!A:H", rows, user_entered=False)
        except Exception as e:
            logger.info("bulk_resync changelog skipped: %s", e)
