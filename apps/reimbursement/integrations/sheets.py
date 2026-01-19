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
from typing import Dict, Tuple, List, Optional, Callable, Any

from django.conf import settings
from django.core.cache import cache
from django.urls import NoReverseMatch, reverse
from django.db import transaction  # ✅ for on_commit to run after DB save

# ✅ IMPORT NEEDED FOR BILL-WISE FILTER (INCLUDED ONLY)
from apps.reimbursement.models import ReimbursementLine

logger = logging.getLogger(__name__)

__all__ = [
    "SPREADSHEET_ID",
    "TAB_MAIN",
    "TAB_CHANGELOG",
    "TAB_SCHEMA",
    "ensure_spreadsheet_structure",
    "sync_request",
    "build_row",   # kept for compatibility; now returns bill-wise rows
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

# 🔼 Bump when schema/behavior changes (bill-wise since v7)
SYNC_VERSION = 7

# structure checks will run at most once per STRUCTURE_TTL per process
STRUCTURE_TTL_SECONDS = int(getattr(settings, "REIMBURSEMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600))  # 10 min default

# token-bucket limiter per-process to stay under 60 read/min *per user* with headroom
READS_PER_MINUTE_BUDGET = int(getattr(settings, "REIMBURSEMENT_SHEETS_READS_PER_MINUTE", 48))

# ---------------- Bill-wise header (one row per ReimbursementLine) ----------------
# A: RowKey is "ReimbID-LineID" to keep idempotent upserts
HEADER = [
    "RowKey","ReimbID","LineID",
    "Employee","Department",
    "ExpenseDate","Category","GSTType",
    "Description","Amount","Currency",
    "RequestStatus","BillStatus",
    "Submitted","StatusUpdated",
    "Manager","Management","FinanceVerifier",
    "PaymentRef","PaidAt",
    "ReceiptLink","EMSLink",
    "CreatedAt","UpdatedAt","SyncedAt","SyncVersion","Extra",
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
            # Unknown type; best effort string
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
# Single place for backoff + per-process token bucket for reads
# ---------------------------------------------------------------------------

_bucket_lock = threading.Lock()
_bucket_next_refill = time.monotonic()
_bucket_tokens = READS_PER_MINUTE_BUDGET

def _consume_read_token(n: int = 1):
    global _bucket_tokens, _bucket_next_refill
    with _bucket_lock:
        now = time.monotonic()
        if now >= _bucket_next_refill:
            _bucket_tokens = READS_PER_MINUTE_BUDGET
            _bucket_next_refill = now + 60.0
        if _bucket_tokens < n:
            sleep_for = max(0.05, _bucket_next_refill - now + 0.01)
            time.sleep(sleep_for)
            now2 = time.monotonic()
            if now2 >= _bucket_next_refill:
                _bucket_tokens = READS_PER_MINUTE_BUDGET
                _bucket_next_refill = now2 + 60.0
        _bucket_tokens = max(0, _bucket_tokens - n)

def _with_backoff(label: str, is_read: bool, fn: Callable[[], Any]) -> Any:
    if is_read:
        _consume_read_token()
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
        meta = _with_backoff("spreadsheets.get", True, _call)
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
    _with_backoff("spreadsheets.batchUpdate", False, _call)

def _values_batch_get(ranges: List[str]) -> List[List[List[str]]]:
    def _call():
        return _svc_sheets().spreadsheets().values().batchGet(
            spreadsheetId=SPREADSHEET_ID, ranges=ranges
        ).execute()
    resp = _with_backoff("values.batchGet", True, _call)
    return [x.get("values", [[]]) for x in resp.get("valueRanges", [])]

def _values_update(range_: str, values: List[List[Any]]) -> None:
    def _call():
        return _svc_sheets().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    _with_backoff("values.update", False, _call)

def _values_append(range_: str, values: List[List[Any]], user_entered: bool = False) -> dict:
    def _call():
        return _svc_sheets().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="USER_ENTERED" if user_entered else "RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    return _with_backoff("values.append", False, _call)

def _values_batch_update(data_blocks: List[Dict[str, Any]], input_option: str = "USER_ENTERED") -> None:
    """Batch multiple range updates in one API call."""
    if not data_blocks:
        return
    def _call():
        return _svc_sheets().spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": input_option, "data": data_blocks},
        ).execute()
    _with_backoff("values.batchUpdate", False, _call)

def _friendly_format_main(sheet_id: int) -> None:
    end_col = len(HEADER)
    requests = []
    requests.append({"updateSheetProperties":{"properties":{"sheetId":sheet_id,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}})
    requests.append({"setBasicFilter":{"filter":{"range":{"sheetId":sheet_id,"startRowIndex":0,"endRowIndex":1_000_000,"startColumnIndex":0,"endColumnIndex":end_col}}}})
    widths={1:130,2:80,3:80,4:200,5:150,6:110,7:130,8:110,9:260,10:110,11:70,12:150,13:150,14:170,15:170,16:150,17:150,18:170,19:170,20:170,21:260,22:160,23:170,24:170,25:110,26:120,27:200}
    for idx,px in widths.items():
        requests.append({"updateDimensionProperties":{"range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":idx-1,"endIndex":idx},"properties":{"pixelSize":px},"fields":"pixelSize"}})
    for col in [9,21,22]:
        requests.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":col-1,"endColumnIndex":col},"cell":{"userEnteredFormat":{"wrapStrategy":"WRAP"}},"fields":"userEnteredFormat.wrapStrategy"}})
    # Amount as number
    requests.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":10-1,"endColumnIndex":10},"cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"#,##0.00"}}},"fields":"userEnteredFormat.numberFormat"}})
    # Date/time columns
    for col in [14,15,19,20,23,24,25]:
        requests.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":col-1,"endColumnIndex":col},"cell":{"userEnteredFormat":{"numberFormat":{"type":"DATE_TIME","pattern":"yyyy-mm-dd hh:mm:ss"}}},"fields":"userEnteredFormat.numberFormat"}})
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
        resp = _with_backoff("drive.files.list", True, _call)
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
        _with_backoff("drive.permissions.create", False, lambda: _call(body))
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
        file = _with_backoff("drive.files.create", False, _call)
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
    """
    Returns a shareable receipt link for a single line.
    - Prefer Google Drive (if folder configured & google available)
    - Fallback to storage URL
    - Fallback to secured EMS download endpoint
    """
    # Google Drive path
    try:
        if _google_available() and _drive_folder_id():
            f = getattr(line, "receipt_file", None) or getattr(line.expense_item, "receipt_file", None)
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
    f = getattr(line, "receipt_file", None) or getattr(line.expense_item, "receipt_file", None)
    if f and getattr(f, "url", None):
        try:
            return f.url
        except Exception:
            pass

    # EMS secured route (if available in urls)
    try:
        return _site_url() + reverse("reimbursement:receipt_line", args=[line.id])
    except Exception:
        return ""

def _employee_display(user) -> str:
    try:
        full = f"{getattr(user,'first_name','').strip()} {getattr(user,'last_name','').strip()}".strip()
        if full:
            return full
        return getattr(user, "username", "") or f"User #{getattr(user,'id','')}"
    except Exception:
        return f"User #{getattr(user,'id','')}"

def _department_for(user) -> str:
    return (
        getattr(user, "department", "")
        or (getattr(user, "profile", None) and getattr(user.profile, "department", ""))  # type: ignore[attr-defined]
        or ""
    )

def _row_key(req_id: int, line_id: int) -> str:
    return f"{req_id}-{line_id}"

# ---------------------------------------------------------------------------
# Row building (BILL-WISE)
# ---------------------------------------------------------------------------

def build_rows(req) -> List[List[Any]]:
    """
    Build one row per INCLUDED ReimbursementLine (bill-wise, read-only).
    """
    rows: List[List[Any]] = []
    employee = getattr(req, "created_by", None)

    # ✅ CRITICAL: only INCLUDED lines are exported (bill-wise truth)
    lines_qs = req.lines.select_related("expense_item").filter(
        status=ReimbursementLine.Status.INCLUDED
    )

    for line in lines_qs:
        expense = line.expense_item
        row = {
            "RowKey": _row_key(req.id, line.id),
            "ReimbID": req.id,
            "LineID": line.id,
            "Employee": _employee_display(employee) if employee else "",
            "Department": _department_for(employee) if employee else "",
            "ExpenseDate": _iso(getattr(expense, "date", None)),
            "Category": getattr(expense, "category", "") or "",
            "GSTType": getattr(expense, "gst_type", "") or "",
            "Description": (line.description or ""),
            "Amount": float(line.amount or 0),
            "Currency": "INR",
            "RequestStatus": req.status,
            "BillStatus": line.bill_status,
            "Submitted": _iso(req.submitted_at),
            "StatusUpdated": _iso(req.updated_at),
            "Manager": getattr(getattr(req, "manager", None), "username", "") if getattr(req, "manager_id", None) else "",
            "Management": getattr(getattr(req, "management", None), "username", "") if getattr(req, "management_id", None) else "",
            "FinanceVerifier": getattr(getattr(req, "verified_by", None), "username", "") if getattr(req, "verified_by_id", None) else "",
            "PaymentRef": getattr(line, "payment_reference", None) or (getattr(req, "finance_payment_reference", None) or ""),
            "PaidAt": _iso(getattr(line, "paid_at", None) or getattr(req, "paid_at", None)),
            "ReceiptLink": _collect_receipt_link_for_line(req, line),
            "EMSLink": f'=HYPERLINK("{_detail_url(req.id)}","Open in EMS")',
            "CreatedAt": _iso(getattr(req, "created_at", None)),
            "UpdatedAt": _iso(getattr(req, "updated_at", None)),
            "SyncedAt": _iso(datetime.now(timezone.utc)),
            "SyncVersion": SYNC_VERSION,
            "Extra": json.dumps({
                "request_status_label": getattr(req, "get_status_display", lambda: req.status)(),
                "category_label": getattr(expense, "get_category_display", lambda: expense.category if expense else "")() if expense else "",
                "gst_label": getattr(expense, "get_gst_type_display", lambda: expense.gst_type if expense else "")() if expense else "",
            }, ensure_ascii=False),
        }
        rows.append([row[h] for h in HEADER])
    return rows

# Backward-compatible alias (older code/tests may import build_row and expect a list)
def build_row(req) -> List[List[Any]]:
    return build_rows(req)

# ---------------------------------------------------------------------------
# Upsert and changelog (bill-wise; key=A: RowKey)
# ---------------------------------------------------------------------------

def _index_by_rowkey() -> Dict[str,int]:
    """Map RowKey -> row number (reads A2:A once)."""
    def _call():
        return _svc_sheets().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!A2:A"
        ).execute()
    resp = _with_backoff("values.get A2:A", True, _call)
    idx: Dict[str,int] = {}
    for i,v in enumerate(resp.get("values", []), start=2):
        if v:
            idx[str(v[0])] = i
    return idx

def _upsert_rows_batch(rows: List[List[Any]]) -> Dict[str, int]:
    """
    ⚡ Performance-critical path:
    - Read the index ONCE
    - Batch update existing rows via values.batchUpdate
    - Append all new rows in ONE append call
    Returns: rowkey -> rownum for written rows (best-effort for appended).
    """
    if not rows:
        return {}

    values_api = _svc_sheets().spreadsheets().values()
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
        # Try to infer first row number from API response (best-effort)
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
    """
    Actual sync body. Avoid calling this from request cycle.
    """
    if not _google_available():
        return
    # Import lazily to avoid circulars in module import time
    from apps.reimbursement.models import ReimbursementRequest  # local import

    try:
        req = (
            ReimbursementRequest.objects.select_related("created_by", "manager", "management", "verified_by")
            .prefetch_related("lines__expense_item")
            .get(pk=req_id)
        )
    except Exception:
        return

    ensure_spreadsheet_structure()

    rows = build_rows(req)  # bill-wise rows (INCLUDED only)

    # Batch upsert to keep API calls minimal and fast
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
            # best-effort logging; never raise
            pass

def sync_request(req) -> None:
    """
    Export-only. No status mutations or audit writes.
    **Non-blocking** for the web request:
      - Debounced per request (30s) via cache
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

    # Simple debounce to avoid storm during bulk actions/resubmits
    lock_key = f"reimb.sheets.sync.lock.{req_id}"
    if not cache.add(lock_key, True, timeout=30):  # skip if a sync is in-flight/recent
        return

    def _kick():
        try:
            t = threading.Thread(target=_sync_request_impl, args=(req_id,), daemon=True)
            t.start()
        except Exception:
            # if thread start fails, release the lock quickly so a later call can retry
            cache.delete(lock_key)

    # Ensure we only sync after all DB changes are committed
    try:
        transaction.on_commit(_kick)
    except Exception:
        # If not in a transaction, run immediately in a thread
        _kick()
