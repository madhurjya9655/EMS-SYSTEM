# apps/reimbursement/integrations/sheets.py
from __future__ import annotations

import io
import json
import logging
import mimetypes
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List, Optional, Callable, Any

from django.conf import settings
from django.core.cache import cache
from django.urls import NoReverseMatch, reverse

__all__ = [
    "SPREADSHEET_ID",
    "TAB_MAIN",
    "TAB_CHANGELOG",
    "TAB_SCHEMA",
    "ensure_spreadsheet_structure",
    "sync_request",
    "build_row",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPREADSHEET_ID = getattr(settings, "REIMBURSEMENT_SHEET_ID", "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A")

TAB_MAIN      = "Reimbursements"
TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA    = "Schema"
TAB_META      = "_Meta"

SYNC_VERSION = 6  # bump when schema/behavior changes

# structure checks will run at most once per STRUCTURE_TTL per process
STRUCTURE_TTL_SECONDS = int(getattr(settings, "REIMBURSEMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600))  # 10 min default

# token-bucket limiter per-process to stay under 60 read/min *per user* with headroom
READS_PER_MINUTE_BUDGET = int(getattr(settings, "REIMBURSEMENT_SHEETS_READS_PER_MINUTE", 48))

HEADER = [
    "ReimbID","EmployeeID","Employee","Department","Categories","Items","Amount","Currency",
    "Submitted","Status","StatusUpdated","Manager","ManagerDecided","Management","ManagementDecided",
    "FinanceVerifier","FinanceVerified","PaymentRef","PaidAt","RejectionReason","FinanceNote",
    "ReceiptLinks","EMSLink","CreatedAt","UpdatedAt","SyncedAt","SyncVersion","Extra",
]

CHANGELOG_HEADER = ["TimestampUTC","Event","ReimbID","OldStatus","NewStatus","RowNum","Actor","Result"]
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
    if not dt: return ""
    if getattr(dt,"tzinfo",None) is None:
        return datetime.fromtimestamp(dt.timestamp(), tz=timezone.utc).isoformat(timespec="seconds")
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

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
        # ✅ correct usage: spreadsheets().get(...)
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
        # ✅ correct usage: spreadsheets().batchUpdate(...)
        return _svc_sheets().spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
        ).execute()
    _with_backoff("spreadsheets.batchUpdate", False, _call)

def _values_batch_get(ranges: List[str]) -> List[List[List[str]]]:
    # ✅ use spreadsheets().values().batchGet(...)
    def _call():
        return _svc_sheets().spreadsheets().values().batchGet(
            spreadsheetId=SPREADSHEET_ID, ranges=ranges
        ).execute()
    resp = _with_backoff("values.batchGet", True, _call)
    return [x.get("values", [[]]) for x in resp.get("valueRanges", [])]

def _values_update(range_: str, values: List[List[Any]]) -> None:
    # ✅ use spreadsheets().values().update(...)
    def _call():
        return _svc_sheets().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    _with_backoff("values.update", False, _call)

def _values_append(range_: str, values: List[List[Any]], user_entered: bool = False) -> dict:
    # ✅ use spreadsheets().values().append(...)
    def _call():
        return _svc_sheets().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="USER_ENTERED" if user_entered else "RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    return _with_backoff("values.append", False, _call)

def _friendly_format_main(sheet_id: int) -> None:
    end_col = len(HEADER)
    requests = []
    requests.append({"updateSheetProperties":{"properties":{"sheetId":sheet_id,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}})
    requests.append({"setBasicFilter":{"filter":{"range":{"sheetId":sheet_id,"startRowIndex":0,"endRowIndex":1_000_000,"startColumnIndex":0,"endColumnIndex":end_col}}}})
    widths={1:90,2:90,3:200,4:150,5:160,6:90,7:120,8:70,9:170,10:150,11:170,12:150,13:170,14:150,15:170,16:150,17:170,18:160,19:170,20:220,21:260,22:260,23:160,24:170,25:170,26:170,27:110,28:180}
    for idx,px in widths.items():
        requests.append({"updateDimensionProperties":{"range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":idx-1,"endIndex":idx},"properties":{"pixelSize":px},"fields":"pixelSize"}})
    for col in [20,21,22]:
        requests.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":col-1,"endColumnIndex":col},"cell":{"userEnteredFormat":{"wrapStrategy":"WRAP"}},"fields":"userEnteredFormat.wrapStrategy"}})
    requests.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":6,"endColumnIndex":7},"cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"#,##0.00"}}},"fields":"userEnteredFormat.numberFormat"}})
    for col in [9,11,13,15,17,19,24,25,26]:
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
                requests.append({"addSheet":{"properties":{"title":title,"gridProperties":{"rowCount":2000,"columnCount":40}}}})
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
# Drive helpers (upload receipts, return shareable links)
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

def _collect_receipt_links_from_drive(req) -> List[str]:
    folder = _drive_folder_id()
    if not folder:
        return []
    links: List[str] = []
    for line in req.lines.select_related("expense_item"):
        f = getattr(line, "receipt_file", None) or getattr(line.expense_item, "receipt_file", None)
        if not f:
            continue
        try:
            name = _receipt_drive_filename(req.id, line.id, getattr(f, "name", "receipt"))
            existing_id = _drive_find_file_by_name(name, folder)
            if existing_id:
                links.append(_drive_link(existing_id)); continue
            with f.open("rb") as fh:
                data = fh.read()
            mime = mimetypes.guess_type(getattr(f, "name", ""))[0]
            file_id = _drive_upload_bytes(name, data, folder, mime)
            if file_id:
                links.append(_drive_link(file_id))
        except Exception as e:
            logger.info("Skipping Drive upload for line %s: %s", getattr(line, "id", None), e)
            continue
    out, seen = [], set()
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def _collect_receipt_urls_storage(req) -> List[str]:
    urls=[]
    for line in req.lines.select_related("expense_item"):
        f = getattr(line, "receipt_file", None) or getattr(line.expense_item, "receipt_file", None)
        if f and getattr(f, "url", None):
            urls.append(f.url)
    out,seen=[],set()
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def _categories_and_count(req) -> Tuple[str,int]:
    cats=[line.expense_item.category for line in req.lines.select_related("expense_item")]
    deduped,seen=[],set()
    for c in cats:
        if c not in seen:
            seen.add(c); deduped.append(c)
    return ",".join(deduped), req.lines.count()

def build_row(req) -> list:
    employee = req.created_by
    dept = getattr(employee, "department", "") or (getattr(employee, "profile", None) and getattr(employee.profile, "department","")) or ""
    cats, line_count = _categories_and_count(req)

    drive_links = []
    try:
        if _google_available() and _drive_folder_id():
            drive_links = _collect_receipt_links_from_drive(req)
    except Exception:
        drive_links = []

    if drive_links:
        receipts_csv = ",".join(drive_links)
    else:
        storage_links = _collect_receipt_urls_storage(req)
        receipts_csv = ",".join(storage_links)

    manager_un = getattr(req.manager, "username", "") if req.manager_id else ""
    management_un = getattr(req.management, "username", "") if req.management_id else ""
    finance_un = getattr(req.verified_by, "username", "") if req.verified_by_id else ""
    extra = {}
    try:
        extra["status_label"] = req.get_status_display()
    except Exception:
        pass

    row = {
        "ReimbID": req.id,
        "EmployeeID": getattr(employee,"id",""),
        "Employee": (f"{getattr(employee,'first_name','')} {getattr(employee,'last_name','')}".strip() or getattr(employee,"username","") or f"User #{getattr(employee,'id','')}"),
        "Department": dept,
        "Categories": cats,
        "Items": line_count,
        "Amount": float(req.total_amount or 0),
        "Currency": "INR",
        "Submitted": _iso(req.submitted_at),
        "Status": req.status,
        "StatusUpdated": _iso(req.updated_at),
        "Manager": manager_un,
        "ManagerDecided": _iso(req.manager_decided_at),
        "Management": management_un,
        "ManagementDecided": _iso(req.management_decided_at),
        "FinanceVerifier": finance_un,
        "FinanceVerified": _iso(req.verified_at),
        "PaymentRef": req.finance_payment_reference or "",
        "PaidAt": _iso(req.paid_at),
        "RejectionReason": req.management_comment if req.status == req.Status.REJECTED else "",
        "FinanceNote": req.finance_note or "",
        "ReceiptLinks": receipts_csv,
        "EMSLink": f'=HYPERLINK("{_detail_url(req.id)}","Open in EMS")',
        "CreatedAt": _iso(req.created_at),
        "UpdatedAt": _iso(req.updated_at),
        "SyncedAt": _iso(datetime.now(timezone.utc)),
        "SyncVersion": SYNC_VERSION,
        "Extra": json.dumps(extra, ensure_ascii=False),
    }
    return [row[h] for h in HEADER]

# ---------------------------------------------------------------------------
# Upsert and changelog
# ---------------------------------------------------------------------------

def _index_by_id() -> Dict[str,int]:
    # ✅ use spreadsheets().values().get(...)
    def _call():
        return _svc_sheets().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!A2:A"
        ).execute()
    resp = _with_backoff("values.get A2:A", True, _call)
    idx: Dict[str,int] = {}
    for i,v in enumerate(resp.get("values", []), start=2):
        if v: idx[str(v[0])] = i
    return idx

def upsert_row(row: list, reimb_id: int):
    # ✅ obtain the values resource from spreadsheets().values()
    values = _svc_sheets().spreadsheets().values()
    idx = _index_by_id()
    end_col = _header_end_col()

    if str(reimb_id) in idx:
        rn = idx[str(reimb_id)]
        def _call_update():
            return values.update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{TAB_MAIN}!A{rn}:{end_col}{rn}",
                valueInputOption="USER_ENTERED",
                body={"values":[row]},
            ).execute()
        _with_backoff("values.update row", False, _call_update)
        return "update", rn

    def _call_append():
        return values.append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A:{end_col}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values":[row]},
        ).execute()
    resp = _with_backoff("values.append row", False, _call_append)
    rng=resp.get("updates", {}).get("updatedRange",""); rn=0
    try: rn=int(rng.split("!")[1].split(":")[0][1:])
    except Exception: pass
    return "insert", rn

def append_changelog(event: str, req_id: int, old: str, new: str, rownum: int, actor: str = "", result: str = "ok", err: str = "") -> None:
    try:
        _values_append(
            f"{TAB_CHANGELOG}!A:H",
            [[_iso(datetime.now(timezone.utc)), event, req_id, old or "", new or "", rownum, actor or "", f"{result}: {err}" if err else result]],
            user_entered=False,
        )
    except Exception as e:
        logger.info("Changelog append skipped: %s", e)

# ---------------------------------------------------------------------------
# Public entry: sync a single request
# ---------------------------------------------------------------------------

def sync_request(req) -> None:
    """
    Export-only. No status mutations or audit writes. Safe fallbacks.
    """
    if not _google_available() or req is None:
        return

    ensure_spreadsheet_structure()

    row = build_row(req)

    prev_status = getattr(req, "status", "") or ""
    try:
        action, rn = upsert_row(row, req.id)
        append_changelog("upsert", req.id, prev_status, req.status, rn, "", action)
    except Exception as e:
        logger.exception("Google Sheets sync failed for req %s", req.id)
        try:
            append_changelog("error", req.id, prev_status, req.status, 0, err=str(e))
        except Exception:
            pass
        return
