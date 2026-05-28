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
    "bulk_resync_all_requests",  # quota-safe full export
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

# Schema version:
# v9 = legacy RowKey self-heal:
#      - current correct rows have RowKey in hidden column A
#      - old broken rows may have RowKey in visible column B
#      - index now reads A:B and updates same row A:Q
SYNC_VERSION = 9

# Structure checks throttle
STRUCTURE_TTL_SECONDS = int(
    getattr(settings, "REIMBURSEMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600)
)

# Quota budgets (per process), defaults keep headroom under Google's 60 writes/min/user limit
READS_PER_MINUTE_BUDGET  = int(getattr(settings, "REIMBURSEMENT_SHEETS_READS_PER_MINUTE", 48))
WRITES_PER_MINUTE_BUDGET = int(getattr(settings, "REIMBURSEMENT_SHEETS_WRITES_PER_MINUTE", 30))

# ---------------------------------------------------------------------------
# ONE ROW PER BILL — business-visible columns.
#
# IMPORTANT:
# - Column A is RowKey and must remain hidden.
# - Visible business columns start from B.
# - This file must always write A:Q, never B:Q, otherwise values shift.
# ---------------------------------------------------------------------------

HEADER = [
    "RowKey",                   # A hidden internal upsert key
    "Req ID",                   # B visible
    "Employee Name",            # C visible
    "date of Bill",             # D visible
    "Category",                 # E visible
    "Description of Bill",      # F visible
    "Amount",                   # G visible
    "Receipt",                  # H visible
    "Gst Type",                 # I visible
    "Bill Status",              # J visible
    "Finance Verifier",         # K visible
    "Manager Verifier",         # L visible
    "Bill Submission Time",     # M visible
    "Finance Verified Time",    # N visible
    "Manager Approved Time",    # O visible
    "Bill Paid At (time)",      # P visible
    "Payment reff",             # Q visible
]

CHANGELOG_HEADER = [
    "TimestampUTC",
    "Event",
    "RowKey",
    "OldStatus",
    "NewStatus",
    "RowNum",
    "Actor",
    "Result",
]

SCHEMA_HEADER = [
    "Version",
    "HeaderJSON",
    "Active",
    "RecordedAtUTC",
    "Note",
]

# ---------------------------------------------------------------------------
# Google client helpers
# ---------------------------------------------------------------------------

_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS  = False


def _excel_col(n: int) -> str:
    out = []
    while n:
        n, r = divmod(n - 1, 26)
        out.append(chr(65 + r))
    return "".join(reversed(out))


def _header_end_col() -> str:
    return _excel_col(len(HEADER))


def _iso(dt):
    """
    Return UTC ISO8601 with seconds for either a datetime or a date.
    - If date, coerce to midnight UTC.
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
    """
    Public base URL for receipt links.

    Render should ideally define SITE_URL or PUBLIC_BASE_URL.
    Fallback keeps local/dev safe but production should not rely on 127.0.0.1.
    """
    return (
        os.environ.get("PUBLIC_BASE_URL")
        or os.environ.get("SITE_URL")
        or os.environ.get("RENDER_EXTERNAL_URL")
        or getattr(settings, "PUBLIC_BASE_URL", "")
        or getattr(settings, "SITE_URL", "")
        or "http://127.0.0.1:8000"
    ).rstrip("/")


def _absolute_url(url: str) -> str:
    """
    Ensure Sheet hyperlinks are absolute.

    Some storage backends return relative /media/... URLs.
    Google Sheets should receive full URLs where possible.
    """
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{_site_url()}{url}"
    return f"{_site_url()}/{url}"


def _detail_url(req_id: int) -> str:
    base = _site_url()
    try:
        return f"{base}{reverse('admin:reimbursement_reimbursementrequest_change', args=[req_id])}"
    except NoReverseMatch:
        pass

    tmpl = (
        getattr(settings, "REIMBURSEMENT_DETAIL_URL_TEMPLATE", None)
        or os.environ.get("REIMBURSEMENT_DETAIL_URL_TEMPLATE")
    )
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
                "Google sync disabled: install deps -> "
                "pip install google-api-python-client google-auth google-auth-httplib2"
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
                "Google sync disabled: credentials missing. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE."
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
        return service_account.Credentials.from_service_account_info(
            json.loads(raw),
            scopes=scopes,
        )

    file_path = (
        os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
        or getattr(settings, "GOOGLE_SERVICE_ACCOUNT_FILE", None)
    )
    if file_path:
        return service_account.Credentials.from_service_account_file(
            file_path,
            scopes=scopes,
        )

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
            logger.info(
                "Retrying %s after %s (attempt %s/%s)",
                label,
                e,
                i,
                len(delays),
            )
            time.sleep(sleep_for)

    raise last_exc


# ---------------------------------------------------------------------------
# Spreadsheet structure + formatting
# ---------------------------------------------------------------------------

_meta_lock = threading.Lock()
_meta_cache: Dict[str, Any] = {}


def _spreadsheets_get() -> dict:
    def _call():
        return _svc_sheets().spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID
        ).execute()

    cache_key = f"sheets.meta.{SPREADSHEET_ID}"

    with _meta_lock:
        meta = _meta_cache.get(cache_key)
        if meta:
            return meta

        meta = _with_backoff("spreadsheets.get", "read", _call)
        _meta_cache[cache_key] = meta
        return meta


def _get_sheet_map_from_meta(meta: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        out[props.get("title")] = props.get("sheetId")
    return out


def _batch_update(requests: list) -> None:
    if not requests:
        return

    def _call():
        return _svc_sheets().spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests},
        ).execute()

    _with_backoff("spreadsheets.batchUpdate", "write", _call)


def _values_batch_get(ranges: List[str]) -> List[List[List[str]]]:
    def _call():
        return _svc_sheets().spreadsheets().values().batchGet(
            spreadsheetId=SPREADSHEET_ID,
            ranges=ranges,
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


def _values_append(
    range_: str,
    values: List[List[Any]],
    user_entered: bool = False,
) -> dict:
    def _call():
        return _svc_sheets().spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="USER_ENTERED" if user_entered else "RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    return _with_backoff("values.append", "write", _call)


def _values_batch_update(
    data_blocks: List[Dict[str, Any]],
    input_option: str = "USER_ENTERED",
) -> None:
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
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": 1},
            },
            "fields": "gridProperties.frozenRowCount",
        }
    })

    # Hide column A — RowKey
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex": 1,
            },
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    })

    # Basic filter on all columns A:Q
    requests.append({
        "setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1_000_000,
                    "startColumnIndex": 0,
                    "endColumnIndex": end_col,
                }
            }
        }
    })

    # Column widths
    widths = {
        2: 90,    # Req ID
        3: 200,   # Employee Name
        4: 150,   # date of Bill
        5: 140,   # Category
        6: 280,   # Description
        7: 110,   # Amount
        8: 120,   # Receipt
        9: 120,   # GST
        10: 150,  # Bill Status
        11: 170,  # Finance Verifier
        12: 170,  # Manager Verifier
        13: 180,  # Submission
        14: 180,  # Finance Verified
        15: 180,  # Manager Approved
        16: 180,  # Paid At
        17: 170,  # Payment ref
    }

    for idx, px in widths.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": idx - 1,
                    "endIndex": idx,
                },
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    # Wrap description
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "startColumnIndex": 6 - 1,
                "endColumnIndex": 6,
            },
            "cell": {
                "userEnteredFormat": {
                    "wrapStrategy": "WRAP",
                }
            },
            "fields": "userEnteredFormat.wrapStrategy",
        }
    })

    # Amount as number
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "startColumnIndex": 7 - 1,
                "endColumnIndex": 7,
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "#,##0.00",
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat",
        }
    })

    # Date/time formatting for date/time columns
    for col in [4, 13, 14, 15, 16]:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": col - 1,
                    "endColumnIndex": col,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE_TIME",
                            "pattern": "yyyy-mm-dd hh:mm:ss",
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        })

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

        requests = []
        for title in [TAB_MAIN, TAB_CHANGELOG, TAB_SCHEMA]:
            if title not in existing:
                requests.append({
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "gridProperties": {
                                "rowCount": 2000,
                                "columnCount": 50,
                            },
                        }
                    }
                })

        if requests:
            _batch_update(requests)
            _meta_cache.clear()
            meta = _spreadsheets_get()
            existing = _get_sheet_map_from_meta(meta)

        # Hide non-main tabs
        requests = []
        for title in [TAB_CHANGELOG, TAB_SCHEMA]:
            sid = existing.get(title)
            if sid is not None:
                requests.append({
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sid,
                            "hidden": True,
                        },
                        "fields": "hidden",
                    }
                })

        _batch_update(requests)

        main_id = existing.get(TAB_MAIN)
        if main_id is not None:
            _friendly_format_main(main_id)

        ranges = [
            f"{TAB_MAIN}!1:1",
            f"{TAB_CHANGELOG}!1:1",
            f"{TAB_SCHEMA}!1:1",
        ]
        values_list = _values_batch_get(ranges)

        main_header = values_list[0][0] if values_list and values_list[0] else []
        changelog_header = values_list[1][0] if len(values_list) > 1 and values_list[1] else []
        schema_header = values_list[2][0] if len(values_list) > 2 and values_list[2] else []

        if main_header != HEADER:
            _values_update(f"{TAB_MAIN}!1:1", [HEADER])

        if changelog_header != CHANGELOG_HEADER:
            _values_update(f"{TAB_CHANGELOG}!1:1", [CHANGELOG_HEADER])

        if schema_header != SCHEMA_HEADER:
            _values_update(f"{TAB_SCHEMA}!1:1", [SCHEMA_HEADER])

        hb = [
            SYNC_VERSION,
            json.dumps(HEADER, ensure_ascii=False),
            True,
            _iso(datetime.now(timezone.utc)),
            "bootstrap/update",
        ]

        try:
            _values_append(f"{TAB_SCHEMA}!A:E", [hb], user_entered=False)
        except Exception:
            pass

        _last_ensured_ts = time.monotonic()
        cache.set(ck, True, timeout=STRUCTURE_TTL_SECONDS)


# ---------------------------------------------------------------------------
# Drive helpers — upload receipts, return shareable link
# ---------------------------------------------------------------------------

def _drive_folder_id() -> Optional[str]:
    return (
        os.environ.get("REIMBURSEMENT_DRIVE_FOLDER_ID")
        or getattr(settings, "REIMBURSEMENT_DRIVE_FOLDER_ID", None)
    )


def _drive_share_anyone() -> bool:
    return (
        os.environ.get("REIMBURSEMENT_DRIVE_LINK_SHARING")
        or getattr(settings, "REIMBURSEMENT_DRIVE_LINK_SHARING", "anyone")
    ).lower() == "anyone"


def _drive_domain() -> Optional[str]:
    return (
        os.environ.get("REIMBURSEMENT_DRIVE_DOMAIN")
        or getattr(settings, "REIMBURSEMENT_DRIVE_DOMAIN", None)
    )


def _drive_find_file_by_name(name: str, parent: str) -> Optional[str]:
    def _call():
        svc = _svc_drive()
        safe_name = name.replace("'", "\\'")
        q = f"name = '{safe_name}' and '{parent}' in parents and trashed = false"
        return svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id,name)",
            pageSize=1,
        ).execute()

    try:
        resp = _with_backoff("drive.files.list", "read", _call)
        items = resp.get("files", [])
        return items[0]["id"] if items else None
    except Exception:
        return None


def _drive_ensure_permission(file_id: str) -> None:
    def _call(body: dict):
        svc = _svc_drive()
        return svc.permissions().create(
            fileId=file_id,
            body=body,
            fields="id",
        ).execute()

    try:
        if _drive_share_anyone():
            body = {"type": "anyone", "role": "reader"}
        else:
            domain = _drive_domain()
            if not domain:
                body = {"type": "anyone", "role": "reader"}
            else:
                body = {
                    "type": "domain",
                    "role": "reader",
                    "domain": domain,
                    "allowFileDiscovery": False,
                }

        _with_backoff("drive.permissions.create", "write", lambda: _call(body))
    except Exception as e:
        logger.info("Drive permission set failed for %s: %s", file_id, e)


def _drive_upload_bytes(
    name: str,
    data: bytes,
    parent: str,
    mime: Optional[str],
) -> Optional[str]:
    from googleapiclient.http import MediaIoBaseUpload

    def _call():
        svc = _svc_drive()
        media = MediaIoBaseUpload(
            io.BytesIO(data),
            mime or mimetypes.guess_type(name)[0] or "application/octet-stream",
            resumable=False,
        )
        body = {"name": name, "parents": [parent]}
        return svc.files().create(
            body=body,
            media_body=media,
            fields="id",
        ).execute()

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
# Helpers for building rows — bill-wise
# ---------------------------------------------------------------------------

def _collect_receipt_link_for_line(req, line) -> str:
    """
    Return receipt link for the bill line.

    Priority:
    1. Google Drive shared link, if Drive folder is configured.
    2. Storage file URL.
    3. EMS secured receipt route.
    """
    # Try Google Drive
    try:
        if _google_available() and _drive_folder_id():
            f = (
                getattr(line, "receipt_file", None)
                or (
                    getattr(line, "expense_item", None)
                    and getattr(line.expense_item, "receipt_file", None)
                )
            )

            if f:
                filename = _receipt_drive_filename(
                    req.id,
                    line.id,
                    getattr(f, "name", "receipt"),
                )

                existing_id = _drive_find_file_by_name(
                    filename,
                    _drive_folder_id(),
                )
                if existing_id:
                    return _drive_link(existing_id)

                with f.open("rb") as fh:
                    data = fh.read()

                fid = _drive_upload_bytes(
                    filename,
                    data,
                    _drive_folder_id(),
                    mimetypes.guess_type(getattr(f, "name", ""))[0],
                )

                if fid:
                    return _drive_link(fid)

    except Exception as e:
        logger.info(
            "Drive link for req=%s line=%s skipped: %s",
            getattr(req, "id", None),
            getattr(line, "id", None),
            e,
        )

    # Storage URL
    f = (
        getattr(line, "receipt_file", None)
        or (
            getattr(line, "expense_item", None)
            and getattr(line.expense_item, "receipt_file", None)
        )
    )

    if f and getattr(f, "url", None):
        try:
            return _absolute_url(f.url)
        except Exception:
            pass

    # EMS secured route
    try:
        return _site_url() + reverse("reimbursement:receipt_line", args=[line.id])
    except Exception:
        return ""


def _employee_name(user) -> str:
    try:
        if not user:
            return ""

        full = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        return full or (user.username or f"User #{getattr(user, 'id', '')}")
    except Exception:
        return f"User #{getattr(user, 'id', '')}"


def _row_key(req_id: int, line_id: int) -> str:
    return f"{req_id}-{line_id}"


# ---------------------------------------------------------------------------
# Row building — BILL-WISE exact column order A:Q
# ---------------------------------------------------------------------------

def build_rows(req) -> List[List[Any]]:
    """
    Build one row per INCLUDED bill.

    Physical columns:
    A = RowKey hidden
    B = Req ID
    C = Employee Name
    D = date of Bill
    E = Category
    F = Description of Bill
    G = Amount
    H = Receipt
    I = Gst Type
    J = Bill Status
    K = Finance Verifier
    L = Manager Verifier
    M = Bill Submission Time
    N = Finance Verified Time
    O = Manager Approved Time
    P = Bill Paid At (time)
    Q = Payment reff

    This function must always return exactly len(HEADER) columns.
    """
    rows: List[List[Any]] = []
    employee = getattr(req, "created_by", None)

    # INCLUDED lines only
    lines_qs = req.lines.select_related("expense_item").filter(
        status=ReimbursementLine.Status.INCLUDED
    )

    for line in lines_qs:
        item = getattr(line, "expense_item", None)

        amount = float(
            getattr(line, "amount", None)
            or getattr(item, "amount", 0)
            or 0
        )

        # Category label
        try:
            category = item.get_category_display() if item else ""
        except Exception:
            category = getattr(item, "category", "") or ""

        # GST label
        try:
            gst = item.get_gst_type_display() if item else ""
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

        finance_verifier = (
            _employee_name(getattr(req, "verified_by", None))
            if getattr(req, "verified_by_id", None)
            else ""
        )

        manager_verifier = (
            _employee_name(getattr(req, "manager", None))
            if getattr(req, "manager_id", None)
            else ""
        )

        # Timestamps
        date_of_bill = getattr(item, "date", None)
        submitted_at = getattr(req, "submitted_at", None)
        verified_at = getattr(req, "verified_at", None)

        manager_approved_at = (
            getattr(req, "manager_decided_at", None)
            if str(getattr(req, "manager_decision", "")).lower() == "approved"
            else None
        )

        paid_at = getattr(line, "paid_at", None) or getattr(req, "paid_at", None)

        payment_ref = (
            (getattr(line, "payment_reference", None) or "")
            or (getattr(req, "finance_payment_reference", None) or "")
        )

        description = (
            (getattr(line, "description", "") or "").strip()
            or (getattr(item, "description", "") or "").strip()
        )

        # RowKey keeps upsert idempotent
        rowkey = _row_key(req.id, line.id)

        row = [
            rowkey,                    # A RowKey hidden
            req.id,                    # B Req ID
            _employee_name(employee),  # C Employee Name
            _iso(date_of_bill),        # D date of Bill
            category,                  # E Category
            description,               # F Description of Bill
            amount,                    # G Amount
            receipt_cell,              # H Receipt
            gst,                       # I Gst Type
            bill_status,               # J Bill Status
            finance_verifier,          # K Finance Verifier
            manager_verifier,          # L Manager Verifier
            _iso(submitted_at),        # M Bill Submission Time
            _iso(verified_at),         # N Finance Verified Time
            _iso(manager_approved_at), # O Manager Approved Time
            _iso(paid_at),             # P Bill Paid At (time)
            payment_ref,               # Q Payment reff
        ]

        if len(row) != len(HEADER):
            raise ValueError(
                f"Invalid reimbursement sheet row length for req={req.id}, "
                f"line={line.id}: {len(row)} != {len(HEADER)}"
            )

        rows.append(row)

    return rows


# Backward-compatible alias
def build_row(req) -> List[List[Any]]:
    return build_rows(req)


# ---------------------------------------------------------------------------
# Upsert helpers — bill-wise; key = RowKey
# ---------------------------------------------------------------------------

def _looks_like_rowkey(value: Any) -> bool:
    """
    RowKey format is '<request_id>-<line_id>', for example '97-380'.

    This is used to detect:
    - correct current RowKey in hidden column A
    - old broken RowKey accidentally written in visible column B

    Plain request IDs like '97' must not match.
    """
    if value is None:
        return False

    text = str(value).strip()
    if not text or "-" not in text:
        return False

    left, right = text.split("-", 1)
    return left.isdigit() and right.isdigit()


def _index_by_rowkey() -> Dict[str, int]:
    """
    Map RowKey -> row number.

    Production-safe behavior:
    - Correct/current layout: RowKey is in hidden column A.
    - Legacy broken layout: RowKey is in visible column B.
    - We read A2:B once and index both positions.
    - If a legacy row is found in B, the next update writes full A:Q on the
      same row, self-healing the shifted alignment without manual Sheet edits.

    Important:
    - This function does not delete rows.
    - This function does not clear data.
    - It only helps upsert find the correct existing row.
    """
    def _call():
        return _svc_sheets().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A2:B",
        ).execute()

    resp = _with_backoff("values.get A2:B", "read", _call)

    idx: Dict[str, int] = {}

    for row_number, values in enumerate(resp.get("values", []), start=2):
        col_a = (
            str(values[0]).strip()
            if len(values) >= 1 and values[0] is not None
            else ""
        )
        col_b = (
            str(values[1]).strip()
            if len(values) >= 2 and values[1] is not None
            else ""
        )

        # Correct current layout
        if _looks_like_rowkey(col_a):
            idx[col_a] = row_number
            continue

        # Legacy shifted layout
        if _looks_like_rowkey(col_b):
            idx[col_b] = row_number

    return idx


def _upsert_rows_batch(rows: List[List[Any]]) -> Dict[str, int]:
    """
    Performance path per request:
    - Read RowKey index once.
    - Batch update existing rows.
    - Append new rows in one append call.

    Because _index_by_rowkey() reads A:B, old shifted rows are now repaired
    in-place instead of duplicated.
    """
    if not rows:
        return {}

    end_col = _header_end_col()
    idx = _index_by_rowkey()

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
        resp = _values_append(
            f"{TAB_MAIN}!A:{end_col}",
            to_append,
            user_entered=True,
        )

        try:
            rng = resp.get("updates", {}).get("updatedRange", "")
            first_rn = int(rng.split("!")[1].split(":")[0][1:])
        except Exception:
            first_rn = 0

        if first_rn:
            for i, row in enumerate(to_append):
                written_map[str(row[0])] = first_rn + i

    return written_map


def append_changelog(
    event: str,
    rowkey: str,
    old: str,
    new: str,
    rownum: int,
    actor: str = "",
    result: str = "ok",
    err: str = "",
) -> None:
    try:
        _values_append(
            f"{TAB_CHANGELOG}!A:H",
            [[
                _iso(datetime.now(timezone.utc)),
                event,
                rowkey,
                old or "",
                new or "",
                rownum,
                actor or "",
                f"{result}: {err}" if err else result,
            ]],
            user_entered=False,
        )
    except Exception as e:
        logger.info("Changelog append skipped: %s", e)


# ---------------------------------------------------------------------------
# Public entry — sync a single request
# ---------------------------------------------------------------------------

def _sync_request_impl(req_id: int) -> None:
    if not _google_available():
        return

    from apps.reimbursement.models import ReimbursementRequest  # lazy import

    try:
        req = (
            ReimbursementRequest.objects
            .select_related(
                "created_by",
                "manager",
                "management",
                "verified_by",
            )
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
        logger.exception(
            "Google Sheets batch upsert failed for req=%s: %s",
            req_id,
            e,
        )
        written = {}

    prev_status = getattr(req, "status", "") or ""

    for row in rows:
        rk = str(row[0])
        rn = int(written.get(rk, 0))
        try:
            append_changelog(
                "upsert",
                rk,
                prev_status,
                req.status,
                rn,
                "",
                "ok",
            )
        except Exception:
            pass


def sync_request(req) -> None:
    """
    Export-only. No status mutations or audit writes.

    Non-blocking for the web request:
    - Debounced per request via cache.
    - Enqueued after DB commit using transaction.on_commit.
    - Runs in a daemon thread.
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
            t = threading.Thread(
                target=_sync_request_impl,
                args=(req_id,),
                daemon=True,
            )
            t.start()
        except Exception:
            cache.delete(lock_key)

    try:
        transaction.on_commit(_kick)
    except Exception:
        _kick()


# ---------------------------------------------------------------------------
# Maintenance helper — clear all data rows under header
# ---------------------------------------------------------------------------

def reset_main_data() -> None:
    """
    Clears all rows below header in TAB_MAIN.

    Use only for controlled full rebuilds.
    This keeps header intact.
    """
    if not _google_available():
        return

    end_col = _header_end_col()

    def _call():
        return _svc_sheets().spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A2:{end_col}",
        ).execute()

    try:
        _with_backoff("values.clear", "write", _call)
        logger.info("Cleared main sheet data under header.")
    except Exception as e:
        logger.exception("Failed to clear main sheet: %s", e)


# ---------------------------------------------------------------------------
# Bulk re-sync — quota-safe
# ---------------------------------------------------------------------------

def _collect_all_rows() -> Tuple[List[List[Any]], Dict[str, List[Any]]]:
    """
    Build rows for all reimbursement requests.

    Returns:
    - all_rows: list of rows
    - rows_by_rowkey: dict RowKey -> row
    """
    from apps.reimbursement.models import ReimbursementRequest

    all_rows: List[List[Any]] = []
    rows_by_rowkey: Dict[str, List[Any]] = {}

    qs = (
        ReimbursementRequest.objects
        .select_related(
            "created_by",
            "manager",
            "management",
            "verified_by",
        )
        .prefetch_related("lines__expense_item")
        .order_by("id")
    )

    for req in qs.iterator(chunk_size=200):
        rows = build_rows(req)
        all_rows.extend(rows)

        for r in rows:
            rows_by_rowkey[str(r[0])] = r

    return all_rows, rows_by_rowkey


def _chunked(seq: List[Any], size: int) -> List[List[Any]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def bulk_resync_all_requests(
    update_chunk_size: int = 500,
    append_chunk_size: int = 500,
    sleep_between_append_chunks_sec: int = 65,
    disable_changelog: bool = True,
) -> None:
    """
    Quota-safe full export.

    Steps:
    1. Ensure structure.
    2. Read main sheet RowKey index once.
       This now reads A:B, so old shifted rows can self-heal.
    3. Compute updates vs appends for all rows.
    4. Batch update existing rows.
    5. Append only truly missing rows.
    6. Optionally skip ChangeLog writes during bulk to preserve quota.
    """
    if not _google_available():
        return

    ensure_spreadsheet_structure()

    all_rows, rows_by_rowkey = _collect_all_rows()

    if not all_rows:
        logger.info("bulk_resync_all_requests: no rows to write.")
        return

    idx = _index_by_rowkey()
    end_col = _header_end_col()

    updates_blocks: List[Dict[str, Any]] = []
    appends_rows: List[List[Any]] = []

    for rowkey, row in rows_by_rowkey.items():
        rn = idx.get(rowkey)

        if rn:
            updates_blocks.append({
                "range": f"{TAB_MAIN}!A{rn}:{end_col}{rn}",
                "values": [row],
            })
        else:
            appends_rows.append(row)

    # Updates in chunks
    if updates_blocks:
        for chunk in _chunked(updates_blocks, update_chunk_size):
            _values_batch_update(chunk, input_option="USER_ENTERED")

        logger.info(
            "bulk_resync_all_requests: updated %s existing rows.",
            len(updates_blocks),
        )

    # Appends in chunks
    if appends_rows:
        first_chunk = True

        for chunk in _chunked(appends_rows, append_chunk_size):
            if not first_chunk:
                time.sleep(max(1, sleep_between_append_chunks_sec))

            _values_append(
                f"{TAB_MAIN}!A:{end_col}",
                chunk,
                user_entered=True,
            )
            first_chunk = False

        logger.info(
            "bulk_resync_all_requests: appended %s new rows.",
            len(appends_rows),
        )

    # Optional changelog
    if not disable_changelog:
        now = _iso(datetime.now(timezone.utc))
        rows = [[now, "bulk_resync", "", "", "", 0, "", "ok"]]

        try:
            _values_append(
                f"{TAB_CHANGELOG}!A:H",
                rows,
                user_entered=False,
            )
        except Exception as e:
            logger.info("bulk_resync changelog skipped: %s", e)