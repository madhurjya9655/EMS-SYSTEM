#apps/reimbursement/integrations/sheets.py
from __future__ import annotations

import io
import json
import logging
import mimetypes
import os
import random
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.urls import NoReverseMatch, reverse

from apps.reimbursement.models import ReimbursementLine

logger = logging.getLogger(__name__)

__all__ = [
    "SPREADSHEET_ID",
    "TAB_MAIN",
    "TAB_CHANGELOG",
    "TAB_SCHEMA",
    "ensure_spreadsheet_structure",
    "sync_request",
    "build_row",
    "build_rows",
    "reset_main_data",
    "bulk_resync_all_requests",
    "rebuild_main_data_from_db",
    "repair_main_sheet_from_db",
    "force_rewrite_main_data_from_db",
]

SPREADSHEET_ID = (
    os.environ.get("REIMBURSEMENT_SHEET_ID")
    or getattr(settings, "REIMBURSEMENT_SHEET_ID", None)
    or getattr(settings, "reimbursement_sheet_id", None)
    or "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A"
)

TAB_MAIN = "Reimbursements"
TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA = "Schema"

SYNC_VERSION = 11

STRUCTURE_TTL_SECONDS = int(
    getattr(settings, "REIMBURSEMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600)
)

READS_PER_MINUTE_BUDGET = int(
    getattr(settings, "REIMBURSEMENT_SHEETS_READS_PER_MINUTE", 48)
)

WRITES_PER_MINUTE_BUDGET = int(
    getattr(settings, "REIMBURSEMENT_SHEETS_WRITES_PER_MINUTE", 30)
)

HEADER = [
    "RowKey",
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

_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS = False

_rw_lock = threading.Lock()
_read_next_refill = time.monotonic()
_write_next_refill = time.monotonic()
_read_tokens = READS_PER_MINUTE_BUDGET
_write_tokens = WRITES_PER_MINUTE_BUDGET

_meta_lock = threading.Lock()
_meta_cache: Dict[str, Any] = {}

_structure_lock = threading.Lock()
_last_ensured_ts: Optional[float] = None


def _excel_col(n: int) -> str:
    out = []
    while n:
        n, r = divmod(n - 1, 26)
        out.append(chr(65 + r))
    return "".join(reversed(out))


def _header_end_col() -> str:
    return _excel_col(len(HEADER))


def _iso(value) -> str:
    if not value:
        return ""

    try:
        if isinstance(value, datetime):
            d = value
        elif isinstance(value, date):
            d = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        else:
            return str(value)

        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        else:
            d = d.astimezone(timezone.utc)

        return d.isoformat(timespec="seconds")
    except Exception:
        return ""


def _sheet_date(value) -> str:
    if not value:
        return ""

    try:
        if isinstance(value, datetime):
            return value.date().isoformat()

        if isinstance(value, date):
            return value.isoformat()

        text = str(value).strip()

        if not text:
            return ""

        if "T" in text:
            return text.split("T", 1)[0]

        if " " in text and len(text) >= 10:
            return text[:10]

        return text
    except Exception:
        return ""


def _sheet_datetime(value) -> str:
    if not value:
        return ""

    try:
        if isinstance(value, datetime):
            d = value
            if d.tzinfo is not None:
                d = d.astimezone(timezone.utc)
            return d.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(value, date):
            return value.isoformat()

        text = str(value).strip()

        if not text:
            return ""

        text = text.replace("T", " ")

        if "+" in text:
            text = text.split("+", 1)[0]

        if "." in text:
            text = text.split(".", 1)[0]

        return text[:19]
    except Exception:
        return ""


def _site_url() -> str:
    return (
        os.environ.get("PUBLIC_BASE_URL")
        or os.environ.get("SITE_URL")
        or os.environ.get("RENDER_EXTERNAL_URL")
        or getattr(settings, "PUBLIC_BASE_URL", "")
        or getattr(settings, "SITE_URL", "")
        or "http://127.0.0.1:8000"
    ).rstrip("/")


def _absolute_url(url: str) -> str:
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


def _looks_like_rowkey(value: Any) -> bool:
    text = str(value or "").strip()

    if "-" not in text:
        return False

    parts = text.split("-")

    if len(parts) != 2:
        return False

    return parts[0].isdigit() and parts[1].isdigit()


def _row_key(req_id: int, line_id: int) -> str:
    return f"{req_id}-{line_id}"


def _employee_name(user) -> str:
    try:
        if not user:
            return ""

        full = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        return full or (user.username or f"User #{getattr(user, 'id', '')}")
    except Exception:
        return f"User #{getattr(user, 'id', '')}"


def _escape_sheet_formula_string(value: str) -> str:
    return str(value or "").replace('"', '""')


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


def _consume_tokens(kind: str, n: int = 1) -> None:
    global _read_tokens, _write_tokens, _read_next_refill, _write_next_refill

    with _rw_lock:
        now = time.monotonic()

        if now >= _read_next_refill:
            _read_tokens = READS_PER_MINUTE_BUDGET
            _read_next_refill = now + 60.0

        if now >= _write_next_refill:
            _write_tokens = WRITES_PER_MINUTE_BUDGET
            _write_next_refill = now + 60.0

        if kind == "read":
            if _read_tokens < n:
                time.sleep(max(0.05, _read_next_refill - now + 0.01))
                _read_tokens = READS_PER_MINUTE_BUDGET
                _read_next_refill = time.monotonic() + 60.0
            _read_tokens = max(0, _read_tokens - n)
            return

        if _write_tokens < n:
            time.sleep(max(0.05, _write_next_refill - now + 0.01))
            _write_tokens = WRITES_PER_MINUTE_BUDGET
            _write_next_refill = time.monotonic() + 60.0

        _write_tokens = max(0, _write_tokens - n)


def _with_backoff(label: str, kind: str, fn: Callable[[], Any]) -> Any:
    _consume_tokens(kind)

    delays = [0.2, 0.5, 1.0, 2.0, 4.0]
    last_exc = None

    for i, delay in enumerate(delays, start=1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            code = getattr(getattr(exc, "resp", None), "status", None)

            if code not in (429, 500, 502, 503, 504):
                raise

            if i == len(delays):
                break

            sleep_for = delay + random.uniform(0.0, 0.2)
            logger.info(
                "Retrying %s after %s, attempt %s/%s",
                label,
                exc,
                i,
                len(delays),
            )
            time.sleep(sleep_for)

    raise last_exc


def _spreadsheets_get() -> dict:
    def _call():
        return _svc_sheets().spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID
        ).execute()

    cache_key = f"sheets.meta.{SPREADSHEET_ID}"

    with _meta_lock:
        cached = _meta_cache.get(cache_key)

        if cached:
            return cached

        meta = _with_backoff("spreadsheets.get", "read", _call)
        _meta_cache[cache_key] = meta
        return meta


def _get_sheet_map_from_meta(meta: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}

    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        title = props.get("title")
        sheet_id = props.get("sheetId")

        if title is not None and sheet_id is not None:
            out[title] = sheet_id

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


def _values_get(range_: str) -> List[List[Any]]:
    def _call():
        return _svc_sheets().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
        ).execute()

    resp = _with_backoff(f"values.get {range_}", "read", _call)
    return resp.get("values", [])


def _values_update(
    range_: str,
    values: List[List[Any]],
    input_option: str = "USER_ENTERED",
) -> None:
    def _call():
        return _svc_sheets().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption=input_option,
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
            body={
                "valueInputOption": input_option,
                "data": data_blocks,
            },
        ).execute()

    _with_backoff("values.batchUpdate", "write", _call)


def _structure_cache_key() -> str:
    return f"reimb.sheets.structure.{SPREADSHEET_ID}.v{SYNC_VERSION}"


def _friendly_format_main(sheet_id: int) -> None:
    end_col = len(HEADER)

    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
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
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1000000,
                        "startColumnIndex": 0,
                        "endColumnIndex": end_col,
                    }
                }
            }
        },
    ]

    widths = {
        2: 90,
        3: 200,
        4: 130,
        5: 150,
        6: 320,
        7: 120,
        8: 120,
        9: 130,
        10: 150,
        11: 180,
        12: 180,
        13: 190,
        14: 190,
        15: 190,
        16: 190,
        17: 170,
    }

    for idx, px in widths.items():
        requests.append(
            {
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
            }
        )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 5,
                    "endColumnIndex": 6,
                },
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat.wrapStrategy",
            }
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 6,
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
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 3,
                    "endColumnIndex": 4,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE",
                            "pattern": "yyyy-mm-dd",
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    )

    for col in [13, 14, 15, 16]:
        requests.append(
            {
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
            }
        )

    try:
        _batch_update(requests)
    except Exception as exc:
        logger.info("Non-fatal main sheet formatting skipped: %s", exc)


def ensure_spreadsheet_structure() -> None:
    if not _google_available():
        return

    global _last_ensured_ts

    now = time.monotonic()
    cache_key = _structure_cache_key()

    if cache.get(cache_key):
        return

    with _structure_lock:
        if _last_ensured_ts and (now - _last_ensured_ts) < STRUCTURE_TTL_SECONDS:
            cache.set(cache_key, True, timeout=STRUCTURE_TTL_SECONDS)
            return

        meta = _spreadsheets_get()
        existing = _get_sheet_map_from_meta(meta)

        requests = []

        for title in [TAB_MAIN, TAB_CHANGELOG, TAB_SCHEMA]:
            if title not in existing:
                requests.append(
                    {
                        "addSheet": {
                            "properties": {
                                "title": title,
                                "gridProperties": {
                                    "rowCount": 3000,
                                    "columnCount": 50,
                                },
                            }
                        }
                    }
                )

        if requests:
            _batch_update(requests)
            _meta_cache.clear()
            meta = _spreadsheets_get()
            existing = _get_sheet_map_from_meta(meta)

        hide_requests = []

        for title in [TAB_CHANGELOG, TAB_SCHEMA]:
            sheet_id = existing.get(title)
            if sheet_id is not None:
                hide_requests.append(
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "hidden": True,
                            },
                            "fields": "hidden",
                        }
                    }
                )

        _batch_update(hide_requests)

        main_id = existing.get(TAB_MAIN)

        if main_id is not None:
            _friendly_format_main(main_id)

        values_list = _values_batch_get(
            [
                f"{TAB_MAIN}!1:1",
                f"{TAB_CHANGELOG}!1:1",
                f"{TAB_SCHEMA}!1:1",
            ]
        )

        main_header = values_list[0][0] if values_list and values_list[0] else []
        changelog_header = values_list[1][0] if len(values_list) > 1 and values_list[1] else []
        schema_header = values_list[2][0] if len(values_list) > 2 and values_list[2] else []

        if main_header != HEADER:
            _values_update(f"{TAB_MAIN}!1:1", [HEADER], input_option="RAW")

        if changelog_header != CHANGELOG_HEADER:
            _values_update(f"{TAB_CHANGELOG}!1:1", [CHANGELOG_HEADER], input_option="RAW")

        if schema_header != SCHEMA_HEADER:
            _values_update(f"{TAB_SCHEMA}!1:1", [SCHEMA_HEADER], input_option="RAW")

        try:
            _values_append(
                f"{TAB_SCHEMA}!A:E",
                [
                    [
                        SYNC_VERSION,
                        json.dumps(HEADER, ensure_ascii=False),
                        True,
                        _iso(datetime.now(timezone.utc)),
                        "bootstrap/update",
                    ]
                ],
                user_entered=False,
            )
        except Exception:
            pass

        _last_ensured_ts = time.monotonic()
        cache.set(cache_key, True, timeout=STRUCTURE_TTL_SECONDS)


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
        safe_name = name.replace("'", "\\'")
        q = f"name = '{safe_name}' and '{parent}' in parents and trashed = false"
        return _svc_drive().files().list(
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
        return _svc_drive().permissions().create(
            fileId=file_id,
            body=body,
            fields="id",
        ).execute()

    try:
        if _drive_share_anyone():
            body = {"type": "anyone", "role": "reader"}
        else:
            domain = _drive_domain()
            if domain:
                body = {
                    "type": "domain",
                    "role": "reader",
                    "domain": domain,
                    "allowFileDiscovery": False,
                }
            else:
                body = {"type": "anyone", "role": "reader"}

        _with_backoff("drive.permissions.create", "write", lambda: _call(body))
    except Exception as exc:
        logger.info("Drive permission set failed for %s: %s", file_id, exc)


def _drive_upload_bytes(
    name: str,
    data: bytes,
    parent: str,
    mime: Optional[str],
) -> Optional[str]:
    from googleapiclient.http import MediaIoBaseUpload

    def _call():
        media = MediaIoBaseUpload(
            io.BytesIO(data),
            mime or mimetypes.guess_type(name)[0] or "application/octet-stream",
            resumable=False,
        )
        return _svc_drive().files().create(
            body={"name": name, "parents": [parent]},
            media_body=media,
            fields="id",
        ).execute()

    try:
        file_obj = _with_backoff("drive.files.create", "write", _call)
        file_id = file_obj.get("id")

        if file_id:
            _drive_ensure_permission(file_id)

        return file_id
    except Exception as exc:
        logger.info("Drive upload failed for %s: %s", name, exc)
        return None


def _receipt_drive_filename(req_id: int, line_id: int, original_name: str) -> str:
    base = os.path.basename(original_name or "") or "receipt"
    return f"reimb_{req_id}_line_{line_id}_{base}"


def _drive_link(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view?usp=drivesdk"


def _receipt_file_for_line(line):
    f = getattr(line, "receipt_file", None)

    if f and getattr(f, "name", None):
        return f

    item = getattr(line, "expense_item", None)

    if item:
        f = getattr(item, "receipt_file", None)
        if f and getattr(f, "name", None):
            return f

    return None


def _collect_receipt_link_for_line(req, line) -> str:
    f = _receipt_file_for_line(line)

    if not f:
        return ""

    try:
        if _google_available() and _drive_folder_id():
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

            file_id = _drive_upload_bytes(
                filename,
                data,
                _drive_folder_id(),
                mimetypes.guess_type(getattr(f, "name", ""))[0],
            )

            if file_id:
                return _drive_link(file_id)
    except Exception as exc:
        logger.info(
            "Drive receipt link skipped for req=%s line=%s: %s",
            getattr(req, "id", None),
            getattr(line, "id", None),
            exc,
        )

    try:
        if getattr(f, "url", None):
            return _absolute_url(f.url)
    except Exception as exc:
        logger.warning(
            "Receipt storage URL unavailable for req=%s line=%s file=%s: %s",
            getattr(req, "id", None),
            getattr(line, "id", None),
            getattr(f, "name", ""),
            exc,
        )

    return ""


def _receipt_cell(link: str) -> str:
    if not link:
        return ""

    return f'=HYPERLINK("{_escape_sheet_formula_string(link)}","View")'


def build_rows(req) -> List[List[Any]]:
    rows: List[List[Any]] = []
    employee = getattr(req, "created_by", None)

    lines_qs = (
        req.lines.select_related("expense_item")
        .filter(status=ReimbursementLine.Status.INCLUDED)
        .order_by("id")
    )

    for line in lines_qs:
        item = getattr(line, "expense_item", None)

        amount = float(
            getattr(line, "amount", None)
            or getattr(item, "amount", 0)
            or 0
        )

        try:
            category = item.get_category_display() if item else ""
        except Exception:
            category = getattr(item, "category", "") or ""

        try:
            gst = item.get_gst_type_display() if item else ""
        except Exception:
            gst = getattr(item, "gst_type", "") or ""

        link = _collect_receipt_link_for_line(req, line)
        receipt_cell = _receipt_cell(link)

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

        row = [
            _row_key(req.id, line.id),
            req.id,
            _employee_name(employee),
            _sheet_date(date_of_bill),
            category,
            description,
            amount,
            receipt_cell,
            gst,
            bill_status,
            finance_verifier,
            manager_verifier,
            _sheet_datetime(submitted_at),
            _sheet_datetime(verified_at),
            _sheet_datetime(manager_approved_at),
            _sheet_datetime(paid_at),
            payment_ref,
        ]

        if len(row) != len(HEADER):
            raise ValueError(
                f"Invalid reimbursement sheet row length for req={req.id}, "
                f"line={line.id}: {len(row)} != {len(HEADER)}"
            )

        rows.append(row)

    return rows


def build_row(req) -> List[Any]:
    rows = build_rows(req)
    return rows[0] if rows else []


def _scan_rowkeys() -> Dict[str, List[int]]:
    values = _values_get(f"{TAB_MAIN}!A2:B")
    out: Dict[str, List[int]] = {}

    for row_number, row in enumerate(values, start=2):
        col_a = str(row[0]).strip() if len(row) >= 1 and row[0] is not None else ""
        col_b = str(row[1]).strip() if len(row) >= 2 and row[1] is not None else ""

        rowkey = ""

        if _looks_like_rowkey(col_a):
            rowkey = col_a
        elif _looks_like_rowkey(col_b):
            rowkey = col_b

        if rowkey:
            out.setdefault(rowkey, []).append(row_number)

    return out


def _index_by_rowkey() -> Dict[str, int]:
    scanned = _scan_rowkeys()

    return {
        rowkey: row_numbers[0]
        for rowkey, row_numbers in scanned.items()
        if row_numbers
    }


def _delete_sheet_rows(row_numbers: List[int]) -> None:
    rows = sorted({rn for rn in row_numbers if rn > 1}, reverse=True)

    if not rows:
        return

    meta = _spreadsheets_get()
    sheet_map = _get_sheet_map_from_meta(meta)
    sheet_id = sheet_map.get(TAB_MAIN)

    if sheet_id is None:
        return

    requests = []

    for rn in rows:
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": rn - 1,
                        "endIndex": rn,
                    }
                }
            }
        )

    _batch_update(requests)
    _meta_cache.clear()


def _delete_stale_or_duplicate_rows_for_request(
    req_id: int,
    keep_rowkeys: set[str],
) -> None:
    prefix = f"{req_id}-"
    scanned = _scan_rowkeys()
    rows_to_delete: List[int] = []

    for rowkey, row_numbers in scanned.items():
        if not rowkey.startswith(prefix):
            continue

        if rowkey not in keep_rowkeys:
            rows_to_delete.extend(row_numbers)
            continue

        if len(row_numbers) > 1:
            rows_to_delete.extend(row_numbers[1:])

    _delete_sheet_rows(rows_to_delete)


def _delete_rows_not_in_db(valid_rowkeys: set[str]) -> None:
    scanned = _scan_rowkeys()
    rows_to_delete: List[int] = []

    for rowkey, row_numbers in scanned.items():
        if rowkey not in valid_rowkeys:
            rows_to_delete.extend(row_numbers)
            continue

        if len(row_numbers) > 1:
            rows_to_delete.extend(row_numbers[1:])

    _delete_sheet_rows(rows_to_delete)


def _parse_updated_range_start_row(resp: dict) -> int:
    try:
        updated_range = resp.get("updates", {}).get("updatedRange", "")

        if "!" not in updated_range:
            return 0

        a1 = updated_range.split("!", 1)[1]
        start = a1.split(":", 1)[0]
        digits = "".join(ch for ch in start if ch.isdigit())

        return int(digits) if digits else 0
    except Exception:
        return 0


def _upsert_rows_batch(rows: List[List[Any]]) -> Dict[str, int]:
    written_map: Dict[str, int] = {}

    if not rows:
        return written_map

    idx = _index_by_rowkey()
    end_col = _header_end_col()

    update_blocks: List[Dict[str, Any]] = []
    append_rows: List[List[Any]] = []

    for row in rows:
        if len(row) != len(HEADER):
            raise ValueError(f"Invalid row length: {len(row)} != {len(HEADER)}")

        rowkey = str(row[0]).strip()
        row_number = idx.get(rowkey)

        if row_number:
            update_blocks.append(
                {
                    "range": f"{TAB_MAIN}!A{row_number}:{end_col}{row_number}",
                    "values": [row],
                }
            )
            written_map[rowkey] = row_number
        else:
            append_rows.append(row)

    if update_blocks:
        _values_batch_update(update_blocks, input_option="USER_ENTERED")

    if append_rows:
        resp = _values_append(
            f"{TAB_MAIN}!A:{end_col}",
            append_rows,
            user_entered=True,
        )

        first_row = _parse_updated_range_start_row(resp)

        if first_row:
            for i, row in enumerate(append_rows):
                written_map[str(row[0])] = first_row + i

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
            [
                [
                    _iso(datetime.now(timezone.utc)),
                    event,
                    rowkey,
                    old or "",
                    new or "",
                    rownum,
                    actor or "",
                    f"{result}: {err}" if err else result,
                ]
            ],
            user_entered=False,
        )
    except Exception as exc:
        logger.info("Changelog append skipped: %s", exc)


def _sync_request_impl(req_id: int) -> None:
    if not _google_available():
        return

    from apps.reimbursement.models import ReimbursementRequest

    ensure_spreadsheet_structure()

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
    except ReimbursementRequest.DoesNotExist:
        try:
            _delete_stale_or_duplicate_rows_for_request(
                req_id=req_id,
                keep_rowkeys=set(),
            )
        except Exception as exc:
            logger.exception(
                "Google Sheets stale cleanup failed for deleted req=%s: %s",
                req_id,
                exc,
            )
        return
    except Exception as exc:
        logger.exception(
            "Failed to load reimbursement request %s for sheet sync: %s",
            req_id,
            exc,
        )
        return

    rows = build_rows(req)
    keep_rowkeys = {str(row[0]) for row in rows}

    try:
        _delete_stale_or_duplicate_rows_for_request(
            req_id=req_id,
            keep_rowkeys=keep_rowkeys,
        )
    except Exception as exc:
        logger.exception(
            "Google Sheets stale/duplicate cleanup failed for req=%s: %s",
            req_id,
            exc,
        )

    try:
        written = _upsert_rows_batch(rows)
    except Exception as exc:
        logger.exception(
            "Google Sheets upsert failed for req=%s: %s",
            req_id,
            exc,
        )
        written = {}

    prev_status = getattr(req, "status", "") or ""

    for row in rows:
        rowkey = str(row[0])
        rownum = int(written.get(rowkey, 0))

        try:
            append_changelog(
                "upsert",
                rowkey,
                prev_status,
                req.status,
                rownum,
                "",
                "ok",
            )
        except Exception:
            pass


def sync_request(req) -> None:
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
            thread = threading.Thread(
                target=_sync_request_impl,
                args=(req_id,),
                daemon=True,
            )
            thread.start()
        except Exception:
            cache.delete(lock_key)

    try:
        transaction.on_commit(_kick)
    except Exception:
        _kick()


def reset_main_data() -> None:
    if not _google_available():
        return

    ensure_spreadsheet_structure()
    end_col = _header_end_col()

    def _call():
        return _svc_sheets().spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A2:{end_col}",
        ).execute()

    try:
        _with_backoff("values.clear", "write", _call)
        logger.info("Cleared Reimbursements data rows under header.")
    except Exception as exc:
        logger.exception("Failed to clear Reimbursements sheet: %s", exc)


def _collect_all_rows() -> Tuple[List[List[Any]], Dict[str, List[Any]]]:
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

        for row in rows:
            rows_by_rowkey[str(row[0])] = row

    return all_rows, rows_by_rowkey


def _chunked(seq: List[Any], size: int) -> List[List[Any]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _write_rows_fixed_position(rows: List[List[Any]], chunk_size: int = 500) -> None:
    if not rows:
        return

    end_col = _header_end_col()
    blocks = []

    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        sheet_start_row = 2 + start
        sheet_end_row = sheet_start_row + len(chunk) - 1

        blocks.append(
            {
                "range": f"{TAB_MAIN}!A{sheet_start_row}:{end_col}{sheet_end_row}",
                "values": chunk,
            }
        )

    _values_batch_update(blocks, input_option="USER_ENTERED")


def bulk_resync_all_requests(
    update_chunk_size: int = 500,
    append_chunk_size: int = 500,
    sleep_between_append_chunks_sec: int = 65,
    disable_changelog: bool = True,
) -> None:
    if not _google_available():
        return

    ensure_spreadsheet_structure()

    all_rows, rows_by_rowkey = _collect_all_rows()

    if not all_rows:
        logger.info("bulk_resync_all_requests: no rows to write.")
        return

    idx = _index_by_rowkey()
    end_col = _header_end_col()

    update_blocks: List[Dict[str, Any]] = []
    append_rows: List[List[Any]] = []

    for rowkey, row in rows_by_rowkey.items():
        row_number = idx.get(rowkey)

        if row_number:
            update_blocks.append(
                {
                    "range": f"{TAB_MAIN}!A{row_number}:{end_col}{row_number}",
                    "values": [row],
                }
            )
        else:
            append_rows.append(row)

    if update_blocks:
        for chunk in _chunked(update_blocks, update_chunk_size):
            _values_batch_update(chunk, input_option="USER_ENTERED")

        logger.info(
            "bulk_resync_all_requests: updated %s existing rows.",
            len(update_blocks),
        )

    if append_rows:
        for chunk in _chunked(append_rows, append_chunk_size):
            _values_append(
                f"{TAB_MAIN}!A:{end_col}",
                chunk,
                user_entered=True,
            )
            if sleep_between_append_chunks_sec and len(append_rows) > append_chunk_size:
                time.sleep(max(1, sleep_between_append_chunks_sec))

        logger.info(
            "bulk_resync_all_requests: appended %s rows.",
            len(append_rows),
        )

    if not disable_changelog:
        try:
            _values_append(
                f"{TAB_CHANGELOG}!A:H",
                [[_iso(datetime.now(timezone.utc)), "bulk_resync", "", "", "", 0, "", "ok"]],
                user_entered=False,
            )
        except Exception as exc:
            logger.info("bulk_resync changelog skipped: %s", exc)


def force_rewrite_main_data_from_db() -> None:
    if not _google_available():
        return

    ensure_spreadsheet_structure()
    reset_main_data()

    rows, _ = _collect_all_rows()

    logger.info("force_rewrite_main_data_from_db: prepared %s rows.", len(rows))

    if rows:
        _write_rows_fixed_position(rows, chunk_size=500)

    logger.info("force_rewrite_main_data_from_db: wrote %s rows.", len(rows))


def rebuild_main_data_from_db() -> None:
    force_rewrite_main_data_from_db()


def repair_main_sheet_from_db() -> None:
    if not _google_available():
        return

    ensure_spreadsheet_structure()

    all_rows, rows_by_rowkey = _collect_all_rows()
    valid_rowkeys = set(rows_by_rowkey.keys())

    try:
        _delete_rows_not_in_db(valid_rowkeys)
    except Exception as exc:
        logger.exception("Failed to delete stale/duplicate rows: %s", exc)

    if all_rows:
        _upsert_rows_batch(all_rows)