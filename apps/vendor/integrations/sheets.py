# apps/vendor/integrations/sheets.py
from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from django.conf import settings
from django.core.cache import cache
from django.db import transaction

logger = logging.getLogger(__name__)


__all__ = [
    "SPREADSHEET_ID",
    "TAB_MAIN",
    "TAB_CHANGELOG",
    "TAB_SCHEMA",
    "ensure_spreadsheet_structure",
    "build_row",
    "sync_request",
    "bulk_resync_all_requests",
]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPREADSHEET_ID = (
    os.environ.get("VENDOR_PAYMENT_SHEET_ID")
    or getattr(settings, "VENDOR_PAYMENT_SHEET_ID", None)
    or getattr(settings, "VENDOR_PAYMENT_SHEET_ID".lower(), None)
    or ""
)

TAB_MAIN = (
    os.environ.get("VENDOR_PAYMENT_SHEET_TAB")
    or getattr(settings, "VENDOR_PAYMENT_SHEET_TAB", None)
    or "Vendor Payments"
)

TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA = "Schema"

SYNC_VERSION = 1

STRUCTURE_TTL_SECONDS = int(
    getattr(settings, "VENDOR_PAYMENT_SHEETS_STRUCTURE_TTL_SECONDS", 600)
)

READS_PER_MINUTE_BUDGET = int(
    getattr(settings, "VENDOR_PAYMENT_SHEETS_READS_PER_MINUTE", 48)
)

WRITES_PER_MINUTE_BUDGET = int(
    getattr(settings, "VENDOR_PAYMENT_SHEETS_WRITES_PER_MINUTE", 30)
)


# ---------------------------------------------------------------------------
# Vendor Payment Sheet columns
#
# Requirement sheet:
# A -> Unique Number or ID
# B -> Vendor Name
# C -> Type of Vendor
# D -> Invoice Date
# E -> Invoice Number
# F -> Type of Bill
# G -> Base Amount
# H -> GST Amount
# I -> Total Amount
# J -> Description of Payment
# K -> Attachment of Invoice
# L -> Copy of Cancelled Cheque
# M -> Bank Details
#
# Production recommendation:
# N -> Status
# O -> Updated At
#
# IMPORTANT:
# Unlike reimbursement, this sheet uses visible Column A as the unique key.
# VendorPaymentRequest is one row per request, so request_id is safe for display.
# Internal fallback RowKey is VP-<pk>, but visible ID remains request_id.
# ---------------------------------------------------------------------------

HEADER = [
    "Unique Number or ID",       # A
    "Vendor Name",               # B
    "Type of Vendor",            # C
    "Invoice Date",              # D
    "Invoice Number",            # E
    "Type of Bill",              # F
    "Base Amount",               # G
    "GST Amount",                # H
    "Total Amount",              # I
    "Description of Payment",    # J
    "Attachment of Invoice",     # K
    "Copy of Cancelled Cheque",  # L
    "Bank Details",              # M
    "Status",                    # N - recommended for approval/payment sync
    "Updated At",                # O - recommended for audit
]

CHANGELOG_HEADER = [
    "TimestampUTC",
    "Event",
    "RequestID",
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
# Google availability and credentials
# ---------------------------------------------------------------------------

_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS = False
_WARNED_MISSING_SHEET_ID = False


def _google_available() -> bool:
    global _WARNED_MISSING_GOOGLE, _WARNED_MISSING_CREDS, _WARNED_MISSING_SHEET_ID

    if not SPREADSHEET_ID:
        if not _WARNED_MISSING_SHEET_ID:
            logger.warning(
                "Vendor Payment Google sync disabled: missing VENDOR_PAYMENT_SHEET_ID."
            )
            _WARNED_MISSING_SHEET_ID = True
        return False

    try:
        import googleapiclient.discovery  # noqa
        import google.oauth2.service_account  # noqa
    except Exception:
        if not _WARNED_MISSING_GOOGLE:
            logger.warning(
                "Vendor Payment Google sync disabled: install deps -> "
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
                "Vendor Payment Google sync disabled: credentials missing. "
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

    return build(
        "sheets",
        "v4",
        credentials=_credentials(),
        cache_discovery=False,
    )


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

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
            dt = value
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        else:
            return str(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat(timespec="seconds")
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


def _file_hyperlink(file_field, label: str = "View") -> str:
    if not file_field:
        return ""

    try:
        url = getattr(file_field, "url", "")
    except Exception:
        url = ""

    if not url:
        return ""

    final_url = _absolute_url(url)

    # USER_ENTERED is used while writing, so this becomes clickable in Sheet.
    return f'=HYPERLINK("{final_url}","{label}")'


def _money(value) -> float:
    if value is None:
        return 0.0

    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except Exception:
        return 0.0


def _status_label(obj) -> str:
    try:
        return obj.get_status_display()
    except Exception:
        return str(getattr(obj, "status", "") or "")


def _bill_type_label(obj) -> str:
    try:
        return obj.get_bill_type_display()
    except Exception:
        return str(getattr(obj, "bill_type", "") or "")


def _vendor_type_label(obj) -> str:
    try:
        return obj.vendor_type_display_safe
    except Exception:
        pass

    try:
        return obj.get_vendor_type_display()
    except Exception:
        return str(getattr(obj, "vendor_type", "") or "")


def _vendor_name(obj) -> str:
    try:
        return obj.vendor_display_name
    except Exception:
        pass

    vendor = getattr(obj, "vendor", None)
    if vendor:
        return getattr(vendor, "name", "") or str(vendor)

    return getattr(obj, "vendor_name_manual", "") or ""


def _row_key(obj) -> str:
    """
    Visible unique ID for the sheet.

    request_id is already unique in VendorPaymentRequest.
    Fallback to VP-<pk> only if request_id is still missing.
    """
    req_id = (getattr(obj, "request_id", "") or "").strip()
    if req_id:
        return req_id

    pk = getattr(obj, "pk", None) or getattr(obj, "id", None)
    return f"VP-{pk}" if pk else ""


# ---------------------------------------------------------------------------
# Backoff + rate limit
# ---------------------------------------------------------------------------

_rw_lock = threading.Lock()
_read_next_refill = time.monotonic()
_write_next_refill = time.monotonic()
_read_tokens = READS_PER_MINUTE_BUDGET
_write_tokens = WRITES_PER_MINUTE_BUDGET


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
                sleep_for = max(0.05, _read_next_refill - now + 0.01)
                time.sleep(sleep_for)
                _read_tokens = READS_PER_MINUTE_BUDGET
                _read_next_refill = time.monotonic() + 60.0

            _read_tokens = max(0, _read_tokens - n)
            return

        if _write_tokens < n:
            sleep_for = max(0.05, _write_next_refill - now + 0.01)
            time.sleep(sleep_for)
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
                "Retrying Vendor Payment sheet %s after %s attempt %s/%s",
                label,
                exc,
                i,
                len(delays),
            )

            time.sleep(sleep_for)

    raise last_exc


# ---------------------------------------------------------------------------
# Google Sheets low-level helpers
# ---------------------------------------------------------------------------

_meta_lock = threading.Lock()
_meta_cache: Dict[str, Any] = {}


def _spreadsheets_get() -> dict:
    def _call():
        return _svc_sheets().spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID
        ).execute()

    cache_key = f"vendor.sheets.meta.{SPREADSHEET_ID}"

    with _meta_lock:
        meta = _meta_cache.get(cache_key)
        if meta:
            return meta

        meta = _with_backoff("spreadsheets.get", "read", _call)
        _meta_cache[cache_key] = meta
        return meta


def _get_sheet_map_from_meta(meta: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}

    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        title = props.get("title")
        sheet_id = props.get("sheetId")

        if title is not None:
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


def _values_update(range_: str, values: List[List[Any]]) -> None:
    def _call():
        return _svc_sheets().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

    _with_backoff(f"values.update {range_}", "write", _call)


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

    return _with_backoff(f"values.append {range_}", "write", _call)


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


# ---------------------------------------------------------------------------
# Spreadsheet structure
# ---------------------------------------------------------------------------

_structure_lock = threading.Lock()
_last_ensured_ts: Optional[float] = None


def _structure_cache_key() -> str:
    return f"vendor.sheets.structure.{SPREADSHEET_ID}.v{SYNC_VERSION}"


def _friendly_format_main(sheet_id: int) -> None:
    end_col = len(HEADER)

    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1,
                    },
                },
                "fields": "gridProperties.frozenRowCount",
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
        1: 150,   # Unique Number or ID
        2: 220,   # Vendor Name
        3: 160,   # Vendor Type
        4: 140,   # Invoice Date
        5: 160,   # Invoice Number
        6: 120,   # Type of Bill
        7: 130,   # Base Amount
        8: 130,   # GST Amount
        9: 130,   # Total Amount
        10: 300,  # Description
        11: 160,  # Invoice Attachment
        12: 180,  # Cancelled Cheque
        13: 300,  # Bank Details
        14: 150,  # Status
        15: 180,  # Updated At
    }

    for col_index, px in widths.items():
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_index - 1,
                        "endIndex": col_index,
                    },
                    "properties": {
                        "pixelSize": px,
                    },
                    "fields": "pixelSize",
                }
            }
        )

    # Wrap description and bank details.
    for col in [10, 13]:
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
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            }
        )

    # Money columns.
    for col in [7, 8, 9]:
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
                                "type": "NUMBER",
                                "pattern": "#,##0.00",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    # Date/time columns.
    for col in [4, 15]:
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
        logger.info("Vendor Payment sheet formatting skipped: %s", exc)


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
                                    "rowCount": 2000,
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

        # Hide helper tabs.
        requests = []

        for title in [TAB_CHANGELOG, TAB_SCHEMA]:
            sheet_id = existing.get(title)

            if sheet_id is not None:
                requests.append(
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
        changelog_header = (
            values_list[1][0]
            if len(values_list) > 1 and values_list[1]
            else []
        )
        schema_header = (
            values_list[2][0]
            if len(values_list) > 2 and values_list[2]
            else []
        )

        if main_header != HEADER:
            _values_update(f"{TAB_MAIN}!1:1", [HEADER])

        if changelog_header != CHANGELOG_HEADER:
            _values_update(f"{TAB_CHANGELOG}!1:1", [CHANGELOG_HEADER])

        if schema_header != SCHEMA_HEADER:
            _values_update(f"{TAB_SCHEMA}!1:1", [SCHEMA_HEADER])

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


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def build_row(obj) -> List[Any]:
    """
    Build one VendorPaymentRequest row.

    Always return exactly len(HEADER) columns.
    """
    row_key = _row_key(obj)

    row = [
        row_key,                                  # A Unique Number or ID
        _vendor_name(obj),                       # B Vendor Name
        _vendor_type_label(obj),                 # C Type of Vendor
        _iso(getattr(obj, "invoice_date", None)),# D Invoice Date
        getattr(obj, "invoice_number", "") or "",# E Invoice Number
        _bill_type_label(obj),                   # F Type of Bill
        _money(getattr(obj, "base_amount", 0)),  # G Base Amount
        _money(getattr(obj, "gst_amount", 0)),   # H GST Amount
        _money(getattr(obj, "total_amount", 0)), # I Total Amount
        getattr(obj, "description", "") or "",   # J Description of Payment
        _file_hyperlink(
            getattr(obj, "attachment", None),
            "Invoice",
        ),                                      # K Attachment of Invoice
        _file_hyperlink(
            getattr(obj, "bank_attachment", None),
            "Cheque",
        ),                                      # L Copy of Cancelled Cheque
        getattr(obj, "bank_details_text", "") or "", # M Bank Details
        _status_label(obj),                      # N Status
        _iso(getattr(obj, "updated_at", None)),  # O Updated At
    ]

    if len(row) != len(HEADER):
        raise ValueError(
            f"Invalid Vendor Payment sheet row length for obj={getattr(obj, 'pk', None)}: "
            f"{len(row)} != {len(HEADER)}"
        )

    return row


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _index_by_request_id() -> Dict[str, int]:
    """
    Map Column A Unique Number or ID -> Google Sheet row number.

    This prevents duplicate rows.
    If Vendor-001 exists, update same row.
    If Vendor-001 does not exist, append new row.
    """
    values = _values_get(f"{TAB_MAIN}!A2:A")

    index: Dict[str, int] = {}

    for row_number, row in enumerate(values, start=2):
        if not row:
            continue

        key = str(row[0]).strip()

        if key:
            index[key] = row_number

    return index


def _upsert_row(row: List[Any]) -> int:
    if not row:
        return 0

    end_col = _header_end_col()
    row_key = str(row[0]).strip()

    if not row_key:
        raise ValueError("Vendor Payment row key is empty.")

    index = _index_by_request_id()
    existing_row_number = index.get(row_key)

    if existing_row_number:
        _values_batch_update(
            [
                {
                    "range": f"{TAB_MAIN}!A{existing_row_number}:{end_col}{existing_row_number}",
                    "values": [row],
                }
            ],
            input_option="USER_ENTERED",
        )
        return existing_row_number

    resp = _values_append(
        f"{TAB_MAIN}!A:{end_col}",
        [row],
        user_entered=True,
    )

    try:
        updated_range = resp.get("updates", {}).get("updatedRange", "")
        first_cell = updated_range.split("!")[1].split(":")[0]
        return int(first_cell[1:])
    except Exception:
        return 0


def append_changelog(
    event: str,
    request_id: str,
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
                    request_id or "",
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
        logger.info("Vendor Payment changelog append skipped: %s", exc)


# ---------------------------------------------------------------------------
# Validation before sync
# ---------------------------------------------------------------------------

def _validate_for_sheet(obj) -> List[str]:
    errors: List[str] = []

    if not getattr(obj, "pk", None):
        errors.append("Object has no primary key.")

    if not _row_key(obj):
        errors.append("Request ID is missing.")

    if not getattr(obj, "vendor_id", None):
        errors.append("Vendor is missing.")

    if not getattr(obj, "invoice_date", None):
        errors.append("Invoice date is missing.")

    if not (getattr(obj, "invoice_number", "") or "").strip():
        errors.append("Invoice number is missing.")

    if getattr(obj, "base_amount", None) is None:
        errors.append("Base amount is missing.")

    if _money(getattr(obj, "base_amount", 0)) < 0:
        errors.append("Base amount cannot be negative.")

    if _money(getattr(obj, "gst_amount", 0)) < 0:
        errors.append("GST amount cannot be negative.")

    if _money(getattr(obj, "total_amount", 0)) < 0:
        errors.append("Total amount cannot be negative.")

    if not (getattr(obj, "description", "") or "").strip():
        errors.append("Description is missing.")

    return errors


# ---------------------------------------------------------------------------
# Public sync entrypoint
# ---------------------------------------------------------------------------

def _sync_request_impl(obj_id: int) -> None:
    if not _google_available():
        return

    from apps.vendor.models import VendorPaymentRequest

    try:
        obj = (
            VendorPaymentRequest.objects
            .select_related(
                "vendor",
                "created_by",
                "finance_approved_by",
                "final_approved_by",
            )
            .get(pk=obj_id)
        )
    except VendorPaymentRequest.DoesNotExist:
        logger.warning(
            "Vendor Payment sheet sync skipped: request id=%s does not exist.",
            obj_id,
        )
        return

    errors = _validate_for_sheet(obj)

    if errors:
        logger.warning(
            "Vendor Payment sheet sync skipped for pk=%s request_id=%s: %s",
            obj.pk,
            getattr(obj, "request_id", ""),
            "; ".join(errors),
        )
        return

    ensure_spreadsheet_structure()

    old_status = ""
    new_status = getattr(obj, "status", "") or ""
    request_id = _row_key(obj)

    try:
        row = build_row(obj)
        rownum = _upsert_row(row)

        append_changelog(
            event="upsert",
            request_id=request_id,
            old=old_status,
            new=new_status,
            rownum=rownum,
            actor="system",
            result="ok",
        )

        logger.info(
            "Vendor Payment sheet sync completed: pk=%s request_id=%s row=%s",
            obj.pk,
            request_id,
            rownum,
        )

    except Exception as exc:
        logger.exception(
            "Vendor Payment sheet sync failed: pk=%s request_id=%s",
            obj.pk,
            request_id,
        )

        try:
            append_changelog(
                event="upsert",
                request_id=request_id,
                old=old_status,
                new=new_status,
                rownum=0,
                actor="system",
                result="error",
                err=str(exc),
            )
        except Exception:
            pass


def sync_request(obj_or_id) -> None:
    """
    Safe public sync function.

    Same production principle as reimbursement:
    - never blocks user request for Google API
    - runs after DB commit
    - uses short cache lock to avoid duplicate sync storms
    - logs errors instead of breaking ERP workflow
    """
    try:
        obj_id = int(getattr(obj_or_id, "pk", obj_or_id))
    except Exception:
        logger.warning(
            "Vendor Payment sheet sync skipped: invalid object/id %r",
            obj_or_id,
        )
        return

    if not obj_id:
        return

    lock_key = f"vendor.payment.sheet.sync.lock.{obj_id}"

    def _kick():
        if not cache.add(lock_key, True, timeout=30):
            logger.info(
                "Vendor Payment sheet sync debounce skipped for id=%s",
                obj_id,
            )
            return

        thread = threading.Thread(
            target=_sync_request_impl,
            args=(obj_id,),
            daemon=True,
            name=f"vendor-payment-sheet-sync-{obj_id}",
        )
        thread.start()

    try:
        transaction.on_commit(_kick)
    except Exception:
        _kick()


def bulk_resync_all_requests(limit: Optional[int] = None) -> int:
    """
    Utility for Render shell.

    Example:
        from apps.vendor.integrations.sheets import bulk_resync_all_requests
        bulk_resync_all_requests()
    """
    if not _google_available():
        return 0

    from apps.vendor.models import VendorPaymentRequest

    qs = (
        VendorPaymentRequest.objects
        .select_related(
            "vendor",
            "created_by",
            "finance_approved_by",
            "final_approved_by",
        )
        .order_by("pk")
    )

    if limit:
        qs = qs[:limit]

    count = 0

    for obj in qs:
        _sync_request_impl(obj.pk)
        count += 1

    return count