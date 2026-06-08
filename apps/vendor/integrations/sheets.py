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

# Version bumped because we added hidden Internal Row Key column.
SYNC_VERSION = 2

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
# Requirement:
# One VendorPaymentRequest can contain multiple invoice rows.
#
# Sheet behavior:
# - One row per invoice.
# - Visible Column A remains same parent Request ID.
# - Hidden Column P stores unique internal row key so Google sync does not
#   overwrite invoice rows sharing same Request ID.
#
# Example:
# A Unique Number or ID | B Vendor | E Invoice Number | I Total | P Internal Row Key
# Vendor-001            | ABC      | INV001           | 10000   | Vendor-001::INVPK::10
# Vendor-001            | ABC      | INV002           | 20000   | Vendor-001::INVPK::11
# Vendor-001            | ABC      | INV003           | 30000   | Vendor-001::INVPK::12
# ---------------------------------------------------------------------------

HEADER = [
    "Unique Number or ID",       # A visible parent request id
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
    "Status",                    # N
    "Updated At",                # O
    "Internal Row Key",          # P hidden technical key
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


def _internal_key_col() -> str:
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
    Visible request ID for the sheet.

    This is shown in Column A and remains the same for every invoice row
    belonging to the same parent request.
    """
    req_id = (getattr(obj, "request_id", "") or "").strip()
    if req_id:
        return req_id

    pk = getattr(obj, "pk", None) or getattr(obj, "id", None)
    return f"VP-{pk}" if pk else ""


def _invoice_row_key(obj, invoice=None) -> str:
    """
    Hidden unique key for Google Sheet row upsert.

    Visible Column A can repeat for multiple invoices.
    Hidden Column P must be unique per row.

    New multi-invoice:
    Vendor-001::INVPK::10

    Legacy fallback:
    Vendor-001::LEGACY
    """
    request_id = _row_key(obj)

    if invoice is not None:
        invoice_pk = getattr(invoice, "pk", None)
        invoice_number = str(getattr(invoice, "invoice_number", "") or "").strip()

        if invoice_pk:
            return f"{request_id}::INVPK::{invoice_pk}"

        if invoice_number:
            return f"{request_id}::INV::{invoice_number}"

        return f"{request_id}::INV::UNKNOWN"

    return f"{request_id}::LEGACY"


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
        1: 150,
        2: 220,
        3: 160,
        4: 140,
        5: 160,
        6: 120,
        7: 130,
        8: 130,
        9: 130,
        10: 300,
        11: 160,
        12: 180,
        13: 300,
        14: 150,
        15: 180,
        16: 180,
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

    # Hide Internal Row Key column P.
    internal_key_index = len(HEADER) - 1
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": internal_key_index,
                    "endIndex": internal_key_index + 1,
                },
                "properties": {
                    "hiddenByUser": True,
                },
                "fields": "hiddenByUser",
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

def build_row(obj, invoice=None) -> List[Any]:
    """
    Build one sheet row.

    Multi-invoice:
    - One row per VendorPaymentInvoice child row.
    - Visible request ID remains same.
    - Hidden internal row key is unique per invoice.

    Legacy:
    - If invoice is None, uses old parent invoice fields.
    """
    visible_request_id = _row_key(obj)

    if invoice is not None:
        invoice_date = getattr(invoice, "invoice_date", None)
        invoice_number = getattr(invoice, "invoice_number", "") or ""
        bill_type_label = (
            invoice.get_bill_type_display()
            if hasattr(invoice, "get_bill_type_display")
            else str(getattr(invoice, "bill_type", "") or "")
        )
        base_amount = _money(getattr(invoice, "base_amount", 0))
        gst_amount = _money(getattr(invoice, "gst_amount", 0))
        total_amount = _money(getattr(invoice, "total_amount", 0))
        description = getattr(invoice, "description", "") or ""
        attachment = getattr(invoice, "invoice_attachment", None)
    else:
        invoice_date = getattr(obj, "invoice_date", None)
        invoice_number = getattr(obj, "invoice_number", "") or ""
        bill_type_label = _bill_type_label(obj)
        base_amount = _money(getattr(obj, "base_amount", 0))
        gst_amount = _money(getattr(obj, "gst_amount", 0))
        total_amount = _money(getattr(obj, "total_amount", 0))
        description = getattr(obj, "description", "") or ""
        attachment = getattr(obj, "attachment", None)

    row = [
        visible_request_id,
        _vendor_name(obj),
        _vendor_type_label(obj),
        _iso(invoice_date),
        invoice_number,
        bill_type_label,
        base_amount,
        gst_amount,
        total_amount,
        description,
        _file_hyperlink(attachment, "Invoice"),
        _file_hyperlink(getattr(obj, "bank_attachment", None), "Cheque"),
        getattr(obj, "bank_details_text", "") or "",
        _status_label(obj),
        _iso(getattr(obj, "updated_at", None)),
        _invoice_row_key(obj, invoice=invoice),
    ]

    if len(row) != len(HEADER):
        raise ValueError(
            f"Invalid Vendor Payment sheet row length for obj={getattr(obj, 'pk', None)}: "
            f"{len(row)} != {len(HEADER)}"
        )

    return row


# ---------------------------------------------------------------------------
# Row cleanup / append helpers
# ---------------------------------------------------------------------------

def _delete_rows_for_request_id(request_id: str) -> None:
    """
    Delete existing sheet rows where visible Column A equals request_id.

    This is intentionally used for multi-invoice requests before rewriting
    all current invoice rows.

    Why:
    If user removes an invoice before approval, that old invoice row must
    disappear from Google Sheet too.
    """
    if not request_id:
        return

    meta = _spreadsheets_get()
    sheet_map = _get_sheet_map_from_meta(meta)
    sheet_id = sheet_map.get(TAB_MAIN)

    if sheet_id is None:
        return

    values = _values_get(f"{TAB_MAIN}!A2:A")

    rows_to_delete = []

    for row_number, row in enumerate(values, start=2):
        if row and str(row[0]).strip() == request_id:
            rows_to_delete.append(row_number)

    if not rows_to_delete:
        return

    requests = []

    for row_number in sorted(rows_to_delete, reverse=True):
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_number - 1,
                        "endIndex": row_number,
                    }
                }
            }
        )

    _batch_update(requests)


def _append_rows(rows: List[List[Any]]) -> int:
    if not rows:
        return 0

    end_col = _header_end_col()

    resp = _values_append(
        f"{TAB_MAIN}!A:{end_col}",
        rows,
        user_entered=True,
    )

    try:
        updated_range = resp.get("updates", {}).get("updatedRange", "")
        first_cell = updated_range.split("!")[1].split(":")[0]
        return int(first_cell[1:])
    except Exception:
        return 0


def _index_by_internal_row_key() -> Dict[str, int]:
    """
    Map hidden Internal Row Key column -> Google Sheet row number.

    Kept for legacy single-row upsert fallback.
    Multi-invoice sync uses delete-and-reappend for request correctness.
    """
    internal_col = _internal_key_col()
    values = _values_get(f"{TAB_MAIN}!{internal_col}2:{internal_col}")

    index: Dict[str, int] = {}

    for row_number, row in enumerate(values, start=2):
        if not row:
            continue

        key = str(row[0]).strip()

        if key:
            index[key] = row_number

    return index


def _upsert_row(row: List[Any]) -> int:
    """
    Upsert one row using hidden Internal Row Key.

    Used mostly for legacy single-invoice fallback.
    """
    if not row:
        return 0

    end_col = _header_end_col()
    internal_key = str(row[-1]).strip()

    if not internal_key:
        raise ValueError("Vendor Payment internal row key is empty.")

    index = _index_by_internal_row_key()
    existing_row_number = index.get(internal_key)

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

    return _append_rows([row])


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

    return errors


def _validate_invoice_for_sheet(invoice) -> List[str]:
    errors: List[str] = []

    if not getattr(invoice, "pk", None):
        errors.append("Invoice has no primary key.")

    if not getattr(invoice, "invoice_date", None):
        errors.append(
            f"Invoice {getattr(invoice, 'invoice_number', '')} is missing date."
        )

    if not (getattr(invoice, "invoice_number", "") or "").strip():
        errors.append("Invoice number is missing.")

    if getattr(invoice, "base_amount", None) is None:
        errors.append(
            f"Invoice {getattr(invoice, 'invoice_number', '')} is missing base amount."
        )

    if _money(getattr(invoice, "base_amount", 0)) < 0:
        errors.append(
            f"Invoice {getattr(invoice, 'invoice_number', '')} base amount cannot be negative."
        )

    if _money(getattr(invoice, "gst_amount", 0)) < 0:
        errors.append(
            f"Invoice {getattr(invoice, 'invoice_number', '')} GST amount cannot be negative."
        )

    if _money(getattr(invoice, "total_amount", 0)) < 0:
        errors.append(
            f"Invoice {getattr(invoice, 'invoice_number', '')} total amount cannot be negative."
        )

    return errors


# ---------------------------------------------------------------------------
# Public sync entrypoint
# ---------------------------------------------------------------------------

def _sync_request_impl(obj_id: int) -> None:
    """
    Sync one VendorPaymentRequest to Google Sheets.

    Multi-invoice path:
    - Delete all existing sheet rows for request_id.
    - Append one fresh row per current child invoice.
    - This prevents stale rows when an invoice is removed before approval.

    Legacy single-invoice path:
    - Upsert one row using hidden Internal Row Key.
    """
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
            .prefetch_related("invoices")
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

    new_status = getattr(obj, "status", "") or ""
    request_id = _row_key(obj)

    invoices = []

    try:
        invoices = list(obj.invoices.all())
    except Exception:
        invoices = []

    try:
        if invoices:
            rows = []

            for invoice in invoices:
                inv_errors = _validate_invoice_for_sheet(invoice)

                if inv_errors:
                    logger.warning(
                        "Skipping invoice pk=%s for request pk=%s: %s",
                        getattr(invoice, "pk", None),
                        obj.pk,
                        "; ".join(inv_errors),
                    )
                    continue

                rows.append(build_row(obj, invoice=invoice))

            if not rows:
                logger.warning(
                    "Vendor Payment sheet sync skipped for request pk=%s request_id=%s because no valid invoice rows were available.",
                    obj.pk,
                    request_id,
                )
                return

            _delete_rows_for_request_id(request_id)
            first_rownum = _append_rows(rows)

            append_changelog(
                event="rewrite",
                request_id=request_id,
                old="",
                new=new_status,
                rownum=first_rownum,
                actor="system",
                result=f"ok: {len(rows)} invoice row(s)",
            )

            logger.info(
                "Vendor Payment sheet sync completed multi-invoice rewrite: pk=%s request_id=%s invoices=%s first_row=%s",
                obj.pk,
                request_id,
                len(rows),
                first_rownum,
            )

        else:
            row = build_row(obj, invoice=None)
            rownum = _upsert_row(row)

            append_changelog(
                event="upsert_legacy",
                request_id=request_id,
                old="",
                new=new_status,
                rownum=rownum,
                actor="system",
                result="ok",
            )

            logger.info(
                "Vendor Payment sheet sync completed legacy: pk=%s request_id=%s row=%s",
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
                event="sync",
                request_id=request_id,
                old="",
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

    Production principle:
    - Never blocks user request for Google API.
    - Runs after DB commit.
    - Uses short cache lock to avoid duplicate sync storms.
    - Logs errors instead of breaking ERP workflow.
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
        .prefetch_related("invoices")
        .order_by("pk")
    )

    if limit:
        qs = qs[:limit]

    count = 0

    for obj in qs:
        _sync_request_impl(obj.pk)
        count += 1

    return count