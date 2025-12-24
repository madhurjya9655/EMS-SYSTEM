# apps/reimbursement/integrations/sheets.py
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Tuple

from django.conf import settings
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
# HARD-CODED SHEET IDENTIFIERS
# ---------------------------------------------------------------------------

SPREADSHEET_ID = "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A"

TAB_MAIN = "Reimbursements"
TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA = "Schema"
TAB_META = "_Meta"  # reserved

# Schema/header version — bump if you change HEADER
SYNC_VERSION = 2

# One-time warning flags per process
_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS = False

# ---------------------------------------------------------------------------
# MAIN TAB HEADER (friendly labels, stable order)
# ---------------------------------------------------------------------------

HEADER = [
    "ReimbID",              # A
    "EmployeeID",           # B
    "Employee",             # C  (friendly)
    "Department",           # D
    "Categories",           # E  (friendly)
    "Items",                # F  (friendly)
    "Amount",               # G  (friendly, INR)
    "Currency",             # H
    "Submitted",            # I  (friendly)
    "Status",               # J
    "StatusUpdated",        # K
    "Manager",              # L
    "ManagerDecided",       # M
    "Management",           # N
    "ManagementDecided",    # O
    "FinanceVerifier",      # P
    "FinanceVerified",      # Q
    "PaymentRef",           # R  (friendly)
    "PaidAt",               # S
    "RejectionReason",      # T
    "FinanceNote",          # U
    "ReceiptLinks",         # V
    "EMSLink",              # W
    "CreatedAt",            # X
    "UpdatedAt",            # Y
    "SyncedAt",             # Z
    "SyncVersion",          # AA
    "Extra",                # AB
]

CHANGELOG_HEADER = [
    "TimestampUTC", "Event", "ReimbID", "OldStatus", "NewStatus", "RowNum", "Actor", "Result"
]
SCHEMA_HEADER = ["Version", "HeaderJSON", "Active", "RecordedAtUTC", "Note"]

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def _excel_col(n: int) -> str:
    out = []
    while n:
        n, r = divmod(n - 1, 26)
        out.append(chr(65 + r))
    return "".join(reversed(out))

def _header_end_col() -> str:
    return _excel_col(len(HEADER))  # "AB" today

def _iso(dt):
    if not dt:
        return ""
    if getattr(dt, "tzinfo", None) is None:
        return datetime.fromtimestamp(dt.timestamp(), tz=timezone.utc).isoformat(timespec="seconds")
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

def _site_url() -> str:
    return (getattr(settings, "SITE_URL", "").rstrip("/")) or "http://127.0.0.1:8000"

def _detail_url(req_id: int) -> str:
    try:
        path = reverse("reimbursement:request_detail", kwargs={"pk": req_id})
    except NoReverseMatch:
        path = f"/reimbursement/request/{req_id}/"
    return f"{_site_url()}{path}"

# ---------------------------------------------------------------------------
# Lazy Google loader (no import at module import time)
# ---------------------------------------------------------------------------

def _google_available() -> bool:
    global _WARNED_MISSING_GOOGLE, _WARNED_MISSING_CREDS
    try:
        import googleapiclient.discovery  # noqa: F401
        import google.oauth2.service_account  # noqa: F401
    except Exception:
        if not _WARNED_MISSING_GOOGLE:
            logger.warning(
                "Google Sheets sync disabled: install deps -> "
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
                "Google Sheets sync disabled: credentials missing. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON (recommended) or GOOGLE_SERVICE_ACCOUNT_FILE."
            )
            _WARNED_MISSING_CREDS = True
        return False

    return True

def _credentials():
    from google.oauth2 import service_account  # lazy
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return service_account.Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    file_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or getattr(settings, "GOOGLE_SERVICE_ACCOUNT_FILE", None)
    if file_path:
        return service_account.Credentials.from_service_account_file(file_path, scopes=scopes)
    raise RuntimeError("Google credentials not found")

def _svc():
    from googleapiclient.discovery import build  # lazy
    return build("sheets", "v4", credentials=_credentials(), cache_discovery=False)

# ---------------------------------------------------------------------------
# Spreadsheet bootstrap / formatting
# ---------------------------------------------------------------------------

def _get_sheet_map() -> Dict[str, int]:
    resp = _svc().spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    out: Dict[str, int] = {}
    for s in resp.get("sheets", []):
        props = s.get("properties", {})
        out[props.get("title")] = props.get("sheetId")
    return out

def _batch_update(requests: list) -> None:
    if not requests:
        return
    _svc().spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
    ).execute()

def _friendly_format_main(sheet_id: int) -> None:
    """Make main tab client-friendly: widths, filter, wrap, formats."""
    end_col = len(HEADER)
    requests = []

    # Freeze header
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }
    })

    # Enable filter on header row
    requests.append({
        "setBasicFilter": {
            "filter": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1_000_000, "startColumnIndex": 0, "endColumnIndex": end_col}
            }
        }
    })

    # Column widths (friendlier)
    widths = {
        1: 90,   # ReimbID
        2: 90,   # EmployeeID
        3: 200,  # Employee
        4: 150,  # Department
        5: 160,  # Categories
        6: 90,   # Items
        7: 120,  # Amount
        8: 70,   # Currency
        9: 170,  # Submitted
        10: 150, # Status
        11: 170, # StatusUpdated
        12: 150, # Manager
        13: 170, # ManagerDecided
        14: 150, # Management
        15: 170, # ManagementDecided
        16: 150, # FinanceVerifier
        17: 170, # FinanceVerified
        18: 160, # PaymentRef
        19: 170, # PaidAt
        20: 220, # RejectionReason
        21: 260, # FinanceNote
        22: 260, # ReceiptLinks
        23: 160, # EMSLink
        24: 170, # CreatedAt
        25: 170, # UpdatedAt
        26: 170, # SyncedAt
        27: 110, # SyncVersion
        28: 180, # Extra
    }
    for idx, px in widths.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx - 1, "endIndex": idx},
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    # Wrap Notes / URLs columns
    wrap_cols = [20, 21, 22]  # RejectionReason, FinanceNote, ReceiptLinks
    for col in wrap_cols:
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": col - 1, "endColumnIndex": col},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat.wrapStrategy",
            }
        })

    # Amount number format (INR style but generic to avoid locale issues)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
            "fields": "userEnteredFormat.numberFormat",
        }
    })

    # Date/time columns format
    dt_cols = [9, 11, 13, 15, 17, 19, 24, 25, 26]  # indices 1-based
    for col in dt_cols:
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": col - 1, "endColumnIndex": col},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

    # Alternating row banding
    requests.append({
        "addBanding": {
            "bandedRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "startColumnIndex": 0, "endColumnIndex": end_col},
                "rowProperties": {"headerColor": {"red": 0.95, "green": 0.95, "blue": 0.95}},
            }
        }
    })

    _batch_update(requests)

def ensure_spreadsheet_structure() -> None:
    """
    Ensure required tabs exist with header rows, friendly formatting,
    and hide internal tabs. Safe & idempotent. No-ops if Google unavailable.
    """
    if not _google_available():
        return

    existing = _get_sheet_map()
    requests = []

    for title in [TAB_MAIN, TAB_CHANGELOG, TAB_SCHEMA]:
        if title not in existing:
            requests.append({
                "addSheet": {
                    "properties": {
                        "title": title,
                        "gridProperties": {"rowCount": 2000, "columnCount": 40},
                    }
                }
            })

    if requests:
        _batch_update(requests)
        existing = _get_sheet_map()

    # Hide internal tabs (non-tech)
    requests = []
    for title in [TAB_CHANGELOG, TAB_SCHEMA]:
        sid = existing.get(title)
        if sid is not None:
            requests.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": sid, "hidden": True},
                    "fields": "hidden",
                }
            })
    _batch_update(requests)

    # MAIN formatting + widths + filter + banding
    main_id = existing.get(TAB_MAIN)
    if main_id is not None:
        _friendly_format_main(main_id)

    # Write header rows if missing/mismatched
    values = _svc().spreadsheets().values()

    # MAIN
    cur = values.get(spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!1:1").execute().get("values", [[]])
    row0 = cur[0] if cur else []
    if row0 != HEADER:
        values.update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!1:1",
            valueInputOption="RAW",
            body={"values": [HEADER]},
        ).execute()

    # CHANGELOG
    cur = values.get(spreadsheetId=SPREADSHEET_ID, range=f"{TAB_CHANGELOG}!1:1").execute().get("values", [[]])
    row0 = cur[0] if cur else []
    if row0 != CHANGELOG_HEADER:
        values.update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_CHANGELOG}!1:1",
            valueInputOption="RAW",
            body={"values": [CHANGELOG_HEADER]},
        ).execute()

    # SCHEMA
    cur = values.get(spreadsheetId=SPREADSHEET_ID, range=f"{TAB_SCHEMA}!1:1").execute().get("values", [[]])
    row0 = cur[0] if cur else []
    if row0 != SCHEMA_HEADER:
        values.update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_SCHEMA}!1:1",
            valueInputOption="RAW",
            body={"values": [SCHEMA_HEADER]},
        ).execute()

    # Schema heartbeat
    hb = [SYNC_VERSION, json.dumps(HEADER, ensure_ascii=False), True, _iso(datetime.now(timezone.utc)), "bootstrap/update"]
    try:
        values.append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_SCHEMA}!A:E",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [hb]},
        ).execute()
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def _collect_receipt_urls(req) -> str:
    urls = []
    for line in req.lines.select_related("expense_item"):
        f = getattr(line, "receipt_file", None) or getattr(line.expense_item, "receipt_file", None)
        if f and getattr(f, "url", None):
            urls.append(f.url)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return ",".join(out)

def _categories_and_count(req) -> Tuple[str, int]:
    cats = [line.expense_item.category for line in req.lines.select_related("expense_item")]
    deduped, seen = [], set()
    for c in cats:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return ",".join(deduped), req.lines.count()

def build_row(req) -> list:
    employee = req.created_by
    dept = (
        getattr(employee, "department", "")
        or (getattr(employee, "profile", None) and getattr(employee.profile, "department", ""))
        or ""
    )
    cats, line_count = _categories_and_count(req)
    receipts_csv = _collect_receipt_urls(req)

    manager_un = getattr(req.manager, "username", "") if req.manager_id else ""
    management_un = getattr(req.management, "username", "") if req.management_id else ""
    finance_un = getattr(req.verified_by, "username", "") if req.verified_by_id else ""

    extra = {}

    row = {
        "ReimbID": req.id,
        "EmployeeID": getattr(employee, "id", ""),
        "Employee": (
            f"{getattr(employee,'first_name','')} {getattr(employee,'last_name','')}".strip()
            or getattr(employee, "username", "")
            or f"User #{getattr(employee, 'id', '')}"
        ),
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
# Upsert & change log
# ---------------------------------------------------------------------------

def _index_by_id() -> Dict[str, int]:
    resp = _svc().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!A2:A"
    ).execute()
    idx: Dict[str, int] = {}
    for i, v in enumerate(resp.get("values", []), start=2):
        if v:
            idx[str(v[0])] = i
    return idx

def upsert_row(row: list, reimb_id: int):
    values = _svc().spreadsheets().values()
    idx = _index_by_id()
    end_col = _header_end_col()

    if str(reimb_id) in idx:
        rn = idx[str(reimb_id)]
        values.update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!A{rn}:{end_col}{rn}",
            valueInputOption="USER_ENTERED",
            body={"values": [row]},
        ).execute()
        return "update", rn

    resp = values.append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_MAIN}!A:{end_col}",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()
    rng = resp.get("updates", {}).get("updatedRange", "")
    rn = 0
    try:
        rn = int(rng.split("!")[1].split(":")[0][1:])
    except Exception:
        pass
    return "insert", rn

def append_changelog(
    event: str,
    req_id: int,
    old: str,
    new: str,
    rownum: int,
    actor: str = "",
    result: str = "ok",
    err: str = "",
) -> None:
    _svc().spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_CHANGELOG}!A:H",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[
            _iso(datetime.now(timezone.utc)),
            event,
            req_id,
            old or "",
            new or "",
            rownum,
            actor or "",
            f"{result}: {err}" if err else result,
        ]]}
    ).execute()

# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def sync_request(req) -> None:
    """
    Idempotent upsert for a single ReimbursementRequest.
    Ensures tabs/headers/formatting first. No-ops if google libs/creds missing.
    """
    if not _google_available() or req is None:
        return

    ensure_spreadsheet_structure()

    row = build_row(req)
    backoffs = [0.2, 0.5, 1, 2, 4]

    prev_status = ""
    try:
        prev_status = getattr(req, "status", "") or ""
    except Exception:
        pass

    for attempt, wait in enumerate(backoffs, start=1):
        try:
            action, rn = upsert_row(row, req.id)
            append_changelog("upsert", req.id, prev_status, req.status, rn, "", action)
            return
        except Exception as e:
            code = getattr(getattr(e, "resp", None), "status", None)
            transient = code in (429, 500, 502, 503, 504)
            if transient and attempt < len(backoffs):
                time.sleep(wait)
                continue
            logger.exception("Google Sheets sync failed for req %s", req.id)
            try:
                append_changelog("error", req.id, prev_status, req.status, 0, err=str(e))
            except Exception:
                pass
            return
