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

# Client’s shared Google Sheet
SPREADSHEET_ID = "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A"

# Tab names (stable; do not rename without data migration)
TAB_MAIN = "Reimbursements"
TAB_CHANGELOG = "ChangeLog"
TAB_SCHEMA = "Schema"
TAB_META = "_Meta"  # reserved for future use

# Schema/header version — bump if you change HEADER
SYNC_VERSION = 1

# One-time warning flags per process
_WARNED_MISSING_GOOGLE = False
_WARNED_MISSING_CREDS = False

# ---------------------------------------------------------------------------
# MAIN TAB HEADER (future-proof, stable order)
# ---------------------------------------------------------------------------

HEADER = [
    "ReimbID",              # A
    "EmployeeID",           # B
    "EmployeeName",         # C
    "Department",           # D
    "CategoryList",         # E
    "LineCount",            # F
    "TotalAmount",          # G
    "Currency",             # H
    "SubmittedAt",          # I
    "Status",               # J
    "StatusUpdatedAt",      # K
    "ManagerUsername",      # L
    "ManagerDecisionAt",    # M
    "ManagementUsername",   # N
    "ManagementDecisionAt", # O
    "FinanceVerifier",      # P
    "FinanceVerifiedAt",    # Q
    "FinanceRef",           # R
    "PaidAt",               # S
    "RejectedReason",       # T
    "FinanceNote",          # U
    "ReceiptURLs",          # V
    "InternalRecordURL",    # W
    "CreatedAt",            # X
    "UpdatedAt",            # Y
    "SyncedAt",             # Z
    "SyncVersion",          # AA
    "ExtraJSON",            # AB
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

def ensure_spreadsheet_structure() -> None:
    """
    Ensure required tabs exist with header rows, frozen row, and widths.
    Safe & idempotent. No-ops if Google unavailable.
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
                        "gridProperties": {"rowCount": 1000, "columnCount": 40},
                    }
                }
            })

    if requests:
        _batch_update(requests)
        existing = _get_sheet_map()

    # Freeze header rows & set widths
    requests = []
    for title in [TAB_MAIN, TAB_CHANGELOG, TAB_SCHEMA]:
        sid = existing.get(title)
        if sid is None:
            continue
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        })
        if title == TAB_MAIN:
            widths = {
                1: 100, 2: 110, 3: 180, 4: 160, 5: 160, 6: 100, 7: 120, 8: 80, 9: 180,
                10: 140, 11: 180, 12: 160, 13: 180, 14: 170, 15: 180, 16: 160, 17: 180,
                18: 160, 19: 180, 20: 220, 21: 220, 22: 280, 23: 160, 24: 180, 25: 180,
                26: 180, 27: 110, 28: 260,
            }
            for idx, px in widths.items():
                requests.append({
                    "updateDimensionProperties": {
                        "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": idx - 1, "endIndex": idx},
                        "properties": {"pixelSize": px},
                        "fields": "pixelSize",
                    }
                })
        else:
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 10},
                    "properties": {"pixelSize": 160},
                    "fields": "pixelSize",
                }
            })
    _batch_update(requests)

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

    # Schema heartbeat (nice to have)
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
        # ignore if sheet protected / concurrent
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
        "EmployeeName": (
            f"{getattr(employee,'first_name','')} {getattr(employee,'last_name','')}".strip()
            or getattr(employee, "username", "")
            or f"User #{getattr(employee, 'id', '')}"
        ),
        "Department": dept,
        "CategoryList": cats,
        "LineCount": line_count,
        "TotalAmount": float(req.total_amount or 0),
        "Currency": "INR",
        "SubmittedAt": _iso(req.submitted_at),
        "Status": req.status,
        "StatusUpdatedAt": _iso(req.updated_at),
        "ManagerUsername": manager_un,
        "ManagerDecisionAt": _iso(req.manager_decided_at),
        "ManagementUsername": management_un,
        "ManagementDecisionAt": _iso(req.management_decided_at),
        "FinanceVerifier": finance_un,
        "FinanceVerifiedAt": _iso(req.verified_at),
        "FinanceRef": req.finance_payment_reference or "",
        "PaidAt": _iso(req.paid_at),
        "RejectedReason": req.management_comment if req.status == req.Status.REJECTED else "",
        "FinanceNote": req.finance_note or "",
        "ReceiptURLs": receipts_csv,
        "InternalRecordURL": f'=HYPERLINK("{_detail_url(req.id)}","Open in EMS")',
        "CreatedAt": _iso(req.created_at),
        "UpdatedAt": _iso(req.updated_at),
        "SyncedAt": _iso(datetime.now(timezone.utc)),
        "SyncVersion": SYNC_VERSION,
        "ExtraJSON": json.dumps(extra, ensure_ascii=False),
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

def upsert_row(row: list, reimb_id: int) -> Tuple[str, int]:
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
    Ensures tabs/headers first. No-ops if google libs/creds missing.
    """
    if not _google_available() or req is None:
        return

    # Ensure spreadsheet structure
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
