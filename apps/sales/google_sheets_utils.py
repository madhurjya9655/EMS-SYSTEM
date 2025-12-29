import os
import logging
from typing import Tuple, List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

logger = logging.getLogger(__name__)

# ---------------------- ENV handling (DO NOT hardcode IDs) ---------------------- #
# Primary (new) + fallbacks to match your last message & legacy names
SHEET_ID = (
    os.getenv("GOOGLE_SHEET_ID_KAM_SALES")
    or os.getenv("GOOGLE_SHEET_ID1")
    or os.getenv("GOOGLE_SHEET_ID")
    or os.getenv("GOOGLE_GOOGLE_SHEET_ID")
    or ""
)
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
# Read scope (kept for existing views that only read)
SCOPES_READ = [
    os.getenv("GOOGLE_SHEET_SCOPES")
    or "https://www.googleapis.com/auth/spreadsheets.readonly"
]
# Write scope (internal for upserts). We intentionally *do not* rely on env for this,
# because a readonly scope there would silently block writes. We request write scope here;
# if credentials file disallows or Render env forbids, we will log and skip updates safely.
SCOPES_WRITE = ["https://www.googleapis.com/auth/spreadsheets"]

# Tabs (canonical names per spec)
TAB_CUSTOMERS = "Customer_Master"
TAB_SALES = "Sales_Data"
TAB_LEADS = "Leads_Data"
TAB_TARGETS = "Targets_Plan"


# ---------------------- Client factories ---------------------- #
def _build_credentials(scopes: List[str]) -> Credentials | None:
    try:
        if not SERVICE_ACCOUNT_FILE:
            logger.error("Google Sheets: SERVICE_ACCOUNT_FILE not set.")
            return None
        return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    except Exception as e:
        logger.error("Google Sheets: failed to build credentials: %s", e)
        return None


def _client_read() -> gspread.Client | None:
    creds = _build_credentials(SCOPES_READ)
    if not creds:
        return None
    try:
        return gspread.authorize(creds)
    except Exception as e:
        logger.error("Google Sheets: authorize(read) failed: %s", e)
        return None


def _client_write() -> gspread.Client | None:
    creds = _build_credentials(SCOPES_WRITE)
    if not creds:
        return None
    try:
        return gspread.authorize(creds)
    except Exception as e:
        logger.error("Google Sheets: authorize(write) failed: %s", e)
        return None


# ---------------------- Worksheet helpers ---------------------- #
def _open_sheet(client: gspread.Client) -> gspread.Spreadsheet | None:
    try:
        if not SHEET_ID:
            logger.error("Google Sheets: SHEET_ID not configured.")
            return None
        return client.open_by_key(SHEET_ID)
    except Exception as e:
        logger.error("Google Sheets: open_by_key failed: %s", e)
        return None


def get_worksheet(title: str, write: bool = False):
    client = _client_write() if write else _client_read()
    if not client:
        return None
    sh = _open_sheet(client)
    if not sh:
        return None
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        logger.error("Google Sheets: worksheet '%s' not found.", title)
        return None
    except Exception as e:
        logger.error("Google Sheets: get worksheet '%s' failed: %s", title, e)
        return None


# ---------------------- Public read APIs used by existing forms/views ---------------------- #
def get_all_sheet_data(tab_name: str) -> Tuple[List[Dict[str, Any]], str | None]:
    ws = get_worksheet(tab_name, write=False)
    if ws is None:
        return [], f"Sheet/tab '{tab_name}' not found or not readable."
    try:
        data = ws.get_all_records()
        return data, None
    except Exception as e:
        return [], f"Could not fetch data from '{tab_name}': {str(e)}"


def normalize_name(name: Any) -> str:
    return str(name or "").strip().lower().replace(" ", "")


def get_sheet_data_for_user(tab_name: str, user_full_name: str):
    data, error = get_all_sheet_data(tab_name)
    if error:
        return [], error
    key = normalize_name(user_full_name)
    out = []
    for row in data:
        row_kam = normalize_name(row.get("KAM Name") or row.get("KAM_Name") or "")
        if row_kam == key:
            out.append(row)
    return out, None


def filter_rows(rows: List[Dict[str, Any]], **kwargs):
    filtered = rows
    for k, v in kwargs.items():
        filtered = [r for r in filtered if str(r.get(k, "")).strip() == str(v).strip()]
    return filtered


def get_unique_customer_names_from_sheet(tab_name="Sheet1", kam_name=None):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    names = set()
    for row in data:
        if kam_name:
            row_kam = row.get('KAM Name') or row.get('KAM_Name') or row.get('kam') or ''
            if normalize_name(row_kam) != normalize_name(kam_name):
                continue
        nm = row.get('Customer Name') or row.get('Customer_Name') or row.get('customer_name')
        if nm:
            names.add(str(nm).strip())
    return [(n, n) for n in sorted(names)]


def get_unique_kam_names_from_sheet(tab_name="Sheet1"):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    names = set()
    for row in data:
        nm = row.get('KAM Name') or row.get('KAM_Name') or row.get('kam_name') or row.get('kam')
        if nm:
            names.add(str(nm).strip())
    return [(n, n) for n in sorted(names)]


def get_unique_location_from_sheet(tab_name="Sheet1"):
    keys = ["Location", "location", "Dispatch From", "dispatch_from"]
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    vals = set()
    for row in data:
        v = None
        for k in keys:
            if row.get(k):
                v = row.get(k)
                break
        if v:
            vals.add(str(v).strip())
    return [(v, v) for v in sorted(vals)]


def get_unique_column_values_from_sheet(column_name, tab_name="Sheet1"):
    data, error = get_all_sheet_data(tab_name)
    if error or not data:
        return []
    keys = [column_name, column_name.replace(" ", "_"), column_name.lower(), column_name.title()]
    vals = set()
    for row in data:
        v = None
        for k in keys:
            if row.get(k):
                v = row.get(k)
                break
        if v:
            vals.add(str(v).strip())
    return [(v, v) for v in sorted(vals)]


# ---------------------- Write/UPSERT helpers (non-blocking) ---------------------- #
def _header_index_map(headers: List[str]) -> dict:
    return {h.strip(): idx for idx, h in enumerate(headers, start=1)}


def _find_row_index(ws, match_col_idx: int, match_value: str) -> int | None:
    """
    Return 1-based row index where the 'match_value' exists in column 'match_col_idx'.
    Skips header row (assumed row 1).
    """
    try:
        col_vals = ws.col_values(match_col_idx)
    except Exception as e:
        logger.error("Google Sheets: col_values failed: %s", e)
        return None
    for i, val in enumerate(col_vals[1:], start=2):
        if str(val).strip() == str(match_value).strip():
            return i
    return None


def upsert_customer_master(payload: dict) -> None:
    """
    payload keys must match sheet headers exactly:
      Customer Name, KAM Name, Address, Email, Mobile No, Person Name, Pincode,
      Type, GST Number, Credit Limit, Agreed Credit Period, Total Exposure (₹),
      Overdues (₹), NBD Flag
    """
    ws = get_worksheet(TAB_CUSTOMERS, write=True)
    if not ws:
        logger.warning("Customer_Master upsert skipped: worksheet not available.")
        return
    try:
        headers = ws.row_values(1)
        hmap = _header_index_map(headers)

        # Find by Customer Name (unique)
        key = str(payload.get("Customer Name", "")).strip()
        if not key:
            logger.warning("Customer_Master upsert skipped: empty Customer Name.")
            return

        row_idx = None
        if "Customer Name" in hmap:
            row_idx = _find_row_index(ws, hmap["Customer Name"], key)

        values_row = []
        for col_name in headers:
            values_row.append(payload.get(col_name, ""))

        if row_idx:
            ws.update(f"A{row_idx}:{gspread.utils.rowcol_to_a1(row_idx, len(headers)).replace(str(row_idx),'')}{row_idx}", [values_row])
        else:
            ws.append_row(values_row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error("Customer_Master upsert failed: %s", e)


def upsert_sales_data(payload: dict) -> None:
    """
    payload keys:
      KAM Name, Invoice Date, Quantity (MT), Revenue (₹ with GST)
    UPSERT key: (KAM Name + Invoice Date)
    """
    ws = get_worksheet(TAB_SALES, write=True)
    if not ws:
        logger.warning("Sales_Data upsert skipped: worksheet not available.")
        return
    try:
        headers = ws.row_values(1)
        hmap = _header_index_map(headers)

        kam = str(payload.get("KAM Name", "")).strip()
        inv_date = str(payload.get("Invoice Date", "")).strip()
        if not kam or not inv_date:
            logger.warning("Sales_Data upsert skipped: missing key KAM/Date.")
            return

        # Build a join key column on the fly if present in sheet; else find by scanning both cols
        row_idx = None
        if "KAM Name" in hmap and "Invoice Date" in hmap:
            # scan rows (efficient enough; avoids requiring an extra hidden key column)
            kam_col = ws.col_values(hmap["KAM Name"])
            date_col = ws.col_values(hmap["Invoice Date"])
            # align lengths
            n = max(len(kam_col), len(date_col))
            kam_col += [""] * (n - len(kam_col))
            date_col += [""] * (n - len(date_col))
            for i in range(1, n):  # skip header @ index 0
                if str(kam_col[i]).strip() == kam and str(date_col[i]).strip() == inv_date:
                    row_idx = i + 1
                    break

        values_row = [payload.get(col, "") for col in headers]

        if row_idx:
            ws.update(f"A{row_idx}:{gspread.utils.rowcol_to_a1(row_idx, len(headers)).replace(str(row_idx),'')}{row_idx}", [values_row])
        else:
            ws.append_row(values_row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error("Sales_Data upsert failed: %s", e)


def upsert_leads_data(payload: dict) -> None:
    """
    payload keys:
      Month, Week, Date of Enquiry, KAM Name, Customer Name, Quantity, Status, Remarks, Grade, Size
    UPSERT key: (KAM Name + Date of Enquiry + Customer Name)
    """
    ws = get_worksheet(TAB_LEADS, write=True)
    if not ws:
        logger.warning("Leads_Data upsert skipped: worksheet not available.")
        return
    try:
        headers = ws.row_values(1)
        hmap = _header_index_map(headers)

        kam = str(payload.get("KAM Name", "")).strip()
        doe = str(payload.get("Date of Enquiry", "")).strip()
        cust = str(payload.get("Customer Name", "")).strip()
        if not (kam and doe and cust):
            logger.warning("Leads_Data upsert skipped: missing KAM/Date/Customer.")
            return

        row_idx = None
        if all(k in hmap for k in ("KAM Name", "Date of Enquiry", "Customer Name")):
            kam_col = ws.col_values(hmap["KAM Name"])
            date_col = ws.col_values(hmap["Date of Enquiry"])
            cust_col = ws.col_values(hmap["Customer Name"])
            n = max(len(kam_col), len(date_col), len(cust_col))
            kam_col += [""] * (n - len(kam_col))
            date_col += [""] * (n - len(date_col))
            cust_col += [""] * (n - len(cust_col))
            for i in range(1, n):
                if (
                    str(kam_col[i]).strip() == kam
                    and str(date_col[i]).strip() == doe
                    and str(cust_col[i]).strip() == cust
                ):
                    row_idx = i + 1
                    break

        values_row = [payload.get(col, "") for col in headers]

        if row_idx:
            ws.update(f"A{row_idx}:{gspread.utils.rowcol_to_a1(row_idx, len(headers)).replace(str(row_idx),'')}{row_idx}", [values_row])
        else:
            ws.append_row(values_row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error("Leads_Data upsert failed: %s", e)


# ---------------------- Targets sync (read-only pull) ---------------------- #
def pull_targets_plan_rows() -> List[Dict[str, Any]]:
    """
    Fetch Targets_Plan rows for manager-controlled targets.
    """
    ws = get_worksheet(TAB_TARGETS, write=False)
    if not ws:
        logger.warning("Targets_Plan fetch skipped: worksheet not available.")
        return []
    try:
        return ws.get_all_records()
    except Exception as e:
        logger.error("Targets_Plan fetch failed: %s", e)
        return []
