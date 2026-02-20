# apps/kam/sheets.py
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.db import transaction
from django.utils import timezone

from .models import Customer, InvoiceFact

logger = logging.getLogger(__name__)

# ----------------------------
# Configuration
# ----------------------------

DEFAULT_WORKSHEET_NAME = os.getenv("KAM_SALES_WORKSHEET", "Sales")
SHEET_ID_ENV = "KAM_SALES_SHEET_ID"
SERVICE_JSON_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON"  # typical name
SERVICE_JSON_ALT_ENV = "GOOGLE_SERVICE_ACCOUNT"   # alternate some people use

# If you store credentials in a file path instead of JSON content, optionally support:
SERVICE_JSON_PATH_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON_PATH"

# ----------------------------
# Helpers
# ----------------------------

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def _load_service_account_info() -> Dict[str, Any]:
    """
    Loads Google service account credentials either from:
      - GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON string)
      - GOOGLE_SERVICE_ACCOUNT (raw JSON string)
      - GOOGLE_SERVICE_ACCOUNT_JSON_PATH (path to JSON file)
    """
    raw = os.getenv(SERVICE_JSON_ENV) or os.getenv(SERVICE_JSON_ALT_ENV)
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Service account JSON in {SERVICE_JSON_ENV}/{SERVICE_JSON_ALT_ENV} is not valid JSON: {e}"
            ) from e

    path = os.getenv(SERVICE_JSON_PATH_ENV)
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except OSError as e:
            raise RuntimeError(f"Cannot read service account JSON file at {path}: {e}") from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Service account JSON file at {path} is invalid JSON: {e}") from e

    raise RuntimeError(
        f"Missing Google service account credentials. Provide one of: "
        f"{SERVICE_JSON_ENV}, {SERVICE_JSON_ALT_ENV}, or {SERVICE_JSON_PATH_ENV}."
    )


def _get_gspread_client():
    """
    Import gspread lazily so this module doesn't crash on import
    if gspread isn't installed in some environments (e.g., during migrations/tests).
    """
    try:
        import gspread  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "gspread is not installed. Add it to your requirements (e.g., `gspread` + `google-auth`)."
        ) from e

    from google.oauth2.service_account import Credentials  # type: ignore

    info = _load_service_account_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _open_worksheet(sheet_id: str, worksheet_name: str):
    gc = _get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(worksheet_name)


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return s


def _parse_decimal(v: Any) -> Optional[Decimal]:
    s = _norm_str(v)
    if not s:
        return None
    # common formatting cleanup: commas, currency symbols, etc.
    s = s.replace(",", "")
    for ch in ["₹", "$", "€", "£"]:
        s = s.replace(ch, "")
    s = s.strip()
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(v: Any) -> Optional[datetime]:
    """
    Accepts:
      - already-a-datetime
      - ISO-like strings
      - dd/mm/yyyy or dd-mm-yyyy (common in sheets)
    """
    if isinstance(v, datetime):
        return v if timezone.is_aware(v) else timezone.make_aware(v)

    s = _norm_str(v)
    if not s:
        return None

    # Try ISO first
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if timezone.is_aware(dt) else timezone.make_aware(dt)
    except ValueError:
        pass

    # Try common D/M/Y formats
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"):
        try:
            dt = datetime.strptime(s, fmt)
            return timezone.make_aware(dt)
        except ValueError:
            continue

    return None


def _stable_hash(parts: Sequence[Any]) -> str:
    """
    Deterministic 64-char hex hash (sha256) from row parts.
    Fits InvoiceFact.row_uuid max_length=64.
    """
    material = "||".join(_norm_str(p) for p in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _safe_row_uuid(inv_no: str, *fallback_parts: Any) -> str:
    """
    Fix for the 'row_uuid overflow' issue:
      - If inv_no exists, do NOT store it raw unless it fits in 64.
      - Use sha256(inv_no) to keep stable uniqueness and max_length=64.
    """
    inv_no = _norm_str(inv_no)
    if inv_no:
        # always hash invoice numbers for safety + consistent length
        return hashlib.sha256(inv_no.encode("utf-8")).hexdigest()
    return _stable_hash(fallback_parts)


@dataclass(frozen=True)
class SheetRow:
    row_uuid: str
    invoice_no: str
    invoice_date: Optional[datetime]
    customer_name: str
    amount: Optional[Decimal]
    raw: Dict[str, Any]


# ----------------------------
# Core import logic
# ----------------------------

def _fetch_sheet_records(sheet_id: str, worksheet_name: str) -> List[Dict[str, Any]]:
    ws = _open_worksheet(sheet_id, worksheet_name)
    # records are dicts keyed by header row labels
    records: List[Dict[str, Any]] = ws.get_all_records()
    return records


def _map_row(rec: Dict[str, Any]) -> Optional[SheetRow]:
    """
    Maps one worksheet record to our normalized internal structure.
    Adjust these header keys to your actual sheet columns.
    """
    # Common header variants
    inv_no = _norm_str(rec.get("Invoice No") or rec.get("Invoice") or rec.get("Inv No") or rec.get("inv_no"))
    cust = _norm_str(rec.get("Customer") or rec.get("Customer Name") or rec.get("Party") or rec.get("customer"))
    amt = _parse_decimal(rec.get("Amount") or rec.get("Total") or rec.get("Net") or rec.get("amount"))
    dt = _parse_date(rec.get("Date") or rec.get("Invoice Date") or rec.get("invoice_date"))

    # Skip clearly empty / unusable rows
    if not inv_no and not cust and amt is None and dt is None:
        return None

    # If customer is missing, we can still import invoice facts if your model allows null customer.
    # But your model is PROTECT and non-null, so we must skip such rows safely.
    if not cust:
        logger.warning("Skipping row because customer is blank. Record=%s", rec)
        return None

    row_uuid = _safe_row_uuid(inv_no, cust, amt, dt)

    return SheetRow(
        row_uuid=row_uuid,
        invoice_no=inv_no,
        invoice_date=dt,
        customer_name=cust,
        amount=amt,
        raw=rec,
    )


@transaction.atomic
def import_sales_records(records: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Upserts InvoiceFact rows based on row_uuid.

    Returns: (created_count, updated_count)
    """
    created = 0
    updated = 0

    for rec in records:
        row = _map_row(rec)
        if row is None:
            continue

        customer, _ = Customer.objects.get_or_create(
            name=row.customer_name,
            defaults={"created_at": timezone.now()} if hasattr(Customer, "created_at") else None,
        )

        defaults: Dict[str, Any] = {
            "customer": customer,
        }

        # Only set fields that exist in your model; keep this conservative.
        if hasattr(InvoiceFact, "invoice_no"):
            defaults["invoice_no"] = row.invoice_no
        if hasattr(InvoiceFact, "invoice_date"):
            defaults["invoice_date"] = row.invoice_date
        if hasattr(InvoiceFact, "amount"):
            defaults["amount"] = row.amount

        obj, was_created = InvoiceFact.objects.update_or_create(
            row_uuid=row.row_uuid,
            defaults=defaults,
        )

        if was_created:
            created += 1
        else:
            updated += 1

    return created, updated


def run_sync_now(
    *,
    worksheet_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Full sync: reads all worksheet records and upserts them.

    Designed to be called from a view or management command.
    """
    sheet_id = _require_env(SHEET_ID_ENV)
    ws_name = worksheet_name or DEFAULT_WORKSHEET_NAME

    records = _fetch_sheet_records(sheet_id, ws_name)
    created, updated = import_sales_records(records)

    result = {
        "worksheet": ws_name,
        "sheet_id": sheet_id,
        "records_seen": len(records),
        "created": created,
        "updated": updated,
        "timestamp": timezone.now().isoformat(),
    }
    logger.info("KAM sheet sync complete: %s", result)
    return result


def step_sync(
    *,
    worksheet_name: Optional[str] = None,
    max_rows: int = 200,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Incremental-ish sync: reads all records, but only processes a slice.
    This is useful when you want to chunk work in a background job.

    Note: Google Sheets API doesn't provide true pagination for get_all_records(),
    so we fetch all and slice in memory; for very large sheets, switch to get_values().
    """
    sheet_id = _require_env(SHEET_ID_ENV)
    ws_name = worksheet_name or DEFAULT_WORKSHEET_NAME

    records = _fetch_sheet_records(sheet_id, ws_name)
    total = len(records)

    batch = records[offset : offset + max_rows]
    created, updated = import_sales_records(batch)

    next_offset = offset + len(batch)
    done = next_offset >= total

    result = {
        "worksheet": ws_name,
        "sheet_id": sheet_id,
        "records_seen": total,
        "processed": len(batch),
        "offset": offset,
        "next_offset": None if done else next_offset,
        "done": done,
        "created": created,
        "updated": updated,
        "timestamp": timezone.now().isoformat(),
    }
    logger.info("KAM sheet step sync: %s", result)
    return result