# FILE: apps/kam/sheets.py

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.db import transaction
from django.utils import timezone

from apps.common.google_auth import GoogleCredentialError, get_google_credentials
from .models import Customer, InvoiceFact

logger = logging.getLogger(__name__)

DEFAULT_WORKSHEET_NAME = os.getenv("KAM_SALES_WORKSHEET", "Sales")
SHEET_ID_ENV = "KAM_SALES_SHEET_ID"


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def _get_gspread_client():
    try:
        import gspread  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError("gspread is not installed. Add `gspread` + `google-auth` to requirements.") from e

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = get_google_credentials(scopes=scopes)
    return gspread.authorize(creds)


def _open_worksheet(sheet_id: str, worksheet_name: str):
    gc = _get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(worksheet_name)


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _parse_decimal(v: Any) -> Optional[Decimal]:
    s = _norm_str(v)
    if not s:
        return None
    s = s.replace(",", "").strip()
    for ch in ["₹", "$", "€", "£"]:
        s = s.replace(ch, "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v if timezone.is_aware(v) else timezone.make_aware(v)

    s = _norm_str(v)
    if not s:
        return None

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if timezone.is_aware(dt) else timezone.make_aware(dt)
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"):
        try:
            dt = datetime.strptime(s, fmt)
            return timezone.make_aware(dt)
        except ValueError:
            continue

    return None


def _stable_hash(parts: Sequence[Any]) -> str:
    material = "||".join(_norm_str(p) for p in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _safe_row_uuid(inv_no: str, *fallback_parts: Any) -> str:
    inv_no = _norm_str(inv_no)
    if inv_no:
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


def _fetch_sheet_records(sheet_id: str, worksheet_name: str) -> List[Dict[str, Any]]:
    ws = _open_worksheet(sheet_id, worksheet_name)
    return ws.get_all_records()  # type: ignore[no-any-return]


def _map_row(rec: Dict[str, Any]) -> Optional[SheetRow]:
    inv_no = _norm_str(rec.get("Invoice No") or rec.get("Invoice") or rec.get("Inv No") or rec.get("inv_no"))
    cust = _norm_str(rec.get("Customer") or rec.get("Customer Name") or rec.get("Party") or rec.get("customer"))
    amt = _parse_decimal(rec.get("Amount") or rec.get("Total") or rec.get("Net") or rec.get("amount"))
    dt = _parse_date(rec.get("Date") or rec.get("Invoice Date") or rec.get("invoice_date"))

    if not inv_no and not cust and amt is None and dt is None:
        return None
    if not cust:
        logger.warning("Skipping row because customer is blank. Record=%s", rec)
        return None

    row_uuid = _safe_row_uuid(inv_no, cust, amt, dt)
    return SheetRow(row_uuid=row_uuid, invoice_no=inv_no, invoice_date=dt, customer_name=cust, amount=amt, raw=rec)


@transaction.atomic
def import_sales_records(records: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    created = 0
    updated = 0

    for rec in records:
        row = _map_row(rec)
        if row is None:
            continue

        customer, _ = Customer.objects.get_or_create(name=row.customer_name)

        defaults: Dict[str, Any] = {"customer": customer}

        if hasattr(InvoiceFact, "invoice_no"):
            defaults["invoice_no"] = row.invoice_no
        if hasattr(InvoiceFact, "invoice_date"):
            defaults["invoice_date"] = row.invoice_date.date() if row.invoice_date else None  # models.py uses DateField
        if hasattr(InvoiceFact, "amount"):
            defaults["amount"] = row.amount

        obj, was_created = InvoiceFact.objects.update_or_create(row_uuid=row.row_uuid, defaults=defaults)

        if was_created:
            created += 1
        else:
            updated += 1

    return created, updated


def run_sync_now(*, worksheet_name: Optional[str] = None) -> Dict[str, Any]:
    sheet_id = _require_env(SHEET_ID_ENV)
    ws_name = worksheet_name or DEFAULT_WORKSHEET_NAME

    try:
        records = _fetch_sheet_records(sheet_id, ws_name)
    except GoogleCredentialError as e:
        raise RuntimeError(str(e)) from e

    created, updated = import_sales_records(records)

    return {
        "worksheet": ws_name,
        "sheet_id": sheet_id,
        "records_seen": len(records),
        "created": created,
        "updated": updated,
        "timestamp": timezone.now().isoformat(),
    }