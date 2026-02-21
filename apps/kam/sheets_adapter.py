# FILE: apps/kam/sheets_adapter.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from hashlib import md5
from typing import Dict, List, Optional, Tuple

import gspread
from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from apps.common.google_auth import GoogleCredentialError, get_google_credentials
from .models import Customer, InvoiceFact, LeadFact, OverdueSnapshot, SyncIntent

User = get_user_model()


def _getenv(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _open_sheet():
    try:
        # Keep existing scopes behavior if you set GOOGLE_SHEET_SCOPES
        scopes_raw = (_getenv("GOOGLE_SHEET_SCOPES") or "").strip()
        scopes = [s.strip() for s in scopes_raw.split(",") if s.strip()] if scopes_raw else None

        creds = get_google_credentials(scopes=scopes or ["https://www.googleapis.com/auth/spreadsheets.readonly"])
    except GoogleCredentialError as e:
        raise RuntimeError(str(e)) from e

    gc = gspread.authorize(creds)
    sheet_id = _getenv("KAM_SALES_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("KAM_SALES_SHEET_ID missing")
    return gc.open_by_key(sheet_id)


def _norm_header(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("’", "'")
    return s


def _datefmt() -> str:
    return _getenv("KAM_DATE_FMT", "%d-%m-%Y")


def _dry_run() -> bool:
    return _getenv("KAM_IMPORT_DRY_RUN", "0") in ("1", "true", "True", "YES", "yes")


def _usermap() -> Dict[str, str]:
    raw = _getenv("KAM_USERMAP_JSON", "{}")
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    for fmt in (_datefmt(), "%d/%m/%Y", "%d-%m-%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _to_decimal(s) -> Decimal:
    if s is None:
        return Decimal(0)
    if isinstance(s, (int, float, Decimal)):
        return Decimal(str(s))
    s = str(s).strip()
    if not s:
        return Decimal(0)
    s = s.replace(",", "").replace(" ", "")
    s = re.sub(r"[₹$]", "", s)
    try:
        return Decimal(s)
    except Exception:
        return Decimal(0)


def _to_int(s) -> int:
    try:
        return int(Decimal(str(s)))
    except Exception:
        return 0


def _hash_row(*parts: str) -> str:
    base = "||".join([p if p is not None else "" for p in parts])
    return md5(base.encode("utf-8")).hexdigest()


def _find_user_by_map(display_name: str, usermap: Dict[str, str]) -> Optional[User]:
    if not display_name:
        return None
    username = usermap.get(display_name.strip())
    if not username:
        try:
            return User.objects.get(username=display_name.strip())
        except User.DoesNotExist:
            return None
    try:
        return User.objects.get(username=username)
    except User.DoesNotExist:
        return None


def _read_tab(ws_name: str) -> Tuple[List[str], List[List[str]]]:
    sh = _open_sheet()
    try:
        ws = sh.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Worksheet not found: {ws_name}")
    values = ws.get_all_values()
    if not values:
        return [], []
    headers = [_norm_header(h) for h in values[0]]
    rows = values[1:]
    return headers, rows


def _index(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _col(idx: Dict[str, int], *names: str) -> Optional[int]:
    norm = [_norm_header(n) for n in names if n]
    for n in norm:
        if n in idx:
            return idx[n]
    return None


@dataclass
class ImportStats:
    customers_upserted: int = 0
    sales_upserted: int = 0
    leads_upserted: int = 0
    overdues_upserted: int = 0
    skipped: int = 0
    unknown_kam: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def add_unknown(self, name: str):
        if name and name not in self.unknown_kam:
            self.unknown_kam.append(name)

    def as_message(self) -> str:
        parts = [
            f"Customers: {self.customers_upserted}",
            f"Sales rows: {self.sales_upserted}",
            f"Leads rows: {self.leads_upserted}",
            f"Overdue snapshots: {self.overdues_upserted}",
            f"Skipped: {self.skipped}",
        ]
        if self.unknown_kam:
            parts.append("Unknown KAM(s): " + ", ".join(self.unknown_kam[:6]) + ("…" if len(self.unknown_kam) > 6 else ""))
        if self.notes:
            parts.append("Notes: " + " | ".join(self.notes[:4]) + ("…" if len(self.notes) > 4 else ""))
        return " | ".join(parts)


# ---- importers below unchanged (uses _open_sheet) ----

def import_customers(stats: ImportStats):
    tab = _getenv("KAM_TAB_CUSTOMERS", "Customer Details")
    headers, rows = _read_tab(tab)
    if not headers:
        stats.notes.append("Customers: empty sheet")
        return
    idx = _index(headers)
    c_customer = _col(idx, "Customer Name", "Customer", "name")
    c_kam = _col(idx, "KAM Name", "KAM", "primary_kam_username")
    if c_customer is None or c_kam is None:
        stats.notes.append("Customers: required columns missing (Customer Name / KAM Name)")
        return
    c_addr = _col(idx, "Address", "address")
    c_email = _col(idx, "Email", "email")
    c_mobile = _col(idx, "Mobile No", "Mobile", "mobile")
    c_credit_limit = _col(idx, "Credit Limit", "credit_limit")
    c_credit_days = _col(idx, "Agreed Credit Period", "agreed_credit_period_days", "Agreed Credit Period ")
    usermap = _usermap()
    dry = _dry_run()

    for r in rows:
        name = (r[c_customer] if c_customer is not None and c_customer < len(r) else "").strip()
        if not name:
            stats.skipped += 1
            continue
        kam_disp = r[c_kam] if c_kam is not None and c_kam < len(r) else ""
        kam = _find_user_by_map(kam_disp, usermap)
        if not kam and kam_disp:
            stats.add_unknown(kam_disp)

        defaults = {}
        addr = r[c_addr] if c_addr is not None and c_addr < len(r) else None
        email = r[c_email] if c_email is not None and c_email < len(r) else None
        mobile = r[c_mobile] if c_mobile is not None and c_mobile < len(r) else None
        credit_limit = _to_decimal(r[c_credit_limit]) if c_credit_limit is not None and c_credit_limit < len(r) else Decimal(0)
        credit_days = _to_int(r[c_credit_days]) if c_credit_days is not None and c_credit_days < len(r) else 0

        if addr:
            defaults["address"] = addr
        if email:
            defaults["email"] = email
        if mobile:
            defaults["mobile"] = mobile
        defaults["credit_limit"] = credit_limit
        defaults["agreed_credit_period_days"] = credit_days

        if dry:
            stats.customers_upserted += 1
            continue

        with transaction.atomic():
            cust, _ = Customer.objects.get_or_create(name=name, defaults=defaults)
            changed = False
            for k, v in defaults.items():
                if getattr(cust, k) != v:
                    setattr(cust, k, v)
                    changed = True
            if kam and cust.primary_kam_id != kam.id:
                cust.primary_kam = kam
                changed = True
            if changed:
                cust.save()
            stats.customers_upserted += 1


def import_sales(stats: ImportStats):
    tab = _getenv("KAM_SALES_TAB", "Sheet1")
    headers, rows = _read_tab(tab)
    if not headers:
        stats.notes.append("Sales: empty sheet")
        return
    idx = _index(headers)
    c_kam = _col(idx, "KAM Name", "KAM", "kam_username")
    c_customer = _col(idx, "Customer Name", "Consignee Name", "Buyer's Name", "Buyer’s Name", "Buyer\'s Name", "customer_name")
    c_date = _col(idx, "Invoice Date", "Date of Invoice", "invoice_date", "Date")
    c_qty = _col(idx, "QTY", "Qty(MT)", "Quantity", "qty_mt")
    c_val = _col(idx, "Invoice Value With GST", "Invoice Value with GST", "Invoice Value", "revenue_gst")
    c_invno = _col(idx, "Invoice Number", "Invoice No", "invoice_number")
    if None in (c_kam, c_date, c_val):
        stats.notes.append("Sales: required columns missing (KAM Name / Invoice Date / Invoice Value With GST)")
        return
    usermap = _usermap()
    dry = _dry_run()

    for r in rows:
        kam_disp = r[c_kam] if c_kam < len(r) else ""
        kam = _find_user_by_map(kam_disp, usermap)
        if not kam:
            stats.add_unknown(kam_disp)
            stats.skipped += 1
            continue
        inv_date = _parse_date(r[c_date] if c_date < len(r) else "")
        if not inv_date:
            stats.skipped += 1
            continue
        cust_name = (r[c_customer] if c_customer is not None and c_customer < len(r) else "").strip()
        if not cust_name:
            stats.skipped += 1
            continue
        qty_mt = _to_decimal(r[c_qty]) if c_qty is not None and c_qty < len(r) else Decimal(0)
        value_gst = _to_decimal(r[c_val] if c_val < len(r) else "0")
        inv_no = (r[c_invno] if c_invno is not None and c_invno < len(r) else "").strip()
        row_uuid = inv_no or _hash_row("sales", tab, cust_name, kam.username, str(inv_date), str(qty_mt), str(value_gst))
        if dry:
            stats.sales_upserted += 1
            continue
        with transaction.atomic():
            cust, _ = Customer.objects.get_or_create(name=cust_name)
            inv, created = InvoiceFact.objects.get_or_create(
                row_uuid=row_uuid,
                defaults=dict(
                    invoice_date=inv_date,
                    customer=cust,
                    kam=kam,
                    qty_mt=qty_mt,
                    revenue_gst=value_gst,
                ),
            )
            if not created:
                changed = False
                if inv.invoice_date != inv_date:
                    inv.invoice_date = inv_date
                    changed = True
                if inv.customer_id != cust.id:
                    inv.customer = cust
                    changed = True
                if inv.kam_id != kam.id:
                    inv.kam = kam
                    changed = True
                if inv.qty_mt != qty_mt:
                    inv.qty_mt = qty_mt
                    changed = True
                if inv.revenue_gst != value_gst:
                    inv.revenue_gst = value_gst
                    changed = True
                if changed:
                    inv.save()
            stats.sales_upserted += 1


def import_leads(stats: ImportStats):
    tab = _getenv("KAM_TAB_LEADS", "Enquiry (F)")
    headers, rows = _read_tab(tab)
    if not headers:
        stats.notes.append("Leads: empty sheet")
        return
    idx = _index(headers)
    c_ts = _col(idx, "Timestamp", "Date of Enquiry", "doe")
    c_kam = _col(idx, "KAM Name", "KAM", "kam_username")
    c_customer = _col(idx, "Customer Name", "customer_name")
    c_qty = _col(idx, "Qty (MT)", "QTY", "Qty", "qty_mt")
    c_status = _col(idx, "Status", "status")
    c_remarks = _col(idx, "Remarks", "remarks")
    c_grade = _col(idx, "Grade", "grade")
    c_size = _col(idx, "Size", "Size(MM)", "size")
    if None in (c_ts, c_kam, c_qty, c_status):
        stats.notes.append("Leads: required columns missing (Timestamp/KAM Name/Qty (MT)/Status)")
        return
    usermap = _usermap()
    dry = _dry_run()

    for r in rows:
        kam_disp = r[c_kam] if c_kam < len(r) else ""
        kam = _find_user_by_map(kam_disp, usermap)
        if not kam:
            stats.add_unknown(kam_disp)
            stats.skipped += 1
            continue
        doe = _parse_date(r[c_ts] if c_ts < len(r) else "")
        if not doe:
            stats.skipped += 1
            continue
        qty_mt = _to_decimal(r[c_qty] if c_qty < len(r) else "0")
        status = (r[c_status] if c_status < len(r) else "").strip().upper()
        if status not in {"OPEN", "NEGOTIATION", "WON", "LOST"}:
            status = "OPEN"
        cust_name = (r[c_customer] if c_customer is not None and c_customer < len(r) else "").strip()
        grade = (r[c_grade] if c_grade is not None and c_grade < len(r) else None) or None
        size = (r[c_size] if c_size is not None and c_size < len(r) else None) or None
        remarks = (r[c_remarks] if c_remarks is not None and c_remarks < len(r) else None) or None
        row_uuid = _hash_row("lead", tab, kam.username, str(doe), cust_name, str(qty_mt), status, str(grade), str(size))
        if dry:
            stats.leads_upserted += 1
            continue
        with transaction.atomic():
            cust = None
            if cust_name:
                cust, _ = Customer.objects.get_or_create(name=cust_name)
            LeadFact.objects.update_or_create(
                row_uuid=row_uuid,
                defaults=dict(
                    doe=doe,
                    kam=kam,
                    customer=cust,
                    qty_mt=qty_mt,
                    status=status,
                    grade=grade,
                    size=size,
                    remarks=remarks,
                ),
            )
            stats.leads_upserted += 1


def import_overdues(stats: ImportStats):
    tab = _getenv("KAM_TAB_OVERDUES", "Overdues")
    headers, rows = _read_tab(tab)
    if not headers:
        stats.notes.append("Overdues: empty sheet")
        return
    idx = _index(headers)
    c_customer = _col(idx, "Customer Name", "Customer", "customer_name")
    c_overdue = _col(idx, "Overdues (Rs)", "Overdue", "overdue")
    c_exposure = _col(idx, "Total Exposure (Rs)", "Exposure", "exposure")
    a0 = _col(idx, "0-30", "ageing_0_30")
    a31 = _col(idx, "31-60", "ageing_31_60")
    a61 = _col(idx, "61-90", "ageing_61_90")
    a90 = _col(idx, "90+", "ageing_90_plus")
    if c_customer is None or c_overdue is None:
        stats.notes.append("Overdues: required columns missing (Customer Name/Overdues (Rs))")
        return
    dry = _dry_run()
    snap_date = timezone.localdate()
    totals: Dict[str, Dict[str, Decimal]] = {}

    for r in rows:
        cust_name = (r[c_customer] if c_customer < len(r) else "").strip()
        if not cust_name:
            continue
        cur = totals.setdefault(
            cust_name, {"overdue": Decimal(0), "exposure": Decimal(0), "a0": Decimal(0), "a31": Decimal(0), "a61": Decimal(0), "a90": Decimal(0)}
        )
        cur["overdue"] += _to_decimal(r[c_overdue] if c_overdue < len(r) else "0")
        cur["exposure"] += _to_decimal(r[c_exposure] if c_exposure is not None and c_exposure < len(r) else "0")
        cur["a0"] += _to_decimal(r[a0] if a0 is not None and a0 < len(r) else "0")
        cur["a31"] += _to_decimal(r[a31] if a31 is not None and a31 < len(r) else "0")
        cur["a61"] += _to_decimal(r[a61] if a61 is not None and a61 < len(r) else "0")
        cur["a90"] += _to_decimal(r[a90] if a90 is not None and a90 < len(r) else "0")

    for cust_name, vals in totals.items():
        if dry:
            stats.overdues_upserted += 1
            continue
        with transaction.atomic():
            cust, _ = Customer.objects.get_or_create(name=cust_name)
            OverdueSnapshot.objects.update_or_create(
                snapshot_date=snap_date,
                customer=cust,
                defaults=dict(
                    exposure=vals["exposure"],
                    overdue=vals["overdue"],
                    ageing_0_30=vals["a0"],
                    ageing_31_60=vals["a31"],
                    ageing_61_90=vals["a61"],
                    ageing_90_plus=vals["a90"],
                ),
            )
            stats.overdues_upserted += 1


def run_sync_now() -> ImportStats:
    stats = ImportStats()
    import_customers(stats)
    import_sales(stats)
    import_leads(stats)
    import_overdues(stats)
    return stats


BATCH_SIZE = 50

def _cursor(current: Optional[str], total_rows: int) -> Tuple[int, Optional[str]]:
    start = int(current) if current else 2
    end = min(start + BATCH_SIZE - 1, total_rows)
    new_cur = str(end + 1) if end < total_rows else None
    return start, new_cur


def _headers(values: List[List[str]]) -> List[str]:
    if not values:
        return []
    return [_norm_header(h) for h in values[0]]


def _idx(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _val(row: List[str], i: Optional[int]) -> str:
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _pick(idm: Dict[str, int], *names: str) -> Optional[int]:
    for n in names:
        n = _norm_header(n)
        if n in idm:
            return idm[n]
        for k in idm.keys():
            if re.fullmatch(n, k, re.IGNORECASE):
                return idm[k]
    return None


def _ws_by_name(tab: str):
    sh = _open_sheet()
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Worksheet not found: {tab}")


def _process_page(tab: str, cursor: Optional[str], handler) -> Tuple[int, Optional[str]]:
    ws = _ws_by_name(tab)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return 0, None
    total_rows = len(values)
    start, new_cur = _cursor(cursor, total_rows)
    headers = _headers(values)
    page_values = [headers] + values[start - 1 : min(start - 1 + BATCH_SIZE, total_rows)]
    processed = handler(page_values)
    return processed, new_cur


def _customers_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    cn = _pick(idm, "Customer Name", "Customer", "name")
    kamn = _pick(idm, "KAM Name", "KAM", "primary_kam_username")
    addr = _pick(idm, "Address", "address")
    email = _pick(idm, "Email", "email")
    mob = _pick(idm, "Mobile No", "Mobile", "mobile")
    cl = _pick(idm, "Credit Limit", "credit_limit")
    acp = _pick(idm, "Agreed Credit Period", "Agreed Credit Period ", "agreed_credit_period_days")
    usermap = _usermap()
    processed = 0
    with transaction.atomic():
        for r in values[1:]:
            name = _val(r, cn)
            if not name:
                continue
            defaults = {
                "address": _val(r, addr) or None,
                "email": _val(r, email) or None,
                "mobile": _val(r, mob) or None,
                "credit_limit": _to_decimal(_val(r, cl)),
                "agreed_credit_period_days": _to_int(_val(r, acp)),
            }
            obj, _ = Customer.objects.get_or_create(name=name, defaults=defaults)
            changed = False
            for f, v in defaults.items():
                if getattr(obj, f) != v:
                    setattr(obj, f, v)
                    changed = True
            kam = _find_user_by_map(_val(r, kamn), usermap)
            if kam and obj.primary_kam_id != kam.id:
                obj.primary_kam = kam
                changed = True
            if changed:
                obj.save()
            processed += 1
    return processed


def _invoices_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    kamn = _pick(idm, "KAM Name", "KAM", "kam_username")
    cust = _pick(idm, "Customer Name", "Consignee Name", "Buyer's Name", "Buyer’s Name", "Buyer\\'s Name", "customer_name")
    invd = _pick(idm, "Invoice Date", "Date of Invoice", "invoice_date", "Date")
    qty = _pick(idm, "QTY", "Qty(MT)", "Quantity", "qty_mt")
    val = _pick(idm, "Invoice Value With GST", "Invoice Value with GST", "Invoice Value", "revenue_gst")
    invno = _pick(idm, "Invoice Number", "Invoice No", "invoice_number")
    usermap = _usermap()
    processed = 0
    with transaction.atomic():
        for r in values[1:]:
            kam = _find_user_by_map(_val(r, kamn), usermap)
            inv_date = _parse_date(_val(r, invd))
            if not kam or not inv_date:
                continue
            customer_name = _val(r, cust)
            if not customer_name:
                continue
            customer, _ = Customer.objects.get_or_create(name=customer_name)
            qty_mt = _to_decimal(_val(r, qty))
            revenue_gst = _to_decimal(_val(r, val))
            inv_no = _val(r, invno)
            row_uuid = inv_no or md5(f"{customer_name}|{kam.username}|{inv_date}|{qty_mt}|{revenue_gst}".encode("utf-8")).hexdigest()
            InvoiceFact.objects.update_or_create(
                row_uuid=row_uuid,
                defaults={
                    "invoice_date": inv_date,
                    "customer": customer,
                    "kam": kam,
                    "qty_mt": qty_mt,
                    "revenue_gst": revenue_gst,
                },
            )
            processed += 1
    return processed


def _leads_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    ts = _pick(idm, "Timestamp", "Date of Enquiry", "doe")
    kamn = _pick(idm, "KAM Name", "KAM", "kam_username")
    cust = _pick(idm, "Customer Name", "customer_name")
    qty = _pick(idm, "Qty (MT)", "QTY", "Qty", "qty_mt")
    status_i = _pick(idm, "Status", "status")
    grade_i = _pick(idm, "Grade", "grade")
    size_i = _pick(idm, "Size", "Size(MM)", "size")
    remarks_i = _pick(idm, "Remarks", "remarks")
    usermap = _usermap()
    processed = 0
    with transaction.atomic():
        for r in values[1:]:
            doe = _parse_date(_val(r, ts))
            kam = _find_user_by_map(_val(r, kamn), usermap)
            if not kam or not doe:
                continue
            cust_name = _val(r, cust)
            qty_mt = _to_decimal(_val(r, qty))
            status = (_val(r, status_i) or "OPEN").upper()
            grade = _val(r, grade_i) or None
            size = _val(r, size_i) or None
            remarks = _val(r, remarks_i) or None
            customer = None
            if cust_name:
                customer, _ = Customer.objects.get_or_create(name=cust_name)
            row_uuid = md5(f"lead|{kam.username}|{doe}|{cust_name}|{qty_mt}|{status}|{grade}|{size}".encode("utf-8")).hexdigest()
            LeadFact.objects.update_or_create(
                row_uuid=row_uuid,
                defaults={
                    "doe": doe,
                    "kam": kam,
                    "customer": customer,
                    "qty_mt": qty_mt,
                    "status": status,
                    "grade": grade,
                    "size": size,
                    "remarks": remarks,
                },
            )
            processed += 1
    return processed


def _overdues_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    cust = _pick(idm, "Customer Name", "customer_name", "Customer")
    overdue_i = _pick(idm, "Overdues (Rs)", "Overdue", "overdue")
    exposure_i = _pick(idm, "Total Exposure (Rs)", "Exposure", "exposure")
    a0 = _pick(idm, "0-30", "ageing_0_30")
    a31 = _pick(idm, "31-60", "ageing_31_60")
    a61 = _pick(idm, "61-90", "ageing_61_90")
    a90 = _pick(idm, "90+", "ageing_90_plus")
    snap_date = timezone.localdate()
    totals: Dict[str, Dict[str, Decimal]] = {}
    for r in values[1:]:
        cust_name = _val(r, cust)
        if not cust_name:
            continue
        cur = totals.setdefault(
            cust_name, {"overdue": Decimal(0), "exposure": Decimal(0), "a0": Decimal(0), "a31": Decimal(0), "a61": Decimal(0), "a90": Decimal(0)}
        )
        cur["overdue"] += _to_decimal(_val(r, overdue_i))
        cur["exposure"] += _to_decimal(_val(r, exposure_i))
        cur["a0"] += _to_decimal(_val(r, a0))
        cur["a31"] += _to_decimal(_val(r, a31))
        cur["a61"] += _to_decimal(_val(r, a61))
        cur["a90"] += _to_decimal(_val(r, a90))
    processed = 0
    with transaction.atomic():
        for cust_name, vals in totals.items():
            cust, _ = Customer.objects.get_or_create(name=cust_name)
            OverdueSnapshot.objects.update_or_create(
                snapshot_date=snap_date,
                customer=cust,
                defaults=dict(
                    exposure=vals["exposure"],
                    overdue=vals["overdue"],
                    ageing_0_30=vals["a0"],
                    ageing_31_60=vals["a31"],
                    ageing_61_90=vals["a61"],
                    ageing_90_plus=vals["a90"],
                ),
            )
            processed += 1
    return processed


def step_sync(intent: SyncIntent) -> Dict:
    sheet_id = _getenv("KAM_SALES_SHEET_ID")
    if not sheet_id:
        return {"last_customer_cursor": None, "last_invoice_cursor": None, "last_lead_cursor": None, "last_overdue_cursor": None, "done": True}

    tabs = {
        "customers": _getenv("KAM_TAB_CUSTOMERS", "Customer Details"),
        "invoices": _getenv("KAM_SALES_TAB", "Sheet1"),
        "leads": _getenv("KAM_TAB_LEADS", "Enquiry (F)"),
        "overdues": _getenv("KAM_TAB_OVERDUES", "Overdues"),
    }

    order = [
        ("last_customer_cursor", tabs["customers"], _customers_handler),
        ("last_invoice_cursor", tabs["invoices"], _invoices_handler),
        ("last_lead_cursor", tabs["leads"], _leads_handler),
        ("last_overdue_cursor", tabs["overdues"], _overdues_handler),
    ]

    for attr, tab, handler in order:
        cur = getattr(intent, attr)
        _processed, new_cur = _process_page(tab, cur, handler)
        setattr(intent, attr, new_cur)
        intent.save(update_fields=[attr, "updated_at"])
        if new_cur is not None:
            break

    done = all(getattr(intent, a) is None for a, _, __ in order)
    return {
        "last_customer_cursor": intent.last_customer_cursor,
        "last_invoice_cursor": intent.last_invoice_cursor,
        "last_lead_cursor": intent.last_lead_cursor,
        "last_overdue_cursor": intent.last_overdue_cursor,
        "done": done,
    }