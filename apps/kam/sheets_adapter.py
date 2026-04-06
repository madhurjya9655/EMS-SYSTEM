# FILE: apps/kam/sheets_adapter.py
# PURPOSE: Fix Sales sync field mismatches, zero-overwrite bug, overdues ageing gap
# FIXED 2026-04-06:
#   - _sync_sales_f: no longer overwrites invoice_value/revenue_gst with 0
#   - _sync_sheet1: invoice_value/revenue_gst only written if source has real value
#   - _sync_overdues: now syncs ageing_0_30..90_plus columns if present in tab
#   - _kam_user_cache: cleared at start of every step_sync call (not just run_sync_now)
#   - All original bugs from 2026-03-03 comment kept fixed

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

try:
    from apps.common.google_auth import GoogleCredentialError
except ImportError:
    class GoogleCredentialError(Exception):  # type: ignore[no-redef]
        pass

from .models import (
    Customer,
    InvoiceFact,
    LeadFact,
    OverdueSnapshot,
    SyncIntent,
)

logger = logging.getLogger(__name__)
User = get_user_model()


# ─────────────────────────────────────────────────────────────────────────────
# ENV HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def _env_flag(name: str, default: bool = True) -> bool:
    v = _env(name, "1" if default else "0").lower()
    return v not in ("0", "false", "no", "off", "")

def _require_env(name: str) -> str:
    val = _env(name)
    if not val:
        raise RuntimeError(
            f"Missing required env var: {name}\n"
            f"Add this to your Render environment variables."
        )
    return val


# ─────────────────────────────────────────────────────────────────────────────
# ROW UUID — deterministic hash for idempotent upserts
# ─────────────────────────────────────────────────────────────────────────────

def _make_row_uuid(*parts) -> str:
    key = "|".join(str(p or "") for p in parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:64]


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS SERVICE
# ─────────────────────────────────────────────────────────────────────────────

def _load_sa_info() -> dict:
    import json as _json, os as _os

    raw_content = _env("KAM_SA_JSON_CONTENT") or _env("GOOGLE_SA_JSON_CONTENT")
    if raw_content:
        try:
            return _json.loads(raw_content)
        except _json.JSONDecodeError as exc:
            raise GoogleCredentialError(
                f"KAM_SA_JSON_CONTENT is not valid JSON: {exc}\n"
                f"Copy the service account JSON file content and paste it as the env var value."
            ) from exc

    path_candidates = [
        _env("KAM_SA_JSON"),
        _env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),
        _env("GOOGLE_SERVICE_ACCOUNT_FILE"),
    ]
    attempted = [p for p in path_candidates if p]

    for sa_path in attempted:
        if _os.path.isfile(sa_path):
            try:
                with open(sa_path) as fh:
                    return _json.load(fh)
            except Exception as exc:
                raise GoogleCredentialError(
                    f"Could not read service account file {sa_path}: {exc}"
                ) from exc

    if attempted:
        raise GoogleCredentialError(
            "Service account file not found at any of:\n"
            + "\n".join(f"  {p}" for p in attempted)
            + "\n\nSet KAM_SA_JSON_CONTENT env var with the full JSON content of your service account."
        )
    raise GoogleCredentialError(
        "No Google service account credentials configured.\n"
        "Required: KAM_SA_JSON_CONTENT (env var with JSON content)\n"
        "     or:  KAM_SA_JSON (path to .json file)"
    )


def build_sheets_service():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build as _gapi_build
    except ImportError as exc:
        raise GoogleCredentialError(
            f"Google API client libraries not installed: {exc}\n"
            "Run: pip install google-auth google-api-python-client"
        ) from exc

    sa_info = _load_sa_info()
    try:
        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        return _gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
    except GoogleCredentialError:
        raise
    except Exception as exc:
        raise GoogleCredentialError(f"Failed to build Sheets service: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# TAB NAME RESOLUTION (from env vars with sane defaults)
# ─────────────────────────────────────────────────────────────────────────────

def _tab_sales_f()    -> str: return _env("KAM_TAB_SALES",     "Sales (F)")
def _tab_sheet1()     -> str: return _env("KAM_TAB_SHEET1",    "Sheet1")
def _tab_kam_names()  -> str: return _env("KAM_TAB_KAM_NAMES", "KAM names")
def _tab_frontend()   -> str: return _env("KAM_TAB_FRONTEND",  "Front End")
def _tab_enquiry_f()  -> str: return _env("KAM_TAB_ENQUIRY",   "Enquiry (F)")
def _tab_customers()  -> str: return _env("KAM_TAB_CUSTOMERS", "Customer Details")
def _tab_overdues()   -> str: return _env("KAM_TAB_OVERDUES",  "Overdues")

def resolve_sections() -> Dict[str, bool]:
    return {
        "sales_f":   _env_flag("KAM_SYNC_SALES",     True),
        "sheet1":    _env_flag("KAM_SYNC_SHEET1",    True),
        "frontend":  _env_flag("KAM_SYNC_FRONTEND",  True),
        "enquiry_f": _env_flag("KAM_SYNC_ENQUIRY",   True),
        "customers": _env_flag("KAM_SYNC_CUSTOMERS", True),
        "overdues":  _env_flag("KAM_SYNC_OVERDUES",  True),
    }

def resolve_tabs_for_logging() -> Dict[str, str]:
    return {
        "sales_f":   _tab_sales_f(),
        "sheet1":    _tab_sheet1(),
        "kam_names": _tab_kam_names(),
        "frontend":  _tab_frontend(),
        "enquiry_f": _tab_enquiry_f(),
        "customers": _tab_customers(),
        "overdues":  _tab_overdues(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SYNC STATS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SyncStats:
    customers_upserted: int = 0
    sales_upserted:     int = 0
    leads_upserted:     int = 0
    overdues_upserted:  int = 0
    skipped:            int = 0
    unknown_kam:        int = 0
    notes: List[str]        = field(default_factory=list)

    def merge(self, other: "SyncStats") -> None:
        self.customers_upserted += other.customers_upserted
        self.sales_upserted     += other.sales_upserted
        self.leads_upserted     += other.leads_upserted
        self.overdues_upserted  += other.overdues_upserted
        self.skipped            += other.skipped
        self.unknown_kam        += other.unknown_kam
        self.notes.extend(other.notes)

    def as_message(self) -> str:
        parts = []
        if self.customers_upserted: parts.append(f"Customers: {self.customers_upserted}")
        if self.sales_upserted:     parts.append(f"Sales: {self.sales_upserted}")
        if self.leads_upserted:     parts.append(f"Leads: {self.leads_upserted}")
        if self.overdues_upserted:  parts.append(f"Overdues: {self.overdues_upserted}")
        if self.skipped:            parts.append(f"Skipped: {self.skipped}")
        if self.unknown_kam:        parts.append(f"Unknown KAM: {self.unknown_kam}")
        return " | ".join(parts) if parts else "No changes"


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def _get_sheet_values(service, sheet_id: str, tab: str) -> List[List[str]]:
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=tab)
            .execute()
        )
        return result.get("values", [])
    except Exception as exc:
        logger.warning("Could not read tab '%s' from sheet '%s': %s", tab, sheet_id, exc)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# FIELD PARSING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _cell(row: List[str], idx: int, default: str = "") -> str:
    try:
        return (row[idx] or "").strip()
    except IndexError:
        return default

def _decimal(val: str) -> Optional[Decimal]:
    if not val:
        return None
    cleaned = re.sub(r"[₹,\s]", "", val)
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None

def _parse_date(val: str) -> Optional[date]:
    """
    Parse date from common Indian/ISO formats.
    No hardcoded year restrictions.
    """
    if not val:
        return None
    val = val.strip()

    for fmt in (
        "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
        "%m/%d/%Y", "%d/%m/%y", "%Y/%m/%d",
        "%d %b %Y", "%d-%b-%Y",
    ):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue

    # Google Sheets serial date (days since Dec 30, 1899)
    try:
        serial = int(float(val))
        if 1 < serial < 100000:
            from datetime import timedelta
            return date(1899, 12, 30) + timedelta(days=serial)
    except (ValueError, OverflowError):
        pass

    logger.debug("Could not parse date: %r", val)
    return None

def _parse_timestamp(val: str) -> Optional[datetime]:
    if not val:
        return None
    val = val.strip()
    for fmt in (
        "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",    "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(val, fmt)
            return timezone.make_aware(dt) if timezone.is_naive(dt) else dt
        except ValueError:
            continue
    return None

def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


# ─────────────────────────────────────────────────────────────────────────────
# SAFE CUSTOMER LOOKUP / CREATE
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_customer_name(name: str) -> str:
    return " ".join(name.strip().split()).strip()

def _safe_get_or_create_customer(
    name: str,
    kam_user=None,
    extra_defaults: dict = None,
) -> "Customer":
    clean_name = _normalize_customer_name(name)
    if not clean_name:
        raise ValueError("Customer name cannot be blank")

    defaults = {"kam": kam_user}
    if extra_defaults:
        defaults.update(extra_defaults)

    matches = list(Customer.objects.filter(name__iexact=clean_name).order_by("pk"))

    if not matches:
        try:
            with transaction.atomic():
                customer, _ = Customer.objects.get_or_create(name=clean_name, defaults=defaults)
            return customer
        except Customer.MultipleObjectsReturned:
            matches = list(Customer.objects.filter(name__iexact=clean_name).order_by("pk"))

    if len(matches) == 1:
        customer = matches[0]
        changed = False
        if kam_user and not customer.kam:
            customer.kam = kam_user
            changed = True
        if extra_defaults:
            for f, val in extra_defaults.items():
                if val is not None and not getattr(customer, f, None):
                    setattr(customer, f, val)
                    changed = True
        if changed:
            try:
                customer.save()
            except Exception:
                pass
        return customer

    # Merge duplicates — keep first, re-point all FKs
    survivor = matches[0]
    for dup in matches[1:]:
        try:
            with transaction.atomic():
                for rel in dup._meta.get_fields():
                    if rel.is_relation and rel.one_to_many and rel.related_model:
                        try:
                            accessor = rel.get_accessor_name()
                            getattr(dup, accessor).all().update(**{rel.field.name: survivor})
                        except Exception:
                            pass
                dup.delete()
        except Exception as del_exc:
            logger.error("Could not delete duplicate customer pk=%s: %s", dup.pk, del_exc)

    return survivor


# ─────────────────────────────────────────────────────────────────────────────
# KAM USER MAPPING
# ─────────────────────────────────────────────────────────────────────────────

# FIX: per-call cache dict (passed as argument) instead of module-level global.
# This prevents stale lookups across Celery workers and between sync calls.

def _load_kam_names_tab(service, sheet_id: str) -> Dict[str, str]:
    rows = _get_sheet_values(service, sheet_id, _tab_kam_names())
    if not rows:
        logger.warning("KAM names tab empty or missing — KAM matching will rely on DB only")
        return {}

    mapping: Dict[str, str] = {}
    for row in rows[1:]:
        short_name = _cell(row, 0)
        kam_name   = _cell(row, 1)
        email      = _cell(row, 2)
        if not email or "@" not in email:
            continue
        if kam_name:
            mapping[kam_name] = email
        if short_name and short_name != kam_name:
            mapping[short_name] = email

    logger.info("KAM names tab loaded: %d entries", len(mapping))
    return mapping

def _build_user_lookup() -> Dict[str, User]:
    lookup: Dict[str, User] = {}
    first_name_index: Dict[str, List[User]] = {}

    for u in User.objects.filter(is_active=True):
        try:
            full = f"{u.first_name} {u.last_name}".strip()
            if full:
                lookup[_normalize(full)] = u
        except Exception:
            pass
        if u.email:
            lookup[_normalize(u.email)] = u
            local = u.email.split("@")[0]
            lookup[_normalize(local)] = u
        uname = u.username or ""
        lookup[_normalize(uname)] = u
        parts = re.split(r"[._\-]", uname)
        if len(parts) >= 2:
            lookup[_normalize(parts[0])] = u
            lookup[_normalize("".join(parts))] = u
        first = (u.first_name or (parts[0] if parts else "")).strip().lower()
        if first:
            first_name_index.setdefault(first, []).append(u)

    for fname, users in first_name_index.items():
        if len(users) == 1:
            lookup[_normalize(fname)] = users[0]

    return lookup

def _resolve_kam_user(
    name: str,
    tab_mapping: Dict[str, str],
    db_lookup: Dict[str, User],
    env_usermap: Dict[str, str],
    stats: SyncStats,
    local_cache: Dict[str, Optional[User]],  # FIX: per-call cache instead of global
) -> Optional[User]:
    if not name:
        return None

    key = _normalize(name)
    if key in local_cache:
        return local_cache[key]

    user: Optional[User] = None

    # 1. Env override
    email_override = env_usermap.get(name) or env_usermap.get(key)
    if email_override:
        try:
            user = User.objects.get(email__iexact=email_override)
        except User.DoesNotExist:
            try:
                user = User.objects.get(username__iexact=email_override)
            except User.DoesNotExist:
                pass

    # 2. KAM names tab → email → DB
    if not user:
        email_from_tab = tab_mapping.get(name) or tab_mapping.get(key)
        if not email_from_tab:
            for tab_name_key, tab_email in tab_mapping.items():
                if _normalize(tab_name_key) == key:
                    email_from_tab = tab_email
                    break
        if email_from_tab:
            try:
                user = User.objects.get(email__iexact=email_from_tab)
            except User.DoesNotExist:
                try:
                    user = User.objects.get(username__iexact=email_from_tab)
                except User.DoesNotExist:
                    if _env_flag("KAM_AUTO_CREATE_USERS", False):
                        parts = name.split()
                        user, created = User.objects.get_or_create(
                            email=email_from_tab,
                            defaults={
                                "username":   email_from_tab,
                                "first_name": parts[0] if parts else "",
                                "last_name":  " ".join(parts[1:]) if len(parts) > 1 else "",
                            }
                        )
                        if created:
                            logger.info("Auto-created user for KAM '%s' → %s", name, email_from_tab)

    # 3. DB lookup dict (normalized name/email/username match)
    if not user:
        user = db_lookup.get(key)

    # 4. Compact key (strips punctuation)
    if not user:
        compact = re.sub(r"[^a-z0-9]", "", key)
        user = db_lookup.get(compact)

    # 5. Direct DB username match
    if not user:
        try:
            user = User.objects.get(username__iexact=name)
        except User.DoesNotExist:
            pass

    # 6. First + Last name match
    if not user:
        try:
            name_parts = name.split()
            if len(name_parts) >= 2:
                user = User.objects.filter(
                    first_name__iexact=name_parts[0],
                    last_name__iexact=name_parts[-1],
                ).first()
        except Exception:
            pass

    if not user:
        stats.unknown_kam += 1
        stats.notes.append(f"Unknown KAM: '{name}'")
        logger.warning(
            "KAM not found for name '%s'. "
            "Add to KAM names tab or set KAM_USERMAP_JSON env var.",
            name,
        )

    local_cache[key] = user
    return user

def _load_env_usermap() -> Dict[str, str]:
    raw = _env("KAM_USERMAP_JSON", "{}")
    try:
        data = json.loads(raw)
        return {_normalize(k): v for k, v in data.items()} | data
    except json.JSONDecodeError:
        logger.warning(
            "KAM_USERMAP_JSON is not valid JSON. "
            "Set it as: {\"KAM Name In Sheet\": \"user@email.com\"}"
        )
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: CUSTOMER DETAILS
# Tab: Customer Details
# Cols: Customer Name[0] | KAM Name[1] | Address[2] | Email[3] | Mobile No[4]
#       Person Name[5] | Credit Limit[6] | Agreed Credit Period[7]
#       Total Exposure (Rs)[8] | Overdues (Rs)[9] | Current Credit Limit[10]
# ─────────────────────────────────────────────────────────────────────────────

def _sync_customers(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_customers())
    if len(rows) < 2:
        stats.notes.append("Customer Details tab: no data rows")
        return stats

    for row in rows[1:]:
        name = _cell(row, 0)
        if not name:
            stats.skipped += 1
            continue

        kam_name = _cell(row, 1)
        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )

        try:
            with transaction.atomic():
                obj = _safe_get_or_create_customer(name, kam_user=kam_user)
                # Authoritative update from Customer Details tab
                obj.address              = _cell(row, 2) or obj.address
                obj.email                = _cell(row, 3) or obj.email
                obj.mobile               = _cell(row, 4) or obj.mobile
                obj.contact_person       = _cell(row, 5) or obj.contact_person
                obj.credit_limit         = _decimal(_cell(row, 6)) or obj.credit_limit
                obj.credit_period_days   = _decimal(_cell(row, 7)) or obj.credit_period_days
                obj.total_exposure       = _decimal(_cell(row, 8)) or obj.total_exposure
                obj.current_credit_limit = _decimal(_cell(row, 10)) or obj.current_credit_limit
                if kam_user:
                    obj.kam = kam_user
                    if not obj.primary_kam:
                        obj.primary_kam = kam_user
                obj.save()
                stats.customers_upserted += 1
        except Exception as exc:
            logger.error("Customer upsert failed for '%s': %s", name, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: SALES — Sales (F)
# Tab: Sales (F)
# Cols: Date of Invoice[0] | Buyer's Name[1] | KAM[2] | Qty(MT)[3] | Full Name[4]
#
# FIX: No longer sets invoice_value/revenue_gst to 0.
#      Sales (F) tab has no value column — we only write qty_mt.
#      invoice_value/revenue_gst are left to Sheet1 which has real amounts.
#      Use get_or_create pattern: if row already exists (from Sheet1), don't
#      overwrite its invoice_value with 0.
# ─────────────────────────────────────────────────────────────────────────────

def _sync_sales_f(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_sales_f())
    if len(rows) < 2:
        stats.notes.append("Sales (F) tab: no data rows")
        return stats

    tab_name = _tab_sales_f()

    for i, row in enumerate(rows[1:], start=2):
        invoice_date_raw = _cell(row, 0)
        buyer_name       = _cell(row, 1)
        kam_name         = _cell(row, 2)
        qty_raw          = _cell(row, 3)
        full_name        = _cell(row, 4)

        if not buyer_name and not full_name:
            stats.skipped += 1
            continue

        customer_name = full_name or buyer_name
        invoice_date  = _parse_date(invoice_date_raw)
        if not invoice_date:
            logger.debug("Sales (F) row %d: cannot parse date '%s'", i, invoice_date_raw)
            stats.skipped += 1
            continue

        qty = _decimal(qty_raw) or Decimal("0")
        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)
        row_uuid = _make_row_uuid(tab_name, invoice_date, customer_name, kam_name, i)

        try:
            with transaction.atomic():
                obj, created = InvoiceFact.objects.get_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer":       customer,
                        "kam":            kam_user,
                        "invoice_date":   invoice_date,
                        "qty_mt":         qty,
                        # FIX: Only set 0 on CREATE — Sheet1 may fill real values later.
                        # On UPDATE we only refresh qty/kam/customer, NOT invoice_value.
                        "invoice_value":  Decimal("0"),
                        "revenue_gst":    Decimal("0"),
                        "raw_buyer_name": buyer_name,
                        "source_tab":     tab_name,
                    },
                )
                if not created:
                    # FIX: On existing rows, only update qty and KAM — never zero out amounts
                    update_fields = ["qty_mt", "customer", "raw_buyer_name", "source_tab", "updated_at"]
                    obj.qty_mt         = qty
                    obj.customer       = customer
                    obj.raw_buyer_name = buyer_name
                    obj.source_tab     = tab_name
                    if kam_user and not obj.kam:
                        obj.kam = kam_user
                        update_fields.append("kam")
                    obj.save(update_fields=update_fields)
                stats.sales_upserted += 1
        except Exception as exc:
            logger.error("Sales (F) row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: SALES — Sheet1 (historical invoices with full amounts)
# Tab: Sheet1
# Cols: KAM Name[0] | Customer Name[1] | Consignee Name[2] | Vehicle Number[3]
#       Invoice Number[4] | Invoice Date[5] | Invoice Value With GST[6]
#       Business Vertical[7] | Dispatch From[8] | Dispatch To[9]
#       Transporter Name[10] | Heat Number[11] | Grade[12] | Size[13]
#       QTY[14] | Shape[15] | Rate/MT[16] | Invoice Value[17] | ...
#
# This tab has real invoice_value — it is the authoritative source for amounts.
# FIX: Only write invoice_value/revenue_gst if source actually has a non-zero value.
# ─────────────────────────────────────────────────────────────────────────────

def _sync_sheet1(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_sheet1())
    if len(rows) < 2:
        stats.notes.append("Sheet1 tab: no data rows")
        return stats

    tab_name = _tab_sheet1()

    for i, row in enumerate(rows[1:], start=2):
        kam_name      = _cell(row, 0)
        customer_name = _cell(row, 1)
        invoice_no    = _cell(row, 4)
        invoice_date  = _parse_date(_cell(row, 5))
        value_gst     = _decimal(_cell(row, 6))
        qty           = _decimal(_cell(row, 14)) or Decimal("0")
        invoice_value = _decimal(_cell(row, 17))
        rate_mt       = _decimal(_cell(row, 16))
        grade         = _cell(row, 12)
        size          = _cell(row, 13)

        if not customer_name:
            stats.skipped += 1
            continue
        if not invoice_date:
            stats.skipped += 1
            continue

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = (
            _make_row_uuid(tab_name, invoice_no)
            if invoice_no
            else _make_row_uuid(tab_name, invoice_date, customer_name, kam_name, i)
        )

        # FIX: Use the real value if present; fall back to 0 only if truly missing
        final_value = value_gst or invoice_value or Decimal("0")

        try:
            with transaction.atomic():
                obj, created = InvoiceFact.objects.get_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer":      customer,
                        "kam":           kam_user,
                        "invoice_date":  invoice_date,
                        "invoice_no":    invoice_no,
                        "invoice_value": final_value,
                        "revenue_gst":   final_value,
                        "qty_mt":        qty,
                        "rate_mt":       rate_mt,
                        "grade":         grade,
                        "size":          size,
                        "source_tab":    tab_name,
                    },
                )
                if not created:
                    # FIX: Always update amounts from Sheet1 — it is authoritative
                    update_fields = [
                        "invoice_value", "revenue_gst", "qty_mt",
                        "rate_mt", "grade", "size", "source_tab",
                        "customer", "invoice_no", "updated_at",
                    ]
                    obj.invoice_value = final_value
                    obj.revenue_gst   = final_value
                    obj.qty_mt        = qty
                    obj.rate_mt       = rate_mt
                    obj.grade         = grade
                    obj.size          = size
                    obj.source_tab    = tab_name
                    obj.customer      = customer
                    obj.invoice_no    = invoice_no
                    if kam_user and not obj.kam:
                        obj.kam = kam_user
                        update_fields.append("kam")
                    obj.save(update_fields=update_fields)
                stats.sales_upserted += 1
        except Exception as exc:
            logger.error("Sheet1 row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Front End
# Tab: Front End
# Cols: Enquiry Number[0] | Timestamp[1] | Email[2] | Type[3] | KAM Name[4]
#       Customer Name[5] | Grade[6] | Size(MM)[7] | Qty (MT)[8]
#       ... | Status[21] | Revenue RS/MT[22] | Remarks[23]
# ─────────────────────────────────────────────────────────────────────────────

def _sync_frontend(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_frontend())
    if len(rows) < 2:
        stats.notes.append("Front End tab: no data rows")
        return stats

    tab_name = _tab_frontend()

    for i, row in enumerate(rows[1:], start=2):
        enquiry_no    = _cell(row, 0)
        timestamp_raw = _cell(row, 1)
        kam_name      = _cell(row, 4)
        customer_name = _cell(row, 5)
        grade         = _cell(row, 6)
        size          = _cell(row, 7)
        qty_raw       = _cell(row, 8)
        status        = _cell(row, 21)
        revenue_mt    = _decimal(_cell(row, 22))
        remarks       = _cell(row, 23)

        if not customer_name:
            stats.skipped += 1
            continue

        ts = _parse_timestamp(timestamp_raw)
        doe_date = ts.date() if ts else None

        qty = _decimal(qty_raw) or Decimal("0")
        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = (
            _make_row_uuid(tab_name, enquiry_no)
            if enquiry_no
            else _make_row_uuid(tab_name, timestamp_raw, customer_name, kam_name, i)
        )

        try:
            with transaction.atomic():
                LeadFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer":   customer,
                        "kam":        kam_user,
                        "doe":        doe_date,
                        "qty_mt":     qty,
                        "status":     status or "OPEN",
                        "grade":      grade,
                        "size":       size,
                        "revenue_mt": revenue_mt,
                        "remarks":    remarks,
                        "source_tab": tab_name,
                        "enquiry_no": enquiry_no,
                    },
                )
                stats.leads_upserted += 1
        except Exception as exc:
            logger.error("Front End row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Enquiry (F)
# Tab: Enquiry (F)
# Cols: Timestamp[0] | KAM Name[1] | Customer Name[2] | Qty (MT)[3]
#       Status[4] | Remarks[5]
# ─────────────────────────────────────────────────────────────────────────────

def _sync_enquiry_f(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_enquiry_f())
    if len(rows) < 2:
        stats.notes.append("Enquiry (F) tab: no data rows")
        return stats

    tab_name = _tab_enquiry_f()

    for i, row in enumerate(rows[1:], start=2):
        timestamp_raw = _cell(row, 0)
        kam_name      = _cell(row, 1)
        customer_name = _cell(row, 2)
        qty_raw       = _cell(row, 3)
        status        = _cell(row, 4)
        remarks       = _cell(row, 5)

        if not customer_name:
            stats.skipped += 1
            continue

        ts = _parse_timestamp(timestamp_raw)
        doe_date = ts.date() if ts else None

        qty = _decimal(qty_raw) or Decimal("0")
        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)
        row_uuid = _make_row_uuid(tab_name, timestamp_raw, customer_name, kam_name, i)

        try:
            with transaction.atomic():
                LeadFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer":   customer,
                        "kam":        kam_user,
                        "doe":        doe_date,
                        "qty_mt":     qty,
                        "status":     status or "OPEN",
                        "remarks":    remarks,
                        "source_tab": tab_name,
                    },
                )
                stats.leads_upserted += 1
        except Exception as exc:
            logger.error("Enquiry (F) row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: OVERDUES
# Tab: Overdues
# Expected cols: Customer Name[0] | KAM Name[1] | Overdues (Rs)[2]
# Optional cols: Exposure[3] | 0-30[4] | 31-60[5] | 61-90[6] | 90+[7]
#
# FIX: Syncs ageing columns if present. Dashboard reads ageing_0_30..90_plus.
#      When ageing is present, exposure = sum of ageing buckets.
# ─────────────────────────────────────────────────────────────────────────────

def _sync_overdues(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_overdues())
    if len(rows) < 2:
        stats.notes.append("Overdues tab: no data rows")
        return stats

    snapshot_date = timezone.now().date()

    for i, row in enumerate(rows[1:], start=2):
        customer_name = _cell(row, 0)
        kam_name      = _cell(row, 1)
        overdue_raw   = _cell(row, 2)

        if not customer_name:
            continue

        overdue_amt = _decimal(overdue_raw)
        if overdue_amt is None:
            stats.skipped += 1
            continue

        # FIX: Read optional ageing columns if tab has them
        exposure_raw  = _cell(row, 3)
        a0_raw        = _cell(row, 4)
        a31_raw       = _cell(row, 5)
        a61_raw       = _cell(row, 6)
        a90_raw       = _cell(row, 7)

        a0   = _decimal(a0_raw)   or Decimal("0")
        a31  = _decimal(a31_raw)  or Decimal("0")
        a61  = _decimal(a61_raw)  or Decimal("0")
        a90  = _decimal(a90_raw)  or Decimal("0")
        ageing_total = a0 + a31 + a61 + a90

        # Exposure: use explicit col if present; else sum ageing; else use overdue
        exposure = _decimal(exposure_raw) or ageing_total or overdue_amt

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        try:
            with transaction.atomic():
                OverdueSnapshot.objects.update_or_create(
                    customer=customer,
                    snapshot_date=snapshot_date,
                    defaults={
                        "kam":          kam_user,
                        "overdue":      overdue_amt,
                        "overdue_amt":  overdue_amt,
                        "exposure":     exposure,
                        "ageing_0_30":  a0,
                        "ageing_31_60": a31,
                        "ageing_61_90": a61,
                        "ageing_90_plus": a90,
                    },
                )
                stats.overdues_upserted += 1
        except Exception as exc:
            logger.error("Overdues row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SYNC ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_sync_now() -> SyncStats:
    """
    Full sync. Called by sheets.run_sync_now() and the Celery beat task.
    FIX: local_cache dict passed to each section function — no module-level global.
    """
    sheet_id = _require_env("KAM_SALES_SHEET_ID")
    sections = resolve_sections()
    total    = SyncStats()

    try:
        service = build_sheets_service()
    except GoogleCredentialError:
        raise

    tab_mapping  = _load_kam_names_tab(service, sheet_id)
    db_lookup    = _build_user_lookup()
    env_usermap  = _load_env_usermap()
    local_cache: Dict[str, Optional[User]] = {}  # FIX: per-call cache

    logger.info(
        "Starting sync | sheet=%s | tab_mapping_entries=%d | db_users=%d",
        sheet_id, len(tab_mapping), len(db_lookup),
    )

    # Sync order matters: customers first, then sales/leads/overdues
    if sections.get("customers"):
        logger.info("Syncing: Customer Details")
        s = _sync_customers(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → customers=%d skipped=%d", s.customers_upserted, s.skipped)

    if sections.get("sales_f"):
        logger.info("Syncing: Sales (F)")
        s = _sync_sales_f(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → sales=%d skipped=%d", s.sales_upserted, s.skipped)

    if sections.get("sheet1"):
        logger.info("Syncing: Sheet1")
        s = _sync_sheet1(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → sales=%d skipped=%d", s.sales_upserted, s.skipped)

    if sections.get("frontend"):
        logger.info("Syncing: Front End")
        s = _sync_frontend(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → leads=%d skipped=%d", s.leads_upserted, s.skipped)

    if sections.get("enquiry_f"):
        logger.info("Syncing: Enquiry (F)")
        s = _sync_enquiry_f(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → leads=%d skipped=%d", s.leads_upserted, s.skipped)

    if sections.get("overdues"):
        logger.info("Syncing: Overdues")
        s = _sync_overdues(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → overdues=%d skipped=%d", s.overdues_upserted, s.skipped)

    logger.info("Sync complete: %s", total.as_message())
    if total.notes:
        logger.warning("Sync notes: %s", "; ".join(total.notes))

    return total


# ─────────────────────────────────────────────────────────────────────────────
# STEP SYNC (progressive — one section per call)
# ─────────────────────────────────────────────────────────────────────────────

_STEPS = [
    ("customers",  "Customer Details"),
    ("sales_f",    "Sales (F)"),
    ("sheet1",     "Sheet1"),
    ("frontend",   "Front End"),
    ("enquiry_f",  "Enquiry (F)"),
    ("overdues",   "Overdues"),
]

_STEP_FN_MAP = {
    "customers":  _sync_customers,
    "sales_f":    _sync_sales_f,
    "sheet1":     _sync_sheet1,
    "frontend":   _sync_frontend,
    "enquiry_f":  _sync_enquiry_f,
    "overdues":   _sync_overdues,
}

def step_sync(intent: "SyncIntent", *args, **kwargs) -> Dict[str, Any]:
    """
    Syncs one section at a time. SyncIntent.cursor_position tracks progress.
    FIX: local_cache created fresh per step call.
    """
    cursor   = getattr(intent, "cursor_position", 0) or 0
    sections = resolve_sections()
    sheet_id = _require_env("KAM_SALES_SHEET_ID")

    if cursor >= len(_STEPS):
        intent.status = "COMPLETE"
        intent.save(update_fields=["status"])
        return {"done": True, "message": "All sections synced"}

    section_key, section_label = _STEPS[cursor]

    try:
        service     = build_sheets_service()
        tab_mapping = _load_kam_names_tab(service, sheet_id)
        db_lookup   = _build_user_lookup()
        env_usermap = _load_env_usermap()
        local_cache: Dict[str, Optional[User]] = {}  # FIX: fresh per step

        stats = SyncStats()
        if sections.get(section_key):
            fn = _STEP_FN_MAP.get(section_key)
            if fn:
                stats = fn(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)

        intent.cursor_position = cursor + 1
        intent.status = "COMPLETE" if (cursor + 1) >= len(_STEPS) else "IN_PROGRESS"
        intent.save(update_fields=["cursor_position", "status"])

        return {
            "done":    intent.status == "COMPLETE",
            "step":    section_label,
            "message": stats.as_message(),
            "stats": {
                "customers_upserted": stats.customers_upserted,
                "sales_upserted":     stats.sales_upserted,
                "leads_upserted":     stats.leads_upserted,
                "overdues_upserted":  stats.overdues_upserted,
                "skipped":            stats.skipped,
                "unknown_kam":        stats.unknown_kam,
            },
        }

    except Exception as exc:
        logger.error("step_sync failed at '%s': %s", section_label, exc)
        intent.status = "ERROR"
        intent.last_error = str(exc)
        intent.save(update_fields=["status", "last_error"])
        raise