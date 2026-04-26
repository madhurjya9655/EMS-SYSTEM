# FILE: apps/kam/sheets_adapter.py
# PURPOSE: Stable Google Sheet → DB sync for KAM dashboards / Customer 360
# FIXES INCLUDED:
#   - Sales(F) and Sheet1 use update_or_create() for full idempotent repair
#   - Existing rows get KAM/customer/value repaired on re-sync
#   - Customer owner fields (kam + primary_kam) are repaired consistently
#   - Collections update existing KAM when corrected mapping is available
#   - Leads remain idempotent and doe never NULL
#   - Overdues remain idempotent
#   - Collection sheet rows are stored with source='GOOGLE_SHEET'

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
    CollectionTxn,
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
    import json as _json
    import os as _os

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
# TAB NAME RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def _tab_sales_f() -> str:
    return _env("KAM_TAB_SALES", "Sales (F)")


def _tab_sheet1() -> str:
    return _env("KAM_TAB_SHEET1", "Sheet1")


def _tab_kam_names() -> str:
    return _env("KAM_TAB_KAM_NAMES", "KAM names")


def _tab_frontend() -> str:
    return _env("KAM_TAB_FRONTEND", "Front End")


def _tab_enquiry_f() -> str:
    return _env("KAM_TAB_ENQUIRY", "Enquiry (F)")


def _tab_customers() -> str:
    return _env("KAM_TAB_CUSTOMERS", "Customer Details")


def _tab_overdues() -> str:
    return _env("KAM_TAB_OVERDUES", "Overdues")


def _tab_collection() -> str:
    return _env("KAM_TAB_COLLECTION", "Collection")


def resolve_sections() -> Dict[str, bool]:
    return {
        "sales_f": _env_flag("KAM_SYNC_SALES", True),
        "sheet1": _env_flag("KAM_SYNC_SHEET1", True),
        "frontend": _env_flag("KAM_SYNC_FRONTEND", True),
        "enquiry_f": _env_flag("KAM_SYNC_ENQUIRY", True),
        "customers": _env_flag("KAM_SYNC_CUSTOMERS", True),
        "overdues": _env_flag("KAM_SYNC_OVERDUES", True),
        "collection": _env_flag("KAM_SYNC_COLLECTION", True),
    }


def resolve_tabs_for_logging() -> Dict[str, str]:
    return {
        "sales_f": _tab_sales_f(),
        "sheet1": _tab_sheet1(),
        "kam_names": _tab_kam_names(),
        "frontend": _tab_frontend(),
        "enquiry_f": _tab_enquiry_f(),
        "customers": _tab_customers(),
        "overdues": _tab_overdues(),
        "collection": _tab_collection(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SYNC STATS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SyncStats:
    customers_upserted: int = 0
    sales_upserted: int = 0
    leads_upserted: int = 0
    overdues_upserted: int = 0
    collections_upserted: int = 0
    skipped: int = 0
    unknown_kam: int = 0
    notes: List[str] = field(default_factory=list)

    def merge(self, other: "SyncStats") -> None:
        self.customers_upserted += other.customers_upserted
        self.sales_upserted += other.sales_upserted
        self.leads_upserted += other.leads_upserted
        self.overdues_upserted += other.overdues_upserted
        self.collections_upserted += other.collections_upserted
        self.skipped += other.skipped
        self.unknown_kam += other.unknown_kam
        self.notes.extend(other.notes)

    def as_message(self) -> str:
        parts = []
        if self.customers_upserted:
            parts.append(f"Customers: {self.customers_upserted}")
        if self.sales_upserted:
            parts.append(f"Sales: {self.sales_upserted}")
        if self.leads_upserted:
            parts.append(f"Leads: {self.leads_upserted}")
        if self.overdues_upserted:
            parts.append(f"Overdues: {self.overdues_upserted}")
        if self.collections_upserted:
            parts.append(f"Collections: {self.collections_upserted}")
        if self.skipped:
            parts.append(f"Skipped: {self.skipped}")
        if self.unknown_kam:
            parts.append(f"Unknown KAM: {self.unknown_kam}")
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
    if not val:
        return None

    val = val.strip().strip("'").strip()
    if not val:
        return None

    for fmt in (
        "%d-%b-%y",
        "%d %b %y",
        "%d-%b-%Y",
        "%d %b %Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%d/%m/%y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue

    try:
        serial = int(float(val))
        if 10000 < serial < 100000:
            return date(1899, 12, 30) + timedelta(days=serial)
    except (ValueError, OverflowError):
        pass

    logger.debug("Could not parse date: %r", val)
    return None


def _parse_timestamp(val: str) -> Optional[datetime]:
    if not val:
        return None
    val = val.strip().strip("'").strip()
    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(val, fmt)
            return timezone.make_aware(dt) if timezone.is_naive(dt) else dt
        except ValueError:
            continue

    d = _parse_date(val)
    if d:
        return timezone.make_aware(datetime(d.year, d.month, d.day))

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

    defaults = {"kam": kam_user, "primary_kam": kam_user}
    if extra_defaults:
        defaults.update(extra_defaults)

    matches = list(Customer.objects.filter(name__iexact=clean_name).order_by("pk"))

    if not matches:
        try:
            with transaction.atomic():
                customer, _ = Customer.objects.get_or_create(
                    name=clean_name,
                    defaults=defaults,
                )
            return customer
        except Customer.MultipleObjectsReturned:
            matches = list(Customer.objects.filter(name__iexact=clean_name).order_by("pk"))

    if len(matches) == 1:
        customer = matches[0]
        changed = False

        if kam_user:
            if customer.kam_id != kam_user.id:
                customer.kam = kam_user
                changed = True
            if customer.primary_kam_id != kam_user.id:
                customer.primary_kam = kam_user
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

    if kam_user:
        changed = False
        if survivor.kam_id != kam_user.id:
            survivor.kam = kam_user
            changed = True
        if survivor.primary_kam_id != kam_user.id:
            survivor.primary_kam = kam_user
            changed = True
        if changed:
            try:
                survivor.save()
            except Exception:
                pass

    return survivor


# ─────────────────────────────────────────────────────────────────────────────
# KAM USER MAPPING
# ─────────────────────────────────────────────────────────────────────────────

def _load_kam_names_tab(service, sheet_id: str) -> Dict[str, str]:
    rows = _get_sheet_values(service, sheet_id, _tab_kam_names())
    if not rows:
        logger.warning("KAM names tab empty or missing — KAM matching will rely on DB only")
        return {}

    mapping: Dict[str, str] = {}
    for row in rows[1:]:
        short_name = _cell(row, 0)
        kam_name = _cell(row, 1)
        email = _cell(row, 2)
        if not email or "@" not in email:
            continue
        if kam_name:
            mapping[kam_name] = email
            mapping[_normalize(kam_name)] = email
        if short_name and short_name != kam_name:
            mapping[short_name] = email
            mapping[_normalize(short_name)] = email

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
    local_cache: Dict[str, Optional[User]],
) -> Optional[User]:
    if not name:
        return None

    key = _normalize(name)
    if key in local_cache:
        return local_cache[key]

    user: Optional[User] = None

    email_override = env_usermap.get(name) or env_usermap.get(key)
    if email_override:
        try:
            user = User.objects.get(email__iexact=email_override)
        except User.DoesNotExist:
            try:
                user = User.objects.get(username__iexact=email_override)
            except User.DoesNotExist:
                pass

    if not user:
        email_from_tab = tab_mapping.get(name) or tab_mapping.get(key)
        if not email_from_tab:
            for tab_name_key, tab_email in tab_mapping.items():
                if _normalize(str(tab_name_key)) == key:
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
                                "username": email_from_tab,
                                "first_name": parts[0] if parts else "",
                                "last_name": " ".join(parts[1:]) if len(parts) > 1 else "",
                            },
                        )
                        if created:
                            logger.info("Auto-created user for KAM '%s' → %s", name, email_from_tab)

    if not user:
        user = db_lookup.get(key)

    if not user:
        compact = re.sub(r"[^a-z0-9]", "", key)
        user = db_lookup.get(compact)

    if not user:
        try:
            user = User.objects.get(username__iexact=name)
        except User.DoesNotExist:
            pass

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
            "KAM not found for name '%s' (normalized: '%s'). "
            "Add to KAM names tab (col A=short, B=full, C=email) "
            "or set KAM_USERMAP_JSON env var.",
            name, key,
        )

    local_cache[key] = user
    return user


def _load_env_usermap() -> Dict[str, str]:
    raw = _env("KAM_USERMAP_JSON", "{}")
    try:
        data = json.loads(raw)
        result = {}
        for k, v in data.items():
            result[k] = v
            result[_normalize(k)] = v
        return result
    except json.JSONDecodeError:
        logger.warning("KAM_USERMAP_JSON is not valid JSON.")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# POST-SYNC BACKFILL
# ─────────────────────────────────────────────────────────────────────────────

def _backfill_customer_kam() -> int:
    from django.db.models import Count

    unmapped = Customer.objects.filter(kam__isnull=True)
    updated = 0

    for customer in unmapped:
        top = (
            InvoiceFact.objects
            .filter(customer=customer, kam__isnull=False)
            .values("kam")
            .annotate(n=Count("kam"))
            .order_by("-n")
            .first()
        )
        if not top:
            logger.warning(
                "KAM backfill: no invoices with KAM for customer '%s' (pk=%s).",
                customer.name, customer.pk,
            )
            continue

        try:
            kam_user = User.objects.get(pk=top["kam"])
            customer.kam = kam_user
            customer.primary_kam = kam_user
            customer.save(update_fields=["kam", "primary_kam"])
            updated += 1
            logger.info(
                "KAM backfill: '%s' → %s",
                customer.name,
                kam_user.get_full_name() or kam_user.username,
            )
        except User.DoesNotExist:
            logger.error("KAM backfill: user pk=%s not found for customer '%s'", top["kam"], customer.name)

    logger.info("KAM backfill complete: %d customers updated", updated)
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: CUSTOMER DETAILS
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
            continue

        kam_name = _cell(row, 1)
        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )

        if kam_name and not kam_user:
            logger.warning(
                "Customer '%s': KAM name '%s' could not be mapped to any user.",
                name, kam_name,
            )

        try:
            with transaction.atomic():
                obj = _safe_get_or_create_customer(name, kam_user=kam_user)
                obj.address = _cell(row, 2) or obj.address
                obj.email = _cell(row, 3) or obj.email
                obj.mobile = _cell(row, 4) or obj.mobile
                obj.contact_person = _cell(row, 5) or obj.contact_person
                obj.credit_limit = _decimal(_cell(row, 6)) or obj.credit_limit
                obj.credit_period_days = _decimal(_cell(row, 7)) or obj.credit_period_days
                obj.total_exposure = _decimal(_cell(row, 8)) or obj.total_exposure
                obj.current_credit_limit = _decimal(_cell(row, 10)) or obj.current_credit_limit

                if kam_user:
                    obj.kam = kam_user
                    obj.primary_kam = kam_user

                obj.save()
                stats.customers_upserted += 1
        except Exception as exc:
            logger.error("Customer upsert failed for '%s': %s", name, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: SALES — Sales (F)
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
        buyer_name = _cell(row, 1)
        kam_name = _cell(row, 2)
        qty_raw = _cell(row, 3)
        full_name = _cell(row, 4)

        if not buyer_name and not full_name:
            stats.skipped += 1
            continue

        customer_name = full_name or buyer_name
        invoice_date = _parse_date(invoice_date_raw)
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
                InvoiceFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer": customer,
                        "kam": kam_user,
                        "invoice_date": invoice_date,
                        "qty_mt": qty,
                        "invoice_value": Decimal("0"),
                        "revenue_gst": Decimal("0"),
                        "raw_buyer_name": buyer_name,
                        "source_tab": tab_name,
                    },
                )
                stats.sales_upserted += 1
        except Exception as exc:
            logger.error("Sales (F) row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: SALES — Sheet1
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
    date_parse_failures: List[str] = []

    for i, row in enumerate(rows[1:], start=2):
        kam_name = _cell(row, 0)
        customer_name = _cell(row, 1)
        invoice_no = _cell(row, 4)
        invoice_date_raw = _cell(row, 5)
        invoice_date = _parse_date(invoice_date_raw)
        value_gst = _decimal(_cell(row, 6))
        qty = _decimal(_cell(row, 14)) or Decimal("0")
        invoice_value = _decimal(_cell(row, 17))
        rate_mt = _decimal(_cell(row, 16))
        grade = _cell(row, 12)
        size = _cell(row, 13)

        if not customer_name:
            stats.skipped += 1
            continue

        if not invoice_date:
            if invoice_date_raw and invoice_date_raw not in date_parse_failures:
                date_parse_failures.append(invoice_date_raw)
                logger.warning(
                    "Sheet1 row %d: unrecognised date format '%s' for customer '%s'.",
                    i, invoice_date_raw, customer_name,
                )
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

        final_value = value_gst or invoice_value or Decimal("0")

        try:
            with transaction.atomic():
                InvoiceFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer": customer,
                        "kam": kam_user,
                        "invoice_date": invoice_date,
                        "invoice_no": invoice_no,
                        "invoice_value": final_value,
                        "revenue_gst": final_value,
                        "qty_mt": qty,
                        "rate_mt": rate_mt,
                        "grade": grade,
                        "size": size,
                        "source_tab": tab_name,
                    },
                )
                stats.sales_upserted += 1
        except Exception as exc:
            logger.error("Sheet1 row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    if date_parse_failures:
        stats.notes.append(
            f"Sheet1: {len(date_parse_failures)} unique unrecognised date formats: "
            + ", ".join(repr(d) for d in date_parse_failures[:10])
        )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Front End
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
    header = [h.strip().lower() for h in rows[0]] if rows else []

    def _hcol(keywords: List[str], fallback: int) -> int:
        for idx, h in enumerate(header):
            for kw in keywords:
                if kw in h:
                    return idx
        return fallback

    col_enquiry_no = _hcol(["enquiry"], 0)
    col_timestamp = _hcol(["timestamp"], 1)
    col_kam_name = _hcol(["kam"], 4)
    col_customer_name = _hcol(["customer"], 5)
    col_grade = _hcol(["grade"], 6)
    col_size = _hcol(["size"], 7)
    col_qty = _hcol(["qty"], 8)
    col_status = _hcol(["status"], 11)
    col_revenue_mt = _hcol(["revenue"], 12)
    col_remarks = _hcol(["remark"], 13)

    logger.info(
        "Front End column map: enquiry=%d ts=%d kam=%d customer=%d "
        "grade=%d size=%d qty=%d status=%d revenue=%d remarks=%d",
        col_enquiry_no, col_timestamp, col_kam_name, col_customer_name,
        col_grade, col_size, col_qty, col_status, col_revenue_mt, col_remarks,
    )

    null_doe_count = 0

    for i, row in enumerate(rows[1:], start=2):
        enquiry_no = _cell(row, col_enquiry_no)
        timestamp_raw = _cell(row, col_timestamp)
        kam_name = _cell(row, col_kam_name)
        customer_name = _cell(row, col_customer_name)
        grade = _cell(row, col_grade)
        size = _cell(row, col_size)
        qty_raw = _cell(row, col_qty)
        status = _cell(row, col_status)
        revenue_mt = _decimal(_cell(row, col_revenue_mt))
        remarks = _cell(row, col_remarks)

        if not customer_name:
            stats.skipped += 1
            continue

        ts = _parse_timestamp(timestamp_raw)
        doe_date = ts.date() if ts else None
        if not doe_date:
            doe_date = timezone.now().date()
            null_doe_count += 1
            logger.debug(
                "Front End row %d: timestamp '%s' → falling back to today (%s) for customer '%s'",
                i, timestamp_raw, doe_date, customer_name,
            )

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
                        "customer": customer,
                        "kam": kam_user,
                        "doe": doe_date,
                        "qty_mt": qty,
                        "status": status or "OPEN",
                        "grade": grade,
                        "size": size,
                        "revenue_mt": revenue_mt,
                        "remarks": remarks,
                        "source_tab": tab_name,
                        "enquiry_no": enquiry_no,
                    },
                )
                stats.leads_upserted += 1
        except Exception as exc:
            logger.error("Front End row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    if null_doe_count:
        stats.notes.append(
            f"Front End: {null_doe_count} rows had missing/unparseable timestamp — doe set to today"
        )
        logger.info("Front End: %d rows used today as fallback doe", null_doe_count)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Enquiry (F)
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
    null_doe_count = 0

    for i, row in enumerate(rows[1:], start=2):
        timestamp_raw = _cell(row, 0)
        kam_name = _cell(row, 1)
        customer_name = _cell(row, 2)
        qty_raw = _cell(row, 3)
        status = _cell(row, 4)
        remarks = _cell(row, 5)

        if not customer_name:
            stats.skipped += 1
            continue

        ts = _parse_timestamp(timestamp_raw)
        doe_date = ts.date() if ts else None
        if not doe_date:
            doe_date = timezone.now().date()
            null_doe_count += 1
            logger.debug(
                "Enquiry (F) row %d: timestamp '%s' → falling back to today for customer '%s'",
                i, timestamp_raw, customer_name,
            )

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
                        "customer": customer,
                        "kam": kam_user,
                        "doe": doe_date,
                        "qty_mt": qty,
                        "status": status or "OPEN",
                        "remarks": remarks,
                        "source_tab": tab_name,
                    },
                )
                stats.leads_upserted += 1
        except Exception as exc:
            logger.error("Enquiry (F) row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    if null_doe_count:
        stats.notes.append(
            f"Enquiry (F): {null_doe_count} rows had missing/unparseable timestamp — doe set to today"
        )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: OVERDUES
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

    header = [h.strip().lower() for h in rows[0]] if rows else []

    def _col(keywords: List[str], fallback: int) -> int:
        for idx, h in enumerate(header):
            for kw in keywords:
                if kw in h:
                    return idx
        return fallback

    col_customer = _col(["customer"], 0)
    col_kam = _col(["kam"], 1)
    col_overdue = _col(["overdue", "dues"], 2)
    col_exposure = _col(["exposure"], 3)
    col_a0 = _col(["0-30", "0_30"], 4)
    col_a31 = _col(["31-60", "31_60"], 5)
    col_a61 = _col(["61-90", "61_90"], 6)
    col_a90 = _col(["90+", "90_plus", "above 90"], 7)

    has_ageing = any(kw in " ".join(header) for kw in ["0-30", "31-60", "61-90", "90+"])
    if not has_ageing:
        logger.warning("Overdues tab: ageing columns not found in header. Header: %s", header[:10])

    snapshot_date = timezone.now().date()

    for i, row in enumerate(rows[1:], start=2):
        customer_name = _cell(row, col_customer)
        kam_name = _cell(row, col_kam)
        overdue_raw = _cell(row, col_overdue)

        if not customer_name:
            continue

        overdue_amt = _decimal(overdue_raw)
        if overdue_amt is None:
            stats.skipped += 1
            continue

        exposure_raw = _cell(row, col_exposure)
        a0_raw = _cell(row, col_a0)
        a31_raw = _cell(row, col_a31)
        a61_raw = _cell(row, col_a61)
        a90_raw = _cell(row, col_a90)

        a0 = _decimal(a0_raw) or Decimal("0")
        a31 = _decimal(a31_raw) or Decimal("0")
        a61 = _decimal(a61_raw) or Decimal("0")
        a90 = _decimal(a90_raw) or Decimal("0")
        ageing_total = a0 + a31 + a61 + a90

        exposure = _decimal(exposure_raw) or (ageing_total if ageing_total > 0 else overdue_amt)

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
                        "kam": kam_user,
                        "overdue": overdue_amt,
                        "overdue_amt": overdue_amt,
                        "exposure": exposure,
                        "ageing_0_30": a0,
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
# SECTION: COLLECTION PLAN SNAPSHOT (from Overdues tab)
# ─────────────────────────────────────────────────────────────────────────────

def _sync_overdues_to_collection_plan(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    """
    Sync Google Sheet Overdues tab → CollectionPlan.overdue_amount snapshot.

    DATA SOURCE: Overdues tab, Yellow section (columns A–C only):
      col A = customer_name
      col B = kam_name
      col C = overdue_amount

    RULES:
      - kam_id is MANDATORY; skip rows where KAM cannot be resolved
      - Creates CollectionPlan if customer+kam pair doesn't exist
      - Updates ONLY overdue_amount on existing entries
      - NEVER overwrites actual_amount, collection_date, payment_details, utr_number
      - Deduplicates within a single sync run (same customer+kam seen twice → skip second)
      - No manual customer or amount entry

    Returns SyncStats with customers_upserted counting rows synced into CollectionPlan.
    """
    from .models import CollectionPlan  # local import avoids circular

    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_overdues())

    if len(rows) < 2:
        stats.notes.append("Overdues tab: no data rows — Collection Plan sync skipped")
        logger.warning("_sync_overdues_to_collection_plan: Overdues tab empty.")
        return stats

    header = [h.strip().lower() for h in rows[0]] if rows else []

    def _col(keywords: List[str], fallback: int) -> int:
        for idx, h in enumerate(header):
            for kw in keywords:
                if kw in h:
                    return idx
        return fallback

    # Yellow section: cols A–C
    col_customer = _col(["customer"], 0)
    col_kam      = _col(["kam"],      1)
    col_overdue  = _col(["overdue", "dues", "amount"], 2)

    logger.info(
        "_sync_overdues_to_collection_plan: col_customer=%d col_kam=%d col_overdue=%d",
        col_customer, col_kam, col_overdue,
    )

    snapshot_date = timezone.now().date()
    now_ts        = timezone.now()

    # Track pairs already processed in this sync run — prevents duplicates
    processed_pairs: set = set()

    for i, row in enumerate(rows[1:], start=2):
        customer_name = _cell(row, col_customer)
        kam_name      = _cell(row, col_kam)
        overdue_raw   = _cell(row, col_overdue)

        if not customer_name:
            stats.skipped += 1
            continue

        overdue_amt = _decimal(overdue_raw)
        if overdue_amt is None or overdue_amt <= 0:
            logger.debug(
                "Collection Plan sync row %d: zero/invalid overdue '%s' for customer '%s' — skipped",
                i, overdue_raw, customer_name,
            )
            stats.skipped += 1
            continue

        # KAM is MANDATORY for collection plan
        if not kam_name:
            logger.warning(
                "Collection Plan sync row %d: customer '%s' has no KAM name — skipped",
                i, customer_name,
            )
            stats.unknown_kam += 1
            continue

        kam_user = _resolve_kam_user(
            kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache
        )

        if not kam_user:
            # unknown_kam already incremented by _resolve_kam_user
            continue

        # Dedup within this sync run
        pair_key = (_normalize(customer_name), kam_user.id)
        if pair_key in processed_pairs:
            logger.debug(
                "Collection Plan sync row %d: duplicate pair ('%s', %s) skipped",
                i, customer_name, kam_user.username,
            )
            stats.skipped += 1
            continue
        processed_pairs.add(pair_key)

        try:
            customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

            with transaction.atomic():
                # Authoritative key for new system: customer + kam
                existing = (
                    CollectionPlan.objects
                    .filter(customer=customer, kam=kam_user)
                    .order_by("-last_synced_at", "-created_at")
                    .first()
                )

                if existing:
                    # UPDATE overdue_amount ONLY — never touch actual collection data
                    existing.overdue_amount = overdue_amt
                    existing.planned_amount = overdue_amt   # backward compat
                    existing.last_synced_at = now_ts
                    existing.save(update_fields=[
                        "overdue_amount", "planned_amount", "last_synced_at", "updated_at",
                    ])
                    logger.debug(
                        "Collection Plan updated: customer='%s' kam='%s' overdue=₹%s",
                        customer.name, kam_user.username, overdue_amt,
                    )
                else:
                    CollectionPlan.objects.create(
                        customer       = customer,
                        kam            = kam_user,
                        overdue_amount = overdue_amt,
                        planned_amount = overdue_amt,
                        period_type    = None,
                        period_id      = None,
                        from_date      = snapshot_date,
                        to_date        = None,
                        last_synced_at = now_ts,
                    )
                    logger.info(
                        "Collection Plan created: customer='%s' kam='%s' overdue=₹%s",
                        customer.name, kam_user.username, overdue_amt,
                    )

                stats.customers_upserted += 1

        except Exception as exc:
            logger.error(
                "Collection Plan sync row %d failed: customer='%s' kam='%s' — %s",
                i, customer_name, kam_name, exc,
            )
            stats.skipped += 1

    logger.info(
        "Collection Plan sync complete: synced=%d skipped=%d unknown_kam=%d",
        stats.customers_upserted, stats.skipped, stats.unknown_kam,
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: COLLECTION
# ─────────────────────────────────────────────────────────────────────────────

COLLECTION_SOURCE_SHEET = "GOOGLE_SHEET"
COLLECTION_SOURCE_ERP = "ERP"


def _sync_collections(
    service, sheet_id: str,
    tab_mapping, db_lookup, env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_collection())
    if len(rows) < 2:
        stats.notes.append("Collection tab: no data rows (tab may not exist — skipping)")
        logger.info("Collection tab empty or not found — skipping collection sync")
        return stats

    tab_name = _tab_collection()
    header = [h.strip().lower() for h in rows[0]] if rows else []

    def _hcol(keywords: List[str], fallback: int) -> int:
        for idx, h in enumerate(header):
            for kw in keywords:
                if kw in h:
                    return idx
        return fallback

    col_date = int(_env("KAM_COLLECTION_COL_DATE", str(_hcol(["date"], 0))))
    col_customer = int(_env("KAM_COLLECTION_COL_CUSTOMER", str(_hcol(["customer"], 1))))
    col_kam = int(_env("KAM_COLLECTION_COL_KAM", str(_hcol(["kam"], 2))))
    col_amount = int(_env("KAM_COLLECTION_COL_AMOUNT", str(_hcol(["amount", "collection", "amt"], 3))))
    col_mode = int(_env("KAM_COLLECTION_COL_MODE", str(_hcol(["mode", "payment"], 4))))
    col_ref = int(_env("KAM_COLLECTION_COL_REF", str(_hcol(["ref", "utr", "cheque"], 5))))
    col_remarks = int(_env("KAM_COLLECTION_COL_REMARKS", str(_hcol(["remark", "note"], 6))))

    logger.info(
        "Collection tab column map: date=%d customer=%d kam=%d amount=%d mode=%d ref=%d remarks=%d",
        col_date, col_customer, col_kam, col_amount, col_mode, col_ref, col_remarks,
    )

    for i, row in enumerate(rows[1:], start=2):
        date_raw = _cell(row, col_date)
        customer_name = _cell(row, col_customer)
        kam_name = _cell(row, col_kam)
        amount_raw = _cell(row, col_amount)
        mode = _cell(row, col_mode)
        reference = _cell(row, col_ref)
        remarks = _cell(row, col_remarks)

        if not customer_name or not amount_raw:
            stats.skipped += 1
            continue

        txn_date = _parse_date(date_raw)
        if not txn_date:
            logger.debug("Collection row %d: cannot parse date '%s'", i, date_raw)
            stats.skipped += 1
            continue

        amount = _decimal(amount_raw)
        if amount is None or amount <= 0:
            stats.skipped += 1
            continue

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name else None
        )
        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)
        row_uuid = _make_row_uuid(tab_name, txn_date, customer_name, amount_raw, i)

        try:
            with transaction.atomic():
                obj, created = CollectionTxn.objects.get_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer": customer,
                        "kam": kam_user,
                        "txn_datetime": timezone.make_aware(
                            datetime(txn_date.year, txn_date.month, txn_date.day)
                        ),
                        "amount": amount,
                        "mode": mode or None,
                        "reference": reference or None,
                        "reference_no": reference or None,
                        "notes": remarks or None,
                        "source": COLLECTION_SOURCE_SHEET,
                    },
                )
                if not created:
                    update_fields = [
                        "customer", "amount", "mode", "reference",
                        "reference_no", "notes", "updated_at",
                    ]
                    obj.customer = customer
                    obj.amount = amount
                    obj.mode = mode or None
                    obj.reference = reference or None
                    obj.reference_no = reference or None
                    obj.notes = remarks or None
                    if kam_user and obj.kam_id != kam_user.id:
                        obj.kam = kam_user
                        update_fields.append("kam")
                    obj.save(update_fields=update_fields)
                stats.collections_upserted += 1
        except Exception as exc:
            logger.error("Collection row %d upsert failed: %s", i, exc)
            stats.skipped += 1

    logger.info(
        "Collection sync complete: %d upserted, %d skipped (source=%s)",
        stats.collections_upserted, stats.skipped, COLLECTION_SOURCE_SHEET,
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SYNC ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_sync_now() -> SyncStats:
    sheet_id = _require_env("KAM_SALES_SHEET_ID")
    sections = resolve_sections()
    total = SyncStats()

    try:
        service = build_sheets_service()
    except GoogleCredentialError:
        raise

    tab_mapping = _load_kam_names_tab(service, sheet_id)
    db_lookup = _build_user_lookup()
    env_usermap = _load_env_usermap()
    local_cache: Dict[str, Optional[User]] = {}

    logger.info(
        "Starting sync | sheet=%s | tab_mapping_entries=%d | db_users=%d",
        sheet_id, len(tab_mapping), len(db_lookup),
    )

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
        logger.info("Syncing: Front End (leads)")
        s = _sync_frontend(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → leads=%d skipped=%d", s.leads_upserted, s.skipped)

    if sections.get("enquiry_f"):
        logger.info("Syncing: Enquiry (F) (leads)")
        s = _sync_enquiry_f(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → leads=%d skipped=%d", s.leads_upserted, s.skipped)

    if sections.get("overdues"):
        logger.info("Syncing: Overdues")
        s = _sync_overdues(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → overdues=%d skipped=%d", s.overdues_upserted, s.skipped)

    # ── NEW: Sync Overdues → CollectionPlan snapshot ────────────────────────
    if sections.get("overdues"):
        logger.info("Syncing: Overdues → CollectionPlan (overdue_amount snapshot)")
        s_cp = _sync_overdues_to_collection_plan(
            service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache
        )
        # Merge into total — use collections_upserted counter to avoid double-counting customers
        total.collections_upserted += s_cp.customers_upserted
        total.skipped              += s_cp.skipped
        if s_cp.notes:
            total.notes.extend(s_cp.notes)
        logger.info(
            "  → collection_plan_synced=%d skipped=%d unknown_kam=%d",
            s_cp.customers_upserted, s_cp.skipped, s_cp.unknown_kam,
        )

    if sections.get("collection"):
        logger.info("Syncing: Collection (source=GOOGLE_SHEET)")
        s = _sync_collections(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(s)
        logger.info("  → collections=%d skipped=%d", s.collections_upserted, s.skipped)

    logger.info("Running KAM backfill for unmapped customers ...")
    backfilled = _backfill_customer_kam()
    if backfilled:
        total.notes.append(f"KAM backfill: {backfilled} customers updated from invoice history")

    logger.info("Sync complete: %s", total.as_message())
    if total.notes:
        logger.warning("Sync notes: %s", "; ".join(total.notes))

    return total


# ─────────────────────────────────────────────────────────────────────────────
# STEP SYNC
# ─────────────────────────────────────────────────────────────────────────────

_STEPS = [
    ("customers",            "Customer Details"),
    ("sales_f",              "Sales (F)"),
    ("sheet1",               "Sheet1"),
    ("frontend",             "Front End"),
    ("enquiry_f",            "Enquiry (F)"),
    ("overdues",             "Overdues"),
    ("collection_plan_sync", "Collection Plan Snapshot"),   # ← NEW
    ("collection",           "Collection"),
]

_STEP_FN_MAP = {
    "customers":            _sync_customers,
    "sales_f":              _sync_sales_f,
    "sheet1":               _sync_sheet1,
    "frontend":             _sync_frontend,
    "enquiry_f":            _sync_enquiry_f,
    "overdues":             _sync_overdues,
    "collection_plan_sync": _sync_overdues_to_collection_plan,   # ← NEW
    "collection":           _sync_collections,
}


def step_sync(intent: "SyncIntent", *args, **kwargs) -> Dict[str, Any]:
    cursor = getattr(intent, "cursor_position", 0) or 0
    sections = resolve_sections()
    sheet_id = _require_env("KAM_SALES_SHEET_ID")

    if cursor >= len(_STEPS):
        intent.status = "COMPLETE"
        intent.save(update_fields=["status"])
        return {"done": True, "message": "All sections synced"}

    section_key, section_label = _STEPS[cursor]

    try:
        service = build_sheets_service()
        tab_mapping = _load_kam_names_tab(service, sheet_id)
        db_lookup = _build_user_lookup()
        env_usermap = _load_env_usermap()
        local_cache: Dict[str, Optional[User]] = {}

        stats = SyncStats()
        if sections.get(section_key):
            fn = _STEP_FN_MAP.get(section_key)
            if fn:
                stats = fn(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)

        next_cursor = cursor + 1
        is_last = next_cursor >= len(_STEPS)

        backfilled = 0
        if is_last:
            logger.info("step_sync final step: running KAM backfill ...")
            backfilled = _backfill_customer_kam()
            if backfilled:
                stats.notes.append(f"KAM backfill: {backfilled} customers updated")

        intent.cursor_position = next_cursor
        intent.status = "COMPLETE" if is_last else "IN_PROGRESS"
        intent.save(update_fields=["cursor_position", "status"])

        return {
            "done": intent.status == "COMPLETE",
            "step": section_label,
            "message": stats.as_message(),
            "stats": {
                "customers_upserted": stats.customers_upserted,
                "sales_upserted": stats.sales_upserted,
                "leads_upserted": stats.leads_upserted,
                "overdues_upserted": stats.overdues_upserted,
                "collections_upserted": stats.collections_upserted,
                "skipped": stats.skipped,
                "unknown_kam": stats.unknown_kam,
                "kam_backfilled": backfilled,
            },
        }

    except Exception as exc:
        logger.error("step_sync failed at '%s': %s", section_label, exc)
        intent.status = "ERROR"
        intent.last_error = str(exc)
        intent.save(update_fields=["status", "last_error"])
        raise