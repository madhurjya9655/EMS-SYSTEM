# FILE: apps/kam/sheets_adapter.py
# PURPOSE: Stable Google Sheet → PostgreSQL sync for KAM dashboards / Customer 360
#
# DATA FLOW:
#   Google Sheet
#     → Django sync command / sync button
#     → PostgreSQL
#     → Dashboard reads PostgreSQL only
#
# IMPORTANT RULES:
#   - Dashboard should never directly read Google Sheets.
#   - Sync must be idempotent.
#   - Sync must repair existing rows when sheet data changes.
#   - No production customer data is hard-deleted here.
#   - Sales(F) stores only "Order Converted" rows.
#   - CollectionPlan is driven from Overdues tab.
#   - CollectionPlan sync must never overwrite actual collection fields.

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
from typing import Any, Dict, List, Optional

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

try:
    from apps.common.google_auth import GoogleCredentialError
except ImportError:

    class GoogleCredentialError(Exception):  # type: ignore[no-redef]
        pass


from .models import (
    CollectionTxn,
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
    value = _env(name, "1" if default else "0").lower()
    return value not in ("0", "false", "no", "off", "")


def _require_env(name: str) -> str:
    value = _env(name)
    if not value:
        raise RuntimeError(
            f"Missing required env var: {name}\n"
            "Add this to your environment variables."
        )
    return value


# ─────────────────────────────────────────────────────────────────────────────
# ROW UUID
# ─────────────────────────────────────────────────────────────────────────────

def _make_row_uuid(*parts) -> str:
    key = "|".join(str(part or "") for part in parts)
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
                "Paste the full service account JSON content as the env var value."
            ) from exc

    path_candidates = [
        _env("KAM_SA_JSON"),
        _env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),
        _env("GOOGLE_SERVICE_ACCOUNT_FILE"),
    ]

    attempted = [path for path in path_candidates if path]

    for sa_path in attempted:
        if _os.path.isfile(sa_path):
            try:
                with open(sa_path, encoding="utf-8") as fh:
                    return _json.load(fh)
            except Exception as exc:
                raise GoogleCredentialError(
                    f"Could not read service account file {sa_path}: {exc}"
                ) from exc

    if attempted:
        raise GoogleCredentialError(
            "Service account file not found at any configured path:\n"
            + "\n".join(f"  {path}" for path in attempted)
            + "\n\nSet KAM_SA_JSON_CONTENT with the full service account JSON."
        )

    raise GoogleCredentialError(
        "No Google service account credentials configured.\n"
        "Required: KAM_SA_JSON_CONTENT with JSON content\n"
        "Or: KAM_SA_JSON with path to .json file"
    )


def build_sheets_service():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build as google_api_build
    except ImportError as exc:
        raise GoogleCredentialError(
            f"Google API client libraries not installed: {exc}\n"
            "Run: pip install google-auth google-api-python-client"
        ) from exc

    sa_info = _load_sa_info()

    try:
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )

        return google_api_build(
            "sheets",
            "v4",
            credentials=credentials,
            cache_discovery=False,
        )
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
    sync_overdues = _env_flag("KAM_SYNC_OVERDUES", True)

    return {
        "sales_f": _env_flag("KAM_SYNC_SALES", True),
        "sheet1": _env_flag("KAM_SYNC_SHEET1", True),
        "frontend": _env_flag("KAM_SYNC_FRONTEND", True),
        "enquiry_f": _env_flag("KAM_SYNC_ENQUIRY", True),
        "customers": _env_flag("KAM_SYNC_CUSTOMERS", True),
        "overdues": sync_overdues,
        "collection_plan_sync": sync_overdues,
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
        logger.warning(
            "Could not read tab %r from sheet %r: %s",
            tab,
            sheet_id,
            exc,
        )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# FIELD PARSING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _cell(row: List[str], idx: int, default: str = "") -> str:
    try:
        return str(row[idx] or "").strip()
    except IndexError:
        return default


def _decimal(val: str) -> Optional[Decimal]:
    """
    Safely parse Google Sheet numeric values.

    Handles:
      ₹1,23,456.00
      1,23,456
      123456
      (1234) -> -1234
      blank  -> None
      -      -> None
    """
    if val is None:
        return None

    raw = str(val).strip()

    if raw in {"", "-", "—", "NA", "N/A", "None", "null"}:
        return None

    negative = False

    if raw.startswith("(") and raw.endswith(")"):
        negative = True
        raw = raw[1:-1]

    cleaned = re.sub(r"[₹,\s]", "", raw)

    if cleaned in {"", "-", "."}:
        return None

    try:
        amount = Decimal(cleaned)
        return -amount if negative else amount
    except InvalidOperation:
        return None


def _parse_date(val: str) -> Optional[date]:
    if not val:
        return None

    raw = str(val).strip().strip("'").strip()

    if not raw:
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
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    try:
        serial = int(float(raw))
        if 10000 < serial < 100000:
            return date(1899, 12, 30) + timedelta(days=serial)
    except (ValueError, OverflowError):
        pass

    logger.debug("Could not parse date: %r", raw)
    return None


def _parse_timestamp(val: str) -> Optional[datetime]:
    """
    Parse Google Sheet timestamp values into timezone-aware datetime.
    """
    if not val:
        return None

    raw = str(val).strip().strip("'").strip()

    if not raw:
        return None

    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%d/%m/%Y %I:%M %p",
        "%d-%m-%Y %I:%M:%S %p",
        "%d-%m-%Y %I:%M %p",
    ):
        try:
            parsed = datetime.strptime(raw, fmt)
            return timezone.make_aware(parsed) if timezone.is_naive(parsed) else parsed
        except ValueError:
            continue

    parsed_date = _parse_date(raw)

    if parsed_date:
        return timezone.make_aware(
            datetime(parsed_date.year, parsed_date.month, parsed_date.day)
        )

    return None


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = value.encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _header_index(
    header: List[str],
    keywords: List[str],
    fallback: int,
    *,
    env_name: str = "",
) -> int:
    """
    Robust column resolver.

    Priority:
      1. Explicit env override
      2. Normalized header match
      3. Fallback index

    Env value must be zero-based index.
    Example:
      KAM_SALES_F_COL_KAM=1
    """
    if env_name:
        raw_env = _env(env_name)
        if raw_env.isdigit():
            return int(raw_env)

    normalized_keywords = [_normalize(keyword) for keyword in keywords if keyword]

    for idx, heading in enumerate(header):
        normalized_heading = _normalize(heading)

        if not normalized_heading:
            continue

        for keyword in normalized_keywords:
            if keyword and keyword in normalized_heading:
                return idx

    return fallback


def _status_key(value: str) -> str:
    return _normalize(value or "")


SALES_CONVERTED_STATUS_KEYS = {
    "orderconverted",
}


LEAD_WON_STATUS_KEYS = {
    "won",
    "converted",
    "orderconverted",
}


def _normalize_lead_status(raw_status: str) -> str:
    key = _status_key(raw_status)

    if key in LEAD_WON_STATUS_KEYS:
        return "WON"

    if key in {
        "lost",
        "closedlost",
        "notconverted",
        "cancelled",
        "canceled",
        "rejected",
    }:
        return "LOST"

    if key in {
        "negotiation",
        "negotiating",
        "discussion",
        "followup",
        "followuprequired",
    }:
        return "NEGOTIATION"

    return "OPEN"


# ─────────────────────────────────────────────────────────────────────────────
# SAFE CUSTOMER LOOKUP / CREATE
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_customer_name(name: str) -> str:
    return " ".join(str(name or "").strip().split()).strip()


def _safe_get_or_create_customer(
    name: str,
    kam_user=None,
    extra_defaults: dict = None,
) -> Customer:
    """
    Production-safe customer resolver.

    Rules:
      - Never hard-delete duplicate customers.
      - Reuse the oldest matching customer by case-insensitive name.
      - Repair kam / primary_kam when KAM is known.
      - Fill missing optional fields only.
    """
    clean_name = _normalize_customer_name(name)

    if not clean_name:
        raise ValueError("Customer name cannot be blank")

    defaults = {
        "name": clean_name,
        "kam": kam_user,
        "primary_kam": kam_user,
        "source": Customer.SOURCE_SHEET,
    }

    if extra_defaults:
        defaults.update(extra_defaults)

    matches = list(
        Customer.objects
        .filter(name__iexact=clean_name)
        .order_by("pk")
    )

    if not matches:
        with transaction.atomic():
            customer, _created = Customer.objects.get_or_create(
                name=clean_name,
                defaults=defaults,
            )
            return customer

    customer = matches[0]

    if len(matches) > 1:
        logger.warning(
            "Duplicate customer names found for %r. Using pk=%s. Duplicate pks=%s. No rows deleted.",
            clean_name,
            customer.pk,
            [item.pk for item in matches[1:]],
        )

    changed = False

    if kam_user:
        if customer.kam_id != kam_user.id:
            customer.kam = kam_user
            changed = True

        if customer.primary_kam_id != kam_user.id:
            customer.primary_kam = kam_user
            changed = True

    if extra_defaults:
        for field_name, value in extra_defaults.items():
            if value is not None and not getattr(customer, field_name, None):
                setattr(customer, field_name, value)
                changed = True

    if changed:
        customer.save()

    return customer


# ─────────────────────────────────────────────────────────────────────────────
# KAM USER MAPPING
# ─────────────────────────────────────────────────────────────────────────────

def _load_kam_names_tab(service, sheet_id: str) -> Dict[str, str]:
    rows = _get_sheet_values(service, sheet_id, _tab_kam_names())

    if not rows:
        logger.warning("KAM names tab empty or missing. KAM matching will rely on DB only.")
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

    for user in User.objects.filter(is_active=True):
        full_name = f"{user.first_name} {user.last_name}".strip()

        if full_name:
            lookup[_normalize(full_name)] = user

        if user.email:
            lookup[_normalize(user.email)] = user
            lookup[_normalize(user.email.split("@")[0])] = user

        username = user.username or ""
        lookup[_normalize(username)] = user

        parts = re.split(r"[._\-]", username)

        if len(parts) >= 2:
            lookup[_normalize(parts[0])] = user
            lookup[_normalize("".join(parts))] = user

        first_name = (user.first_name or (parts[0] if parts else "")).strip().lower()

        if first_name:
            first_name_index.setdefault(first_name, []).append(user)

    for first_name, users in first_name_index.items():
        if len(users) == 1:
            lookup[_normalize(first_name)] = users[0]

    return lookup


def _load_env_usermap() -> Dict[str, str]:
    raw = _env("KAM_USERMAP_JSON", "{}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("KAM_USERMAP_JSON is not valid JSON.")
        return {}

    result: Dict[str, str] = {}

    for key, value in data.items():
        result[key] = value
        result[_normalize(key)] = value

    return result


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

    clean_key = _normalize(name)

    if clean_key in local_cache:
        return local_cache[clean_key]

    user: Optional[User] = None

    email_override = env_usermap.get(name) or env_usermap.get(clean_key)

    if email_override:
        user = (
            User.objects
            .filter(email__iexact=email_override)
            .first()
            or User.objects
            .filter(username__iexact=email_override)
            .first()
        )

    if not user:
        email_from_tab = tab_mapping.get(name) or tab_mapping.get(clean_key)

        if not email_from_tab:
            for tab_name_key, tab_email in tab_mapping.items():
                if _normalize(tab_name_key) == clean_key:
                    email_from_tab = tab_email
                    break

        if email_from_tab:
            user = (
                User.objects
                .filter(email__iexact=email_from_tab)
                .first()
                or User.objects
                .filter(username__iexact=email_from_tab)
                .first()
            )

            if not user and _env_flag("KAM_AUTO_CREATE_USERS", False):
                parts = str(name).split()
                user, created = User.objects.get_or_create(
                    email=email_from_tab,
                    defaults={
                        "username": email_from_tab,
                        "first_name": parts[0] if parts else "",
                        "last_name": " ".join(parts[1:]) if len(parts) > 1 else "",
                    },
                )

                if created:
                    logger.info("Auto-created user for KAM %r → %s", name, email_from_tab)

    if not user:
        user = db_lookup.get(clean_key)

    if not user:
        compact_key = re.sub(r"[^a-z0-9]", "", clean_key)
        user = db_lookup.get(compact_key)

    if not user:
        user = User.objects.filter(username__iexact=name).first()

    if not user:
        name_parts = str(name).split()

        if len(name_parts) >= 2:
            user = User.objects.filter(
                first_name__iexact=name_parts[0],
                last_name__iexact=name_parts[-1],
            ).first()

    if not user:
        stats.unknown_kam += 1
        stats.notes.append(f"Unknown KAM: '{name}'")
        logger.warning(
            "KAM not found for name %r, normalized=%r. "
            "Add it to KAM names tab or KAM_USERMAP_JSON.",
            name,
            clean_key,
        )

    local_cache[clean_key] = user
    return user


# ─────────────────────────────────────────────────────────────────────────────
# POST-SYNC BACKFILL
# ─────────────────────────────────────────────────────────────────────────────

def _backfill_customer_kam() -> int:
    from django.db.models import Count

    updated = 0

    for customer in Customer.objects.filter(kam__isnull=True):
        top = (
            InvoiceFact.objects
            .filter(customer=customer, kam__isnull=False)
            .values("kam")
            .annotate(total=Count("kam"))
            .order_by("-total")
            .first()
        )

        if not top:
            logger.warning(
                "KAM backfill: no invoices with KAM for customer %r, pk=%s.",
                customer.name,
                customer.pk,
            )
            continue

        kam_user = User.objects.filter(pk=top["kam"]).first()

        if not kam_user:
            logger.error(
                "KAM backfill: user pk=%s not found for customer %r.",
                top["kam"],
                customer.name,
            )
            continue

        customer.kam = kam_user
        customer.primary_kam = kam_user
        customer.save(update_fields=["kam", "primary_kam", "updated_at"])

        updated += 1

        logger.info(
            "KAM backfill: %r → %s",
            customer.name,
            kam_user.get_full_name() or kam_user.username,
        )

    logger.info("KAM backfill complete: %d customers updated", updated)
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: CUSTOMER DETAILS
# ─────────────────────────────────────────────────────────────────────────────

def _sync_customers(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
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
            if kam_name
            else None
        )

        try:
            with transaction.atomic():
                customer = _safe_get_or_create_customer(name, kam_user=kam_user)

                customer.address = _cell(row, 2) or customer.address
                customer.email = _cell(row, 3) or customer.email
                customer.mobile = _cell(row, 4) or customer.mobile
                customer.contact_person = _cell(row, 5) or customer.contact_person
                customer.credit_limit = _decimal(_cell(row, 6)) or customer.credit_limit
                customer.credit_period_days = _decimal(_cell(row, 7)) or customer.credit_period_days
                customer.total_exposure = _decimal(_cell(row, 8)) or customer.total_exposure
                customer.current_credit_limit = _decimal(_cell(row, 10)) or customer.current_credit_limit

                if kam_user:
                    customer.kam = kam_user
                    customer.primary_kam = kam_user

                customer.save()
                stats.customers_upserted += 1

        except Exception as exc:
            logger.error("Customer upsert failed for %r: %s", name, exc)
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
    """
    Sync Sales (F) tab into InvoiceFact.

    Actual Sales (F) sheet structure:

      0 = Date of Invoice
      1 = Buyer's Name
      2 = KAM
      3 = Qty(MT)
      4 = Full Name

    Important:
      Sales (F) does NOT have a status column.
      Every valid row in this tab is treated as sales.
    """
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_sales_f())

    if len(rows) < 2:
        stats.notes.append("Sales (F) tab: no data rows")
        return stats

    tab_name = _tab_sales_f()
    header = [h.strip().lower() for h in rows[0]] if rows else []

    def _hcol(keywords: List[str], fallback: int) -> int:
        for idx, h in enumerate(header):
            for kw in keywords:
                if kw in h:
                    return idx
        return fallback

    # Allow env override, but use correct Sales (F) defaults.
    col_invoice_date = int(
        _env("KAM_SALES_F_COL_DATE", str(_hcol(["date of invoice", "invoice date", "date"], 0)))
    )
    col_customer = int(
        _env("KAM_SALES_F_COL_CUSTOMER", str(_hcol(["buyer", "customer"], 1)))
    )
    col_kam = int(
        _env("KAM_SALES_F_COL_KAM", str(_hcol(["kam"], 2)))
    )
    col_qty = int(
        _env("KAM_SALES_F_COL_QTY", str(_hcol(["qty", "mt"], 3)))
    )
    col_full_name = int(
        _env("KAM_SALES_F_COL_FULL_NAME", str(_hcol(["full name"], 4)))
    )

    # Safety correction for this exact sheet:
    # Header contains KAM at col 2 and Full Name at col 4.
    if header[:5] == ["date of invoice", "buyer's name", "kam", "qty(mt)", "full name"]:
        col_invoice_date = 0
        col_customer = 1
        col_kam = 2
        col_qty = 3
        col_full_name = 4

    logger.info(
        "Sales (F) column map: invoice_date=%d customer=%d kam=%d qty=%d full_name=%d",
        col_invoice_date,
        col_customer,
        col_kam,
        col_qty,
        col_full_name,
    )

    for i, row in enumerate(rows[1:], start=2):
        invoice_date_raw = _cell(row, col_invoice_date)
        customer_name = _cell(row, col_customer)
        kam_name = _cell(row, col_kam)
        qty_raw = _cell(row, col_qty)
        full_name = _cell(row, col_full_name)

        if not customer_name:
            stats.skipped += 1
            logger.debug("Sales (F) row %d skipped: blank customer", i)
            continue

        invoice_date = _parse_date(invoice_date_raw)
        if not invoice_date:
            stats.skipped += 1
            logger.warning(
                "Sales (F) row %d skipped: invalid invoice date %r for customer %r",
                i,
                invoice_date_raw,
                customer_name,
            )
            continue

        qty = _decimal(qty_raw)
        if qty is None:
            stats.skipped += 1
            logger.warning(
                "Sales (F) row %d skipped: invalid Qty(MT) %r for customer %r",
                i,
                qty_raw,
                customer_name,
            )
            continue

        # Prefer short KAM column. If blank, use Full Name column.
        effective_kam_name = kam_name or full_name

        kam_user = (
            _resolve_kam_user(
                effective_kam_name,
                tab_mapping,
                db_lookup,
                env_usermap,
                stats,
                local_cache,
            )
            if effective_kam_name
            else None
        )

        if not kam_user:
            stats.skipped += 1
            logger.warning(
                "Sales (F) row %d skipped: unknown KAM %r / full name %r for customer %r",
                i,
                kam_name,
                full_name,
                customer_name,
            )
            continue

        try:
            customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

            row_uuid = _make_row_uuid(
                tab_name,
                invoice_date_raw,
                customer_name,
                effective_kam_name,
                qty_raw,
                i,
            )

            with transaction.atomic():
                InvoiceFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer": customer,
                        "kam": kam_user,
                        "invoice_date": invoice_date,
                        "source_timestamp": None,
                        "qty_mt": qty,
                        "invoice_value": Decimal("0"),
                        "revenue_gst": Decimal("0"),
                        "raw_buyer_name": customer_name,
                        "source_tab": tab_name,
                        "source_status": "IMPORTED",
                    },
                )

            stats.sales_upserted += 1

        except Exception as exc:
            stats.skipped += 1
            logger.error("Sales (F) row %d upsert failed: %s", i, exc)

    return stats

# ─────────────────────────────────────────────────────────────────────────────
# SECTION: SALES — Sheet1
# ─────────────────────────────────────────────────────────────────────────────

def _sync_sheet1(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_sheet1())

    if len(rows) < 2:
        stats.notes.append("Sheet1 tab: no data rows")
        return stats

    tab_name = _tab_sheet1()
    date_parse_failures: List[str] = []

    for row_number, row in enumerate(rows[1:], start=2):
        kam_name = _cell(row, 0)
        customer_name = _cell(row, 1)
        invoice_no = _cell(row, 4)
        invoice_date_raw = _cell(row, 5)
        value_gst = _decimal(_cell(row, 6))
        grade = _cell(row, 12)
        size = _cell(row, 13)
        qty = _decimal(_cell(row, 14)) or Decimal("0")
        rate_mt = _decimal(_cell(row, 16))
        invoice_value = _decimal(_cell(row, 17))

        if not customer_name:
            stats.skipped += 1
            continue

        invoice_date = _parse_date(invoice_date_raw)

        if not invoice_date:
            if invoice_date_raw and invoice_date_raw not in date_parse_failures:
                date_parse_failures.append(invoice_date_raw)
                logger.warning(
                    "Sheet1 row %d: unrecognised date format %r for customer %r.",
                    row_number,
                    invoice_date_raw,
                    customer_name,
                )

            stats.skipped += 1
            continue

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name
            else None
        )

        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = (
            _make_row_uuid(tab_name, invoice_no)
            if invoice_no
            else _make_row_uuid(tab_name, row_number, invoice_date, customer_name, kam_name)
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
            logger.error("Sheet1 row %d upsert failed: %s", row_number, exc)
            stats.skipped += 1

    if date_parse_failures:
        stats.notes.append(
            f"Sheet1: {len(date_parse_failures)} unique unrecognised date formats: "
            + ", ".join(repr(item) for item in date_parse_failures[:10])
        )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Front End
# ─────────────────────────────────────────────────────────────────────────────

def _sync_frontend(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_frontend())

    if len(rows) < 2:
        stats.notes.append("Front End tab: no data rows")
        return stats

    tab_name = _tab_frontend()
    header = [str(heading or "").strip() for heading in rows[0]] if rows else []

    col_enquiry_no = _header_index(header, ["enquiry"], 0)
    col_timestamp = _header_index(header, ["timestamp"], 1)
    col_kam_name = _header_index(header, ["kam name", "kam"], 4)
    col_customer_name = _header_index(header, ["customer name", "customer"], 5)
    col_grade = _header_index(header, ["grade"], 6)
    col_size = _header_index(header, ["size"], 7)
    col_qty = _header_index(header, ["qty", "quantity"], 8)
    col_status = _header_index(header, ["status"], 11)
    col_revenue_mt = _header_index(header, ["revenue"], 12)
    col_remarks = _header_index(header, ["remark"], 13)

    logger.info(
        "Front End column map: enquiry=%d ts=%d kam=%d customer=%d grade=%d size=%d qty=%d status=%d revenue=%d remarks=%d",
        col_enquiry_no,
        col_timestamp,
        col_kam_name,
        col_customer_name,
        col_grade,
        col_size,
        col_qty,
        col_status,
        col_revenue_mt,
        col_remarks,
    )

    fallback_doe_count = 0

    for row_number, row in enumerate(rows[1:], start=2):
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

        timestamp = _parse_timestamp(timestamp_raw)
        doe_date = timestamp.date() if timestamp else None

        if not doe_date:
            doe_date = timezone.localdate()
            fallback_doe_count += 1

        qty = _decimal(qty_raw) or Decimal("0")

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name
            else None
        )

        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = (
            _make_row_uuid(tab_name, enquiry_no)
            if enquiry_no
            else _make_row_uuid(tab_name, row_number, timestamp_raw, customer_name, kam_name)
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
                        "status": _normalize_lead_status(status),
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
            logger.error("Front End row %d upsert failed: %s", row_number, exc)
            stats.skipped += 1

    if fallback_doe_count:
        stats.notes.append(
            f"Front End: {fallback_doe_count} rows had missing/unparseable timestamp — doe set to today"
        )
        logger.info("Front End: %d rows used today as fallback doe", fallback_doe_count)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: LEADS — Enquiry (F)
# ─────────────────────────────────────────────────────────────────────────────

def _sync_enquiry_f(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_enquiry_f())

    if len(rows) < 2:
        stats.notes.append("Enquiry (F) tab: no data rows")
        return stats

    tab_name = _tab_enquiry_f()
    fallback_doe_count = 0

    for row_number, row in enumerate(rows[1:], start=2):
        timestamp_raw = _cell(row, 0)
        kam_name = _cell(row, 1)
        customer_name = _cell(row, 2)
        qty_raw = _cell(row, 3)
        status = _cell(row, 4)
        remarks = _cell(row, 5)

        if not customer_name:
            stats.skipped += 1
            continue

        timestamp = _parse_timestamp(timestamp_raw)
        doe_date = timestamp.date() if timestamp else None

        if not doe_date:
            doe_date = timezone.localdate()
            fallback_doe_count += 1

        qty = _decimal(qty_raw) or Decimal("0")

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name
            else None
        )

        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = _make_row_uuid(
            tab_name,
            row_number,
            timestamp_raw,
            customer_name,
            kam_name,
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
                        "status": _normalize_lead_status(status),
                        "remarks": remarks,
                        "source_tab": tab_name,
                    },
                )
                stats.leads_upserted += 1

        except Exception as exc:
            logger.error("Enquiry (F) row %d upsert failed: %s", row_number, exc)
            stats.skipped += 1

    if fallback_doe_count:
        stats.notes.append(
            f"Enquiry (F): {fallback_doe_count} rows had missing/unparseable timestamp — doe set to today"
        )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: OVERDUES
# ─────────────────────────────────────────────────────────────────────────────

def _sync_overdues(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_overdues())

    if len(rows) < 2:
        stats.notes.append("Overdues tab: no data rows")
        return stats

    header = [str(heading or "").strip() for heading in rows[0]] if rows else []

    col_customer = _header_index(header, ["customer name", "customer"], 0)
    col_kam = _header_index(header, ["kam name", "kam"], 1)
    col_overdue = _header_index(header, ["overdue", "dues"], 2)
    col_exposure = _header_index(header, ["exposure"], 3)
    col_a0 = _header_index(header, ["0-30", "0_30"], 4)
    col_a31 = _header_index(header, ["31-60", "31_60"], 5)
    col_a61 = _header_index(header, ["61-90", "61_90"], 6)
    col_a90 = _header_index(header, ["90+", "90_plus", "above 90"], 7)

    joined_header = " ".join(header).lower()
    has_ageing = any(
        keyword in joined_header
        for keyword in ["0-30", "31-60", "61-90", "90+"]
    )

    if not has_ageing:
        logger.warning(
            "Overdues tab: ageing columns not found in header. Header: %s",
            header[:10],
        )

    snapshot_date = timezone.localdate()

    for row_number, row in enumerate(rows[1:], start=2):
        customer_name = _cell(row, col_customer)
        kam_name = _cell(row, col_kam)
        overdue_raw = _cell(row, col_overdue)

        if not customer_name:
            continue

        overdue_amount = _decimal(overdue_raw)

        if overdue_amount is None:
            stats.skipped += 1
            continue

        ageing_0_30 = _decimal(_cell(row, col_a0)) or Decimal("0")
        ageing_31_60 = _decimal(_cell(row, col_a31)) or Decimal("0")
        ageing_61_90 = _decimal(_cell(row, col_a61)) or Decimal("0")
        ageing_90_plus = _decimal(_cell(row, col_a90)) or Decimal("0")

        ageing_total = ageing_0_30 + ageing_31_60 + ageing_61_90 + ageing_90_plus

        exposure = (
            _decimal(_cell(row, col_exposure))
            or (ageing_total if ageing_total > 0 else overdue_amount)
        )

        kam_user = (
            _resolve_kam_user(kam_name, tab_mapping, db_lookup, env_usermap, stats, local_cache)
            if kam_name
            else None
        )

        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        try:
            with transaction.atomic():
                OverdueSnapshot.objects.update_or_create(
                    customer=customer,
                    snapshot_date=snapshot_date,
                    defaults={
                        "kam": kam_user,
                        "overdue": overdue_amount,
                        "overdue_amt": overdue_amount,
                        "exposure": exposure,
                        "ageing_0_30": ageing_0_30,
                        "ageing_31_60": ageing_31_60,
                        "ageing_61_90": ageing_61_90,
                        "ageing_90_plus": ageing_90_plus,
                    },
                )
                stats.overdues_upserted += 1

        except Exception as exc:
            logger.error("Overdues row %d upsert failed: %s", row_number, exc)
            stats.skipped += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: COLLECTION PLAN SNAPSHOT FROM OVERDUES
# ─────────────────────────────────────────────────────────────────────────────

def _sync_overdues_to_collection_plan(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    """
    Sync Google Sheet Overdues tab → CollectionPlan.overdue_amount snapshot.

    Source columns:
      A = Customer Name
      B = KAM Name
      C = Overdue Amount

    Rules:
      - KAM is mandatory.
      - Creates CollectionPlan by customer + kam pair.
      - Updates only overdue_amount, planned_amount, last_synced_at.
      - Never overwrites actual_amount, collection_date, payment_details, utr_number.
    """
    from .models import CollectionPlan

    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_overdues())

    if len(rows) < 2:
        stats.notes.append("Overdues tab: no data rows — Collection Plan sync skipped")
        logger.warning("_sync_overdues_to_collection_plan: Overdues tab empty.")
        return stats

    header = [str(heading or "").strip() for heading in rows[0]] if rows else []

    col_customer = _header_index(header, ["customer name", "customer"], 0)
    col_kam = _header_index(header, ["kam name", "kam"], 1)
    col_overdue = _header_index(header, ["overdue", "dues", "amount"], 2)

    logger.info(
        "_sync_overdues_to_collection_plan: col_customer=%d col_kam=%d col_overdue=%d",
        col_customer,
        col_kam,
        col_overdue,
    )

    snapshot_date = timezone.localdate()
    now_ts = timezone.now()
    processed_pairs = set()

    for row_number, row in enumerate(rows[1:], start=2):
        customer_name = _cell(row, col_customer)
        kam_name = _cell(row, col_kam)
        overdue_raw = _cell(row, col_overdue)

        if not customer_name:
            stats.skipped += 1
            continue

        overdue_amount = _decimal(overdue_raw)

        if overdue_amount is None or overdue_amount <= 0:
            stats.skipped += 1
            continue

        if not kam_name:
            logger.warning(
                "Collection Plan sync row %d skipped: customer %r has no KAM name.",
                row_number,
                customer_name,
            )
            stats.unknown_kam += 1
            stats.skipped += 1
            continue

        kam_user = _resolve_kam_user(
            kam_name,
            tab_mapping,
            db_lookup,
            env_usermap,
            stats,
            local_cache,
        )

        if not kam_user:
            stats.skipped += 1
            continue

        pair_key = (_normalize(customer_name), kam_user.id)

        if pair_key in processed_pairs:
            logger.debug(
                "Collection Plan sync row %d skipped: duplicate pair customer=%r kam=%r.",
                row_number,
                customer_name,
                kam_user.username,
            )
            stats.skipped += 1
            continue

        processed_pairs.add(pair_key)

        try:
            customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

            with transaction.atomic():
                existing = (
                    CollectionPlan.objects
                    .filter(customer=customer, kam=kam_user)
                    .order_by("-last_synced_at", "-created_at")
                    .first()
                )

                if existing:
                    existing.overdue_amount = overdue_amount
                    existing.planned_amount = overdue_amount
                    existing.last_synced_at = now_ts
                    existing.save(
                        update_fields=[
                            "overdue_amount",
                            "planned_amount",
                            "last_synced_at",
                            "updated_at",
                        ]
                    )
                else:
                    CollectionPlan.objects.create(
                        customer=customer,
                        kam=kam_user,
                        overdue_amount=overdue_amount,
                        planned_amount=overdue_amount,
                        period_type=None,
                        period_id=None,
                        from_date=snapshot_date,
                        to_date=None,
                        last_synced_at=now_ts,
                    )

                stats.customers_upserted += 1

        except Exception as exc:
            logger.error(
                "Collection Plan sync row %d failed: customer=%r kam=%r error=%s",
                row_number,
                customer_name,
                kam_name,
                exc,
            )
            stats.skipped += 1

    logger.info(
        "Collection Plan sync complete: synced=%d skipped=%d unknown_kam=%d",
        stats.customers_upserted,
        stats.skipped,
        stats.unknown_kam,
    )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SECTION: COLLECTION TXN
# ─────────────────────────────────────────────────────────────────────────────

COLLECTION_SOURCE_SHEET = "GOOGLE_SHEET"
COLLECTION_SOURCE_ERP = "ERP"


def _sync_collections(
    service,
    sheet_id: str,
    tab_mapping,
    db_lookup,
    env_usermap,
    local_cache: Dict,
) -> SyncStats:
    stats = SyncStats()
    rows = _get_sheet_values(service, sheet_id, _tab_collection())

    if len(rows) < 2:
        stats.notes.append("Collection tab: no data rows (tab may not exist — skipping)")
        logger.info("Collection tab empty or not found — skipping collection sync")
        return stats

    tab_name = _tab_collection()
    header = [str(heading or "").strip() for heading in rows[0]] if rows else []

    col_date = int(
        _env(
            "KAM_COLLECTION_COL_DATE",
            str(_header_index(header, ["date"], 0)),
        )
    )
    col_customer = int(
        _env(
            "KAM_COLLECTION_COL_CUSTOMER",
            str(_header_index(header, ["customer name", "customer"], 1)),
        )
    )
    col_kam = int(
        _env(
            "KAM_COLLECTION_COL_KAM",
            str(_header_index(header, ["kam name", "kam"], 2)),
        )
    )
    col_amount = int(
        _env(
            "KAM_COLLECTION_COL_AMOUNT",
            str(_header_index(header, ["amount", "collection", "amt"], 3)),
        )
    )
    col_mode = int(
        _env(
            "KAM_COLLECTION_COL_MODE",
            str(_header_index(header, ["mode", "payment"], 4)),
        )
    )
    col_ref = int(
        _env(
            "KAM_COLLECTION_COL_REF",
            str(_header_index(header, ["ref", "utr", "cheque"], 5)),
        )
    )
    col_remarks = int(
        _env(
            "KAM_COLLECTION_COL_REMARKS",
            str(_header_index(header, ["remark", "note"], 6)),
        )
    )

    logger.info(
        "Collection tab column map: date=%d customer=%d kam=%d amount=%d mode=%d ref=%d remarks=%d",
        col_date,
        col_customer,
        col_kam,
        col_amount,
        col_mode,
        col_ref,
        col_remarks,
    )

    for row_number, row in enumerate(rows[1:], start=2):
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
            logger.warning(
                "Collection row %d skipped: cannot parse date %r.",
                row_number,
                date_raw,
            )
            stats.skipped += 1
            continue

        amount = _decimal(amount_raw)

        if amount is None or amount <= 0:
            logger.warning(
                "Collection row %d skipped: invalid amount %r.",
                row_number,
                amount_raw,
            )
            stats.skipped += 1
            continue

        if not kam_name:
            logger.warning(
                "Collection row %d skipped: missing KAM for customer %r.",
                row_number,
                customer_name,
            )
            stats.unknown_kam += 1
            stats.skipped += 1
            continue

        kam_user = _resolve_kam_user(
            kam_name,
            tab_mapping,
            db_lookup,
            env_usermap,
            stats,
            local_cache,
        )

        if not kam_user:
            logger.warning(
                "Collection row %d skipped: unknown KAM %r for customer %r.",
                row_number,
                kam_name,
                customer_name,
            )
            stats.skipped += 1
            continue

        customer = _safe_get_or_create_customer(customer_name, kam_user=kam_user)

        row_uuid = _make_row_uuid(
            tab_name,
            row_number,
            txn_date,
            customer_name,
            kam_name,
            amount_raw,
            reference,
        )

        try:
            with transaction.atomic():
                transaction_datetime = timezone.make_aware(
                    datetime(txn_date.year, txn_date.month, txn_date.day)
                )

                obj, created = CollectionTxn.objects.get_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "customer": customer,
                        "kam": kam_user,
                        "txn_datetime": transaction_datetime,
                        "amount": amount,
                        "mode": mode or None,
                        "reference": reference or None,
                        "reference_no": reference or None,
                        "notes": remarks or None,
                        "source": COLLECTION_SOURCE_SHEET,
                    },
                )

                if not created:
                    obj.customer = customer
                    obj.kam = kam_user
                    obj.txn_datetime = transaction_datetime
                    obj.amount = amount
                    obj.mode = mode or None
                    obj.reference = reference or None
                    obj.reference_no = reference or None
                    obj.notes = remarks or None
                    obj.source = COLLECTION_SOURCE_SHEET
                    obj.save(
                        update_fields=[
                            "customer",
                            "kam",
                            "txn_datetime",
                            "amount",
                            "mode",
                            "reference",
                            "reference_no",
                            "notes",
                            "source",
                            "updated_at",
                        ]
                    )

                stats.collections_upserted += 1

        except Exception as exc:
            logger.error("Collection row %d upsert failed: %s", row_number, exc)
            stats.skipped += 1

    logger.info(
        "Collection sync complete: %d upserted, %d skipped, source=%s.",
        stats.collections_upserted,
        stats.skipped,
        COLLECTION_SOURCE_SHEET,
    )

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SYNC ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_sync_now() -> SyncStats:
    sheet_id = _require_env("KAM_SALES_SHEET_ID")
    sections = resolve_sections()
    total = SyncStats()

    service = build_sheets_service()

    tab_mapping = _load_kam_names_tab(service, sheet_id)
    db_lookup = _build_user_lookup()
    env_usermap = _load_env_usermap()
    local_cache: Dict[str, Optional[User]] = {}

    logger.info(
        "Starting sync | sheet=%s | tab_mapping_entries=%d | db_users=%d",
        sheet_id,
        len(tab_mapping),
        len(db_lookup),
    )

    if sections.get("customers"):
        logger.info("Syncing: Customer Details")
        stats = _sync_customers(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → customers=%d skipped=%d", stats.customers_upserted, stats.skipped)

    if sections.get("sales_f"):
        logger.info("Syncing: Sales (F)")
        stats = _sync_sales_f(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → sales=%d skipped=%d", stats.sales_upserted, stats.skipped)

    if sections.get("sheet1"):
        logger.info("Syncing: Sheet1")
        stats = _sync_sheet1(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → sales=%d skipped=%d", stats.sales_upserted, stats.skipped)

    if sections.get("frontend"):
        logger.info("Syncing: Front End (leads)")
        stats = _sync_frontend(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → leads=%d skipped=%d", stats.leads_upserted, stats.skipped)

    if sections.get("enquiry_f"):
        logger.info("Syncing: Enquiry (F) (leads)")
        stats = _sync_enquiry_f(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → leads=%d skipped=%d", stats.leads_upserted, stats.skipped)

    if sections.get("overdues"):
        logger.info("Syncing: Overdues")
        stats = _sync_overdues(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → overdues=%d skipped=%d", stats.overdues_upserted, stats.skipped)

    if sections.get("collection_plan_sync"):
        logger.info("Syncing: Overdues → CollectionPlan")
        stats = _sync_overdues_to_collection_plan(
            service,
            sheet_id,
            tab_mapping,
            db_lookup,
            env_usermap,
            local_cache,
        )

        total.collections_upserted += stats.customers_upserted
        total.skipped += stats.skipped
        total.unknown_kam += stats.unknown_kam
        total.notes.extend(stats.notes)

        logger.info(
            "  → collection_plan_synced=%d skipped=%d unknown_kam=%d",
            stats.customers_upserted,
            stats.skipped,
            stats.unknown_kam,
        )

    if sections.get("collection"):
        logger.info("Syncing: Collection")
        stats = _sync_collections(service, sheet_id, tab_mapping, db_lookup, env_usermap, local_cache)
        total.merge(stats)
        logger.info("  → collections=%d skipped=%d", stats.collections_upserted, stats.skipped)

    logger.info("Running KAM backfill for unmapped customers.")
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
    ("customers", "Customer Details"),
    ("sales_f", "Sales (F)"),
    ("sheet1", "Sheet1"),
    ("frontend", "Front End"),
    ("enquiry_f", "Enquiry (F)"),
    ("overdues", "Overdues"),
    ("collection_plan_sync", "Collection Plan Snapshot"),
    ("collection", "Collection"),
]


_STEP_FN_MAP = {
    "customers": _sync_customers,
    "sales_f": _sync_sales_f,
    "sheet1": _sync_sheet1,
    "frontend": _sync_frontend,
    "enquiry_f": _sync_enquiry_f,
    "overdues": _sync_overdues,
    "collection_plan_sync": _sync_overdues_to_collection_plan,
    "collection": _sync_collections,
}


def step_sync(intent: SyncIntent, *args, **kwargs) -> Dict[str, Any]:
    cursor = getattr(intent, "cursor_position", 0) or 0
    sections = resolve_sections()
    sheet_id = _require_env("KAM_SALES_SHEET_ID")

    if cursor >= len(_STEPS):
        intent.status = SyncIntent.STATUS_SUCCESS
        intent.save(update_fields=["status", "updated_at"])
        return {
            "done": True,
            "message": "All sections synced",
        }

    section_key, section_label = _STEPS[cursor]

    try:
        service = build_sheets_service()
        tab_mapping = _load_kam_names_tab(service, sheet_id)
        db_lookup = _build_user_lookup()
        env_usermap = _load_env_usermap()
        local_cache: Dict[str, Optional[User]] = {}

        stats = SyncStats()

        if sections.get(section_key):
            sync_function = _STEP_FN_MAP.get(section_key)

            if sync_function:
                stats = sync_function(
                    service,
                    sheet_id,
                    tab_mapping,
                    db_lookup,
                    env_usermap,
                    local_cache,
                )

        next_cursor = cursor + 1
        is_last = next_cursor >= len(_STEPS)

        backfilled = 0

        if is_last:
            logger.info("step_sync final step: running KAM backfill.")
            backfilled = _backfill_customer_kam()

            if backfilled:
                stats.notes.append(f"KAM backfill: {backfilled} customers updated")

        intent.cursor_position = next_cursor
        intent.status = SyncIntent.STATUS_SUCCESS if is_last else SyncIntent.STATUS_RUNNING
        intent.save(update_fields=["cursor_position", "status", "updated_at"])

        return {
            "done": is_last,
            "step": section_label,
            "message": stats.as_message(),
            "notes": stats.notes,
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
        logger.error("step_sync failed at %r: %s", section_label, exc)
        intent.status = SyncIntent.STATUS_ERROR
        intent.last_error = str(exc)
        intent.save(update_fields=["status", "last_error", "updated_at"])
        raise