# FILE: apps/kam/sheets_adapter.py
# PURPOSE: Fix KAM sheet sync — corrected KAM user matching (username decomposition + pattern fallback),
#          leads import fix, and broader tab fallbacks. Sales/Leads now populate correctly.
# UPDATED: 2026-03-02
# NON-NEGOTIABLE BUSINESS RULES
# - KAM sees only own data (kam_id filter in views unchanged)
# - Manager sees only mapped KAM data
# - Admin sees all
# - No cross-data leakage
# - No approval/leave/reimbursement/mail logic touched
# - No schema changes

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import md5
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import gspread
from django.contrib.auth import get_user_model
from django.db import OperationalError, transaction
from django.db.models import Q
from django.utils import timezone

from apps.common.google_auth import GoogleCredentialError, get_google_credentials
from .models import Customer, InvoiceFact, LeadFact, OverdueSnapshot, SyncIntent

User = get_user_model()
logger = logging.getLogger(__name__)


# ----------------------------
# SQLite / write safety
# ----------------------------

WRITE_RETRY_COUNT = 5
WRITE_RETRY_SLEEP_SECONDS = 0.35


def _is_db_locked_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return "database is locked" in msg or "database table is locked" in msg


def _with_write_retry(fn: Callable[[], object]):
    last_exc = None
    for attempt in range(1, WRITE_RETRY_COUNT + 1):
        try:
            return fn()
        except OperationalError as exc:
            last_exc = exc
            if not _is_db_locked_error(exc) or attempt >= WRITE_RETRY_COUNT:
                raise
            sleep_for = WRITE_RETRY_SLEEP_SECONDS * attempt
            logger.warning(
                "KAM sync write retry due to DB lock (attempt=%s/%s, sleep=%.2fs): %s",
                attempt,
                WRITE_RETRY_COUNT,
                sleep_for,
                exc,
            )
            time.sleep(sleep_for)
    if last_exc:
        raise last_exc
    return None


# ----------------------------
# Env helpers / flexibility
# ----------------------------

def _getenv(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _truthy(v: Optional[str]) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _csv(v: Optional[str]) -> List[str]:
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def resolve_sections() -> List[str]:
    raw = _getenv("KAM_SYNC_SECTIONS")
    if not raw:
        return ["customers", "sales", "leads", "overdues"]
    allowed = {"customers", "sales", "leads", "overdues"}
    out = [s.strip().lower() for s in raw.split(",") if s.strip()]
    out = [s for s in out if s in allowed]
    return out or ["customers", "sales", "leads", "overdues"]


# ----------------------------
# Header normalization / parsing
# ----------------------------

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _canon_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u2019", "'").replace("`", "'")
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", s)
    s = _norm_spaces(s)
    return s.casefold()


def _canon_header(s: str) -> str:
    s = _canon_text(s)
    s = s.replace("&", " and ")
    s = s.replace("/", " ")
    s = s.replace("\\", " ")
    s = re.sub(r"[\(\)\[\]\{\}:;,#]", " ", s)
    s = re.sub(r"[^a-z0-9.+%'\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _datefmt() -> str:
    return _getenv("KAM_DATE_FMT", "%d-%m-%Y")


def _dry_run() -> bool:
    return _truthy(_getenv("KAM_IMPORT_DRY_RUN", "0"))


def _usermap() -> Dict[str, str]:
    raw = _getenv("KAM_USERMAP_JSON", "{}")
    try:
        data = json.loads(raw) if raw else {}
    except Exception:
        return {}

    out: Dict[str, str] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            key = _canon_text(str(k or ""))
            val = str(v or "").strip()
            if key and val:
                out[key] = val
    return out


def _excel_serial_to_date(s: str) -> Optional[date]:
    try:
        num = Decimal(str(s).strip())
    except Exception:
        return None
    if num <= 0:
        return None
    whole = int(num)
    if whole < 20000 or whole > 80000:
        return None
    base = date(1899, 12, 30)
    try:
        return base + timedelta(days=whole)
    except Exception:
        return None


def _parse_date(s: str) -> Optional[date]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    serial_dt = _excel_serial_to_date(s)
    if serial_dt:
        return serial_dt

    direct_patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%m-%y",
        "%d/%m/%y",
        "%m/%d/%Y",
        "%m-%d-%Y",
    ]
    datetime_patterns = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%y %H:%M:%S",
        "%d-%m-%y %H:%M",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y %H:%M",
    ]

    tried = []
    if _datefmt():
        tried.append(_datefmt())
    tried.extend(direct_patterns)
    tried.extend(datetime_patterns)

    seen = set()
    for fmt in tried:
        if fmt in seen:
            continue
        seen.add(fmt)
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    iso_candidate = s.replace("T", " ")
    if "." in iso_candidate:
        iso_candidate = iso_candidate.split(".", 1)[0]
    try:
        return datetime.fromisoformat(iso_candidate).date()
    except Exception:
        pass

    digits = re.findall(r"\d+", s)
    if len(digits) >= 3:
        a, b, c = digits[0], digits[1], digits[2]
        candidates = [f"{a}-{b}-{c}", f"{a}/{b}/{c}"]
        for candidate in candidates:
            for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%m-%d-%Y", "%m/%d/%Y"):
                try:
                    return datetime.strptime(candidate, fmt).date()
                except Exception:
                    pass

    return None


def _to_decimal(s) -> Decimal:
    if s is None:
        return Decimal(0)
    if isinstance(s, Decimal):
        return s
    if isinstance(s, (int, float)):
        return Decimal(str(s))
    s = str(s).strip()
    if not s:
        return Decimal(0)
    s = s.replace(",", "")
    s = s.replace(" ", "")
    s = re.sub(r"[₹$€£]", "", s)
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _to_int(s) -> int:
    try:
        return int(Decimal(str(s)))
    except Exception:
        return 0


def _hash_row(*parts: str) -> str:
    base = "||".join([p if p is not None else "" for p in parts])
    return md5(base.encode("utf-8")).hexdigest()


def _index(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _col(idx: Dict[str, int], *names: str) -> Optional[int]:
    aliases = [_canon_header(n) for n in names if n]
    for alias in aliases:
        if alias in idx:
            return idx[alias]
    for alias in aliases:
        alias_compact = re.sub(r"[^a-z0-9]+", "", alias)
        for header, pos in idx.items():
            if re.sub(r"[^a-z0-9]+", "", header) == alias_compact:
                return pos
    return None


def _normalize_status(raw: str) -> str:
    value = _canon_text(raw)
    if not value:
        return "OPEN"

    mapping = {
        "open": "OPEN",
        "new": "OPEN",
        "fresh": "OPEN",
        "active": "OPEN",
        "negotiation": "NEGOTIATION",
        "under negotiation": "NEGOTIATION",
        "in negotiation": "NEGOTIATION",
        "negotiating": "NEGOTIATION",
        "won": "WON",
        "closed won": "WON",
        "converted": "WON",
        "booked": "WON",
        "lost": "LOST",
        "closed lost": "LOST",
        "dropped": "LOST",
        "cancelled": "LOST",
        "canceled": "LOST",
    }
    return mapping.get(value, "OPEN")


# ----------------------------
# Google Sheet open helpers
# ----------------------------

def _open_sheet():
    try:
        scopes_raw = (_getenv("GOOGLE_SHEET_SCOPES") or "").strip()
        scopes = [s.strip() for s in scopes_raw.split(",") if s.strip()] if scopes_raw else None

        creds_bundle = get_google_credentials(
            scopes=scopes
            or [
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
            ]
        )
    except GoogleCredentialError as e:
        raise RuntimeError(str(e)) from e

    gc = gspread.authorize(creds_bundle.credentials)
    sheet_id = _getenv("KAM_SALES_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("KAM_SALES_SHEET_ID missing")
    return gc.open_by_key(sheet_id)


def _ws_by_name(tab: str):
    sh = _open_sheet()
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Worksheet not found: {tab}")


def _worksheet_headers(tab: str) -> List[str]:
    ws = _ws_by_name(tab)
    values = ws.get_all_values()
    if not values:
        return []
    return [_canon_header(h) for h in values[0]]


def _worksheet_has_data(tab: str) -> bool:
    ws = _ws_by_name(tab)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return False
    for r in values[1:]:
        if any((c or "").strip() for c in r):
            return True
    return False


def _read_tab(ws_name: str) -> Tuple[List[str], List[List[str]]]:
    ws = _ws_by_name(ws_name)
    values = ws.get_all_values()
    if not values:
        return [], []
    headers = [_canon_header(h) for h in values[0]]
    rows = values[1:]
    return headers, rows


# ----------------------------
# User matching  *** CORE FIX ***
# ----------------------------

def _full_name_of_user(user: User) -> str:
    first = (getattr(user, "first_name", "") or "").strip()
    last = (getattr(user, "last_name", "") or "").strip()
    return _norm_spaces(f"{first} {last}".strip())


def _split_username_to_name_parts(username: str) -> List[str]:
    """
    Decompose a separator-based username into word parts.

    Examples:
      "jivan.more"      → ["jivan", "more"]
      "jivan_more"      → ["jivan", "more"]
      "saurabh.kumavat" → ["saurabh", "kumavat"]
      "jm"              → []   (single token, no split)
      "jmore"           → []   (no separator)

    This is the KEY FIX: Django users typically have blank first_name/last_name
    but username like 'jivan.more'. The Google Sheet stores full display names
    like 'Jivan More'. Without decomposing the username, 'jivan more' is never
    registered as a lookup key and matching fails for every row → Sales/Leads = 0.
    """
    if not username:
        return []
    parts = re.split(r"[._\-]+", username.strip().lower())
    # Keep only parts that look like real name words: at least 2 chars, all alpha
    return [p for p in parts if p and len(p) >= 2 and re.match(r"^[a-z]+$", p)]


def _username_pattern_candidates(display_name: str) -> List[str]:
    """
    Given a display name like 'Jivan More', generate common username patterns
    that an admin might have assigned: jivan.more, jivan_more, jivanmore,
    j.more, jmore, more.jivan, etc.

    Used as a last-resort fallback when exact/compact lookup fails.
    Handles abbreviated usernames like 'jmore' or 'j.more'.
    """
    raw = display_name.strip()
    if not raw:
        return []
    words = [w.lower() for w in re.split(r"\s+", raw) if w]
    if not words:
        return []

    first = words[0]
    candidates: List[str] = []

    if len(words) >= 2:
        last = words[-1]
        # Most common patterns first (highest hit rate)
        candidates += [
            f"{first}.{last}",       # jivan.more
            f"{first}_{last}",       # jivan_more
            f"{first}{last}",        # jivanmore
            f"{first[0]}.{last}",   # j.more
            f"{first[0]}_{last}",   # j_more
            f"{first[0]}{last}",    # jmore
            f"{last}.{first}",       # more.jivan
            f"{last}_{first}",       # more_jivan
            f"{last}{first}",        # morejivan
        ]
        # Handle 3-word names
        if len(words) >= 3:
            middle = words[1]
            candidates += [
                f"{first}.{middle}.{last}",
                f"{first}{middle[0]}{last}",
            ]

    # Always include bare first name as final fallback
    candidates.append(first)

    # Deduplicate preserving order
    seen: set = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _build_user_lookup() -> Dict[str, User]:
    """
    Build a normalised-display-name → User dictionary.

    *** CORE FIX ***
    For users with username like 'jivan.more' (separator-based), we now register
    'jivan more' (space-joined parts) as an additional lookup key.

    Before this fix: sheet display name 'Jivan More' normalised to 'jivan more'
    had NO matching key in the dict (because first_name/last_name were blank),
    causing every such row to be skipped → Sales/Leads imported as 0.

    After this fix: 'jivan more' is registered → direct match → row imported
    correctly with kam=<correct User> → dashboard filter kam_id=user.id works.
    """
    exact_lookup: Dict[str, User] = {}
    first_name_buckets: Dict[str, List[User]] = {}

    for user in User.objects.filter(is_active=True):
        exact_keys: set = set()

        username = (getattr(user, "username", "") or "").strip()
        email = (getattr(user, "email", "") or "").strip()
        full_name = _full_name_of_user(user)
        first = (getattr(user, "first_name", "") or "").strip()
        last = (getattr(user, "last_name", "") or "").strip()

        # 1. Raw username (e.g. "jivan.more")
        if username:
            exact_keys.add(_canon_text(username))

            # *** CORE FIX: decompose username by separators ***
            # "jivan.more" → parts=["jivan","more"] → register "jivan more" + "more jivan"
            uname_parts = _split_username_to_name_parts(username)
            if len(uname_parts) >= 2:
                forward = " ".join(uname_parts)
                backward = " ".join(reversed(uname_parts))
                exact_keys.add(_canon_text(forward))   # "jivan more"  ← matches sheet
                exact_keys.add(_canon_text(backward))  # "more jivan"  ← reverse guard
                # Also register separator variants in case sheet uses them
                exact_keys.add(_canon_text(".".join(uname_parts)))  # "jivan.more"
                exact_keys.add(_canon_text("_".join(uname_parts)))  # "jivan_more"
                # First word bucket for single-first-name fallback
                if len(uname_parts[0]) >= 3:
                    first_name_buckets.setdefault(
                        _canon_text(uname_parts[0]), []
                    ).append(user)

        # 2. Email and email-local-part
        if email:
            exact_keys.add(_canon_text(email))
            email_local = email.split("@", 1)[0].strip()
            if email_local:
                exact_keys.add(_canon_text(email_local))
                # Decompose email local part too ("jivan.more@co.in" → "jivan more")
                email_parts = _split_username_to_name_parts(email_local)
                if len(email_parts) >= 2:
                    exact_keys.add(_canon_text(" ".join(email_parts)))
                    exact_keys.add(_canon_text(" ".join(reversed(email_parts))))

        # 3. first_name + last_name (standard Django profile fields, if populated)
        if full_name:
            exact_keys.add(_canon_text(full_name))
            exact_keys.add(_canon_text(full_name.replace(".", " ")))
            if first and last:
                exact_keys.add(_canon_text(f"{first}.{last}"))
                exact_keys.add(_canon_text(f"{first}_{last}"))
                exact_keys.add(_canon_text(f"{last} {first}"))
        if first and last:
            exact_keys.add(_canon_text(f"{first} {last}"))

        # 4. Register all collected keys → first writer wins (no overwrite)
        for key in exact_keys:
            if key and key not in exact_lookup:
                exact_lookup[key] = user

        # 5. First-name-only bucket (from first_name field)
        if first and len(first) >= 3:
            first_key = _canon_text(first)
            first_name_buckets.setdefault(first_key, []).append(user)

    # Single-user first-name shortcuts
    for first_key, users in first_name_buckets.items():
        if len(users) == 1 and first_key not in exact_lookup:
            exact_lookup[first_key] = users[0]

    return exact_lookup


def _find_user_by_map(
    display_name: str,
    usermap: Dict[str, str],
    user_lookup: Optional[Dict[str, User]] = None,
) -> Optional[User]:
    """
    Resolve a sheet display name (e.g. 'Jivan More') to a Django User.

    Matching order — first match wins:
      1. KAM_USERMAP_JSON explicit override  (admin config, highest trust)
      2. Exact key in pre-built lookup       (covers username, email, decomposed username)
      3. Compact alphanumeric key match      (handles minor punctuation differences)
      4. Direct DB username/email query      (case-insensitive)
      5. DB first_name + last_name query     (if profile fields are populated)
      6. Pattern-based username generation   (last resort: tries jivan.more, jmore, etc.)
    """
    raw = _norm_spaces(display_name)
    if not raw:
        return None

    normalized = _canon_text(raw)
    user_lookup = user_lookup or _build_user_lookup()

    # Step 1: explicit usermap override
    mapped_username = usermap.get(normalized)
    if mapped_username:
        mapped_raw = mapped_username.strip()
        user = User.objects.filter(
            Q(username__iexact=mapped_raw)
            | Q(email__iexact=mapped_raw)
            | Q(first_name__iexact=mapped_raw),
            is_active=True,
        ).first()
        if user:
            return user
        mapped_norm = _canon_text(mapped_raw)
        if mapped_norm in user_lookup:
            return user_lookup[mapped_norm]
        if " " in mapped_raw:
            parts = [p for p in mapped_raw.split() if p]
            if len(parts) >= 2:
                user = User.objects.filter(
                    first_name__iexact=parts[0],
                    last_name__iexact=" ".join(parts[1:]),
                    is_active=True,
                ).first()
                if user:
                    return user

    # Step 2: exact lookup match
    # After _build_user_lookup fix, 'jivan more' is now registered as a key
    # for user with username 'jivan.more', making this the primary success path.
    if normalized in user_lookup:
        return user_lookup[normalized]

    # Step 3: compact (strip non-alphanumeric) match
    # "jivan more" compact → "jivanmore" == "jivan.more" compact → match
    compact = re.sub(r"[^a-z0-9]+", "", normalized)
    if compact:
        for key, user in user_lookup.items():
            if re.sub(r"[^a-z0-9]+", "", key) == compact:
                return user

    # Step 4: direct DB query (case-insensitive username / email)
    direct = User.objects.filter(
        Q(username__iexact=raw) | Q(email__iexact=raw), is_active=True
    ).first()
    if direct:
        return direct

    # Step 5: DB query by first_name + last_name (if profile is populated)
    parts = [p for p in re.split(r"\s+", raw) if p]
    if len(parts) >= 2:
        user = User.objects.filter(
            first_name__iexact=parts[0],
            last_name__iexact=" ".join(parts[1:]),
            is_active=True,
        ).first()
        if user:
            return user

    # Step 6: pattern-based username generation from display name
    # Handles cases where username is 'jmore', 'j.more', 'jivan' etc.
    # Generates jivan.more, jivan_more, jivanmore, j.more, jmore, ...
    # Checks lookup dict first (free, no DB), then one batched DB OR query.
    candidates = _username_pattern_candidates(raw)
    if candidates:
        for candidate in candidates:
            ckey = _canon_text(candidate)
            if ckey in user_lookup:
                logger.debug(
                    "KAM match via pattern candidate '%s' for display_name='%s'",
                    candidate,
                    raw,
                )
                return user_lookup[ckey]

        # Batched DB query — max 8 patterns to keep it fast
        q = Q()
        for c in candidates[:8]:
            q |= Q(username__iexact=c)
        user = User.objects.filter(q, is_active=True).first()
        if user:
            logger.debug(
                "KAM match via DB pattern query for display_name='%s' → username='%s'",
                raw,
                user.username,
            )
            return user

    logger.warning(
        "KAM matching failed for display_name='%s' (normalised='%s'). "
        "Fix: set KAM_USERMAP_JSON env var. "
        'Example: KAM_USERMAP_JSON=\'{"%(n)s": "actual_django_username"}\'',
        raw,
        normalized,
        {"n": normalized},
    )
    return None


def _pick_row_kam(
    row: Sequence[str],
    idx: Dict[str, int],
    usermap: Dict[str, str],
    user_lookup: Dict[str, User],
) -> Tuple[Optional[User], str]:
    candidate_cols = [
        _col(idx, "KAM Name", "KAM", "kam_username", "kam name", "sales person", "marketing person"),
        _col(idx, "Full Name", "Employee Name", "Sales Person Name", "Marketing Person Name", "Owner Name"),
        _col(idx, "primary_kam_username"),
    ]

    seen_values: set = set()
    for col_idx in candidate_cols:
        if col_idx is None or col_idx >= len(row):
            continue
        raw = _norm_spaces(row[col_idx])
        if not raw or raw in seen_values:
            continue
        seen_values.add(raw)
        user = _find_user_by_map(raw, usermap, user_lookup=user_lookup)
        if user:
            return user, raw

    for raw in seen_values:
        if raw:
            return None, raw

    return None, ""


# ----------------------------
# Customer upsert
# ----------------------------

def _get_or_create_customer(
    *,
    name: str,
    kam: Optional[User] = None,
    address: Optional[str] = None,
    email: Optional[str] = None,
    mobile: Optional[str] = None,
    credit_limit: Optional[Decimal] = None,
    agreed_credit_period_days: Optional[int] = None,
    force_kam_assignment: bool = False,
) -> Customer:
    clean_name = _norm_spaces(name)
    cust = Customer.objects.filter(name__iexact=clean_name).first()

    if not cust:
        cust = Customer(name=clean_name)

    changed = False

    if clean_name and cust.name != clean_name:
        cust.name = clean_name
        changed = True

    if address is not None and address != getattr(cust, "address", None):
        cust.address = address or None
        changed = True
    if email is not None and email != getattr(cust, "email", None):
        cust.email = email or None
        changed = True
    if mobile is not None and mobile != getattr(cust, "mobile", None):
        cust.mobile = mobile or None
        changed = True
    if credit_limit is not None and credit_limit != getattr(cust, "credit_limit", None):
        cust.credit_limit = credit_limit
        changed = True
    if agreed_credit_period_days is not None and agreed_credit_period_days != getattr(
        cust, "agreed_credit_period_days", None
    ):
        cust.agreed_credit_period_days = agreed_credit_period_days
        changed = True

    if getattr(cust, "source", None) != Customer.SOURCE_SHEET:
        cust.source = Customer.SOURCE_SHEET
        changed = True

    if kam:
        should_assign = force_kam_assignment or (not cust.kam_id and not cust.primary_kam_id)
        if should_assign:
            if cust.kam_id != kam.id:
                cust.kam = kam
                changed = True
            if cust.primary_kam_id != kam.id:
                cust.primary_kam = kam
                changed = True

    if not getattr(cust, "pk", None):
        cust.save()
    elif changed:
        cust.save()

    return cust


# ----------------------------
# Header-aware tab resolution
# ----------------------------

def _required_customers_groups() -> List[List[str]]:
    return [["Customer Name", "Customer", "Name", "customer_name", "party name"]]


def _required_sales_groups() -> List[List[str]]:
    return [
        ["KAM Name", "KAM", "kam_username", "kam name", "sales person", "marketing person", "Full Name"],
        ["Customer Name", "Consignee Name", "Buyer's Name", "Buyer's Name", "customer_name", "party name", "customer"],
        ["Invoice Date", "Date of Invoice", "invoice_date", "Date", "bill date"],
    ]


def _required_leads_groups() -> List[List[str]]:
    # FIXED: Added more date column aliases to improve tab detection
    return [
        [
            "Timestamp",
            "Date of Enquiry",
            "doe",
            "enquiry date",
            "lead date",
            "created at",
            "date",
            "enquiry_date",
            "Entry Date",
            "entry date",
        ],
        [
            "KAM Name",
            "KAM",
            "kam_username",
            "kam name",
            "sales person",
            "marketing person",
            "Full Name",
            "Employee Name",
        ],
    ]


def _required_overdues_groups() -> List[List[str]]:
    return [
        ["Customer Name", "Customer", "customer_name", "party name"],
        ["Overdues (Rs)", "Overdue", "overdue", "total overdue"],
    ]


def _header_group_match_score(headers: List[str], groups: List[List[str]]) -> int:
    idx = _index(headers)
    score = 0
    for aliases in groups:
        if _col(idx, *aliases) is not None:
            score += 1
    return score


def _resolve_tab_with_headers(
    *,
    canonical: str,
    default: str,
    aliases: List[str],
    fallbacks: List[str],
    required_groups: List[List[str]],
) -> str:
    candidates = [canonical] + aliases
    explicit = None
    for k in candidates:
        v = _getenv(k)
        if v:
            explicit = v.strip()
            break

    if explicit and explicit.upper() not in ("AUTO", "*"):
        return explicit

    best_tab = None
    best_score = -1

    for tab in fallbacks:
        tab = tab.strip()
        if not tab:
            continue
        try:
            headers = _worksheet_headers(tab)
            if not headers or not _worksheet_has_data(tab):
                continue
            score = _header_group_match_score(headers, required_groups)
            if score > best_score:
                best_tab = tab
                best_score = score
            if score == len(required_groups):
                return tab
        except Exception:
            continue

    return best_tab or default


def _resolve_customers_tab() -> str:
    return _resolve_tab_with_headers(
        canonical="KAM_TAB_CUSTOMERS",
        default="Customer Details",
        aliases=["KAM_CUSTOMERS_TAB"],
        fallbacks=_csv(
            _getenv(
                "KAM_TAB_CUSTOMERS_FALLBACKS",
                "Customer Details,Customers,Customer Master",
            )
        ),
        required_groups=_required_customers_groups(),
    )


def _resolve_sales_tab() -> str:
    return _resolve_tab_with_headers(
        canonical="KAM_TAB_SALES",
        default="Sales (F)",
        aliases=["KAM_SALES_TAB", "KAM_TAB_INVOICES", "KAM_INVOICES_TAB"],
        fallbacks=_csv(
            _getenv(
                "KAM_TAB_SALES_FALLBACKS",
                "Sales (F),Sales,Sheet1,Front End,Invoice",
            )
        ),
        required_groups=_required_sales_groups(),
    )


def _resolve_leads_tab() -> str:
    # FIXED: Significantly expanded fallback list.
    # Previous list was too narrow, causing leads to silently skip on many deployments.
    return _resolve_tab_with_headers(
        canonical="KAM_TAB_LEADS",
        default="Enquiry (F)",
        aliases=["KAM_LEADS_TAB"],
        fallbacks=_csv(
            _getenv(
                "KAM_TAB_LEADS_FALLBACKS",
                (
                    "Enquiry (F),Enquiry,Leads,Lead,Enquiries,"
                    "Lead Data,Leads Data,Lead Form,Enquiry Form,"
                    "Lead Entry,Enquiry Entry,NBD,New Business,Prospects,"
                    "Pipeline,CRM,Opportunities"
                ),
            )
        ),
        required_groups=_required_leads_groups(),
    )


def _resolve_overdues_tab() -> str:
    return _resolve_tab_with_headers(
        canonical="KAM_TAB_OVERDUES",
        default="Overdues",
        aliases=["KAM_OVERDUES_TAB"],
        fallbacks=_csv(_getenv("KAM_TAB_OVERDUES_FALLBACKS", "Overdues,Overdue")),
        required_groups=_required_overdues_groups(),
    )


def resolve_tabs_for_logging() -> Dict[str, str]:
    return {
        "customers": _resolve_customers_tab(),
        "sales": _resolve_sales_tab(),
        "leads": _resolve_leads_tab(),
        "overdues": _resolve_overdues_tab(),
    }


# ----------------------------
# ImportStats + importers
# ----------------------------

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
        clean = _norm_spaces(name)
        if clean and clean not in self.unknown_kam:
            self.unknown_kam.append(clean)

    def as_message(self) -> str:
        parts = [
            f"Customers: {self.customers_upserted}",
            f"Sales rows: {self.sales_upserted}",
            f"Leads rows: {self.leads_upserted}",
            f"Overdue snapshots: {self.overdues_upserted}",
            f"Skipped: {self.skipped}",
        ]
        if self.unknown_kam:
            parts.append(
                "Unknown KAM(s): "
                + ", ".join(self.unknown_kam[:6])
                + ("\u2026" if len(self.unknown_kam) > 6 else "")
            )
        if self.notes:
            parts.append(
                "Notes: "
                + " | ".join(self.notes[:4])
                + ("\u2026" if len(self.notes) > 4 else "")
            )
        return " | ".join(parts)


def import_customers(stats: ImportStats):
    tab = _resolve_customers_tab()
    headers, rows = _read_tab(tab)
    if not headers or not rows:
        stats.notes.append(f"Customers: empty sheet ({tab})")
        return

    idx = _index(headers)
    c_customer = _col(idx, "Customer Name", "Customer", "Name", "customer_name", "party name")
    if c_customer is None:
        stats.notes.append(f"Customers: required customer column missing in ({tab})")
        return

    c_addr = _col(idx, "Address", "address", "customer address")
    c_email = _col(idx, "Email", "email", "mail id")
    c_mobile = _col(
        idx, "Mobile No", "Mobile", "mobile", "phone", "contact no", "mobile number"
    )
    c_credit_limit = _col(idx, "Credit Limit", "credit_limit")
    c_credit_days = _col(
        idx, "Agreed Credit Period", "agreed_credit_period_days", "Agreed Credit Period "
    )

    usermap = _usermap()
    user_lookup = _build_user_lookup()
    dry = _dry_run()

    for r in rows:
        name = (r[c_customer] if c_customer < len(r) else "").strip()
        if not name:
            stats.skipped += 1
            continue

        kam, kam_raw = _pick_row_kam(r, idx, usermap, user_lookup)
        if not kam and kam_raw:
            stats.add_unknown(kam_raw)

        addr = (r[c_addr] if c_addr is not None and c_addr < len(r) else "").strip() or None
        email = (r[c_email] if c_email is not None and c_email < len(r) else "").strip() or None
        mobile = (r[c_mobile] if c_mobile is not None and c_mobile < len(r) else "").strip() or None
        credit_limit = (
            _to_decimal(r[c_credit_limit])
            if c_credit_limit is not None and c_credit_limit < len(r)
            else Decimal(0)
        )
        credit_days = (
            _to_int(r[c_credit_days])
            if c_credit_days is not None and c_credit_days < len(r)
            else 0
        )

        if dry:
            stats.customers_upserted += 1
            continue

        def _write():
            with transaction.atomic():
                _get_or_create_customer(
                    name=name,
                    kam=kam,
                    address=addr,
                    email=email,
                    mobile=mobile,
                    credit_limit=credit_limit,
                    agreed_credit_period_days=credit_days,
                    force_kam_assignment=True,
                )
            return None

        _with_write_retry(_write)
        stats.customers_upserted += 1


def import_sales(stats: ImportStats):
    tab = _resolve_sales_tab()
    headers, rows = _read_tab(tab)
    if not headers or not rows:
        stats.notes.append(f"Sales: empty sheet ({tab})")
        return

    idx = _index(headers)
    c_customer = _col(
        idx,
        "Customer Name",
        "Consignee Name",
        "Buyer's Name",
        "Buyer's Name",
        "Buyer\\'s Name",
        "customer_name",
        "party name",
        "customer",
    )
    c_date = _col(idx, "Invoice Date", "Date of Invoice", "invoice_date", "Date", "bill date")
    c_qty = _col(
        idx, "QTY", "Qty(MT)", "Quantity", "qty_mt", "qty mt", "quantity mt", "invoice qty"
    )
    c_val = _col(
        idx,
        "Invoice Value With GST",
        "Invoice Value with GST",
        "Invoice Value",
        "revenue_gst",
        "invoice value with gst rs",
        "invoice amount",
        "invoice value rs",
    )
    c_invno = _col(
        idx, "Invoice Number", "Invoice No", "invoice_number", "bill no", "bill number"
    )
    c_grade = _col(idx, "Grade", "grade")
    c_size = _col(idx, "Size", "size", "Size(MM)", "size mm")

    if None in (c_customer, c_date):
        stats.notes.append(f"Sales: required columns missing in ({tab})")
        return

    usermap = _usermap()
    user_lookup = _build_user_lookup()
    dry = _dry_run()

    for r in rows:
        kam, kam_raw = _pick_row_kam(r, idx, usermap, user_lookup)
        if not kam:
            if kam_raw:
                stats.add_unknown(kam_raw)
            stats.skipped += 1
            continue

        inv_date = _parse_date(r[c_date] if c_date < len(r) else "")
        if not inv_date:
            stats.skipped += 1
            continue

        cust_name = (
            r[c_customer] if c_customer is not None and c_customer < len(r) else ""
        ).strip()
        if not cust_name:
            stats.skipped += 1
            continue

        qty_mt = (
            _to_decimal(r[c_qty])
            if c_qty is not None and c_qty < len(r)
            else Decimal(0)
        )
        value_gst = (
            _to_decimal(r[c_val])
            if c_val is not None and c_val < len(r)
            else Decimal(0)
        )
        inv_no = (r[c_invno] if c_invno is not None and c_invno < len(r) else "").strip()
        grade = (r[c_grade] if c_grade is not None and c_grade < len(r) else "").strip() or None
        size = (r[c_size] if c_size is not None and c_size < len(r) else "").strip() or None

        row_uuid = inv_no or _hash_row(
            "sales",
            tab,
            cust_name,
            kam.username,
            str(inv_date),
            str(qty_mt),
            str(value_gst),
            str(grade or ""),
            str(size or ""),
        )

        if dry:
            stats.sales_upserted += 1
            continue

        def _write():
            with transaction.atomic():
                cust = _get_or_create_customer(
                    name=cust_name, kam=kam, force_kam_assignment=False
                )
                inv, created = InvoiceFact.objects.get_or_create(
                    row_uuid=row_uuid,
                    defaults=dict(
                        invoice_date=inv_date,
                        customer=cust,
                        kam=kam,
                        grade=grade,
                        size=size,
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
                    if inv.grade != grade:
                        inv.grade = grade
                        changed = True
                    if inv.size != size:
                        inv.size = size
                        changed = True
                    if inv.qty_mt != qty_mt:
                        inv.qty_mt = qty_mt
                        changed = True
                    if inv.revenue_gst != value_gst:
                        inv.revenue_gst = value_gst
                        changed = True
                    if changed:
                        inv.save()
            return None

        _with_write_retry(_write)
        stats.sales_upserted += 1


def import_leads(stats: ImportStats):
    tab = _resolve_leads_tab()
    headers, rows = _read_tab(tab)
    if not headers or not rows:
        stats.notes.append(f"Leads: empty sheet ({tab})")
        return

    idx = _index(headers)
    c_ts = _col(
        idx,
        "Timestamp",
        "Date of Enquiry",
        "doe",
        "enquiry date",
        "lead date",
        "created at",
        "date",
        "enquiry_date",
        "Entry Date",
        "entry date",
    )
    c_customer = _col(
        idx,
        "Customer Name",
        "customer_name",
        "party name",
        "customer",
        "company name",
        "Company Name",
        "Firm Name",
        "firm name",
    )
    c_qty = _col(
        idx,
        "Qty (MT)",
        "QTY",
        "Qty",
        "qty_mt",
        "qty",
        "quantity",
        "requirement mt",
        "requirement",
        "Requirement (MT)",
        "Req (MT)",
    )
    c_status = _col(
        idx,
        "Status",
        "status",
        "lead status",
        "enquiry status",
        "Lead Status",
        "Enquiry Status",
    )
    c_remarks = _col(
        idx, "Remarks", "remarks", "remark", "notes", "comment", "Notes", "Comments"
    )
    c_grade = _col(idx, "Grade", "grade")
    c_size = _col(idx, "Size", "Size(MM)", "size", "size mm")

    if c_ts is None:
        actual_headers = list(idx.keys())[:20]
        stats.notes.append(
            f"Leads: required date column missing in tab '{tab}'. "
            f"Actual headers (first 20): {actual_headers}. "
            f"Set KAM_TAB_LEADS env var to the correct tab name."
        )
        logger.warning(
            "Leads import skipped: date column not found in tab '%s'. "
            "Actual canonicalized headers: %s",
            tab,
            list(idx.keys()),
        )
        return

    usermap = _usermap()
    user_lookup = _build_user_lookup()
    dry = _dry_run()

    for r in rows:
        kam, kam_raw = _pick_row_kam(r, idx, usermap, user_lookup)
        if not kam:
            if kam_raw:
                stats.add_unknown(kam_raw)
            stats.skipped += 1
            continue

        doe = _parse_date(r[c_ts] if c_ts < len(r) else "")
        if not doe:
            stats.skipped += 1
            continue

        qty_mt = (
            _to_decimal(r[c_qty]) if c_qty is not None and c_qty < len(r) else Decimal(0)
        )
        status = _normalize_status(
            r[c_status] if c_status is not None and c_status < len(r) else ""
        )
        cust_name = (
            r[c_customer] if c_customer is not None and c_customer < len(r) else ""
        ).strip()
        grade = (
            r[c_grade] if c_grade is not None and c_grade < len(r) else ""
        ).strip() or None
        size = (
            r[c_size] if c_size is not None and c_size < len(r) else ""
        ).strip() or None
        remarks = (
            r[c_remarks] if c_remarks is not None and c_remarks < len(r) else ""
        ).strip() or None

        row_uuid = _hash_row(
            "lead",
            tab,
            kam.username,
            str(doe),
            cust_name,
            str(qty_mt),
            status,
            str(grade or ""),
            str(size or ""),
            str(remarks or ""),
        )

        if dry:
            stats.leads_upserted += 1
            continue

        def _write():
            with transaction.atomic():
                cust = (
                    _get_or_create_customer(
                        name=cust_name, kam=kam, force_kam_assignment=False
                    )
                    if cust_name
                    else None
                )
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
            return None

        _with_write_retry(_write)
        stats.leads_upserted += 1


def import_overdues(stats: ImportStats):
    tab = _resolve_overdues_tab()
    headers, rows = _read_tab(tab)
    if not headers or not rows:
        stats.notes.append(f"Overdues: empty sheet ({tab})")
        return

    idx = _index(headers)
    c_customer = _col(idx, "Customer Name", "Customer", "customer_name", "party name")
    c_overdue = _col(idx, "Overdues (Rs)", "Overdue", "overdue", "total overdue")
    c_exposure = _col(idx, "Total Exposure (Rs)", "Exposure", "exposure", "total exposure")
    a0 = _col(idx, "0-30", "ageing_0_30", "0 30")
    a31 = _col(idx, "31-60", "ageing_31_60", "31 60")
    a61 = _col(idx, "61-90", "ageing_61_90", "61 90")
    a90 = _col(idx, "90+", "ageing_90_plus", "90 plus")

    if c_customer is None or c_overdue is None:
        stats.notes.append(f"Overdues: required columns missing in ({tab})")
        return

    dry = _dry_run()
    snap_date = timezone.localdate()
    totals: Dict[str, Dict[str, Decimal]] = {}

    for r in rows:
        cust_name = (r[c_customer] if c_customer < len(r) else "").strip()
        if not cust_name:
            continue
        cur = totals.setdefault(
            cust_name,
            {
                "overdue": Decimal(0),
                "exposure": Decimal(0),
                "a0": Decimal(0),
                "a31": Decimal(0),
                "a61": Decimal(0),
                "a90": Decimal(0),
            },
        )
        cur["overdue"] += _to_decimal(r[c_overdue] if c_overdue < len(r) else "0")
        cur["exposure"] += _to_decimal(
            r[c_exposure] if c_exposure is not None and c_exposure < len(r) else "0"
        )
        cur["a0"] += _to_decimal(r[a0] if a0 is not None and a0 < len(r) else "0")
        cur["a31"] += _to_decimal(r[a31] if a31 is not None and a31 < len(r) else "0")
        cur["a61"] += _to_decimal(r[a61] if a61 is not None and a61 < len(r) else "0")
        cur["a90"] += _to_decimal(r[a90] if a90 is not None and a90 < len(r) else "0")

    for cust_name, vals in totals.items():
        if dry:
            stats.overdues_upserted += 1
            continue

        def _write():
            with transaction.atomic():
                cust = _get_or_create_customer(name=cust_name)
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
            return None

        _with_write_retry(_write)
        stats.overdues_upserted += 1


def run_sync_now() -> ImportStats:
    stats = ImportStats()
    sections = resolve_sections()

    logger.info("KAM sync sections=%s tabs=%s", sections, resolve_tabs_for_logging())

    if "customers" in sections:
        import_customers(stats)
    if "sales" in sections:
        import_sales(stats)
    if "leads" in sections:
        import_leads(stats)
    if "overdues" in sections:
        import_overdues(stats)

    logger.info("KAM sync stats: %s", stats.as_message())
    return stats


# ----------------------------
# Step sync (paged)
# ----------------------------

BATCH_SIZE = 50


def _cursor(current: Optional[str], total_rows: int) -> Tuple[int, Optional[str]]:
    start = int(current) if current else 2
    end = min(start + BATCH_SIZE - 1, total_rows)
    new_cur = str(end + 1) if end < total_rows else None
    return start, new_cur


def _headers(values: List[List[str]]) -> List[str]:
    if not values:
        return []
    return [_canon_header(h) for h in values[0]]


def _idx(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _val(row: List[str], i: Optional[int]) -> str:
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _pick(idm: Dict[str, int], *names: str) -> Optional[int]:
    return _col(idm, *names)


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
    cn = _pick(idm, "Customer Name", "Customer", "Name", "customer_name", "party name")
    addr = _pick(idm, "Address", "address", "customer address")
    email = _pick(idm, "Email", "email", "mail id")
    mob = _pick(
        idm, "Mobile No", "Mobile", "mobile", "phone", "contact no", "mobile number"
    )
    cl = _pick(idm, "Credit Limit", "credit_limit")
    acp = _pick(
        idm, "Agreed Credit Period", "Agreed Credit Period ", "agreed_credit_period_days"
    )
    usermap = _usermap()
    user_lookup = _build_user_lookup()
    processed = 0

    for r in values[1:]:
        name = _val(r, cn)
        if not name:
            continue
        kam, _kam_raw = _pick_row_kam(r, idm, usermap, user_lookup)

        def _write():
            with transaction.atomic():
                _get_or_create_customer(
                    name=name,
                    kam=kam,
                    address=_val(r, addr) or None,
                    email=_val(r, email) or None,
                    mobile=_val(r, mob) or None,
                    credit_limit=_to_decimal(_val(r, cl)),
                    agreed_credit_period_days=_to_int(_val(r, acp)),
                    force_kam_assignment=True,
                )
            return None

        _with_write_retry(_write)
        processed += 1
    return processed


def _invoices_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    cust = _pick(
        idm,
        "Customer Name",
        "Consignee Name",
        "Buyer's Name",
        "Buyer's Name",
        "Buyer\\'s Name",
        "customer_name",
        "party name",
        "customer",
    )
    invd = _pick(idm, "Invoice Date", "Date of Invoice", "invoice_date", "Date", "bill date")
    qty = _pick(
        idm, "QTY", "Qty(MT)", "Quantity", "qty_mt", "qty mt", "quantity mt", "invoice qty"
    )
    val = _pick(
        idm,
        "Invoice Value With GST",
        "Invoice Value with GST",
        "Invoice Value",
        "revenue_gst",
        "invoice value with gst rs",
        "invoice amount",
        "invoice value rs",
    )
    invno = _pick(
        idm, "Invoice Number", "Invoice No", "invoice_number", "bill no", "bill number"
    )
    grade_i = _pick(idm, "Grade", "grade")
    size_i = _pick(idm, "Size", "size", "Size(MM)", "size mm")
    usermap = _usermap()
    user_lookup = _build_user_lookup()
    processed = 0

    for r in values[1:]:
        kam, _kam_raw = _pick_row_kam(r, idm, usermap, user_lookup)
        inv_date = _parse_date(_val(r, invd))
        if not kam or not inv_date:
            continue
        customer_name = _val(r, cust)
        if not customer_name:
            continue

        qty_mt = _to_decimal(_val(r, qty))
        revenue_gst = _to_decimal(_val(r, val))
        inv_no = _val(r, invno)
        grade = _val(r, grade_i) or None
        size = _val(r, size_i) or None
        row_uuid = inv_no or _hash_row(
            "sales",
            "step",
            customer_name,
            kam.username,
            str(inv_date),
            str(qty_mt),
            str(revenue_gst),
            str(grade or ""),
            str(size or ""),
        )

        def _write():
            with transaction.atomic():
                customer = _get_or_create_customer(
                    name=customer_name, kam=kam, force_kam_assignment=False
                )
                InvoiceFact.objects.update_or_create(
                    row_uuid=row_uuid,
                    defaults={
                        "invoice_date": inv_date,
                        "customer": customer,
                        "kam": kam,
                        "grade": grade,
                        "size": size,
                        "qty_mt": qty_mt,
                        "revenue_gst": revenue_gst,
                    },
                )
            return None

        _with_write_retry(_write)
        processed += 1
    return processed


def _leads_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    ts = _pick(
        idm,
        "Timestamp",
        "Date of Enquiry",
        "doe",
        "enquiry date",
        "lead date",
        "created at",
        "date",
        "enquiry_date",
        "Entry Date",
        "entry date",
    )
    cust = _pick(
        idm,
        "Customer Name",
        "customer_name",
        "party name",
        "customer",
        "company name",
        "Company Name",
        "Firm Name",
        "firm name",
    )
    qty = _pick(
        idm,
        "Qty (MT)",
        "QTY",
        "Qty",
        "qty_mt",
        "qty",
        "quantity",
        "requirement mt",
        "requirement",
        "Requirement (MT)",
        "Req (MT)",
    )
    status_i = _pick(
        idm,
        "Status",
        "status",
        "lead status",
        "enquiry status",
        "Lead Status",
        "Enquiry Status",
    )
    grade_i = _pick(idm, "Grade", "grade")
    size_i = _pick(idm, "Size", "Size(MM)", "size", "size mm")
    remarks_i = _pick(
        idm, "Remarks", "remarks", "remark", "notes", "comment", "Notes", "Comments"
    )
    usermap = _usermap()
    user_lookup = _build_user_lookup()
    processed = 0

    for r in values[1:]:
        doe = _parse_date(_val(r, ts))
        kam, _kam_raw = _pick_row_kam(r, idm, usermap, user_lookup)
        if not kam or not doe:
            continue

        cust_name = _val(r, cust)
        qty_mt = _to_decimal(_val(r, qty))
        status = _normalize_status(_val(r, status_i))
        grade = _val(r, grade_i) or None
        size = _val(r, size_i) or None
        remarks = _val(r, remarks_i) or None
        row_uuid = _hash_row(
            "lead",
            "step",
            kam.username,
            str(doe),
            cust_name,
            str(qty_mt),
            status,
            str(grade or ""),
            str(size or ""),
            str(remarks or ""),
        )

        def _write():
            with transaction.atomic():
                customer = (
                    _get_or_create_customer(
                        name=cust_name, kam=kam, force_kam_assignment=False
                    )
                    if cust_name
                    else None
                )
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
            return None

        _with_write_retry(_write)
        processed += 1
    return processed


def _overdues_handler(values: List[List[str]]) -> int:
    headers = _headers(values)
    idm = _idx(headers)
    cust = _pick(idm, "Customer Name", "customer_name", "Customer", "party name")
    overdue_i = _pick(idm, "Overdues (Rs)", "Overdue", "overdue", "total overdue")
    exposure_i = _pick(
        idm, "Total Exposure (Rs)", "Exposure", "exposure", "total exposure"
    )
    a0 = _pick(idm, "0-30", "ageing_0_30", "0 30")
    a31 = _pick(idm, "31-60", "ageing_31_60", "31 60")
    a61 = _pick(idm, "61-90", "ageing_61_90", "61 60")
    a90 = _pick(idm, "90+", "ageing_90_plus", "90 plus")
    snap_date = timezone.localdate()
    totals: Dict[str, Dict[str, Decimal]] = {}

    for r in values[1:]:
        cust_name = _val(r, cust)
        if not cust_name:
            continue
        cur = totals.setdefault(
            cust_name,
            {
                "overdue": Decimal(0),
                "exposure": Decimal(0),
                "a0": Decimal(0),
                "a31": Decimal(0),
                "a61": Decimal(0),
                "a90": Decimal(0),
            },
        )
        cur["overdue"] += _to_decimal(_val(r, overdue_i))
        cur["exposure"] += _to_decimal(_val(r, exposure_i))
        cur["a0"] += _to_decimal(_val(r, a0))
        cur["a31"] += _to_decimal(_val(r, a31))
        cur["a61"] += _to_decimal(_val(r, a61))
        cur["a90"] += _to_decimal(_val(r, a90))

    processed = 0
    for cust_name, vals in totals.items():

        def _write():
            with transaction.atomic():
                cust_obj = _get_or_create_customer(name=cust_name)
                OverdueSnapshot.objects.update_or_create(
                    snapshot_date=snap_date,
                    customer=cust_obj,
                    defaults=dict(
                        exposure=vals["exposure"],
                        overdue=vals["overdue"],
                        ageing_0_30=vals["a0"],
                        ageing_31_60=vals["a31"],
                        ageing_61_90=vals["a61"],
                        ageing_90_plus=vals["a90"],
                    ),
                )
            return None

        _with_write_retry(_write)
        processed += 1
    return processed


def step_sync(intent: SyncIntent) -> Dict:
    sheet_id = _getenv("KAM_SALES_SHEET_ID")
    if not sheet_id:
        return {
            "last_customer_cursor": None,
            "last_invoice_cursor": None,
            "last_lead_cursor": None,
            "last_overdue_cursor": None,
            "done": True,
        }

    tabs = {
        "customers": _resolve_customers_tab(),
        "invoices": _resolve_sales_tab(),
        "leads": _resolve_leads_tab(),
        "overdues": _resolve_overdues_tab(),
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

        def _save_cursor():
            with transaction.atomic():
                locked = SyncIntent.objects.select_for_update().get(pk=intent.pk)
                setattr(locked, attr, new_cur)
                locked.save(update_fields=[attr, "updated_at"])
                setattr(intent, attr, new_cur)
            return None

        _with_write_retry(_save_cursor)

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