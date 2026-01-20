# apps/common/email_guard.py
from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Sequence, Tuple

from django.conf import settings

logger = logging.getLogger(__name__)


def _as_lower_list(v) -> List[str]:
    """
    Tolerant normalizer: accepts str, list/tuple/iterable; returns deduped lowercase list.
    """
    out: List[str] = []
    seen = set()
    if not v:
        return out
    try:
        if isinstance(v, str):
            v = [v]
        for x in v:
            s = (x or "").strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
    except Exception:
        # Never raise from the guard – just return best effort
        pass
    return out


def _starts_with_any(s: str, prefixes: Sequence[str]) -> bool:
    s = (s or "").strip().lower()
    if not s:
        return False
    for p in prefixes or []:
        p = (p or "").strip().lower()
        if not p:
            continue
        if s == p or s.startswith(p + ".") or s.startswith(p + ":") or s.startswith(p + "/"):
            return True
    return False


def _cfg() -> dict:
    """
    Read configuration from settings (no hardcoded emails here).

    Supported shapes (all optional):
      1) EMAIL_RESTRICTIONS = {
             "pankaj": {
                 "emails": ["pankaj@blueoceansteels.com"],   # 👈 restricted person
                 "usernames": ["pankaj"],
                 "allow": [
                     "delegation.assigned_by",         # CC to assigner for delegations when assigner is Pankaj
                     "delegation.pending_digest"       # consolidated delegation-pending digest to Pankaj
                 ]
             }
         }

      2) Compatibility single keys (if the dict above is not provided):
         PANKAJ_EMAILS = [...]
         PANKAJ_USERNAMES = [...]
         PANKAJ_ALLOWED_CATEGORIES = [...]

    All comparisons are case-insensitive.
    """
    by_dict = getattr(settings, "EMAIL_RESTRICTIONS", {}) or {}
    section = {}
    try:
        section = by_dict.get("pankaj") or {}
    except Exception:
        section = {}

    emails = section.get("emails") if isinstance(section, dict) else None
    usernames = section.get("usernames") if isinstance(section, dict) else None
    allow = section.get("allow") if isinstance(section, dict) else None

    # Fallback keys (still fully config-driven)
    emails = emails or getattr(settings, "PANKAJ_EMAILS", None)
    usernames = usernames or getattr(settings, "PANKAJ_USERNAMES", None)
    allow = allow or getattr(settings, "PANKAJ_ALLOWED_CATEGORIES", None)

    return {
        "emails": _as_lower_list(emails),
        "usernames": _as_lower_list(usernames),
        "allow": _as_lower_list(allow),
    }


def _is_pankaj_identity(email: Optional[str] = None, username: Optional[str] = None) -> bool:
    """
    True if the provided email/username matches the configured Pankaj identity.
    """
    cfg = _cfg()
    e = (email or "").strip().lower()
    u = (username or "").strip().lower()
    if not cfg["emails"] and not cfg["usernames"]:
        return False
    return (bool(e) and e in cfg["emails"]) or (bool(u) and u in cfg["usernames"])


def _filtered(lst: Optional[Iterable[str]], *, allow_category: bool) -> Tuple[List[str], List[str]]:
    """
    Return (kept, removed_pankaj) after applying the category rule for Pankaj.
    For non-Pankaj addresses the list is unchanged.
    """
    kept: List[str] = []
    removed: List[str] = []
    if not lst:
        return kept, removed

    cfg = _cfg()
    allowed_emails = set(cfg["emails"])

    for s in lst:
        addr = (s or "").strip()
        low = addr.lower()
        if not low:
            continue

        if low in allowed_emails:
            # It's Pankaj. Keep only if category is allowed right now.
            if allow_category:
                kept.append(addr)
            else:
                removed.append(addr)
        else:
            kept.append(addr)
    return kept, removed


def filter_recipients_for_category(
    *,
    category: str,
    to: Optional[Iterable[str]] = None,
    cc: Optional[Iterable[str]] = None,
    bcc: Optional[Iterable[str]] = None,
    # Optional context to qualify "delegation.assigned_by" allowance
    assigner_email: Optional[str] = None,
    assigner_username: Optional[str] = None,
) -> Tuple[List[str], List[str], List[str]]:
    """
    Central guard used by all mail senders.

    Behavior:
      • If Pankaj is configured and present in any recipient list, remove him
        unless the category is explicitly allowed by settings.
      • Allowed categories use prefix matching so you can configure coarse keys:
            "delegation"                 -> allows all delegation categories
            "delegation.assigned_by"     -> allows only CC-to-assigner delegation mails
            "delegation.pending_digest"  -> allows the pending delegation digest to Pankaj
      • The assigner_email/username is used only to qualify "delegation.assigned_by":
            if the assigner IS Pankaj and the category starts with "delegation.assigned_by",
            we treat it as allowed for Pankaj (so he can be CC’d on tasks he assigned).

    This function never raises; on any error it returns the original lists.
    """
    try:
        cfg = _cfg()
        allowed = cfg["allow"]
        if not cfg["emails"] and not cfg["usernames"]:
            # No restriction configured – leave as-is
            return list(to or []), list(cc or []), list(bcc or [])

        cat = (category or "").strip().lower()

        # Determine if Pankaj is allowed for this category:
        allow_for_category = _starts_with_any(cat, allowed)

        # Special case: "delegation.assigned_by" is allowed only when the assigner is Pankaj
        if not allow_for_category and _starts_with_any(cat, ["delegation.assigned_by"]):
            if _is_pankaj_identity(assigner_email, assigner_username):
                allow_for_category = True

        kept_to, _ = _filtered(to, allow_category=allow_for_category)
        kept_cc, _ = _filtered(cc, allow_category=allow_for_category)
        kept_bcc, _ = _filtered(bcc, allow_category=allow_for_category)
        return kept_to, kept_cc, kept_bcc
    except Exception as e:
        logger.warning("email_guard failed for category=%r: %s", category, e)
        return list(to or []), list(cc or []), list(bcc or [])


def strip_rows_to_delegations_only_if_pankaj_target(
    rows: List[dict],
    *,
    target_email: str,
) -> List[dict]:
    """
    Helper for the consolidated 'admin' digest:
      If the target is Pankaj (by configured identity), keep ONLY rows with task_type == 'Delegation'.
      Otherwise return rows unchanged.
    """
    try:
        if not _is_pankaj_identity(email=target_email):
            return rows
        # Keep only delegation rows for Pankaj
        return [r for r in rows or [] if (r.get("task_type") or "").strip().lower() == "delegation"]
    except Exception:
        return rows or []
