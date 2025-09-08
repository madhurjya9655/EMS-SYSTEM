# apps/users/routing.py
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model

User = get_user_model()

# Default location (can be overridden via LEAVE_ROUTING_FILE in settings.py)
_DEFAULT_FILE = Path(settings.BASE_DIR) / "apps" / "users" / "data" / "leave_routing.json"


def _routing_path() -> Path:
    path = getattr(settings, "LEAVE_ROUTING_FILE", None)
    return Path(path) if path else _DEFAULT_FILE


def _lower(s: str | None) -> str:
    return (s or "").strip().lower()


def _norm_list(values) -> List[str]:
    """
    Normalize into a de-duplicated lowercase list.
    Accepts list/tuple or comma/semicolon-separated string.
    """
    if not values:
        return []
    if isinstance(values, str):
        values = values.replace(";", ",").split(",")
    out, seen = [], set()
    for v in values:
        v = _lower(v)
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _normalize_map(raw: Dict) -> Dict[str, Dict[str, List[str]]]:
    """
    Accept either:
      1) Flat:
         {
           "emp@x": {"to": "manager@x", "cc": ["a@x"]},
           ...
         }
      2) Flat (alternate keys):
         {
           "emp@x": {"manager_email": "manager@x", "cc_emails": ["a@x"]},
           ...
         }
      3) Nested under "by_employee" with either key style:
         {
           "by_employee": {
             "emp@x": {"manager": "manager@x", "cc": ["a@x"]}
           }
         }

    Returns normalized map:
        { emp_email: {"manager": str, "cc": [str, ...]}, ... }
    """
    data = raw.get("by_employee") or raw
    out: Dict[str, Dict[str, List[str]]] = {}
    for emp, row in (data or {}).items():
        row = row or {}

        # Manager key aliases: "manager", "manager_email", "to"
        manager = (
            row.get("manager")
            or row.get("manager_email")
            or row.get("to")
            or ""
        )

        # CC key aliases: "cc", "cc_emails"
        cc_values = row.get("cc")
        if cc_values is None:
            cc_values = row.get("cc_emails", [])

        out[_lower(emp)] = {"manager": _lower(manager), "cc": _norm_list(cc_values)}
    return out


@lru_cache(maxsize=1)
def load_routing_map() -> Dict[str, Dict[str, List[str]]]:
    """
    Read and normalize the admin-maintained JSON mapping.
    Cache is cleared only when explicitly called from admin/commands.
    """
    path = _routing_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f) or {}
    except Exception:
        return {}
    return _normalize_map(raw)


def clear_cache() -> None:
    load_routing_map.cache_clear()  # type: ignore[attr-defined]


def for_employee_email(emp_email: str) -> Tuple[str, List[str]]:
    """
    Resolve (manager_email, cc_list) for the given employee email.

    Precedence:
      1) Profile.manager_override_email (if set by Admin)
      2) JSON map manager (apps/users/data/leave_routing.json)
      3) Profile.team_leader.email (if present)
      4) Any superuser email (first by date_joined)
    CC list:
      Profile.cc_override_emails + JSON map CC + settings.LEAVE_DEFAULT_CC (deduped, lowercase)
    """
    emp_email = _lower(emp_email)
    mapping = load_routing_map()
    row = mapping.get(emp_email, {})

    file_mgr = row.get("manager") or ""
    file_cc = row.get("cc", [])

    # Global default CC from settings
    default_cc = getattr(settings, "LEAVE_DEFAULT_CC", []) or []

    prof = None
    try:
        from apps.users.models import Profile  # local import to avoid import cycles
        prof = Profile.objects.select_related("team_leader", "user").filter(user__email__iexact=emp_email).first()
    except Exception:
        prof = None

    # Manager resolution (overrides > file > team_leader > superuser)
    mgr = ""
    if prof:
        mo = _lower(getattr(prof, "manager_override_email", "") or "")
        if mo:
            mgr = mo

    if not mgr and file_mgr:
        mgr = _lower(file_mgr)

    if not mgr and prof and getattr(prof, "team_leader", None) and getattr(prof.team_leader, "email", ""):
        mgr = _lower(prof.team_leader.email)

    if not mgr:
        su = User.objects.filter(is_superuser=True, is_active=True).order_by("date_joined").first()
        mgr = _lower(su.email if su and su.email else "")

    # CC resolution (override + file + global defaults)
    cc_override = []
    if prof:
        raw_override = getattr(prof, "cc_override_emails", "") or ""
        cc_override = _norm_list(raw_override)

    cc = _norm_list([*cc_override, *file_cc, *default_cc])
    return mgr, cc


def recipients_for_leave(employee_email: str) -> Dict[str, List[str] | str]:
    """
    Convenience wrapper for leave emails.
    Returns: {"to": "manager@x", "cc": ["cc1@x", ...]}
    """
    mgr, cc = for_employee_email(employee_email)
    return {"to": mgr, "cc": cc}
