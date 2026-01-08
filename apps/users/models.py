# apps/users/models.py
from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models

logger = logging.getLogger(__name__)
User = get_user_model()

# ---------------------------------------------------------------------
# Phone helpers
# ---------------------------------------------------------------------

_DIGITS = re.compile(r"\D+")


def normalize_phone(value: Optional[str]) -> Optional[str]:
    """
    Keep digits only; collapse blanks to None.
    Trim leading 0 and keep last up to 13 digits (to allow country codes).
    """
    if not value:
        return None
    digits = _DIGITS.sub("", str(value)).lstrip("0")
    if not digits:
        return None
    return digits[-13:] if len(digits) > 13 else digits


# ---------------------------------------------------------------------
# Admin-only Leave Routing Map (manager/CC) stored in one JSON file
# ---------------------------------------------------------------------
# Supported JSON schemas (all case-insensitive):
#
# 1) Flat (legacy):
#   {
#     "employee@example.com": {
#       "manager_email": "manager@example.com",
#       "cc_emails": ["cc1@example.com", "cc2@example.com"]
#     }
#   }
#
# 2) Flat (new UI help text):
#   {
#     "employee@example.com": {
#       "to": "manager@example.com",
#       "cc": ["cc1@example.com", "cc2@example.com"]
#     }
#   }
#
# 3) Optional wrapper:
#   {
#     "by_employee": { ...one of the above... }
#   }
#
# The resolvers below normalize all of these to:
#   { "<emp>": { "manager": "<email or ''>", "cc": ["..."] } }
# ---------------------------------------------------------------------

DEFAULT_ROUTING_FILE = Path(settings.BASE_DIR) / "apps" / "users" / "data" / "leave_routing.json"


def _routing_file_path() -> Path:
    path = getattr(settings, "LEAVE_ROUTING_FILE", None)
    return Path(path) if path else DEFAULT_ROUTING_FILE


def _safe_lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _split_emails(value) -> List[str]:
    """
    Accept list/tuple or a comma/semicolon-separated string.
    Returns a deduped, lowercased list.
    """
    if not value:
        return []
    items: List[str]
    if isinstance(value, str):
        items = [p.strip() for p in value.replace(";", ",").split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = [str(x).strip() for x in value]
    else:
        try:
            items = [str(x).strip() for x in value]  # type: ignore[arg-type]
        except Exception:
            items = []
    out: List[str] = []
    seen = set()
    for it in items:
        low = _safe_lower(it)
        if low and low not in seen:
            seen.add(low)
            out.append(low)
    return out


def _normalize_map(raw: Dict) -> Dict[str, Dict[str, List[str]]]:
    """
    Normalize any supported schema to:
      { "<emp>": { "manager": "<email or ''>", "cc": ["..."] } }
    """
    if not isinstance(raw, dict):
        return {}

    # allow "by_employee" wrapper
    data = raw.get("by_employee")
    if isinstance(data, dict):
        working = data
    else:
        working = raw

    result: Dict[str, Dict[str, List[str]]] = {}
    for key, val in (working or {}).items():
        if not isinstance(val, dict):
            continue
        emp = _safe_lower(key)
        # accept both pairs
        mgr = _safe_lower(val.get("manager") or val.get("manager_email") or val.get("to"))
        cc = _split_emails(val.get("cc") or val.get("cc_emails"))
        result[emp] = {"manager": mgr, "cc": cc}
    return result


@lru_cache(maxsize=1)
def load_leave_routing_map() -> Dict[str, Dict[str, List[str]]]:
    """
    Load and normalize the admin-only leave routing map from JSON.
    Returns: { "<emp>": {"manager": "<email or ''>", "cc": ["..."]}, ... }
    """
    path = _routing_file_path()
    try:
        if not path.exists():
            logger.warning("Leave routing file not found: %s", path)
            return {}
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f) or {}
        return _normalize_map(raw)
    except Exception as e:
        logger.exception("Failed to load leave routing file %s: %s", path, e)
        return {}


def resolve_routing_for_email(employee_email: str) -> Tuple[Optional[str], List[str]]:
    """
    Return (manager_email, cc_list) from the admin-only file for the given employee email.
    Accepts any of the supported schemas; values are lowercased.
    """
    row = load_leave_routing_map().get(_safe_lower(employee_email) or "")
    if not row:
        return None, []
    return (row.get("manager") or None), list(row.get("cc") or [])


# ---------------------------------------------------------------------
# Profile model (adds admin overrides used by routing)
# ---------------------------------------------------------------------

class Profile(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="profile",
    )

    # Profile photo used by multiple modules (name chosen from the allowed list)
    photo = models.ImageField(
        upload_to="profiles/%Y/%m/",
        null=True,
        blank=True,
        verbose_name="Profile Photo",
    )

    # Core fields you already use
    phone = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
        default=None,
        help_text="Digits only; leave blank if unknown.",
    )
    role = models.CharField(
        max_length=50,
        choices=[
            ("Admin", "Admin"),
            ("Manager", "Manager"),
            ("HR", "HR"),
            ("Finance", "Finance"),
            ("Sales Executive", "Sales Executive"),
            ("Employee", "Employee"),
            ("EA", "EA"),
            ("CEO", "CEO"),
        ],
    )
    branch = models.CharField(max_length=100, blank=True, default="")
    department = models.CharField(max_length=100, blank=True, default="")
    team_leader = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="team_members",
    )
    permissions = models.JSONField(default=list, blank=True)

    # New fields used across modules
    employee_id = models.CharField(max_length=50, blank=True, default="")

    # Admin-only overrides for routing (take precedence over JSON map)
    manager_override_email = models.EmailField(
        blank=True,
        default="",
        help_text="Admin-only: override manager email for leave approvals.",
    )
    cc_override_emails = models.TextField(
        blank=True,
        default="",
        help_text="Admin-only: comma-separated CC emails for leave notifications.",
    )

    def save(self, *args, **kwargs):
        # Normalize phone before saving so '' doesn't hit the UNIQUE index
        self.phone = normalize_phone(self.phone)
        super().save(*args, **kwargs)

    def __str__(self) -> str:  # pragma: no cover
        name = getattr(self.user, "get_full_name", lambda: "")() or self.user.username
        return f"{name} – {self.role}"

    class Meta:
        ordering = ["user__username"]
        indexes = [
            models.Index(fields=["role"]),
        ]

    # ---------------------------
    # Routing helpers (read-only for users)
    # ---------------------------
    def cc_override_list(self) -> List[str]:
        return _split_emails(self.cc_override_emails or "")

    def resolve_manager_and_cc(self) -> Tuple[Optional[str], List[str]]:
        """
        Final routing (in order of precedence):
          1) manager := manager_override_email (if set)
          2) else from admin-only file mapping (apps/users/data/leave_routing.json)
          3) else team_leader.email (if present)

          CC := cc_override_emails + file CC + LEAVE_DEFAULT_CC (deduped, lowercased)
        """
        global_defaults = getattr(settings, "LEAVE_DEFAULT_CC", [])  # may be absent

        # Manager
        mgr_email = _safe_lower(self.manager_override_email)
        file_mgr, file_cc = resolve_routing_for_email(self.user.email or "")
        if not mgr_email:
            mgr_email = _safe_lower(file_mgr)
        if not mgr_email and self.team_leader and _safe_lower(getattr(self.team_leader, "email", "")):
            mgr_email = _safe_lower(self.team_leader.email)

        # CC combine (override first, then file, then global)
        cc_all: List[str] = []
        for bucket in (self.cc_override_list(), file_cc, global_defaults):
            for e in bucket or []:
                e_low = _safe_lower(e)
                if e_low and e_low not in cc_all:
                    cc_all.append(e_low)

        return (mgr_email or None), cc_all

    @property
    def manager_email(self) -> Optional[str]:
        mgr, _ = self.resolve_manager_and_cc()
        return mgr

    @property
    def cc_emails(self) -> List[str]:
        _, cc = self.resolve_manager_and_cc()
        return cc

    @property
    def avatar_url(self) -> str:
        try:
            if self.photo and hasattr(self.photo, "url"):
                return self.photo.url
        except Exception:
            pass
        return ""
