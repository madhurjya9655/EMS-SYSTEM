from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from django.apps import apps
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.text import slugify

User = get_user_model()


# --------------------------------------------------------------------------------------
# Your sheet’s exact headers (baked in from: APP Sheet - Employee data.csv)
# Detected columns:
#   E-mail, Employee Name, Mobile Number, Employee ID, MD Name,
#   To Reporting officer, MD Whatsapp Number, CC Email, Employee Designation
#
# Mapping below is used first; we still auto-detect as a fallback if headers change.
# --------------------------------------------------------------------------------------
EXACT_HEADER_MAPPING: Dict[str, Optional[str]] = {
    "name": "Employee Name",
    "email": "E-mail",
    "designation": "Employee Designation",
    "department": None,  # not present in the current sheet
    # Manager resolution:
    #   - Primary: "To Reporting officer" (appears to contain manager email)
    #   - Fallback name: "MD Name"
    #   - Secondary email fallback: first email from "CC Email" if needed
    "manager_email": "To Reporting officer",
    "manager_name": "MD Name",
    "cc_email": "CC Email",  # optional; may contain multiple comma-separated emails
}

# Header candidates (used only if EXACT_HEADER_MAPPING doesn’t match the CSV)
CANDIDATES = {
    "name": {"name", "employee name", "full name", "emp name", "employee"},
    "email": {"email", "e-mail", "official email", "work email", "office email", "employee email"},
    "designation": {"designation", "title", "role", "job title", "position", "employee designation"},
    "department": {"department", "dept", "team"},
    "manager_email": {
        "manager email",
        "manager_email",
        "manager e-mail",
        "reporting manager email",
        "reports to email",
        "supervisor email",
        "to reporting officer",
        "to reporting officer email",
    },
    "manager_name": {
        "manager",
        "manager name",
        "reporting manager",
        "reports to",
        "supervisor",
        "line manager",
        "md name",
    },
    "cc_email": {"cc", "cc email", "cc emails"},
}


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _norm_email(s: str | None) -> str:
    return _norm(s).lower()


def _split_name(full_name: str) -> Tuple[str, str]:
    """Best-effort split into first/last names."""
    s = " ".join(_norm(full_name).split())
    if not s:
        return "", ""
    parts = s.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def _extract_emails(s: str | None) -> List[str]:
    """
    Extract one or more email addresses from a free-text field.
    Also fixes a common typo we observed: 'bluleoceansteels.com' -> 'blueoceansteels.com'
    """
    raw = (_norm(s) or "")
    if not raw:
        return []
    # Normalize common domain typo
    raw = raw.replace("bluleoceansteels.com", "blueoceansteels.com")
    # Split on commas/semicolons/whitespace
    parts = re.split(r"[,\s;]+", raw)
    emails: List[str] = []
    for p in parts:
        p = p.strip().lower()
        if not p:
            continue
        # crude email check
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", p):
            emails.append(p)
    # de-duplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for e in emails:
        if e not in seen:
            uniq.append(e)
            seen.add(e)
    return uniq


def _first_email(*candidates: str | None) -> str:
    for cand in candidates:
        emails = _extract_emails(cand)
        if emails:
            return emails[0]
    return ""


@dataclass
class Row:
    name: str
    email: str
    designation: str
    department: str
    manager_email: str
    manager_name: str


class Command(BaseCommand):
    help = (
        "Import/Sync employees from 'APP Sheet - Employee data.csv'. "
        "Creates/updates Users and Profiles; resolves manager by email/name; idempotent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            dest="path",
            default="APP Sheet - Employee data.csv",
            help="CSV path (default: 'APP Sheet - Employee data.csv' at project root).",
        )
        parser.add_argument(
            "--activate-new",
            action="store_true",
            help="If given, new users will be active. Default is INACTIVE for newly created users.",
        )

    # ----------------------------- Profile helpers ----------------------------- #

    def _get_profile_model(self):
        """Try to find a profile model under apps.users"""
        for name in ("Profile", "UserProfile", "EmployeeProfile"):
            try:
                M = apps.get_model("users", name)
                # Ensure it has O2O to User
                for f in M._meta.get_fields():
                    if getattr(getattr(f, "remote_field", None), "model", None) is User and f.one_to_one:
                        return M
            except Exception:
                continue
        return None

    def _get_profile_for(self, user):
        """get_or_create a profile instance if model exists; else return None."""
        Profile = self._get_profile_model()
        if not Profile:
            return None
        # Find o2o field to User
        user_field_name = None
        for f in Profile._meta.get_fields():
            if getattr(getattr(f, "remote_field", None), "model", None) is User and f.one_to_one:
                user_field_name = f.name
                break
        if not user_field_name:
            return None

        obj, _ = Profile.objects.get_or_create(**{user_field_name: user})
        return obj

    def _set_if_has(self, obj, field: str, value):
        if hasattr(obj, field):
            setattr(obj, field, value)
            return True
        return False

    def _find_manager_user(self, *, email: str, name: str) -> Optional[User]:
        email = _norm_email(email)
        name = _norm(name)
        if email:
            try:
                return User.objects.filter(email__iexact=email).first()
            except Exception:
                pass
        if name:
            fn, ln = _split_name(name)
            qs = User.objects.all()
            if fn and ln:
                u = qs.filter(first_name__iexact=fn, last_name__iexact=ln).first()
                if u:
                    return u
            # Fallback: username or full_name contains
            name_slug = slugify(name).replace("-", " ")
            for u in qs.only("id", "username", "first_name", "last_name"):
                try:
                    full = f"{u.first_name} {u.last_name}".strip().lower()
                    if full == name.lower() or slugify(full).replace("-", " ") == name_slug:
                        return u
                    if (u.username or "").strip().lower() == name.lower():
                        return u
                except Exception:
                    continue
        return None

    # ----------------------------- CSV mapping ----------------------------- #

    def _map_headers(self, headers: list[str]) -> Dict[str, str]:
        """
        Return a mapping of our logical keys -> actual header in CSV.

        Strategy:
          1) If EXACT_HEADER_MAPPING fits the CSV, use it (ignoring keys with None).
          2) Else, try auto-detection using CANDIDATES.
        """
        existing = {h.strip().lower(): h for h in headers}

        # Try exact first
        exact: Dict[str, str] = {}
        fits_exact = True
        for key, hdr in EXACT_HEADER_MAPPING.items():
            if hdr is None:
                continue
            if hdr in headers:
                exact[key] = hdr
            else:
                fits_exact = False
                break
        if fits_exact:
            return exact

        # Fallback: candidates
        mapping: Dict[str, str] = {}
        for key, options in CANDIDATES.items():
            for opt in options:
                if opt in existing:
                    mapping[key] = existing[opt]
                    break

        # Minimal required
        missing = [k for k in ("name", "email") if k not in mapping]
        if missing:
            raise CommandError(
                f"CSV must contain columns for {', '.join(missing)}. "
                f"Detected headers: {headers}"
            )
        return mapping

    def _row_from(self, raw: dict, mapping: Dict[str, str]) -> Row:
        g = lambda k: raw.get(mapping.get(k, ""), "")
        # Choose manager email: primary field; else first email from cc; else ""
        mgr_email = _first_email(g("manager_email"), raw.get(EXACT_HEADER_MAPPING.get("cc_email") or "", ""))
        # Manager name fallback (may be top boss / MD)
        mgr_name = _norm(g("manager_name"))
        return Row(
            name=_norm(g("name")),
            email=_norm_email(g("email")),
            designation=_norm(g("designation")),
            department=_norm(g("department")) if mapping.get("department") else "",
            manager_email=_norm_email(mgr_email),
            manager_name=mgr_name,
        )

    # ----------------------------- Main logic ----------------------------- #

    @transaction.atomic
    def handle(self, *args, **options):
        default_path = Path(options["path"])
        if not default_path.is_absolute():
            from django.conf import settings
            csv_path = Path(settings.BASE_DIR) / options["path"]
        else:
            csv_path = default_path

        if not csv_path.exists():
            self.stdout.write(self.style.WARNING(f"CSV not found: {csv_path}. No-op."))
            return

        activate_new: bool = options.get("activate_new", False)

        created_users = 0
        updated_users = 0
        profiles_touched = 0
        managers_resolved = 0
        rows_seen = 0

        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            headers = reader.fieldnames or []
            if not headers:
                raise CommandError("CSV has no headers.")

            mapping = self._map_headers(headers)

            self.stdout.write(self.style.NOTICE("Detected headers:"))
            self.stdout.write("  " + ", ".join(headers))
            self.stdout.write(self.style.NOTICE("Using mapping:"))
            display_keys = ("name", "email", "designation", "department", "manager_email", "manager_name")
            for k in display_keys:
                self.stdout.write(f"  {k:>14} -> {mapping.get(k, '—')}")

            for raw in reader:
                rows_seen += 1
                row = self._row_from(raw, mapping)
                if not row.email:
                    self.stdout.write(self.style.WARNING(f"Row {rows_seen}: skipped (missing email)."))
                    continue

                # ----- User create/update -----
                try:
                    user = User.objects.filter(email__iexact=row.email).first()
                except Exception as e:
                    raise CommandError(f"Failed querying for user {row.email}: {e}")

                first_name, last_name = _split_name(row.name)
                if user:
                    changed = False
                    if first_name and user.first_name != first_name:
                        user.first_name = first_name
                        changed = True
                    if last_name and user.last_name != last_name:
                        user.last_name = last_name
                        changed = True
                    if not user.username:
                        user.username = row.email.split("@", 1)[0]
                        changed = True
                    if changed:
                        user.save(update_fields=["first_name", "last_name", "username"])
                        updated_users += 1
                else:
                    username_base = (row.email.split("@", 1)[0] or slugify(row.name) or "user").lower()
                    username = username_base
                    # Ensure unique username
                    suffix = 1
                    while User.objects.filter(username__iexact=username).exists():
                        suffix += 1
                        username = f"{username_base}{suffix}"

                    user = User.objects.create(
                        username=username,
                        email=row.email,
                        first_name=first_name,
                        last_name=last_name,
                        is_active=bool(activate_new),
                    )
                    created_users += 1

                # ----- Profile create/update (best-effort) -----
                prof = self._get_profile_for(user)
                if prof:
                    touched = False
                    touched |= self._set_if_has(prof, "designation", row.designation)
                    touched |= self._set_if_has(prof, "department", row.department)

                    # Resolve manager (prefer email; else name)
                    manager = self._find_manager_user(email=row.manager_email, name=row.manager_name)
                    if manager:
                        # Find a likely field for manager (FK to User)
                        manager_field = None
                        for f in prof._meta.get_fields():
                            if getattr(getattr(f, "remote_field", None), "model", None) is User:
                                if f.name in {"manager", "reporting_manager", "reports_to", "supervisor"}:
                                    manager_field = f.name
                                    break
                        if not manager_field:
                            # fallback to first FK to User that's not the O2O to user
                            for f in prof._meta.get_fields():
                                if (
                                    getattr(getattr(f, "remote_field", None), "model", None) is User
                                    and not getattr(f, "one_to_one", False)
                                ):
                                    manager_field = f.name
                                    break
                        if manager_field:
                            setattr(prof, manager_field, manager)
                            touched = True
                            managers_resolved += 1

                    # Photo placeholder only if field exists and empty
                    if hasattr(prof, "photo") and not getattr(prof, "photo"):
                        # leave empty; template can show placeholder
                        pass

                    if touched:
                        prof.save()
                        profiles_touched += 1

        self.stdout.write(self.style.SUCCESS("Import finished."))
        self.stdout.write(
            f"Rows: {rows_seen} | Users created: {created_users} | Users updated: {updated_users} | "
            f"Profiles touched: {profiles_touched} | Managers resolved: {managers_resolved}"
        )
