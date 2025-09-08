# apps/users/management/commands/import_employee_sheet.py
from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

User = get_user_model()

# Columns expected in the APP Sheet CSV
REQUIRED_COLUMNS = [
    "E-mail",
    "Employee Name",
    "Mobile Number",
    "Employee ID",
    "To Reporting officer",
    "CC Email",
    "Employee Designation",
]

# Where we persist the admin-only routing data (manager/CC) per employee
DEFAULT_ROUTING_FILE = Path(settings.BASE_DIR) / "apps" / "users" / "data" / "leave_routing.json"


def _routing_file_path() -> Path:
    path = getattr(settings, "LEAVE_ROUTING_FILE", None)
    return Path(path) if path else DEFAULT_ROUTING_FILE


def _safe_lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _split_emails(value: str) -> List[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.replace(";", ",").split(",")]
    out, seen = [], set()
    for p in parts:
        low = _safe_lower(p)
        if low and low not in seen:
            seen.add(low)
            out.append(low)
    return out


def _load_existing_mapping(path: Path) -> Dict[str, Dict[str, List[str]]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f) or {}
        # normalize keys/values
        out: Dict[str, Dict[str, List[str]]] = {}
        for emp_email, row in raw.items():
            mgr = _safe_lower((row or {}).get("manager_email"))
            cc = row.get("cc_emails") or []
            if isinstance(cc, str):
                cc = _split_emails(cc)
            else:
                cc = [_safe_lower(x) for x in cc if _safe_lower(x)]
            out[_safe_lower(emp_email)] = {"manager_email": mgr, "cc_emails": cc}
        return out
    except Exception:
        return {}


def _save_mapping(path: Path, mapping: Dict[str, Dict[str, List[str]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)


@dataclass
class Row:
    employee_email: str
    employee_name: str
    employee_phone: str
    employee_id: str
    manager_email: str
    cc_emails: List[str]
    designation: str


def _read_csv(csv_path: Path) -> List[Row]:
    if not csv_path.exists():
        raise CommandError(f"CSV not found: {csv_path}")

    # Handle common BOM
    try_encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    last_err: Optional[Exception] = None
    for enc in try_encodings:
        try:
            with csv_path.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                headers = set(reader.fieldnames or [])
                missing = [c for c in REQUIRED_COLUMNS if c not in headers]
                if missing:
                    raise CommandError(
                        f"CSV missing required columns: {', '.join(missing)}. "
                        f"Found columns: {', '.join(headers)}"
                    )
                rows: List[Row] = []
                for r in reader:
                    rows.append(
                        Row(
                            employee_email=_safe_lower(r.get("E-mail", "")),
                            employee_name=(r.get("Employee Name", "") or "").strip(),
                            employee_phone=(r.get("Mobile Number", "") or "").strip(),
                            employee_id=(r.get("Employee ID", "") or "").strip(),
                            manager_email=_safe_lower(r.get("To Reporting officer", "")),
                            cc_emails=_split_emails(r.get("CC Email", "")),
                            designation=(r.get("Employee Designation", "") or "").strip(),
                        )
                    )
                return rows
        except Exception as e:
            last_err = e
            continue
    raise CommandError(f"Failed to read CSV {csv_path}: {last_err}")


def _ensure_profile_model():
    try:
        return apps.get_model("users", "Profile")
    except Exception:
        raise CommandError("users.Profile model not found")


def _normalize_phone(value: str) -> Optional[str]:
    # Use the helper from Profile if available, else a simple fallback
    try:
        from apps.users.models import normalize_phone  # type: ignore
        return normalize_phone(value)
    except Exception:
        digits = "".join([c for c in (value or "") if c.isdigit()])
        digits = digits.lstrip("0")
        if not digits:
            return None
        return digits[-13:]


def _username_from_email(email: str) -> str:
    base = (email.split("@")[0] if "@" in email else email).replace(".", "_").replace("-", "_")
    candidate = base or "user"
    exists = set(User.objects.filter(username__startswith=candidate).values_list("username", flat=True))
    if candidate not in exists:
        return candidate
    i = 1
    while f"{candidate}{i}" in exists:
        i += 1
    return f"{candidate}{i}"


class Command(BaseCommand):
    help = (
        "Import the APP Sheet CSV and pre-code manager/CC routing for leave emails into a single admin-owned file.\n"
        "Also updates/creates Profile entries for phone & employee_id, and (optionally) links team_leader."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            dest="csv_path",
            default="APP Sheet - Employee data.csv",
            help="Path to the APP Sheet CSV file.",
        )
        parser.add_argument(
            "--routing-file",
            dest="routing_file",
            default=str(_routing_file_path()),
            help="Path to write admin-only leave routing JSON.",
        )
        parser.add_argument(
            "--create-missing-users",
            action="store_true",
            default=False,
            help="Create User rows for missing employees/managers (inactive passwords).",
        )
        parser.add_argument(
            "--link-manager",
            action="store_true",
            default=False,
            help="Set Profile.team_leader for employees to the resolved manager user (if found/created).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Parse and show summary without writing JSON or touching DB.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        routing_path = Path(options["routing_file"])
        create_users = bool(options["create_missing_users"])
        link_manager = bool(options["link_manager"])
        dry_run = bool(options["dry_run"])

        rows = _read_csv(csv_path)
        Profile = _ensure_profile_model()

        existing_map = _load_existing_mapping(routing_path)
        new_map = dict(existing_map)  # copy

        created_users = 0
        updated_profiles = 0
        linked_team_leaders = 0
        routed_count = 0

        # First pass: ensure/collect manager users if requested
        manager_cache: Dict[str, User] = {}
        if create_users:
            for row in rows:
                m_email = row.manager_email
                if not m_email:
                    continue
                if m_email in manager_cache:
                    continue
                user = User.objects.filter(email__iexact=m_email).first()
                if not user:
                    user = User.objects.create(
                        username=_username_from_email(m_email),
                        email=m_email,
                        first_name="",  # unknown in CSV
                        is_active=True,
                    )
                    created_users += 1
                manager_cache[m_email] = user

        # Process each employee row
        for row in rows:
            emp_email = row.employee_email
            mgr_email = row.manager_email
            if not emp_email:
                self.stdout.write(self.style.WARNING("Skipping row with empty employee email"))
                continue

            # Build routing mapping entry
            entry = new_map.get(emp_email, {"manager_email": "", "cc_emails": []})
            if mgr_email:
                entry["manager_email"] = mgr_email
            # Merge CCs (dedup)
            cc_set = {c for c in entry.get("cc_emails", [])}
            for c in row.cc_emails:
                cc_set.add(_safe_lower(c))
            entry["cc_emails"] = sorted([c for c in cc_set if c])
            new_map[emp_email] = entry
            routed_count += 1

            if dry_run:
                continue

            # Ensure employee user/profile
            user = User.objects.filter(email__iexact=emp_email).first()
            if not user and create_users:
                user = User.objects.create(
                    username=_username_from_email(emp_email),
                    email=emp_email,
                    first_name=(row.employee_name or "").split(" ", 1)[0],
                    last_name=(row.employee_name or "").split(" ", 1)[1] if " " in (row.employee_name or "") else "",
                    is_active=True,
                )
                created_users += 1

            if not user:
                # Can't update Profile without a user
                continue

            prof, _ = Profile.objects.get_or_create(user=user)

            # Update phone & employee_id (idempotent)
            phone_norm = _normalize_phone(row.employee_phone or "")
            changed = False
            if phone_norm and prof.phone != phone_norm:
                prof.phone = phone_norm
                changed = True
            if row.employee_id and getattr(prof, "employee_id", "") != row.employee_id:
                setattr(prof, "employee_id", row.employee_id)
                changed = True

            # Optionally set team_leader
            if link_manager and mgr_email:
                mgr_user = manager_cache.get(mgr_email) or User.objects.filter(email__iexact=mgr_email).first()
                if mgr_user and prof.team_leader_id != getattr(mgr_user, "id", None):
                    prof.team_leader = mgr_user
                    changed = True
                    linked_team_leaders += 1

            if changed:
                prof.save()
                updated_profiles += 1

        # Write the admin-only routing JSON
        if not dry_run:
            _save_mapping(routing_path, new_map)
            # Bust the in-memory cache used by the app
            try:
                from apps.users.models import load_leave_routing_map  # type: ignore
                load_leave_routing_map.cache_clear()  # type: ignore[attr-defined]
            except Exception:
                pass

        # Summary
        self.stdout.write(self.style.SUCCESS("=== Import Summary ==="))
        self.stdout.write(f"CSV rows read         : {len(rows)}")
        self.stdout.write(f"Routing entries written: {routed_count} → {routing_path}")
        self.stdout.write(f"Users created         : {created_users}")
        self.stdout.write(f"Profiles updated      : {updated_profiles}")
        if link_manager:
            self.stdout.write(f"Team leaders linked   : {linked_team_leaders}")
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run mode: no files written, no DB changes."))

        # Friendly reminder about global CC defaults
        if getattr(settings, "LEAVE_DEFAULT_CC", None):
            self.stdout.write(f"Global default CC (settings.LEAVE_DEFAULT_CC): {settings.LEAVE_DEFAULT_CC}")
        else:
            self.stdout.write("Tip: you can set a global default CC list via settings.LEAVE_DEFAULT_CC = ['ops@example.com', ...]")
