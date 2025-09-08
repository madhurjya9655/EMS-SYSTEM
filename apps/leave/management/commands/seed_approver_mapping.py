# apps/leave/management/commands/seed_approver_mapping.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

User = get_user_model()


@dataclass
class Row:
    employee_email: str
    manager_email: str
    cc_emails: list[str]


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _pick_cc_user(cc_candidates: Iterable[str], rp_user) -> Optional[User]:
    """
    Deterministically choose a CC user:
      1) first email that resolves to a user and is NOT the RP
      2) else first email that resolves to a user (even if equals RP)
      3) else None (caller may fallback to RP or skip row)
    """
    cc_users: list[Tuple[str, Optional[User]]] = []
    for e in cc_candidates:
        u = User.objects.filter(email__iexact=_norm(e)).first()
        cc_users.append((e, u))

    # prefer not-equal to RP
    for _, u in cc_users:
        if u and (not rp_user or u.id != getattr(rp_user, "id", None)):
            return u
    # else any resolvable
    for _, u in cc_users:
        if u:
            return u
    return None


def _resolve_user_by_email(email: str) -> Optional[User]:
    if not email:
        return None
    return User.objects.filter(email__iexact=_norm(email)).first()


def _default_routing_path() -> Path:
    rel = getattr(settings, "LEAVE_ROUTING_FILE", "apps/users/data/leave_routing.json")
    p = Path(rel)
    if not p.is_absolute():
        p = Path(settings.BASE_DIR) / p
    return p


class Command(BaseCommand):
    help = (
        "Seed or update ApproverMapping from a JSON routing file.\n"
        "JSON format:\n"
        '{\n'
        '  "employee@example.com": {\n'
        '    "manager_email": "manager@example.com",\n'
        '    "cc_emails": ["hr1@example.com", "hr2@example.com"]\n'
        '  },\n'
        '  ...\n'
        '}\n\n'
        "Notes:\n"
        "• Users must already exist (matched by email, case-insensitive); missing users are skipped.\n"
        "• For multiple CC addresses, the first resolvable user (prefer not RP) is chosen.\n"
        "• If no CC resolves, you may allow fallback to RP via --fallback-cc-to-rp.\n"
        "• Idempotent: only creates/updates when values change.\n"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            dest="file",
            default=None,
            help="Path to routing JSON (defaults to settings.LEAVE_ROUTING_FILE).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Compute and print changes without writing to the database.",
        )
        parser.add_argument(
            "--strict",
            action="store_true",
            default=False,
            help="Exit with error if any row cannot be resolved (missing users/emails).",
        )
        parser.add_argument(
            "--fallback-cc-to-rp",
            action="store_true",
            default=False,
            help="If no CC user resolves, set cc_person = reporting_person (required because cc_person is non-null).",
        )

    def handle(self, *args, **opts):
        file_arg = opts["file"]
        dry = bool(opts["dry_run"])
        strict = bool(opts["strict"])
        fallback_cc_to_rp = bool(opts["fallback_cc_to_rp"])

        path = Path(file_arg) if file_arg else _default_routing_path()
        if not path.exists():
            raise CommandError(f"Routing file not found: {path}")

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Top-level JSON must be an object mapping employee_email -> { ... }")
        except Exception as e:
            raise CommandError(f"Failed to parse JSON: {e}")

        # Normalize into rows
        rows: list[Row] = []
        for emp_email, cfg in data.items():
            if not isinstance(cfg, dict):
                self.stderr.write(f"Skipping {emp_email!r}: value must be an object.")
                continue
            rows.append(
                Row(
                    employee_email=_norm(emp_email),
                    manager_email=_norm(cfg.get("manager_email")),
                    cc_emails=[_norm(x) for x in (cfg.get("cc_emails") or []) if x],
                )
            )

        if not rows:
            self.stdout.write(self.style.WARNING("No rows found in routing file; nothing to do."))
            return

        # Import model here to avoid circulars at import time
        from apps.leave.models import ApproverMapping  # type: ignore

        created = 0
        updated = 0
        unchanged = 0
        skipped = 0
        errors = 0

        @transaction.atomic
        def _apply_row(r: Row):
            nonlocal created, updated, unchanged, skipped, errors

            emp = _resolve_user_by_email(r.employee_email)
            if not emp:
                msg = f"[SKIP] Employee not found: {r.employee_email}"
                if strict:
                    raise CommandError(msg)
                self.stderr.write(msg)
                skipped += 1
                return

            rp = _resolve_user_by_email(r.manager_email)
            if not rp:
                msg = f"[SKIP] RP (manager) not found for employee={r.employee_email}: {r.manager_email}"
                if strict:
                    raise CommandError(msg)
                self.stderr.write(msg)
                skipped += 1
                return

            cc_user = _pick_cc_user(r.cc_emails, rp)
            if not cc_user:
                if fallback_cc_to_rp:
                    cc_user = rp
                else:
                    msg = f"[SKIP] CC not resolvable for employee={r.employee_email}; candidates={r.cc_emails or ['(none)']}"
                    if strict:
                        raise CommandError(msg)
                    self.stderr.write(msg)
                    skipped += 1
                    return

            # Upsert
            obj, was_created = ApproverMapping.objects.get_or_create(
                employee=emp,
                defaults={"reporting_person": rp, "cc_person": cc_user, "notes": ""},
            )
            if was_created:
                created += 1
                self.stdout.write(self.style.SUCCESS(f"[CREATE] {emp.email} → RP:{rp.email} CC:{cc_user.email}"))
                return

            # Compare & update if needed
            need_update = (
                (obj.reporting_person_id != rp.id)
                or (obj.cc_person_id != cc_user.id)
            )
            if not need_update:
                unchanged += 1
                self.stdout.write(f"[OK] {emp.email} already mapped to RP:{rp.email} CC:{cc_user.email}")
                return

            if not dry:
                obj.reporting_person = rp
                obj.cc_person = cc_user
                obj.save(update_fields=["reporting_person", "cc_person", "updated_at"])
                updated += 1
                self.stdout.write(self.style.WARNING(f"[UPDATE] {emp.email} → RP:{rp.email} CC:{cc_user.email}"))
            else:
                updated += 1  # count as would-update in dry-run
                self.stdout.write(self.style.NOTICE(f"[DRY] Would update {emp.email} → RP:{rp.email} CC:{cc_user.email}"))

        # Apply all rows (each within its own transaction)
        for r in rows:
            try:
                if dry:
                    # avoid wrapping every row in a db transaction if dry-run — still resolve & check DB
                    _apply_row.__wrapped__(r)  # type: ignore[attr-defined]
                else:
                    _apply_row(r)
            except CommandError:
                raise
            except Exception as e:
                errors += 1
                self.stderr.write(self.style.ERROR(f"[ERR] {r.employee_email}: {e}"))

        # Summary
        self.stdout.write(self.style.SUCCESS("=== ApproverMapping Seed Summary ==="))
        self.stdout.write(f"File              : {path}")
        self.stdout.write(f"Dry-run           : {dry}")
        self.stdout.write(f"Fallback CC→RP    : {fallback_cc_to_rp}")
        self.stdout.write(f"Created           : {created}")
        self.stdout.write(f"Updated           : {updated}")
        self.stdout.write(f"Unchanged         : {unchanged}")
        self.stdout.write(f"Skipped           : {skipped}")
        self.stdout.write(f"Errors            : {errors}")

        if strict and (skipped or errors):
            raise CommandError("Strict mode: some rows were skipped or errored.")
