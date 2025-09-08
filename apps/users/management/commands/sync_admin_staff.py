from __future__ import annotations

import sys
from typing import Any

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

from apps.users.models import Profile

User = get_user_model()


class Command(BaseCommand):
    help = (
        "Ensure every Profile with role 'Admin' has a linked User with is_staff=True. "
        "This does NOT change is_superuser and will NOT demote staff."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would change without saving.",
        )

    def handle(self, *args: Any, **options: Any) -> str | None:
        dry = bool(options.get("dry_run"))
        qs = Profile.objects.select_related("user").filter(role="Admin")

        changed = 0
        total = qs.count()

        for p in qs:
            u = getattr(p, "user", None)
            if not u:
                self.stdout.write(self.style.WARNING(f"[SKIP] Profile {p.pk} has no linked user"))
                continue
            if not u.is_staff:
                if dry:
                    self.stdout.write(f"[DRY] Would mark User {u.pk} ({u.username}) as staff")
                else:
                    u.is_staff = True
                    u.save(update_fields=["is_staff"])
                    changed += 1
                    self.stdout.write(self.style.SUCCESS(f"[OK] Marked User {u.pk} ({u.username}) as staff"))

        msg = f"Checked {total} Admin profiles; {'would change' if dry else 'changed'} {changed} user(s)."
        self.stdout.write(self.style.SUCCESS(msg))
        return None
