# apps/users/models.py
from __future__ import annotations

import re
from typing import Optional

from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

# digits-only normalizer
_DIGITS = re.compile(r"\D+")


def normalize_phone(value: Optional[str]) -> Optional[str]:
    """
    Keep digits only; collapse blanks to None.
    Trim leading 0 and keep last 10–13 digits to allow country codes.
    """
    if not value:
        return None
    digits = _DIGITS.sub("", str(value))
    digits = digits.lstrip("0")
    if not digits:
        return None
    if len(digits) > 13:
        digits = digits[-13:]
    return digits


class Profile(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="profile",
    )

    # IMPORTANT: nullable + blank → many rows can be NULL without violating UNIQUE
    phone = models.CharField(
        max_length=20,           # allow country codes
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
