#E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\settings\models.py
from __future__ import annotations

import logging
from datetime import date as dt_date, datetime as dt_datetime

from django.db import models
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

logger = logging.getLogger(__name__)


class AuthorizedNumber(models.Model):
    label = models.CharField(max_length=100, help_text="A friendly name for this number")
    number = models.CharField(max_length=20, help_text="Phone number or code")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"{self.label} ({self.number})"


class Holiday(models.Model):
    date = models.DateField(unique=True)
    name = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-date"]

    def __str__(self) -> str:
        return f"{self.date:%Y-%m-%d} – {self.name}"

    @classmethod
    def normalize_to_date(cls, value):
        """
        Normalize a date-like input into a pure date.

        Accepts:
          - date
          - datetime
          - objects with .date()

        Returns:
          - date or None
        """
        if value is None:
            return None

        if isinstance(value, dt_datetime):
            return value.date()

        if isinstance(value, dt_date):
            return value

        try:
            if hasattr(value, "date"):
                converted = value.date()
                if isinstance(converted, dt_date):
                    return converted
        except Exception:
            logger.exception("Holiday.normalize_to_date failed for value=%r", value)

        return None

    @classmethod
    def is_holiday(cls, d) -> bool:
        """
        Fast check to know if a given date is a holiday.
        Accepts a date/datetime or any object exposing .date().
        """
        try:
            normalized = cls.normalize_to_date(d)
            if normalized is None:
                return False
            return cls.objects.filter(date=normalized).exists()
        except Exception:
            logger.exception("Holiday.is_holiday check failed")
            return False


class SystemSetting(models.Model):
    whatsapp_vendor = models.CharField(max_length=100, blank=True)
    whatsapp_api_key = models.CharField(max_length=255, blank=True)
    whatsapp_sender_id = models.CharField(max_length=100, blank=True)
    whatsapp_webhook_url = models.URLField(blank=True)

    authorized_phones = models.TextField(
        blank=True,
        help_text="Comma-separated phone numbers allowed for WhatsApp.",
    )
    authorized_emails = models.TextField(
        blank=True,
        help_text="Comma-separated emails allowed for notifications.",
    )

    send_daily_doer = models.BooleanField(default=False)
    send_daily_admin = models.BooleanField(default=False)
    send_weekly_doer = models.BooleanField(default=False)
    send_weekly_admin = models.BooleanField(default=False)
    send_monthly_doer = models.BooleanField(default=False)
    send_monthly_admin = models.BooleanField(default=False)

    notify_wapp_pending_checklist = models.BooleanField(default=False)
    notify_wapp_pending_delegation = models.BooleanField(default=False)
    notify_email_pending_checklist = models.BooleanField(default=False)
    notify_email_pending_delegation = models.BooleanField(default=False)

    notify_wapp_checklist = models.BooleanField(default=False)
    notify_wapp_fms = models.BooleanField(default=False)
    notify_email_checklist = models.BooleanField(default=False)
    notify_email_delegation = models.BooleanField(default=False)
    notify_email_helpticket = models.BooleanField(default=False)
    notify_email_helpticket_reminder = models.BooleanField(default=False)
    all_doer_report_generate = models.BooleanField(default=False)

    MIS_MODES = [("equal", "Equal"), ("weighted", "Weighted")]
    mis_performance_mode = models.CharField(max_length=20, choices=MIS_MODES, default="equal")
    checklist_weightage = models.PositiveIntegerField(default=1)
    delegation_weightage = models.PositiveIntegerField(default=1)
    fms_weightage = models.PositiveIntegerField(default=1)
    weight_low = models.PositiveIntegerField(default=1)
    weight_medium = models.PositiveIntegerField(default=1)
    weight_high = models.PositiveIntegerField(default=1)

    smtp_from_name = models.CharField(max_length=100, blank=True)
    smtp_username = models.EmailField(blank=True)
    smtp_password = models.CharField(max_length=255, blank=True)

    high_stock_notification_freq = models.PositiveIntegerField(default=7)
    low_stock_notification_freq = models.PositiveIntegerField(default=7)
    stockout_notification_freq = models.PositiveIntegerField(default=7)
    max_fast_flowing_product = models.PositiveIntegerField(default=50)
    max_slow_flowing_product = models.PositiveIntegerField(default=50)

    MARKETING_MODES = [("random", "Random"), ("sequential", "Sequential")]
    marketing_mode = models.CharField(max_length=20, choices=MARKETING_MODES, default="random")
    marketing_freeze_min = models.PositiveIntegerField(default=1)
    marketing_freeze_max = models.PositiveIntegerField(default=5)
    marketing_after_sending = models.PositiveIntegerField(default=10)
    marketing_sleep_min = models.PositiveIntegerField(default=1)
    marketing_sleep_max = models.PositiveIntegerField(default=3)

    logo = models.ImageField(upload_to="system_logos/", blank=True, null=True)

    def __str__(self) -> str:
        return "System Settings"

    class Meta:
        verbose_name = "System Setting"
        verbose_name_plural = "System Settings"


def _call_holiday_hook(*, action: str, holiday_date: dt_date) -> None:
    """
    Best-effort holiday reconciliation hook.

    Safe behavior:
      - if module does not exist -> log and do nothing
      - if function does not exist -> log and do nothing
      - if function exists but fails -> log exception
    """
    try:
        from apps.tasks.services import auto_assign
    except Exception as exc:
        logger.warning(
            "Holiday %s hook skipped for %s: could not import apps.tasks.services.auto_assign (%s)",
            action,
            holiday_date,
            exc,
        )
        return

    func_name = "handle_holiday_added" if action == "added" else "handle_holiday_removed"
    hook = getattr(auto_assign, func_name, None)

    if not callable(hook):
        logger.info(
            "Holiday %s hook skipped for %s: %s is not available.",
            action,
            holiday_date,
            func_name,
        )
        return

    try:
        hook(holiday_date)
    except Exception:
        logger.exception("Holiday %s hook failed for %s", action, holiday_date)


@receiver(post_save, sender=Holiday)
def _on_holiday_saved(sender, instance: Holiday, created: bool, **kwargs):
    """
    When a holiday is added/updated, notify task services if that hook exists.
    Never fail the save if downstream reconciliation is unavailable.
    """
    _call_holiday_hook(action="added", holiday_date=instance.date)


@receiver(post_delete, sender=Holiday)
def _on_holiday_deleted(sender, instance: Holiday, **kwargs):
    """
    When a holiday is removed, notify task services if that hook exists.
    Never fail the delete if downstream reconciliation is unavailable.
    """
    _call_holiday_hook(action="removed", holiday_date=instance.date)