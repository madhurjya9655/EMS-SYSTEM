#apps\leave\services\daily_digest.py
from __future__ import annotations

import logging
from datetime import date
from typing import List
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.html import strip_tags

from apps.leave.models import LeaveRequest, LeaveStatus

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _today_ist() -> date:
    return timezone.localtime(timezone.now(), IST).date()


def _recipient_emails() -> List[str]:
    User = get_user_model()
    seen, result = set(), []
    for email in User.objects.filter(is_active=True).exclude(email__isnull=True).exclude(email="").values_list("email", flat=True):
        normalized = (email or "").strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def current_on_leave_rows(target_date: date | None = None):
    target_date = target_date or _today_ist()
    return list(
        LeaveRequest.objects.filter(
            status__in=[LeaveStatus.PENDING, LeaveStatus.APPROVED],
            start_date__lte=target_date,
            end_date__gte=target_date,
        )
        .select_related("employee", "leave_type")
        .order_by("employee__first_name", "employee__last_name", "employee__username", "start_at")
    )


def send_daily_leave_digest_email(target_date: date | None = None) -> int:
    target_date = target_date or _today_ist()
    leaves = current_on_leave_rows(target_date)
    if not leaves:
        logger.info("Daily leave digest skipped for %s: nobody is on leave.", target_date)
        return 0
    recipients = _recipient_emails()
    if not recipients:
        return 0
    context = {"digest_date": target_date, "leaves": leaves, "leave_count": len(leaves)}
    html_body = render_to_string("leave/email/daily_leave_digest.html", context)
    text_body = render_to_string("leave/email/daily_leave_digest.txt", context)
    if not text_body.strip():
        text_body = strip_tags(html_body)
    from_email = getattr(settings, "LEAVE_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None)
    with get_connection() as connection:
        message = EmailMultiAlternatives(
            subject=f"Employees on Leave Today - {target_date:%d %b %Y}",
            body=text_body,
            from_email=from_email,
            to=[from_email] if from_email else [],
            bcc=recipients,
            connection=connection,
        )
        message.attach_alternative(html_body, "text/html")
        return int(message.send(fail_silently=False))