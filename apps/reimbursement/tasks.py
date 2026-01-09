# -*- coding: utf-8 -*-
from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Tuple

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.db.models import Sum, Count
from django.utils import timezone

from .models import ReimbursementRequest, ReimbursementSettings

logger = logging.getLogger(__name__)


@dataclass
class MonthlySummaryRow:
    employee_id: int
    employee_name: str
    employee_email: str
    total_amount: Decimal
    request_count: int


def _month_range(target: date | None = None) -> Tuple[datetime, datetime]:
    """
    Returns (start_dt, end_dt) for the full calendar month containing `target`.
    """
    if target is None:
        target = timezone.localdate()

    first_day = date(target.year, target.month, 1)
    last_day_num = calendar.monthrange(target.year, target.month)[1]
    last_day = date(target.year, target.month, last_day_num)

    start_dt = datetime.combine(
        first_day,
        datetime.min.time(),
        tzinfo=timezone.get_current_timezone(),
    )
    end_dt = datetime.combine(
        last_day,
        datetime.max.time(),
        tzinfo=timezone.get_current_timezone(),
    )
    return start_dt, end_dt


def _build_monthly_rows(start: datetime, end: datetime) -> List[MonthlySummaryRow]:
    """
    Aggregate totals per employee between [start, end].

    NOTE (flow aligned):
    - Totals are based on ReimbursementRequest.total_amount for any request
      submitted in the month. Each request's total comes from INCLUDED lines
      only, and lines can move independently across Finance/Manager/Payment.
    """
    qs = (
        ReimbursementRequest.objects.filter(
            submitted_at__gte=start,
            submitted_at__lte=end,
        )
        .values(
            "created_by__id",
            "created_by__first_name",
            "created_by__last_name",
            "created_by__username",
            "created_by__email",
        )
        .annotate(
            total_amount=Sum("total_amount"),
            request_count=Count("id"),
        )
        .order_by("-total_amount")
    )

    rows: List[MonthlySummaryRow] = []
    for row in qs:
        first = row.get("created_by__first_name") or ""
        last = row.get("created_by__last_name") or ""
        username = row.get("created_by__username") or ""
        full_name = (first + " " + last).strip() or username or f"User #{row['created_by__id']}"
        rows.append(
            MonthlySummaryRow(
                employee_id=row["created_by__id"],
                employee_name=full_name,
                employee_email=row.get("created_by__email") or "",
                total_amount=row["total_amount"] or Decimal("0"),
                request_count=row["request_count"] or 0,
            )
        )
    return rows


def _render_month_label(start: datetime, end: datetime) -> str:
    if start.year == end.year and start.month == end.month:
        return start.strftime("%B %Y")
    return f"{start.strftime('%Y-%m-%d')} – {end.strftime('%Y-%m-%d')}"


def _render_email_body(month_label: str, rows: List[MonthlySummaryRow]) -> tuple[str, str]:
    """
    Returns (html, text) for the monthly admin summary email.
    """
    if not rows:
        txt_lines = [
            f"Reimbursement Summary — {month_label}",
            "",
            "No reimbursements were submitted in this period.",
        ]
        txt = "\n".join(txt_lines)
        html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;">
    <div style="max-width:640px;margin:0 auto;padding:20px;">
      <h2>Reimbursement Summary — {month_label}</h2>
      <p>No reimbursements were submitted in this period.</p>
    </div>
  </body>
</html>
""".strip()
        return html, txt

    # Text body
    txt_lines = [
        f"Reimbursement Summary — {month_label}",
        "",
        f"{'Employee':30} {'Count':>5} {'Total Amount':>12}",
        "-" * 54,
    ]
    for r in rows:
        name = (r.employee_name or "")[:30]
        txt_lines.append(f"{name:30} {r.request_count:5d} {r.total_amount:12.2f}")
    txt = "\n".join(txt_lines)

    # HTML table rows
    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
          <td style="padding:6px 8px;border-bottom:1px solid #e5e7eb;">{r.employee_name}</td>
          <td style="padding:6px 8px;text-align:center;border-bottom:1px solid #e5e7eb;">{r.request_count}</td>
          <td style="padding:6px 8px;text-align:right;border-bottom:1px solid #e5e7eb;">₹{r.total_amount:.2f}</td>
        </tr>
""".rstrip()

    html = f"""
<html>
  <body style="font-family:system-ui,Segoe UI,Helvetica,Arial,sans-serif;background:#f9fafb;padding:16px;">
    <div style="max-width:720px;margin:0 auto;background:#ffffff;border-radius:10px;
                border:1px solid #e5e7eb;padding:20px;">
      <h2 style="margin-top:0;">Reimbursement Summary — {month_label}</h2>
      <table style="width:100%;border-collapse:collapse;font-size:14px;margin-top:8px;">
        <thead>
          <tr style="background:#f3f4f6;">
            <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb;">Employee</th>
            <th style="text-align:center;padding:6px 8px;border-bottom:1px solid #e5e7eb;">Requests</th>
            <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb;">Total Amount</th>
          </tr>
        </thead>
        <tbody>
{rows_html}
        </tbody>
      </table>
      <p style="font-size:12px;color:#6b7280;margin-top:16px;">
        This is an automated monthly summary from BOS Lakshya.
      </p>
    </div>
  </body>
</html>
""".strip()

    return html, txt


def send_monthly_admin_summary(target_month: date | None = None, dry_run: bool = False) -> None:
    """
    Build and (optionally) send a monthly reimbursement summary to Admins.

    - If `target_month` is None, summarises the previous calendar month.
    - If `dry_run` is True, prints the summary to stdout instead of sending email.

    Flow alignment:
    - Bill-level independence is already reflected in request totals and statuses.
      No special handling is required here beyond using request totals.
    """
    # Default: previous month
    if target_month is None:
        today = timezone.localdate()
        if today.month == 1:
            target_month = date(today.year - 1, 12, 1)
        else:
            target_month = date(today.year, today.month - 1, 1)

    start, end = _month_range(target_month)
    rows = _build_monthly_rows(start, end)
    month_label = _render_month_label(start, end)

    # Resolve admin recipients from settings model
    settings_obj = ReimbursementSettings.get_solo()
    admin_emails = settings_obj.admin_email_list()
    if not admin_emails:
        default_admin = getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
        if default_admin:
            admin_emails = [default_admin]

    if not admin_emails:
        # Nowhere to send
        if dry_run:
            print("No admin emails configured; summary not sent.")
        logger.info("Monthly summary not sent for %s (no admin recipients).", month_label)
        return

    html_body, txt_body = _render_email_body(month_label, rows)
    subject = f"Reimbursement Summary — {month_label}"

    if dry_run:
        print(subject)
        print()
        print(txt_body)
        return

    # Sender fallback chain
    from_email = getattr(settings, "REIMBURSEMENT_EMAIL_FROM", None) or getattr(settings, "DEFAULT_FROM_EMAIL", None)
    to = [admin_emails[0]]
    cc = list(dict.fromkeys(admin_emails[1:]))

    try:
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=txt_body,
                from_email=from_email,
                to=to,
                cc=cc or None,
                connection=conn,
            )
            msg.attach_alternative(html_body, "text/html")
            msg.send(fail_silently=getattr(settings, "EMAIL_FAIL_SILENTLY", True))
        logger.info("Monthly summary email sent for %s to %s (cc=%s).", month_label, to, cc)
    except Exception:
        logger.exception("Failed to send monthly summary email for %s", month_label)
