# apps/tasks/services/pending_digest.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import get_template
from django.utils import timezone

from apps.tasks.models import Checklist, Delegation, HelpTicket

logger = logging.getLogger(__name__)
User = get_user_model()


@dataclass
class DigestRow:
    task_id: str
    task_title: str
    assigned_to: str
    assigned_by: str
    due_date: str
    task_type: str
    status: str


def _fmt_user(u) -> str:
    if not u:
        return "-"
    try:
        return (u.get_full_name() or u.username or "").strip() or (u.email or "-")
    except Exception:
        return getattr(u, "username", None) or getattr(u, "email", None) or "-"


def _fmt_dt(dt) -> str:
    if not dt:
        return "-"
    try:
        z = timezone.get_current_timezone()
        loc = timezone.localtime(dt, z)
        tzname = loc.tzname() or "IST"
        return loc.strftime(f"%d %b %Y, %H:%M {tzname}")
    except Exception:
        return str(dt)


def _gather_rows_for_user(user: User, *, today_ist_date=None) -> List[DigestRow]:
    """
    Collect the employee's pending items across: Checklist, Delegation, Help Ticket.
    - Checklist: status = Pending
    - Delegation: status = Pending
    - Help Ticket: not Closed (Open / In Progress)
    """
    rows: List[DigestRow] = []

    # Checklist (Pending)
    qs_chk = (
        Checklist.objects.filter(assign_to=user, status="Pending")
        .select_related("assign_by", "assign_to")
        .order_by("planned_date", "id")
    )
    for t in qs_chk:
        rows.append(
            DigestRow(
                task_id=f"CL-{t.id}",
                task_title=t.task_name or "-",
                assigned_to=_fmt_user(t.assign_to),
                assigned_by=_fmt_user(t.assign_by),
                due_date=_fmt_dt(t.planned_date),
                task_type="Checklist",
                status=t.status,
            )
        )

    # Delegation (Pending)
    qs_del = (
        Delegation.objects.filter(assign_to=user, status="Pending")
        .select_related("assign_by", "assign_to")
        .order_by("planned_date", "id")
    )
    for t in qs_del:
        rows.append(
            DigestRow(
                task_id=f"DL-{t.id}",
                task_title=t.task_name or "-",
                assigned_to=_fmt_user(t.assign_to),
                assigned_by=_fmt_user(t.assign_by),
                due_date=_fmt_dt(t.planned_date),
                task_type="Delegation",
                status=t.status,
            )
        )

    # Help Tickets (not Closed)
    qs_ht = (
        HelpTicket.objects.filter(assign_to=user)
        .exclude(status="Closed")
        .select_related("assign_by", "assign_to")
        .order_by("planned_date", "id")
    )
    for t in qs_ht:
        rows.append(
            DigestRow(
                task_id=f"HT-{t.id}",
                task_title=t.title or "-",
                assigned_to=_fmt_user(t.assign_to),
                assigned_by=_fmt_user(t.assign_by),
                due_date=_fmt_dt(t.planned_date),
                task_type="Help Ticket",
                status=t.status,
            )
        )

    return rows


def _render_email(rows: List[DigestRow], *, report_date_str: str) -> Tuple[str, str]:
    """
    Returns (subject, html_body, text_body) for the digest.
    """
    subject = f"Daily Pending Task Summary — {report_date_str}"

    html_tmpl = get_template("email/daily_pending_tasks_summary.html")

    ctx: Dict[str, Any] = {
        "title": "Daily Pending Task Summary",
        "report_date": report_date_str,
        "total_pending": len(rows),
        "has_rows": bool(rows),
        "items_table": [r.__dict__ for r in rows],
        "site_url": getattr(settings, "SITE_URL", ""),
    }
    html = html_tmpl.render(ctx)

    # Plaintext fallback
    if rows:
        lines = [
            f"Daily Pending Task Summary — {report_date_str}",
            f"Total Pending: {len(rows)}",
            "",
            "Task ID | Title | Assigned To | Assigned By | Due Date | Type | Status",
            "-" * 90,
        ]
        for r in rows:
            lines.append(
                f"{r.task_id} | {r.task_title} | {r.assigned_to} | {r.assigned_by} | {r.due_date} | {r.task_type} | {r.status}"
            )
        lines.append("")
        if getattr(settings, "SITE_URL", ""):
            lines.append(f"Open Dashboard: {getattr(settings, 'SITE_URL')}")
    else:
        lines = [
            f"Daily Pending Task Summary — {report_date_str}",
            "No pending tasks as of end-of-day.",
        ]
        if getattr(settings, "SITE_URL", ""):
            lines.append(f"Open Dashboard: {getattr(settings, 'SITE_URL')}")

    text = "\n".join(lines)
    return subject, html, text


def _send_email(to_email: str, subject: str, html: str, text: str) -> bool:
    """
    Sends an individual email. Respects your EMAIL_* settings and default sender.
    """
    if not to_email:
        return False

    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "")
    reply_to = [from_email] if from_email else None

    try:
        with get_connection() as conn:
            msg = EmailMultiAlternatives(
                subject=subject,
                body=text,
                from_email=from_email or None,
                to=[to_email],
                reply_to=reply_to,
                connection=conn,
            )
            msg.attach_alternative(html, "text/html")
            sent = msg.send(fail_silently=getattr(settings, "EMAIL_FAIL_SILENTLY", True))
            return bool(sent)
    except Exception as e:
        logger.exception("Failed sending pending digest to %s: %s", to_email, e)
        return False


def send_daily_pending_digests_for_all_users() -> Dict[str, int]:
    """
    Main entry:
    - Iterate all active users that have an email.
    - For each one, gather rows and send exactly one email (even if there are 0 tasks).
    Returns counters for logging.
    """
    if not getattr(settings, "FEATURES", {}).get("EMAIL_NOTIFICATIONS", True):
        logger.info("FEATURES.EMAIL_NOTIFICATIONS is OFF — skipping pending task digests.")
        return {"processed": 0, "sent": 0}

    # IST "report date"
    now = timezone.now()
    try:
        # Convert to app timezone (Asia/Kolkata in your settings)
        now_loc = timezone.localtime(now)
    except Exception:
        now_loc = now
    report_date_str = now_loc.strftime("%d %b %Y")

    users = (
        User.objects.filter(is_active=True)
        .exclude(email__isnull=True)
        .exclude(email__exact="")
        .order_by("id")
    )

    processed = 0
    sent = 0

    for u in users:
        processed += 1
        try:
            rows = _gather_rows_for_user(u)
            subject, html, text = _render_email(rows, report_date_str=report_date_str)
            if _send_email(u.email.strip(), subject, html, text):
                sent += 1
        except Exception as e:
            logger.exception("Digest failed for user %s: %s", getattr(u, "username", u.pk), e)

    logger.info("Daily pending digests: processed=%s sent=%s", processed, sent)
    return {"processed": processed, "sent": sent}
