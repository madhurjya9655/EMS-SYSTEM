# apps/tasks/utils/__init__.py
from __future__ import annotations

import logging
import re
from typing import Iterable, List, Optional, Sequence, Callable, Any

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import render_to_string
from django.utils import timezone as dj_tz

logger = logging.getLogger(__name__)

# =============================================================================
# Base URL helpers
# =============================================================================
def _compute_site_url() -> str:
    """
    Prefer settings.SITE_URL; else build from ALLOWED_HOSTS or fall back to localhost:8000.
    """
    v = (getattr(settings, "SITE_URL", "") or "").strip()
    if v:
        return v.rstrip("/")

    try:
        host = ((getattr(settings, "ALLOWED_HOSTS", None) or [])[0] or "").strip()
        if host:
            scheme = "https" if getattr(settings, "SECURE_SSL_REDIRECT", False) else "http"
            return f"{scheme}://{host}".rstrip("/")
    except Exception:
        pass

    port = 8000
    try:
        port = int(getattr(settings, "RUNSERVER_PORT", 8000))
    except Exception:
        pass
    return f"http://localhost:{port}"

SITE_URL = _compute_site_url()

def build_absolute_url(path: str) -> str:
    if not path:
        return SITE_URL
    if path.startswith(("http://", "https://")):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return f"{SITE_URL}{path}"

# =============================================================================
# Logging-safe text
# =============================================================================
def _safe_console_text(s: object) -> str:
    try:
        text = str(s)
    except Exception:
        text = repr(s)
    return re.sub(r"[^\x09\x0a\x0d\x20-\x7E]", "?", text)

# =============================================================================
# Email utilities
# =============================================================================
def _dedupe_emails(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for e in items or []:
        if not e:
            continue
        em = str(e).strip().lower()
        if em and em not in seen:
            seen.add(em)
            out.append(em)
    return out

def get_admin_emails(exclude: Optional[Sequence[str]] = None) -> List[str]:
    emails: List[str] = []
    try:
        for _name, _email in getattr(settings, "ADMINS", []) or []:
            if _email:
                emails.append(_email)
    except Exception:
        pass

    try:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        qs = (
            User.objects.filter(is_staff=True)
            .exclude(email__isnull=True)
            .exclude(email__exact="")
            .only("email")
        )
        emails += [u.email for u in qs]
    except Exception:
        pass

    if exclude:
        ex = {str(x).lower() for x in exclude}
        emails = [e for e in emails if e.lower() not in ex]

    return _dedupe_emails(emails)

def send_html_email(
    *,
    subject: str,
    template_name: Optional[str] = None,
    context: Optional[dict] = None,
    to: Sequence[str],
    body_fallback: Optional[str] = None,
    fail_silently: bool = True,
) -> None:
    """
    Simple, robust HTML email sender with template fallback.
    """
    html_body = ""
    txt_body = body_fallback or (subject or "")

    if template_name:
        try:
            html_body = render_to_string(template_name, context or {})
        except Exception as e:
            logger.warning(_safe_console_text(f"[MAIL] Template render failed for {template_name}: {e}"))

    if not html_body:
        safe_text = (body_fallback or "").replace("\n", "<br/>")
        html_body = f"<html><body><h3>{subject}</h3><p>{safe_text}</p></body></html>"

    to = [t for t in (to or []) if t]
    if not to:
        logger.info(_safe_console_text(f"[MAIL] Skipped send: no recipients for '{subject}'"))
        return

    try:
        connection = get_connection(fail_silently=fail_silently)
        msg = EmailMultiAlternatives(
            subject=subject,
            body=txt_body,
            to=list(to),
            connection=connection,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send()
    except Exception as e:
        logger.error(_safe_console_text(f"[MAIL] Send failed for '{subject}' to {to}: {e}"))
        if not fail_silently:
            raise

def _fmt_dt_date(dt_obj) -> str:
    """
    ISO date (YYYY-MM-DD) for datetime/date; empty string if None.
    """
    try:
        if not dt_obj:
            return ""
        if hasattr(dt_obj, "astimezone"):
            return dj_tz.localtime(dt_obj).date().isoformat()
        from datetime import date as _date
        if isinstance(dt_obj, _date):
            return dt_obj.isoformat()
        return str(dt_obj)
    except Exception:
        return ""

# =============================================================================
# Concrete assignment notifiers (used directly by tasks.py / views)
# =============================================================================
def _user_email(obj) -> Optional[str]:
    user = getattr(obj, "assign_to", None)
    return getattr(user, "email", None) if user else None

def send_checklist_assignment_to_user(*, task, complete_url: str, subject_prefix: str = "Checklist") -> None:
    try:
        recipient = _user_email(task)
        if not recipient:
            logger.info(_safe_console_text(f"[MAIL] No recipient for checklist id={getattr(task, 'id', '?')}"))
            return
        title = getattr(task, "task_name", "") or getattr(task, "title", "") or "Checklist"
        subject = f"{subject_prefix}: {title}"
        ctx = {
            "task": task,
            "title": title,
            "assignee": getattr(task, "assign_to", None),
            "complete_url": complete_url or build_absolute_url("/"),
            "site_url": SITE_URL,
        }
        send_html_email(
            subject=subject,
            template_name="email/checklist_assignment.html",
            context=ctx,
            to=[recipient],
            body_fallback=f"{title}\n\nComplete: {complete_url or build_absolute_url('/')}",
            fail_silently=True,
        )
    except Exception as e:
        logger.error(_safe_console_text(f"[MAIL] Checklist assignment failed: {e}"))

def send_delegation_assignment_to_user(*, delegation, complete_url: str, subject_prefix: str = "Delegation") -> None:
    try:
        recipient = _user_email(delegation)
        if not recipient:
            logger.info(_safe_console_text(f"[MAIL] No recipient for delegation id={getattr(delegation, 'id', '?')}"))
            return
        title = getattr(delegation, "task_name", "") or getattr(delegation, "title", "") or "Delegation"
        subject = f"{subject_prefix}: {title}"
        ctx = {
            "delegation": delegation,
            "title": title,
            "assignee": getattr(delegation, "assign_to", None),
            "complete_url": complete_url or build_absolute_url("/"),
            "site_url": SITE_URL,
        }
        send_html_email(
            subject=subject,
            template_name="email/delegation_assignment.html",
            context=ctx,
            to=[recipient],
            body_fallback=f"{title}\n\nComplete: {complete_url or build_absolute_url('/')}",
            fail_silently=True,
        )
    except Exception as e:
        logger.error(_safe_console_text(f"[MAIL] Delegation assignment failed: {e}"))

def send_help_ticket_assignment_to_user(
    *, ticket, detail_url: Optional[str] = None, subject_prefix: str = "Help Ticket Assigned"
) -> None:
    try:
        recipient = _user_email(ticket)
        if not recipient:
            logger.info(_safe_console_text(f"[MAIL] No recipient for help ticket id={getattr(ticket, 'id', '?')}"))
            return
        title = getattr(ticket, "title", "") or "Help Ticket"
        subject = f"{subject_prefix}: {title}"
        desc = (getattr(ticket, "description", "") or "").strip()
        ctx = {
            "ticket": ticket,
            "title": title,
            "assignee": getattr(ticket, "assign_to", None),
            "detail_url": detail_url or build_absolute_url("/"),
            "site_url": SITE_URL,
            "description": desc,
        }
        send_html_email(
            subject=subject,
            template_name="email/help_ticket_assignment.html",
            context=ctx,
            to=[recipient],
            body_fallback=f"{title}\n\n{desc}\n\nOpen: {detail_url or build_absolute_url('/')}",
            fail_silently=True,
        )
    except Exception as e:
        logger.error(_safe_console_text(f"[MAIL] Help ticket assignment failed: {e}"))

# =============================================================================
# Dynamic fallbacks for any other send_* helpers
# =============================================================================
# Cache dynamically created stubs so repeated imports/calls are fast.
__DYNAMIC_STUBS__: dict[str, Callable[..., Any]] = {}

def __getattr__(name: str) -> Any:
    """
    If code imports any unknown helper like:
        from apps.tasks.utils import send_help_ticket_unassigned_notice
    we return a no-op stub that logs once per call, rather than crashing the app.
    Only names that start with 'send_' are stubbed; others raise AttributeError.
    """
    if not name.startswith("send_"):
        # Keep AttributeError for non-mail helpers so real bugs are visible.
        raise AttributeError(name)

    if name in __DYNAMIC_STUBS__:
        return __DYNAMIC_STUBS__[name]

    def _stub(*args, **kwargs):
        logger.warning(_safe_console_text(f"[MAIL-STUB] Called missing helper '{name}'. No email sent."))
        # Intentionally no return value.

    __DYNAMIC_STUBS__[name] = _stub
    return _stub

# =============================================================================
# Public exports
# =============================================================================
__all__ = [
    "SITE_URL",
    "build_absolute_url",
    "_safe_console_text",
    "send_html_email",
    "_dedupe_emails",
    "get_admin_emails",
    "_fmt_dt_date",
    # concrete helpers
    "send_checklist_assignment_to_user",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
]
