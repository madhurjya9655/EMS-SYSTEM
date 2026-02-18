from __future__ import annotations

from typing import Iterable, List, Sequence, Optional, Dict, Any, Tuple
from datetime import datetime, time as _time, date as _date
import logging
import os

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives, send_mail
from django.template.loader import render_to_string
from django.utils import timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

User = get_user_model()
logger = logging.getLogger(__name__)

SITE_URL = getattr(settings, "SITE_URL", "https://ems-system-d26q.onrender.com")
IST = ZoneInfo(getattr(settings, "TIME_ZONE", "Asia/Kolkata")) if ZoneInfo else timezone.get_fixed_timezone(330)
DEFAULT_ASSIGN_T = _time(10, 0)

# -------------------------------------------------------------------
# EMAIL RESTRICTIONS (config-driven; settings.EMAIL_RESTRICTIONS)
# -------------------------------------------------------------------
def _email_restrictions() -> Dict[str, Dict[str, Any]]:
    """
    Expected shape (from settings.py):
      EMAIL_RESTRICTIONS = {
        "pankaj": {
           "emails": [...],
           "usernames": [...],
           "allow": ["delegation.assigned_by", "delegation.pending_digest"]
        }
      }
    """
    try:
        v = getattr(settings, "EMAIL_RESTRICTIONS", None)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}

def _restriction_profile_for_email(addr: str) -> Optional[Dict[str, Any]]:
    addr_lc = (addr or "").strip().lower()
    if not addr_lc:
        return None
    for _, prof in _email_restrictions().items():
        try:
            emails = prof.get("emails", []) if isinstance(prof, dict) else []
            emails_lc = {(e or "").strip().lower() for e in (emails or []) if e}
            if addr_lc in emails_lc:
                return prof
        except Exception:
            continue
    return None

def _infer_email_category(subject: str, template_name: str, context: Dict[str, Any]) -> str:
    """
    Best-effort categorization so allow-lists can work without changing callers.
    Callers can override by passing context["email_category"] explicitly.
    """
    try:
        cat = (context or {}).get("email_category")
        if isinstance(cat, str) and cat.strip():
            return cat.strip()
    except Exception:
        pass

    subj = (subject or "").lower()
    tmpl = (template_name or "").lower()
    kind = str((context or {}).get("kind") or "").strip().lower()

    # Delegation assignment-style emails
    if kind == "delegation":
        return "delegation.assigned_by"

    # Pending digest / summary
    if "pending" in subj or "pending" in tmpl:
        # if the subject/title mentions delegation explicitly, treat as delegation digest
        title = str((context or {}).get("title") or "")
        if "delegation" in subj or "delegation" in title.lower():
            return "delegation.pending_digest"
        return "pending.digest"

    return "general"

def _is_allowed_by_profile(profile: Dict[str, Any], category: str) -> bool:
    try:
        allow = profile.get("allow", [])
        allow_set = {str(x).strip() for x in (allow or []) if str(x).strip()}
        return category in allow_set
    except Exception:
        return False

# -------------------------------------------------------------------
# Legacy "Pankaj D1–D4" compatibility (kept, but now config-driven first)
# -------------------------------------------------------------------
_PANKAJ_EMAIL_LEGACY = (
    getattr(settings, "PANKAJ_EMAIL", None)
    or os.getenv("PANKAJ_EMAIL", "")
    or "pankaj@blueoceansteels.com"
).strip().lower()

_AMREEN_EMAIL = (
    getattr(settings, "AMREEN_EMAIL", None)
    or os.getenv("AMREEN_EMAIL", "")
    or getattr(settings, "REIMBURSEMENT_SENDER_EMAIL", "")
).strip().lower()

def _ist_today() -> _date:
    now = timezone.now()
    try:
        if ZoneInfo:
            return now.astimezone(IST).date()  # type: ignore[arg-type]
    except Exception:
        pass
    return timezone.localdate()

def _is_restricted_primary(addr: str | None) -> bool:
    """
    True if addr matches any restricted profile OR legacy Pankaj email.
    """
    if not addr:
        return False
    a = addr.strip().lower()
    if _restriction_profile_for_email(a) is not None:
        return True
    return a == _PANKAJ_EMAIL_LEGACY

def _legacy_pankaj_allowed_context(ctx: Dict[str, Any] | None) -> bool:
    """
    Legacy allow-list:
      • Only for Delegation
      • Status == 'Pending'
      • planned_date < today (IST)
      • Assigned by Pankaj (self-assign)
      • Amreen is CC'd (enforced when building recipients)
    """
    if not isinstance(ctx, dict):
        return False
    try:
        if (ctx.get("kind") or "").strip() != "Delegation":
            return False

        status = (ctx.get("raw_status") or "").strip()
        assigned_by_email = (ctx.get("raw_assigned_by_email") or "").strip().lower()
        planned_dt = ctx.get("raw_planned_date")

        if status != "Pending":
            return False

        if not planned_dt:
            return False
        if isinstance(planned_dt, datetime):
            tz = timezone.get_current_timezone()
            aware = planned_dt if timezone.is_aware(planned_dt) else timezone.make_aware(planned_dt, tz)
            try:
                planned_date_ist = timezone.localtime(aware, IST).date() if ZoneInfo else timezone.localtime(aware).date()
            except Exception:
                planned_date_ist = aware.date()
        else:
            planned_date_ist = planned_dt

        if not (planned_date_ist < _ist_today()):
            return False

        # legacy: must be assigned_by Pankaj
        if assigned_by_email != _PANKAJ_EMAIL_LEGACY:
            return False

        return True
    except Exception:
        return False

# -------------------------------------------------------------------
# Generic helpers
# -------------------------------------------------------------------
def _safe_console_text(s: object) -> str:
    try:
        text = "" if s is None else str(s)
    except Exception:
        text = repr(s)
    try:
        return text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    except Exception:
        return text

def _dedupe_emails(emails: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for e in emails or []:
        s = (e or "").strip()
        if s and "@" in s:
            low = s.lower()
            if low not in seen:
                seen.add(low)
                out.append(s)
    return out

def _without_emails(emails: Sequence[str], exclude: Sequence[str] | None) -> List[str]:
    if not emails:
        return []
    excl = {e.strip().lower() for e in (exclude or []) if e}
    return [e for e in emails if e and e.strip().lower() not in excl]

def get_admin_emails(exclude: Sequence[str] | None = None) -> List[str]:
    try:
        qs = User.objects.filter(is_active=True).exclude(email__isnull=True).exclude(email__exact="")
        admins = list(qs.filter(is_superuser=True).values_list("email", flat=True))
        groups = list(
            qs.filter(groups__name__in=["Admin", "Manager", "EA", "CEO"])
            .values_list("email", flat=True)
            .distinct()
        )
        all_emails = _dedupe_emails(admins + groups)
        if exclude:
            all_emails = _without_emails(all_emails, exclude)
        return all_emails
    except Exception as e:
        logger.error("get_admin_emails failed: %s", e)
        return []

def _display_name(user) -> str:
    if not user:
        return "System"
    try:
        full = getattr(user, "get_full_name", lambda: "")() or ""
        if full.strip():
            return full.strip()
        uname = getattr(user, "username", "") or ""
        return uname if uname else "System"
    except Exception:
        return "System"

def _fmt_value(v: Any) -> Any:
    if isinstance(v, datetime):
        tz = timezone.get_current_timezone()
        aware = v if timezone.is_aware(v) else timezone.make_aware(v, tz)
        return timezone.localtime(aware, tz).strftime("%Y-%m-%d %H:%M")
    if hasattr(v, "get_full_name") or hasattr(v, "username"):
        try:
            name = getattr(v, "get_full_name", lambda: "")() or getattr(v, "username", "")
            return name
        except Exception:
            return str(v)
    return v

def _fmt_items(items: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    return [{"label": str(r.get("label", "")), "value": _fmt_value(r.get("value"))} for r in (items or [])]

def _fmt_rows(rows: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        new_row: Dict[str, Any] = {}
        for k, v in r.items():
            new_row[str(k)] = _fmt_value(v)
        out.append(new_row)
    return out

def _fmt_dt_date(dt: Any) -> str:
    if not dt:
        return ""
    try:
        tz = IST or timezone.get_current_timezone()
        aware = dt if timezone.is_aware(dt) else timezone.make_aware(dt, tz)
        ist = timezone.localtime(aware, tz)
        base = ist.strftime("%Y-%m-%d")
        t = ist.timetz().replace(tzinfo=None)
        if t not in (DEFAULT_ASSIGN_T, _time(0, 0)):
            return f"{base} {ist.strftime('%H:%M')}"
        return base
    except Exception as e:
        logger.error("Failed to format datetime %r: %s", dt, e)
        return str(dt)

def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", None) or getattr(settings, "EMAIL_HOST_USER", None) or "EMS <no-reply@example.com>"

def _fail_silently() -> bool:
    return bool(getattr(settings, "EMAIL_FAIL_SILENTLY", False) or getattr(settings, "DEBUG", False))

def _is_tombstoned(obj) -> bool:
    try:
        return bool(getattr(obj, "is_skipped_due_to_leave", False))
    except Exception:
        return False

def _is_self_assigned(obj) -> bool:
    try:
        return bool(getattr(obj, "assign_by_id", None) and getattr(obj, "assign_to_id", None) and obj.assign_by_id == obj.assign_to_id)
    except Exception:
        return False

# -------------------------------------------------------------------
# Core sending helpers
# -------------------------------------------------------------------
def _render_or_fallback(template_name: str, context: Dict[str, Any], fallback: str) -> str:
    try:
        return render_to_string(template_name, context)
    except Exception as e:
        logger.warning("Template %s not found or failed to render (%s). Using fallback.", template_name, e)
        return fallback

def _apply_restrictions_to_primary(to_email: str, subject: str, template_name: str, context: Dict[str, Any], cc_list: List[str]) -> Tuple[Optional[str], List[str]]:
    """
    Config-driven restriction gate:
      - If to_email matches a restricted profile:
          allow ONLY if category is allow-listed OR legacy strict delegation rule matches.
      - If allowed and legacy requires Amreen CC, enforce it (legacy behavior preserved).
    """
    to_email = (to_email or "").strip()
    if not to_email:
        return None, cc_list

    prof = _restriction_profile_for_email(to_email)
    category = _infer_email_category(subject, template_name, context)

    if prof is None:
        # Legacy single-email restriction still supported
        if to_email.strip().lower() != _PANKAJ_EMAIL_LEGACY:
            return to_email, cc_list
        # Legacy: allow only strict delegation overdue case
        if _legacy_pankaj_allowed_context(context):
            cc_lc = [e.lower() for e in cc_list]
            if _AMREEN_EMAIL and _AMREEN_EMAIL not in cc_lc:
                cc_list.append(_AMREEN_EMAIL)
            return to_email, cc_list
        logger.info("Suppressed email to legacy restricted recipient per rules (category=%s)", category)
        return None, cc_list

    # Config profile exists
    if _is_allowed_by_profile(prof, category):
        return to_email, cc_list

    # Preserve the legacy strict allow-rule if it matches (so you don't break existing ops behavior)
    if _legacy_pankaj_allowed_context(context):
        cc_lc = [e.lower() for e in cc_list]
        if _AMREEN_EMAIL and _AMREEN_EMAIL not in cc_lc:
            cc_list.append(_AMREEN_EMAIL)
        return to_email, cc_list

    logger.info("Suppressed email to restricted recipient per EMAIL_RESTRICTIONS (category=%s)", category)
    return None, cc_list

def _filter_restricted_from_list(seq: Sequence[str], subject: str, template_name: str, context: Dict[str, Any]) -> List[str]:
    """
    Removes restricted recipients from any list unless allowed by EMAIL_RESTRICTIONS
    (or legacy strict delegation rule).
    """
    out: List[str] = []
    category = _infer_email_category(subject, template_name, context)

    for s in seq or []:
        addr = (s or "").strip()
        if not addr:
            continue

        prof = _restriction_profile_for_email(addr)
        if prof is None:
            # legacy single-email restriction
            if addr.strip().lower() == _PANKAJ_EMAIL_LEGACY and not _legacy_pankaj_allowed_context(context):
                logger.info("Suppressed legacy restricted recipient from list (category=%s)", category)
                continue
            out.append(addr)
            continue

        # profile exists
        if _is_allowed_by_profile(prof, category) or _legacy_pankaj_allowed_context(context):
            out.append(addr)
        else:
            logger.info("Suppressed restricted recipient from list per EMAIL_RESTRICTIONS (category=%s)", category)

    return _dedupe_emails(out)

def _send_unified_assignment_email(
    *,
    subject: str,
    to_email: str,
    context: Dict[str, Any],
    cc: Optional[Sequence[str]] = None,
) -> None:
    to_email = (to_email or "").strip()
    if not to_email:
        return

    cc_list = _dedupe_emails(cc or [])

    # Apply restriction gate to primary recipient (uses EMAIL_RESTRICTIONS first)
    to_email, cc_list = _apply_restrictions_to_primary(to_email, subject, "", context, cc_list)
    if not to_email:
        return

    text_fallback = (
        f"Task Assignment: {context.get('task_title', 'New Task')}\n\n"
        f"Dear {context.get('assignee_name', 'Team Member')},\n\n"
        f"You have been assigned a new {context.get('kind', 'task')}.\n"
        f"Task ID: {context.get('task_code', 'N/A')}\n"
        f"Priority: {context.get('priority_display', 'Normal')}\n"
        f"Planned Date: {context.get('planned_date_display', 'Not specified')}\n"
        f"Assigned By: {context.get('assign_by_display', 'System')}\n\n"
        f"{context.get('cta_text', 'Please complete this task as soon as possible.')}\n"
        f"Open URL: {context.get('complete_url', 'N/A')}\n"
        f"\nRegards,\nEMS System"
    )

    html_fallback = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{subject}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    .card {{ border: 1px solid #ddd; padding: 16px; border-radius: 6px; }}
    .btn {{ display: inline-block; background: #0d6efd; color: #fff; padding: 10px 16px; text-decoration: none; border-radius: 4px; }}
    .muted {{ color: #666; }}
    table td {{ padding: 4px 8px; vertical-align: top; }}
  </style>
</head>
<body>
  <div class="card">
    <h2>{context.get('task_title', 'New Task')}</h2>
    <p>Dear {context.get('assignee_name', 'Team Member')},</p>
    <p>You have been assigned a new <strong>{context.get('kind', 'task')}</strong>.</p>
    <table>
      <tr><td><strong>Task ID:</strong></td><td>{context.get('task_code', 'N/A')}</td></tr>
      <tr><td><strong>Priority:</strong></td><td>{context.get('priority_display', 'Normal')}</td></tr>
      <tr><td><strong>Planned Date:</strong></td><td>{context.get('planned_date_display', 'Not specified')}</td></tr>
      <tr><td><strong>Assigned By:</strong></td><td>{context.get('assign_by_display', 'System')}</td></tr>
    </table>
    <p>{context.get('cta_text', 'Please complete this task as soon as possible.')}</p>
    <p><a href="{context.get('complete_url', '#')}" class="btn">Open Task</a></p>
    <p class="muted">EMS System</p>
  </div>
</body>
</html>
""".strip()

    try:
        text_body = _render_or_fallback("email/task_assigned.txt", context, text_fallback)
        html_body = _render_or_fallback("email/task_assigned.html", context, html_fallback)

        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=_from_email(),
            to=[to_email],
            cc=cc_list or None,
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=_fail_silently())
        logger.info("Sent assignment email to %s (CC=%s, %s)", to_email, ", ".join(cc_list) or "-", subject)
    except Exception as e:
        logger.error("Failed sending assignment email to %s: %s", to_email, e)

def send_html_email(
    *,
    subject: str,
    template_name: str,
    context: Dict[str, Any],
    to: Sequence[str],
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    fail_silently: bool = False,
) -> None:
    """
    Render and send an HTML email using a Django template, with safe fallbacks.
    Applies EMAIL_RESTRICTIONS (and legacy strict rule for compatibility).
    """
    to_list = _filter_restricted_from_list(list(to or []), subject, template_name, context)
    cc_list = _filter_restricted_from_list(list(cc or []), subject, template_name, context)
    bcc_list = _filter_restricted_from_list(list(bcc or []), subject, template_name, context)

    if not to_list and not cc_list and not bcc_list:
        logger.info("Suppressed email entirely due to recipient filtering (subject=%s)", subject)
        return

    effective_fail_silently = fail_silently or _fail_silently()

    try:
        ctx = dict(context or {})
        if isinstance(ctx.get("items"), (list, tuple)):
            ctx["items"] = _fmt_items(ctx["items"])
        if isinstance(ctx.get("items_table"), (list, tuple)):
            ctx["items_table"] = _fmt_rows(ctx["items_table"])

        html_message = _render_or_fallback(
            template_name,
            ctx,
            f"<html><body><h3>{ctx.get('title', subject)}</h3><p>Automated notification.</p></body></html>",
        )

        msg = EmailMultiAlternatives(
            subject=subject,
            body=html_message,
            from_email=_from_email(),
            to=to_list or None,
            cc=cc_list or None,
            bcc=bcc_list or None,
        )
        msg.attach_alternative(html_message, "text/html")
        msg.send(fail_silently=effective_fail_silently)

        logger.info(
            "Sent HTML email (to=%d, cc=%d, bcc=%d): %s",
            len(to_list or []),
            len(cc_list or []),
            len(bcc_list or []),
            subject,
        )
    except Exception as e:
        logger.error("send_html_email failed: %s", e)
        if not effective_fail_silently:
            raise

def _send_email(
    subject: str,
    template_name: str,
    context: Dict[str, Any],
    to: Sequence[str],
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    fail_silently: bool = False,
) -> None:
    send_html_email(
        subject=subject,
        template_name=template_name,
        context=context,
        to=to,
        cc=cc,
        bcc=bcc,
        fail_silently=fail_silently,
    )

def _build_subject(subject_prefix: str, task_title: str) -> str:
    sp = (subject_prefix or "").strip()
    if not sp:
        return task_title

    low = sp.lower()
    markers = ("reminder", "scheduled for", "due", "overdue")
    if any(m in low for m in markers):
        return sp
    if task_title and task_title.strip().lower() in low:
        return sp
    return f"{sp}: {task_title}"

def send_checklist_assignment_to_user(
    *, task, complete_url: str, subject_prefix: str = "Checklist Assigned"
) -> None:
    if _is_tombstoned(task):
        return
    if _is_self_assigned(task):
        return

    to_email = getattr(getattr(task, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(task, "task_name", "Checklist")
    subject = _build_subject(subject_prefix, task_title)

    ctx = {
        "kind": "Checklist",
        "task_title": task_title,
        "task_code": f"CL-{task.id}",
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(getattr(task, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the task and mark it complete when done.",
        "task_message": getattr(task, "message", "") or "",
        "instructions": getattr(task, "message", "") or "",
        "task_frequency": (
            f"{getattr(task, 'mode', '')} (Every {getattr(task, 'frequency', '')})"
            if getattr(task, "mode", None) and getattr(task, "frequency", None)
            else "One-time task"
        ),
        "task_group": getattr(task, "group_name", "") or "No group",
        "task_time_minutes": getattr(task, "time_per_task_minutes", 0) or 0,
        "attachment_required": getattr(task, "attachment_mandatory", False),
        "remind_before_days": getattr(task, "remind_before_days", 0) or 0,
        "site_url": SITE_URL,
        "is_recurring": bool(getattr(task, "mode", None) and getattr(task, "frequency", None)),
        "task_id": task.id,
    }

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
    )

def send_delegation_assignment_to_user(
    *,
    delegation,
    complete_url: str,
    subject_prefix: str = "Delegation Assigned",
    cc_users: Optional[Sequence[User]] = None,
    cc_emails: Optional[Sequence[str]] = None,
) -> None:
    if _is_tombstoned(delegation):
        return
    if _is_self_assigned(delegation):
        return

    to_email = getattr(getattr(delegation, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(delegation, "task_name", "Delegation")
    subject = _build_subject(subject_prefix, task_title)

    cc_pool: List[str] = []

    for u in cc_users or []:
        try:
            em = (getattr(u, "email", "") or "").strip()
            if em:
                cc_pool.append(em)
        except Exception:
            continue

    for raw in cc_emails or []:
        em = (raw or "").strip()
        if em:
            cc_pool.append(em)

    if hasattr(delegation, "cc_users"):
        try:
            for u in delegation.cc_users.all():
                em = (getattr(u, "email", "") or "").strip()
                if em:
                    cc_pool.append(em)
        except Exception:
            pass

    if hasattr(delegation, "cc_emails"):
        try:
            raw_str = getattr(delegation, "cc_emails", "") or ""
            for part in str(raw_str).split(","):
                em = part.strip()
                if em:
                    cc_pool.append(em)
        except Exception:
            pass

    assigner_email = ""
    try:
        assigner_email = (getattr(getattr(delegation, "assign_by", None), "email", "") or "").strip()
    except Exception:
        assigner_email = ""

    cc_final = _dedupe_emails(_without_emails(cc_pool, [assigner_email] if assigner_email else []))

    ctx = {
        "kind": "Delegation",
        "task_title": task_title,
        "task_code": f"DL-{delegation.id}",
        "planned_date_display": _fmt_dt_date(getattr(delegation, "planned_date", None)),
        "priority_display": getattr(delegation, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(delegation, "assign_by", None)),
        "assignee_name": _display_name(getattr(delegation, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the task and mark it complete when done.",
        "instructions": getattr(delegation, "message", "") or "",
        "task_frequency": (
            f"{getattr(delegation, 'mode', '')} (Every {getattr(delegation, 'frequency', '')})"
            if getattr(delegation, "mode", None) and getattr(delegation, "frequency", None)
            else "One-time task"
        ),
        "task_time_minutes": getattr(delegation, "time_per_task_minutes", 0) or 0,
        "attachment_required": getattr(delegation, "attachment_mandatory", False),
        "site_url": SITE_URL,
        "is_recurring": bool(getattr(delegation, "mode", None) and getattr(delegation, "frequency", None)),
        "task_id": delegation.id,
        "raw_planned_date": getattr(delegation, "planned_date", None),
        "raw_status": getattr(delegation, "status", None),
        "raw_assigned_by_email": assigner_email,
    }

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
        cc=cc_final,
    )

def send_help_ticket_assignment_to_user(
    *, ticket, complete_url: str, subject_prefix: str = "Help Ticket Assigned"
) -> None:
    if _is_tombstoned(ticket):
        return
    if _is_self_assigned(ticket):
        return

    to_email = getattr(getattr(ticket, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    task_title = getattr(ticket, "title", "Help Ticket")
    subject = _build_subject(subject_prefix, task_title)

    ctx = {
        "kind": "Help Ticket",
        "task_title": task_title,
        "task_code": f"HT-{ticket.id}",
        "planned_date_display": _fmt_dt_date(getattr(ticket, "planned_date", None)),
        "priority_display": getattr(ticket, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(ticket, "assign_by", None)),
        "assignee_name": _display_name(getattr(ticket, "assign_to", None)),
        "complete_url": complete_url,
        "cta_text": "Open the ticket to add notes or close it when resolved.",
        "task_message": getattr(ticket, "description", "") or "",
        "instructions": getattr(ticket, "description", "") or "",
        "estimated_minutes": getattr(ticket, "estimated_minutes", 0) or 0,
        "site_url": SITE_URL,
        "task_id": ticket.id,
    }

    _send_unified_assignment_email(
        subject=subject,
        to_email=to_email,
        context=ctx,
    )

def send_checklist_admin_confirmation(*, task, subject_prefix: str = "Checklist Assignment") -> None:
    exclude = []
    try:
        if getattr(task, "assign_by", None) and getattr(task.assign_by, "email", None):
            exclude = [task.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {task.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {task.task_name}",
            "items": _fmt_items(
                [
                    {"label": "Task Name", "value": task.task_name},
                    {"label": "Task ID", "value": f"CL-{task.id}"},
                    {"label": "Assignee", "value": task.assign_to},
                    {"label": "Assigned By", "value": task.assign_by},
                    {"label": "Planned Date", "value": task.planned_date},
                    {"label": "Priority", "value": task.priority},
                    {"label": "Group", "value": getattr(task, "group_name", "") or "No group"},
                    {"label": "Time Estimate", "value": f"{getattr(task, 'time_per_task_minutes', 0) or 0} minutes"},
                    {
                        "label": "Recurring",
                        "value": f"{task.mode} (Every {task.frequency})" if getattr(task, "mode", None) else "One-time",
                    },
                    {"label": "Message", "value": getattr(task, "message", "") or "No message"},
                ]
            ),
        },
        to=admins,
    )

def send_delegation_admin_confirmation(*, delegation, subject_prefix: str = "Delegation Assignment") -> None:
    exclude = []
    try:
        if getattr(delegation, "assign_by", None) and getattr(delegation.assign_by, "email", None):
            exclude = [delegation.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {delegation.task_name}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {delegation.task_name}",
            "items": _fmt_items(
                [
                    {"label": "Task Name", "value": delegation.task_name},
                    {"label": "Task ID", "value": f"DL-{delegation.id}"},
                    {"label": "Assignee", "value": delegation.assign_to},
                    {"label": "Assigned By", "value": delegation.assign_by},
                    {"label": "Planned Date", "value": delegation.planned_date},
                    {"label": "Priority", "value": delegation.priority},
                    {
                        "label": "Time Estimate",
                        "value": f"{getattr(delegation, 'time_per_task_minutes', 0) or 0} minutes",
                    },
                    {
                        "label": "Recurring",
                        "value": f"{delegation.mode} (Every {delegation.frequency})"
                        if getattr(delegation, "mode", None)
                        else "One-time",
                    },
                ]
            ),
        },
        to=admins,
    )

def send_help_ticket_admin_confirmation(*, ticket, subject_prefix: str = "Help Ticket Assignment") -> None:
    exclude = []
    try:
        if getattr(ticket, "assign_by", None) and getattr(ticket.assign_by, "email", None):
            exclude = [ticket.assign_by.email]
    except Exception:
        pass

    admins = get_admin_emails(exclude=exclude)
    if not admins:
        return

    send_html_email(
        subject=f"{subject_prefix}: {ticket.title}",
        template_name="email/admin_assignment_summary.html",
        context={
            "title": f"{subject_prefix} - {ticket.title}",
            "items": _fmt_items(
                [
                    {"label": "Ticket Title", "value": ticket.title},
                    {"label": "Ticket ID", "value": f"HT-{ticket.id}"},
                    {"label": "Assignee", "value": ticket.assign_to},
                    {"label": "Assigned By", "value": ticket.assign_by},
                    {"label": "Planned Date", "value": ticket.planned_date},
                    {"label": "Priority", "value": ticket.priority},
                    {
                        "label": "Estimated Time",
                        "value": f"{getattr(ticket, 'estimated_minutes', 0) or 0} minutes",
                    },
                    {"label": "Description", "value": getattr(ticket, "description", "") or "No description"},
                ]
            ),
        },
        to=admins,
    )

def send_checklist_unassigned_notice(*, task, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Checklist Unassigned: {task.task_name}",
        template_name="email/checklist_unassigned.html",
        context={
            "task": task,
            "old_user": old_user,
            "task_title": task.task_name,
            "task_id": f"CL-{task.id}",
            "new_assignee": _display_name(getattr(task, "assign_to", None)) if getattr(task, "assign_to", None) else "Unassigned",
        },
        to=[email],
    )

def send_delegation_unassigned_notice(*, delegation, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Delegation Unassigned: {delegation.task_name}",
        template_name="email/delegation_unassigned.html",
        context={
            "delegation": delegation,
            "old_user": old_user,
            "task_title": delegation.task_name,
            "task_id": f"DL-{delegation.id}",
            "new_assignee": _display_name(getattr(delegation, "assign_to", None)) if getattr(delegation, "assign_to", None) else "Unassigned",
        },
        to=[email],
    )

def send_help_ticket_unassigned_notice(*, ticket, old_user) -> None:
    email = getattr(old_user, "email", "") or ""
    if not email.strip():
        return
    send_html_email(
        subject=f"Help Ticket Unassigned: {ticket.title}",
        template_name="email/help_ticket_unassigned.html",
        context={
            "ticket": ticket,
            "old_user": old_user,
            "task_title": ticket.title,
            "task_id": f"HT-{ticket.id}",
            "new_assignee": _display_name(getattr(ticket, "assign_to", None)) if getattr(ticket, "assign_to", None) else "Unassigned",
        },
        to=[email],
    )

def send_task_reminder_email(*, task, task_type: str = "Checklist") -> None:
    if _is_tombstoned(task):
        return
    if _is_self_assigned(task):
        return

    to_email = getattr(getattr(task, "assign_to", None), "email", "") or ""
    if not to_email.strip():
        return

    pd_raw = getattr(task, "planned_date", None)
    if not pd_raw:
        return

    try:
        if isinstance(pd_raw, datetime):
            pd_date = timezone.localtime(pd_raw, IST or timezone.get_current_timezone()).date()
        else:
            pd_date = pd_raw
    except Exception:
        return

    today_ist = _ist_today()
    if pd_date > today_ist:
        return

    kind = (task_type or "").strip().lower()
    status_val = getattr(task, "status", None)
    if kind in {"checklist", "delegation"} and status_val and status_val != "Pending":
        return

    try:
        days_until = (pd_date - today_ist).days
    except Exception:
        days_until = None

    if days_until is None:
        urgency = "DUE"
    elif days_until < 0:
        urgency = "OVERDUE"
    elif days_until == 0:
        urgency = "DUE TODAY"
    elif days_until == 1:
        urgency = "DUE TOMORROW"
    else:
        urgency = f"DUE IN {days_until} DAYS"

    task_name = getattr(task, "task_name", None) or getattr(task, "title", "Task")
    task_code = f"{task_type[:2].upper()}-{task.id}"

    ctx = {
        "kind": task_type,
        "task_title": task_name,
        "task_code": task_code,
        "planned_date_display": _fmt_dt_date(getattr(task, "planned_date", None)),
        "priority_display": getattr(task, "priority", "") or "Low",
        "assign_by_display": _display_name(getattr(task, "assign_by", None)),
        "assignee_name": _display_name(getattr(task, "assign_to", None)),
        "urgency": urgency,
        "days_until": days_until,
        "site_url": SITE_URL,
        "task_id": task.id,
        "cta_text": "Please review and complete this item.",
        "complete_url": SITE_URL,
    }

    _send_unified_assignment_email(
        subject=f"Reminder: {urgency} - {task_name}",
        to_email=to_email,
        context=ctx,
    )

def send_admin_bulk_summary(*, title: str, rows: Sequence[dict], exclude_assigner_email: str | None = None) -> None:
    exclude = [exclude_assigner_email] if exclude_assigner_email else None
    admins = get_admin_emails(exclude=exclude)
    if not admins or not rows:
        return

    summary_stats = [
        {"label": "Total Items", "value": len(rows)},
        {"label": "Status", "value": "Completed"},
        {"label": "System", "value": "EMS Task Management"},
    ]

    send_html_email(
        subject=title,
        template_name="email/admin_assignment_summary.html",
        context={
            "title": title,
            "items": _fmt_items(summary_stats),
            "items_table": _fmt_rows(rows),
            "is_bulk_summary": True,
            "bulk_count": len(rows),
        },
        to=admins,
    )

def send_bulk_completion_summary(*, user, completed_tasks: List, date_range: str = "today") -> None:
    email = getattr(user, "email", "") or ""
    if not email.strip() or not completed_tasks:
        return

    total_tasks = len(completed_tasks)
    total_time = sum(getattr(t, "actual_duration_minutes", 0) or 0 for t in completed_tasks)

    task_groups: Dict[str, List[Any]] = {}
    for t in completed_tasks:
        task_groups.setdefault(t.__class__.__name__, []).append(t)

    send_html_email(
        subject=f"Task Completion Summary - {total_tasks} tasks {date_range}",
        template_name="email/completion_summary.html",
        context={
            "user": user,
            "total_tasks": total_tasks,
            "total_time": total_time,
            "total_time_display": f"{total_time // 60}h {total_time % 60}m" if total_time >= 60 else f"{total_time}m",
            "date_range": date_range,
            "task_groups": task_groups,
            "site_url": SITE_URL,
        },
        to=[email],
    )

def send_welcome_email(*, user: User, raw_password: str | None = None) -> None:
    to_email = (getattr(user, "email", "") or "").strip()
    if not to_email:
        return

    username = getattr(user, "username", "") or ""
    ctx = {
        "title": "Welcome to EMS",
        "full_name": _display_name(user),
        "username": username,
        "raw_password": raw_password or "",
        "login_url": SITE_URL,
    }

    fallback_html = f"""
    <html><body>
      <h3>Welcome to EMS</h3>
      <p>Hi {ctx['full_name']},</p>
      <p>Your account has been created.</p>
      <p><strong>Username:</strong> {username}</p>
      {"<p><strong>Password:</strong> " + ctx["raw_password"] + "</p>" if raw_password else ""}
      <p><a href="{SITE_URL}">Login here</a></p>
    </body></html>
    """.strip()

    html_body = _render_or_fallback("email/welcome_user.html", ctx, fallback_html)

    try:
        msg = EmailMultiAlternatives(
            subject="👋 Welcome to EMS",
            body=html_body,
            from_email=_from_email(),
            to=[to_email],
        )
        msg.attach_alternative(html_body, "text/html")
        msg.send(fail_silently=_fail_silently())
        logger.info("Welcome email sent to %s", to_email)
    except Exception as e:
        logger.error("Failed to send welcome email to %s: %s", to_email, e)

def test_email_configuration() -> bool:
    try:
        from_addr = _from_email()
        to_addr = from_addr
        send_mail(
            subject="EMS Email Configuration Test",
            message="This is a test email to verify email configuration.",
            from_email=from_addr,
            recipient_list=[to_addr],
            fail_silently=False,
        )
        logger.info("Email configuration test successful")
        return True
    except Exception as e:
        logger.error("Email configuration test failed: %s", e)
        return False

def get_email_statistics() -> Dict[str, Any]:
    return {
        "emails_sent_today": 0,
        "emails_failed_today": 0,
        "email_service_status": "active",
        "last_email_sent": timezone.now(),
    }

__all__ = [
    "send_html_email",
    "get_admin_emails",
    "test_email_configuration",
    "get_email_statistics",
    "send_checklist_assignment_to_user",
    "send_delegation_assignment_to_user",
    "send_help_ticket_assignment_to_user",
    "send_checklist_admin_confirmation",
    "send_delegation_admin_confirmation",
    "send_help_ticket_admin_confirmation",
    "send_checklist_unassigned_notice",
    "send_delegation_unassigned_notice",
    "send_help_ticket_unassigned_notice",
    "send_admin_bulk_summary",
    "send_bulk_completion_summary",
    "send_task_reminder_email",
    "send_welcome_email",
    "_dedupe_emails",
    "_fmt_value",
    "_fmt_items",
    "_fmt_rows",
    "_display_name",
    "_fmt_dt_date",
    "_render_or_fallback",
    "_send_unified_assignment_email",
    "_send_email",
    "_safe_console_text",
]
