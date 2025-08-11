from datetime import date, timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.conf import settings

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.recruitment.models import Employee


def previous_week_window(today: date) -> tuple[date, date]:
    """
    Last full Monday–Sunday window before 'today'.
    If today is Monday, that's the week ending yesterday (Sunday).
    """
    # weekday(): Mon=0..Sun=6 → last Sunday is (weekday+1) days ago
    end = today - timedelta(days=today.weekday() + 1)
    start = end - timedelta(days=6)
    return start, end


def previous_month_window(today: date) -> tuple[date, date]:
    """Start/end of the previous calendar month."""
    first_this = today.replace(day=1)
    end = first_this - timedelta(days=1)              # last day of previous month
    start = end.replace(day=1)                        # first day of previous month
    return start, end


def normalize_whatsapp(phone: str) -> str | None:
    """
    Very light normalization:
    - if already +E.164, keep
    - if 10 digits (likely India), prefix +91
    - else return None (don’t attempt to send)
    """
    if not phone:
        return None
    p = "".join(ch for ch in phone.strip() if ch.isdigit() or ch == '+')
    if p.startswith('+') and len(p) >= 11:
        return f"whatsapp:{p}"
    digits = "".join(ch for ch in p if ch.isdigit())
    if len(digits) == 10:  # naïve India default
        return f"whatsapp:+91{digits}"
    return None


class Command(BaseCommand):
    help = "Send weekly/monthly WhatsApp score summaries to employees."

    def add_arguments(self, parser):
        parser.add_argument('period', choices=['weekly', 'monthly'])

    def handle(self, *args, **opts):
        period = opts['period']
        today = timezone.localdate()

        if period == 'weekly':
            start, end = previous_week_window(today)
        else:
            start, end = previous_month_window(today)

        # Twilio config checks
        sid = getattr(settings, 'TWILIO_ACCOUNT_SID', None)
        tok = getattr(settings, 'TWILIO_AUTH_TOKEN', None)
        w_from = getattr(settings, 'TWILIO_WHATSAPP_FROM', None)
        if not (sid and tok and w_from):
            self.stderr.write("Twilio settings missing; aborting send.")
            return

        client = Client(sid, tok)

        # Preload employees by email for quick lookup
        users = list(get_user_model().objects.filter(is_active=True).only('id', 'email', 'first_name', 'last_name'))
        emails = [u.email for u in users if u.email]
        employees = {
            e.email: e
            for e in Employee.objects.filter(email__in=emails).only('email', 'phone', 'department')
        }

        # Determine whether Delegation.planned_date is a Date or DateTime
        del_is_dt = Delegation._meta.get_field('planned_date').get_internal_type() == 'DateTimeField'
        del_date_lookup = 'planned_date__date__range' if del_is_dt else 'planned_date__range'

        sent = 0
        skipped = 0

        for user in users:
            emp = employees.get(user.email)
            if not emp:
                skipped += 1
                continue

            wa_to = normalize_whatsapp(emp.phone)
            if not wa_to:
                skipped += 1
                continue

            # Counts
            checklist_count = Checklist.objects.filter(
                assign_to=user,
                planned_date__date__range=(start, end)
            ).count()

            delegation_filter = {
                del_date_lookup: (start, end),
                'assign_to': user,
            }
            delegation_count = Delegation.objects.filter(**delegation_filter).count()

            help_closed = HelpTicket.objects.filter(
                assign_to=user,
                planned_date__date__range=(start, end),
                status='Closed'
            ).count()

            total = checklist_count + delegation_count
            score = round((help_closed / total) * 100, 2) if total > 0 else 0.0

            body = (
                f"{user.get_full_name() or user.username} "
                f"({emp.phone}) – Dept: {emp.department or '-'}\n"
                f"Period: {start:%d %b %Y} → {end:%d %b %Y}\n"
                f"Checklist: {checklist_count} | Delegation: {delegation_count} | Help closed: {help_closed}\n"
                f"Score: {score}%"
            )

            try:
                client.messages.create(
                    body=body,
                    from_=f"whatsapp:{w_from}" if not str(w_from).startswith("whatsapp:") else w_from,
                    to=wa_to
                )
                sent += 1
            except TwilioRestException as e:
                self.stderr.write(f"Twilio error for {user.email or user.username}: {e}")
                skipped += 1
            except Exception as e:
                self.stderr.write(f"Unexpected error for {user.email or user.username}: {e}")
                skipped += 1

        self.stdout.write(f"Done. Sent: {sent}, Skipped: {skipped}, Period: {start} → {end}")
