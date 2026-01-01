# File: apps/users/management/commands/send_new_year_2026_emails.py
from typing import List, Tuple, Dict
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import render_to_string, get_template
from django.template import TemplateDoesNotExist
from django.conf import settings
from django.utils import timezone as dj_tz

SUBJECT = "✨ Happy New Year 2026 – Wishing You Success & Growth Ahead!"
# IMPORTANT: matches your actual folder name: templates/email/new_year_2026.html
TEMPLATE = "email/new_year_2026.html"


def _build_message(subject: str, to_email: str, context: Dict, connection) -> EmailMultiAlternatives:
    """Build a single personalized HTML email with a minimal text fallback."""
    html_body = render_to_string(TEMPLATE, context)
    text_body = (
        f"Happy New Year 2026!\n\n"
        f"Dear {context.get('first_name') or 'there'},\n"
        "Thank you for being part of our journey. Wishing you growth and success in 2026!\n\n"
        "Warm regards,\n"
        "BOS Lakshya"
    )
    msg = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        to=[to_email],
        connection=connection,
    )
    msg.attach_alternative(html_body, "text/html")
    return msg


class Command(BaseCommand):
    help = (
        "Send the New Year 2026 greetings email.\n"
        "Default: all active users. Use --only for targeted test sends. --dry-run to preview."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--only",
            type=str,
            default=None,
            help="Comma-separated recipient list for testing. Example: --only mbora209@gmail.com",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit number of users in bulk mode (smoke test).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Render and log without sending any email.",
        )

    # ---------- template preflight ----------
    def _preflight_template(self) -> bool:
        """Fail fast with helpful info if the template cannot be found."""
        try:
            get_template(TEMPLATE)
            render_to_string(TEMPLATE, {"first_name": "Friend", "full_name": "Friend", "year": 2026})
            return True
        except TemplateDoesNotExist:
            dirs = []
            for conf in settings.TEMPLATES:
                for d in conf.get("DIRS", []):
                    dirs.append(str(d))
            self.stdout.write(self.style.ERROR(f"Template not found: {TEMPLATE}"))
            if dirs:
                self.stdout.write(self.style.NOTICE("Searched template DIRS:"))
                for d in dirs:
                    self.stdout.write(f" - {d}")
            self.stdout.write(self.style.NOTICE("Fix: ensure file exists at 'templates/email/new_year_2026.html'."))
            return False

    # ---------- main ----------
    def handle(self, *args, **options):
        # Ensure the template is available before doing anything else
        if not self._preflight_template():
            self.stdout.write(self.style.ERROR("Aborting: template missing."))
            return

        only_arg = options.get("only")
        dry_run = options.get("dry_run", False)
        limit = options.get("limit")

        targets: List[Dict] = []
        mode = "users"

        if only_arg:
            mode = "only"
            raw_emails = [e.strip() for e in only_arg.split(",") if e.strip()]
            seen = set()
            for email in raw_emails:
                lower = email.lower()
                if lower in seen:
                    continue
                seen.add(lower)
                local = email.split("@")[0]
                first_name = local.replace(".", " ").replace("_", " ").title()
                targets.append({"email": email, "first_name": first_name, "full_name": first_name})
        else:
            User = get_user_model()
            qs = User.objects.filter(is_active=True).exclude(email__isnull=True).exclude(email__exact="")
            if limit:
                qs = qs[:limit]
            rows = list(qs.values("email", "first_name", "last_name"))
            seen = set()
            for r in rows:
                email = (r["email"] or "").strip()
                if not email:
                    continue
                lower = email.lower()
                if lower in seen:
                    continue
                seen.add(lower)

                first = (r.get("first_name") or "").strip()
                last = (r.get("last_name") or "").strip()
                if not (first or last):
                    local = email.split("@")[0]
                    first = local.replace(".", " ").replace("_", " ").title()
                targets.append(
                    {"email": email, "first_name": first, "full_name": f"{first} {last}".strip()}
                )

        total_targets = len(targets)
        self.stdout.write(self.style.NOTICE(f"[{dj_tz.now()}] Mode: {mode}. Targets: {total_targets}"))
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN: Emails will NOT be sent."))

        success_count = 0
        failures: List[Tuple[str, str]] = []

        connection = None
        try:
            if not dry_run:
                connection = get_connection()
                connection.open()

            for t in targets:
                email = t["email"]
                ctx = {"first_name": t.get("first_name") or "", "full_name": t.get("full_name") or "", "year": 2026}
                try:
                    if dry_run:
                        preview = render_to_string(TEMPLATE, ctx)
                        self.stdout.write(self.style.HTTP_INFO(f"[DRY RUN] Would send to: {email} | HTML size: {len(preview)} bytes"))
                        success_count += 1
                    else:
                        msg = _build_message(SUBJECT, email, ctx, connection)
                        msg.send()
                        success_count += 1
                        self.stdout.write(self.style.SUCCESS(f"Sent ✔ {email}"))
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                    failures.append((email, err))
                    self.stdout.write(self.style.ERROR(f"Failed ✖ {email} — {err}"))
        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE("---- SUMMARY ----"))
        self.stdout.write(self.style.NOTICE(f"Total targets: {total_targets}"))
        self.stdout.write(self.style.SUCCESS(f"Sent successfully: {success_count}"))
        if failures:
            self.stdout.write(self.style.ERROR(f"Failures: {len(failures)}"))
            for email, err in failures[:50]:
                self.stdout.write(self.style.ERROR(f" - {email}: {err}"))
        else:
            self.stdout.write(self.style.SUCCESS("No failures recorded."))
        self.stdout.write(self.style.NOTICE(f"Completed at: {dj_tz.now()}"))
