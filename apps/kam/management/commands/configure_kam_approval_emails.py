from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from apps.kam.models import KAMEmailApprovalSettings


class Command(BaseCommand):
    help = "Configure initial KAM approval email recipients (Sangam and Vilas)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--approver",
            action="append",
            dest="approvers",
            default=[],
            help="Approval user email. May be supplied more than once.",
        )
        parser.add_argument(
            "--cc",
            action="append",
            dest="cc_users",
            default=[],
            help="CC user email. May be supplied more than once.",
        )
        parser.add_argument(
            "--exclude-mapped-manager",
            action="store_true",
            help="Do not automatically include the employee's mapped manager.",
        )

    def handle(self, *args, **options):
        User = get_user_model()
        approver_emails = options["approvers"] or [
            "sangam@blueoceansteels.com",
            "vilas@blueoceansteels.com",
        ]
        cc_emails = options["cc_users"] or []

        approvers = list(User.objects.filter(email__in=approver_emails, is_active=True))
        found_approver_emails = {u.email.lower() for u in approvers if u.email}
        missing = [e for e in approver_emails if e.lower() not in found_approver_emails]
        if missing:
            raise CommandError("Active users not found: " + ", ".join(missing))

        cc_users = list(User.objects.filter(email__in=cc_emails, is_active=True))
        found_cc_emails = {u.email.lower() for u in cc_users if u.email}
        missing_cc = [e for e in cc_emails if e.lower() not in found_cc_emails]
        if missing_cc:
            raise CommandError("Active CC users not found: " + ", ".join(missing_cc))

        config = KAMEmailApprovalSettings.get_solo()
        config.is_active = True
        config.include_mapped_manager = not options["exclude_mapped_manager"]
        config.save(update_fields=["is_active", "include_mapped_manager", "updated_at"])
        config.approval_users.set(approvers)
        config.cc_users.set(cc_users)

        self.stdout.write(self.style.SUCCESS(
            "KAM approval email settings updated. "
            f"Approvers={[u.email for u in approvers]} "
            f"CC={[u.email for u in cc_users]} "
            f"include_mapped_manager={config.include_mapped_manager}"
        ))
