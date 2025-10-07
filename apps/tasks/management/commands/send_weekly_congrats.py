from django.core.management.base import BaseCommand
from apps.tasks.services.weekly_performance import send_weekly_congratulations_mails

class Command(BaseCommand):
    help = "Send 'Congratulations' emails to employees with >=90% completion last week (Mon..Sun)."

    def handle(self, *args, **options):
        summary = send_weekly_congratulations_mails()
        self.stdout.write(self.style.SUCCESS(f"Done. Sent={summary['sent']} Skipped={summary['skipped']} Users={summary['total_users']}"))
