# E:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\tasks\management\commands\install_bulk_templates.py

import os
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = 'Install bulk upload template CSV files to static directory'

    def handle(self, *args, **options):
        # Create static directory if it doesn't exist
        static_dir = os.path.join(settings.BASE_DIR, 'static', 'bulk_upload_templates')
        os.makedirs(static_dir, exist_ok=True)
        
        # Checklist template content
        checklist_template = """Task Name,Message,Assign To,Planned Date,Priority,Mode,Frequency,Time per Task (minutes),Reminder Before Days,Assign PC,Group Name,Notify To,Auditor,Set Reminder,Reminder Mode,Reminder Frequency,Reminder Starting Time,Checklist Auto Close,Checklist Auto Close Days
Weekly Report Submission,Submit weekly progress report,john_doe,2025-08-18 15:30,Medium,Weekly,1,30,1,jane_manager,Operations,admin_user,audit_user,Yes,Daily,1,09:00,No,0
Monthly Budget Review,Review department budget,jane_manager,2025-08-25 10:00,High,Monthly,1,60,2,,,admin_user,,No,,,,,
Daily Standup Meeting,Attend daily team standup,team_lead,2025-08-17 09:15,Low,Daily,1,15,0,,,,,No,,,,,"""

        # Delegation template content
        delegation_template = """Task Name,Assign To,Planned Date,Priority,Mode,Frequency,Time per Task (minutes)
Prepare Client Presentation,john_doe,2025-08-18 14:00,High,,,45
Update Website Content,web_dev,2025-08-19 10:30,Medium,Weekly,2,120
Process Invoice Payments,finance_team,2025-08-17 16:00,Low,Daily,1,20"""

        # Write checklist template
        checklist_path = os.path.join(static_dir, 'checklist_template.csv')
        with open(checklist_path, 'w', encoding='utf-8') as f:
            f.write(checklist_template)
        
        # Write delegation template
        delegation_path = os.path.join(static_dir, 'delegation_template.csv')
        with open(delegation_path, 'w', encoding='utf-8') as f:
            f.write(delegation_template)
        
        self.stdout.write(
            self.style.SUCCESS(f'Successfully installed template files:')
        )
        self.stdout.write(f'  - {checklist_path}')
        self.stdout.write(f'  - {delegation_path}')
        
        self.stdout.write('\nNote: Replace example usernames (john_doe, jane_manager, etc.) with actual usernames from your system.')
        self.stdout.write('\nDate format examples:')
        self.stdout.write('  - 2025-08-18 15:30 (24-hour format)')
        self.stdout.write('  - 8/18/2025 15:30 (US format)')
        self.stdout.write('  - For date-only, time will default to 10:00 AM')