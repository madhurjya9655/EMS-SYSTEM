from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from apps.leave.models import CCConfiguration

User = get_user_model()


class Command(BaseCommand):
    help = 'Setup initial CC Configuration for all active users'

    def add_arguments(self, parser):
        parser.add_argument(
            '--departments',
            nargs='*',
            default=['HR', 'Management', 'Admin'],
            help='Departments to create CC configurations for'
        )

    def handle(self, *args, **options):
        departments = options['departments']
        
        # Get all active users with emails
        users = User.objects.filter(is_active=True).exclude(
            email__isnull=True
        ).exclude(email__exact='')
        
        created_count = 0
        updated_count = 0
        
        for user in users:
            # Determine department based on user attributes
            department = 'Other'
            if user.is_superuser:
                department = 'Admin'
            elif user.is_staff:
                department = 'Management' 
            elif hasattr(user, 'profile') and user.profile:
                profile_dept = getattr(user.profile, 'department', '') or getattr(user.profile, 'role', '')
                if profile_dept and any(dept.lower() in profile_dept.lower() for dept in departments):
                    department = profile_dept
            
            # Create or update CC configuration
            config, created = CCConfiguration.objects.get_or_create(
                user=user,
                defaults={
                    'is_active': True,
                    'department': department,
                    'sort_order': 0 if user.is_superuser else (10 if user.is_staff else 20)
                }
            )
            
            if created:
                created_count += 1
                self.stdout.write(f"Created CC config for {user.username} ({department})")
            else:
                # Update existing if needed
                if not config.department:
                    config.department = department
                    config.save()
                    updated_count += 1
                    self.stdout.write(f"Updated CC config for {user.username}")
        
        self.stdout.write(
            self.style.SUCCESS(
                f'CC Configuration setup complete: {created_count} created, {updated_count} updated'
            )
        )