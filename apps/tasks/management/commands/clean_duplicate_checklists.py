from django.core.management.base import BaseCommand
from django.utils.timezone import localtime
from apps.tasks.models import Checklist
from collections import defaultdict

class Command(BaseCommand):
    help = "Removes duplicate Checklist tasks for the same user, task, and date (keeps earliest)."

    def handle(self, *args, **options):
        # Group all checklists by (task_name, assign_to_id, planned_date.date())
        group_map = defaultdict(list)
        for cl in Checklist.objects.all():
            key = (cl.task_name, cl.assign_to_id, localtime(cl.planned_date).date())
            group_map[key].append(cl)
        
        total_deleted = 0
        for key, checklists in group_map.items():
            if len(checklists) > 1:
                # Sort: earliest planned_date, then id
                checklists.sort(key=lambda x: (localtime(x.planned_date), x.id))
                # Keep the first, delete the rest
                to_delete = checklists[1:]
                ids = [c.id for c in to_delete]
                Checklist.objects.filter(id__in=ids).delete()
                total_deleted += len(ids)
                print(f"Deleted {len(ids)} duplicate(s) for {key}")
        if total_deleted:
            self.stdout.write(self.style.SUCCESS(
                f"Deleted {total_deleted} duplicate checklist(s)."
            ))
        else:
            self.stdout.write(self.style.SUCCESS("No duplicate checklists found."))
