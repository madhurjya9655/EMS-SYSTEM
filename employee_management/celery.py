# employee_management/celery.py
import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "employee_management.settings")

app = Celery("employee_management")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Beat schedule (aligned with your final rules)
app.conf.beat_schedule = {
    # Safety net: ensure next recurring checklist is created when eligible.
    # Runs hourly.
    "generate-recurring-checklists-hourly": {
        "task": "apps.tasks.tasks.generate_recurring_checklists",
        "schedule": crontab(minute=0),  # every hour at :00
    },

    # Send assignment emails at 10:00 IST for items due today.
    # Fire every 2 minutes between 10:00–10:10 IST to be resilient.
    "send-due-today-assignments-10am-burst": {
        "task": "apps.tasks.tasks.send_due_today_assignments",
        "schedule": crontab(minute="0-10/2", hour="10"),  # 10:00, 10:02, …, 10:10 IST
    },

    # Lightweight sanity check on recurring series (once daily).
    "audit-recurring-health-daily": {
        "task": "apps.tasks.tasks.audit_recurring_health",
        "schedule": crontab(minute=30, hour=3),  # 03:30 IST daily
    },
}

app.conf.timezone = "Asia/Kolkata"

@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
