import os
from celery import Celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "employee_management.settings")
app = Celery("employee_management")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
app.conf.beat_schedule = {
    "generate-recurring-tasks": {"task": "apps.tasks.tasks.generate_recurring_tasks_celery","schedule": 60.0,},
    "send-delegation-reminders": {"task": "apps.leave.tasks.send_delegation_reminders","schedule": 3600.0,},
    "cleanup-expired-handovers": {"task": "apps.leave.tasks.cleanup_expired_handovers","schedule": 86400.0,},
}
app.conf.timezone = "Asia/Kolkata"
@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")