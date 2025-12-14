# employee_management/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "employee_management.settings")

app = Celery("employee_management")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Do NOT define app.conf.beat_schedule here.
# Schedules are controlled via Django settings + django-celery-beat.

app.conf.timezone = "Asia/Kolkata"

@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
