from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from django.contrib.auth import get_user_model
from django.conf import settings
from twilio.rest import Client
from apps.tasks.models import Checklist, Delegation, HelpTicket
from apps.recruitment.models import Employee

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('period', choices=['weekly','monthly'])
    def handle(self, *args, **opts):
        period = opts['period']
        today = timezone.now().date()
        if period=='weekly':
            end = today - timedelta(days=today.weekday()+1)
            start = end - timedelta(days=6)
        else:
            first_last = (today.replace(day=1)-timedelta(days=1)).replace(day=1)
            start = first_last
            end = today.replace(day=1)-timedelta(days=1)
        client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
        for user in get_user_model().objects.filter(is_active=True):
            emp = Employee.objects.filter(email=user.email).first()
            if not emp: continue
            phone, dept = emp.phone, emp.department
            chk = Checklist.objects.filter(assign_to=user, planned_date__date__range=(start,end)).count()
            dlg = Delegation.objects.filter(assign_to=user, planned_date__range=(start,end)).count()
            help_done = HelpTicket.objects.filter(assign_to=user, planned_date__date__range=(start,end), status='Closed').count()
            total = chk+dlg
            score = round(((total+help_done)/total*100)-100,2) if total>0 else 0
            body = f"{user.get_full_name()} ({phone}) (Department - {dept}) - {start.strftime('%d %b %Y')} to {end.strftime('%d %b %Y')} - Score: {score}%"
            client.messages.create(body=body, from_=f"whatsapp:{settings.TWILIO_WHATSAPP_FROM}", to=f"whatsapp:{phone}")
