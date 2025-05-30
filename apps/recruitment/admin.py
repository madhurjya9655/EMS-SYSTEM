from django.contrib import admin
from .models import Employee, Candidate, InterviewSchedule, InterviewFeedback

admin.site.register(Employee)
admin.site.register(Candidate)
admin.site.register(InterviewSchedule)
admin.site.register(InterviewFeedback)
