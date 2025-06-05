from django.urls import path
from . import views

app_name = 'reports'

urlpatterns = [
    path('doer-tasks/',         views.list_doer_tasks,        name='doer_tasks'),
    path('weekly-mis-score/',   views.weekly_mis_score,       name='weekly_mis_score'),
    path('performance-score/',  views.performance_score,      name='performance_score'),
    path('auditor-report/',     views.auditor_report,         name='auditor_report'),
]
