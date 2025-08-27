from django.urls import path
from apps.tasks import views as task_views

app_name = "dashboard"

urlpatterns = [
    # /dashboard/ → dashboard home
    path("", task_views.dashboard_home, name="home"),
]
