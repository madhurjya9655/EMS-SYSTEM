from django.urls import path
# ✅ Use the new dashboard view living in the dashboard app
from dashboard import views as dashboard_views

app_name = "dashboard"

urlpatterns = [
    # /dashboard/ → final rules (10:00 IST gate for today, show past pending, hide future, never show completed)
    path("", dashboard_views.dashboard_home, name="home"),
]
