from django.urls import path
from . import views

app_name = "leave"

urlpatterns = [
    path("apply/", views.apply_leave, name="apply_leave"),
    path("my/", views.my_leaves, name="my_leaves"),

    # Manager views
    path("manager/", views.manager_pending, name="manager_pending"),
    path("manager/decide/<int:pk>/approve", views.manager_decide_approve, name="manager_decide_approve"),
    path("manager/decide/<int:pk>/reject", views.manager_decide_reject, name="manager_decide_reject"),
]
