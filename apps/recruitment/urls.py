from django.urls import path

from .views import (
    EmployeeListView,
    EmployeeCreateView,
    EmployeeDetailView,
    EmployeeUpdateView,
    EmployeeDeleteView,
    # new endpoints used by the list page
    employee_create,
    employee_delete,
)

app_name = "recruitment"

urlpatterns = [
    # Landing on recruitment app → Employees list
    path("", EmployeeListView.as_view(), name="home"),

    # Employees (grid)
    path("employees/", EmployeeListView.as_view(), name="employee_list"),

    # POST-only endpoints used by modal add & per-row delete in the grid
    path("employees/add/", employee_create, name="employee_add"),
    path("employees/<int:user_id>/delete/", employee_delete, name="employee_delete"),

    # Legacy CRUD (kept for compatibility with older pages; not used by the grid)
    path("employees/model/add/", EmployeeCreateView.as_view(), name="employee_add_model"),
    path("employees/model/<int:pk>/", EmployeeDetailView.as_view(), name="employee_detail"),
    path("employees/model/<int:pk>/edit/", EmployeeUpdateView.as_view(), name="employee_edit"),
    path("employees/model/<int:pk>/delete/", EmployeeDeleteView.as_view(), name="employee_delete_model"),
]
