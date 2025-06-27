from django.urls import path
from .views import list_users, add_user, edit_user, delete_user, toggle_active

app_name = 'users'

urlpatterns = [
    path('',                list_users,      name='list_users'),
    path('add/',            add_user,        name='add_user'),
    path('<int:pk>/edit/',  edit_user,       name='edit_user'),
    path('<int:pk>/delete/', delete_user,     name='delete_user'),
    path('<int:pk>/toggle-active/', toggle_active, name='toggle_active'),
]
