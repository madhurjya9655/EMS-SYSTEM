# apps/users/urls.py
from django.urls import path
from .views import list_users, add_user, delete_user

app_name = 'users'

urlpatterns = [
    path('',                list_users, name='user_list'),
    path('add/',            add_user,   name='user_add'),
    path('<int:pk>/delete/', delete_user,name='user_delete'),
]
