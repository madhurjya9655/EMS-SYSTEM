from django.urls import path
from . import views

app_name = 'leave'

urlpatterns = [
    path('apply/',   views.apply_leave,    name='apply_leave'),
    path('my/',      views.my_leaves,      name='my_leaves'),
    path('pending/', views.pending_leaves, name='pending_leaves'),
    path('hr/',      views.hr_leaves,      name='hr_leaves'),
]
