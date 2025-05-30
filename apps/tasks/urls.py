from django.urls import path
from . import views

app_name = 'tasks'   # <— add this!

urlpatterns = [
    path('checklist/'      , views.list_checklist , name='list_checklist'),
    path('checklist/new/'  , views.add_checklist  , name='add_checklist'),
    path('delegation/'     , views.list_delegation, name='list_delegation'),
    path('delegation/new/' , views.add_delegation , name='add_delegation'),
    path('bulk-upload/'    , views.bulk_upload    , name='bulk_upload'),
]
