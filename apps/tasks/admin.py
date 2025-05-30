from django.contrib import admin
from .models import Checklist, Delegation, BulkUpload

admin.site.register(Checklist)
admin.site.register(Delegation)
admin.site.register(BulkUpload)
