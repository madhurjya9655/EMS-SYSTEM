from django.contrib import admin
from .models import Vendor, VendorPaymentRequest, VendorApprovalConfig


@admin.register(Vendor)
class VendorAdmin(admin.ModelAdmin):
    list_display = ['name', 'type', 'is_active', 'created_at']
    list_filter = ['is_active', 'type']
    search_fields = ['name']


@admin.register(VendorPaymentRequest)
class VendorPaymentRequestAdmin(admin.ModelAdmin):
    list_display = ['request_id', 'vendor_display_name', 'total_amount', 'status', 'created_by', 'created_at']
    list_filter = ['status', 'bill_type', 'vendor_type']
    search_fields = ['request_id', 'vendor__name', 'vendor_name_manual', 'invoice_number']
    readonly_fields = ['request_id', 'total_amount', 'created_at', 'updated_at']
    raw_id_fields = ['created_by', 'finance_approved_by', 'final_approved_by']


@admin.register(VendorApprovalConfig)
class VendorApprovalConfigAdmin(admin.ModelAdmin):
    filter_horizontal = ['finance_users', 'mumbai_accounts']