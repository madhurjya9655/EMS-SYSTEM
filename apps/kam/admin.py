# apps/kam/admin.py

from django.contrib import admin

from . import models


# ---------------------------------------------------------------------------
# KAM Approval Email Settings
# ---------------------------------------------------------------------------

@admin.register(models.KAMEmailApprovalSettings)
class KAMEmailApprovalSettingsAdmin(admin.ModelAdmin):
    """
    Admin page for managing KAM visit approval email recipients.

    Only one settings row is allowed.
    """

    filter_horizontal = (
        "approval_users",
        "cc_users",
    )

    list_display = (
        "id",
        "is_active",
        "include_mapped_manager",
        "updated_by",
        "updated_at",
    )

    readonly_fields = (
        "updated_at",
    )

    fieldsets = (
        (
            "KAM Approval Email Recipients",
            {
                "fields": (
                    "is_active",
                    "approval_users",
                    "cc_users",
                    "include_mapped_manager",
                ),
            },
        ),
        (
            "Audit",
            {
                "fields": (
                    "updated_by",
                    "updated_at",
                ),
            },
        ),
    )

    def save_model(self, request, obj, form, change):
        obj.updated_by = request.user
        super().save_model(request, obj, form, change)

    def has_add_permission(self, request):
        """
        Allow only one global settings record.
        """
        return not models.KAMEmailApprovalSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        """
        Prevent accidental deletion of the global settings row.
        """
        return False


# ---------------------------------------------------------------------------
# Core KAM Models
# ---------------------------------------------------------------------------

# Register core KAM models.
# These are mostly read-heavy and write-minimal in Django Admin.

CORE_KAM_MODELS = [
    models.Customer,
    models.KAMAssignment,
    models.InvoiceFact,
    models.LeadFact,
    models.OverdueSnapshot,
    models.TargetHeader,
    models.TargetLine,
    models.VisitPlan,
    models.VisitActual,
    models.CallLog,
    models.CollectionTxn,
    models.KpiSnapshotDaily,
    models.VisitApprovalAudit,
    models.SyncIntent,
]


for model in CORE_KAM_MODELS:
    try:
        admin.site.register(model)
    except admin.sites.AlreadyRegistered:
        pass