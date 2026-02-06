from django.contrib import admin
from . import models

# Register core KAM models (read-heavy; write-minimal)
for m in [
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
    # NEW: audit + sync intent
    models.VisitApprovalAudit,
    models.SyncIntent,
]:
    admin.site.register(m)
