# This is the controlled ONE-WAY import entry.
# Hook into your existing Google Sheets client here; we stay idempotent via row_uuid.
from django.db import transaction
from datetime import date
from ..models import Customer, KAMAssignment, InvoiceFact, LeadFact, OverdueSnapshot

def run_manual_import_safe():
    # Pseudocode placeholders — replace with real sheet readers
    with transaction.atomic():
        # import_customer_master(rows)
        # import_invoices(rows)
        # import_leads(rows)
        # import_overdues(rows)
        pass  # demo keeps it no-op

# Example idempotent upsert (pattern)
def upsert_invoice(row):
    InvoiceFact.objects.update_or_create(
        row_uuid=row["ROW_UUID"],
        defaults={
            "invoice_date": row["DATE"],
            "customer_id": row["CUSTOMER_ID"],
            "kam_id": row["KAM_ID"],
            "grade": row.get("GRADE"),
            "size": row.get("SIZE"),
            "qty_mt": row["QTY_MT"],
            "revenue_gst": row["REVENUE_GST"],
        }
    )

def snapshot_today_overdues():
    # optional helper to copy latest sheet numbers into today’s snapshot
    pass
