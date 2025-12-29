import logging
from datetime import date

from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import Customer, SalesInvoice, Lead, TargetsPlan
from .google_sheets_utils import (
    upsert_customer_master,
    upsert_sales_data,
    upsert_leads_data,
    pull_targets_plan_rows,
)

logger = logging.getLogger(__name__)


# ----------------------- Customer -> Customer_Master ----------------------- #
@receiver(post_save, sender=Customer)
def _sync_customer_to_sheet(sender, instance: Customer, **kwargs):
    try:
        payload = {
            "Customer Name": instance.name or "",
            "KAM Name": instance.kam_name or "",
            "Address": instance.address or "",
            "Email": instance.email or "",
            "Mobile No": instance.mobile_no or "",
            "Person Name": instance.person_name or "",
            "Pincode": instance.pincode or "",
            "Type": instance.type or "",
            "GST Number": instance.gst_number or "",
            "Credit Limit": float(instance.credit_limit or 0),
            "Agreed Credit Period": int(instance.agreed_credit_period or 0),
            "Total Exposure (₹)": float(instance.total_exposure or 0),
            "Overdues (₹)": float(instance.overdues or 0),
            "NBD Flag": "Yes" if instance.nbd_flag else "No",
        }
        upsert_customer_master(payload)
    except Exception as e:
        logger.error("Customer sheet sync failed (non-blocking): %s", e)


# ----------------------- SalesInvoice -> Sales_Data ------------------------ #
@receiver(post_save, sender=SalesInvoice)
def _sync_invoice_to_sheet(sender, instance: SalesInvoice, **kwargs):
    try:
        payload = {
            "KAM Name": instance.kam_name or "",
            "Invoice Date": instance.invoice_date.isoformat() if instance.invoice_date else "",
            "Quantity (MT)": float(instance.quantity_mt or 0),
            "Revenue (₹ with GST)": float(instance.revenue_inr_with_gst or 0),
        }
        upsert_sales_data(payload)
    except Exception as e:
        logger.error("Sales_Data sheet sync failed (non-blocking): %s", e)


# ------------------------- Lead -> Leads_Data ------------------------------ #
@receiver(post_save, sender=Lead)
def _sync_lead_to_sheet(sender, instance: Lead, **kwargs):
    try:
        # Derive Month/Week text if not set
        if not instance.month_text and instance.date_of_enquiry:
            instance.month_text = instance.date_of_enquiry.strftime("%B")
        if not instance.week_text and instance.date_of_enquiry:
            # Week-1..Week-5 based on day-of-month buckets (simple & deterministic)
            day = instance.date_of_enquiry.day
            wk = 1 + (day - 1) // 7
            instance.week_text = f"Week-{min(max(wk, 1), 5)}"
            instance.save(update_fields=["month_text", "week_text"])

        payload = {
            "Month": instance.month_text or "",
            "Week": instance.week_text or "",
            "Date of Enquiry": instance.date_of_enquiry.isoformat() if instance.date_of_enquiry else "",
            "KAM Name": instance.kam_name or "",
            "Customer Name": instance.customer_name or "",
            "Quantity": float(instance.quantity or 0),
            "Status": instance.status or "",
            "Remarks": instance.remarks or "",
            "Grade": instance.grade or "",
            "Size": instance.size or "",
        }
        upsert_leads_data(payload)
    except Exception as e:
        logger.error("Leads_Data sheet sync failed (non-blocking): %s", e)


# ------------------- Optional: pull Targets_Plan into DB ------------------- #
def sync_targets_plan_from_sheet() -> int:
    """
    Idempotent upsert from sheet Targets_Plan into TargetsPlan model.
    Manager edits in sheet; EMS reads and stores for fast dashboards/ACL.
    """
    try:
        rows = pull_targets_plan_rows()
        count = 0
        for r in rows:
            kam = (r.get("KAM Name") or "").strip()
            week = (r.get("Week") or "").strip()
            if not kam or not week:
                continue
            obj, _ = TargetsPlan.objects.get_or_create(kam_name=kam, week=week)
            obj.sales_target_mt = float(r.get("Sales Target (MT)") or 0)
            obj.calls_target = int(r.get("Calls Target (Fixed = 24)") or 24)
            obj.visits_target = int(r.get("Visits Target (Fixed = 6)") or 6)
            obj.leads_target_mt = float(r.get("Leads Target (250 MT)") or 250)
            obj.nbd_target_monthly = int(r.get("NBD Target (Monthly)") or 0)
            obj.save()
            count += 1
        return count
    except Exception as e:
        logger.error("Targets_Plan pull failed: %s", e)
        return 0
