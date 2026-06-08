from decimal import Decimal

from django.db import migrations
from django.db.models import Sum


def copy_legacy_invoice_to_child(apps, schema_editor):
    VendorPaymentRequest = apps.get_model("vendor", "VendorPaymentRequest")
    VendorPaymentInvoice = apps.get_model("vendor", "VendorPaymentInvoice")

    for request in VendorPaymentRequest.objects.all().iterator():
        if request.invoice_number:
            VendorPaymentInvoice.objects.get_or_create(
                payment_request=request,
                invoice_number=request.invoice_number,
                defaults={
                    "invoice_date": request.invoice_date,
                    "bill_type": request.bill_type or "non_gst",
                    "base_amount": request.base_amount or Decimal("0"),
                    "gst_amount": request.gst_amount or Decimal("0"),
                    "total_amount": request.total_amount or Decimal("0"),
                    "description": request.description or "",
                    "invoice_attachment": request.attachment,
                },
            )

        grand_total = (
            VendorPaymentInvoice.objects
            .filter(payment_request=request)
            .aggregate(total=Sum("total_amount"))
            .get("total")
            or Decimal("0")
        )

        if grand_total == Decimal("0") and request.total_amount:
            grand_total = request.total_amount

        request.grand_total = grand_total
        request.save(update_fields=["grand_total"])


def reverse_copy(apps, schema_editor):
    # Keep child invoice rows safe on rollback.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("vendor", "0005_vendorpaymentinvoice_and_more"),
    ]

    operations = [
        migrations.RunPython(copy_legacy_invoice_to_child, reverse_copy),
    ]