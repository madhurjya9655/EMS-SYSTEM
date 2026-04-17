"""
Migration: 0017_approver_mapping_fk_to_m2m

Converts ReimbursementApproverMapping.manager (FK) → managers (M2M)
and ReimbursementApproverMapping.finance (FK) → finance_users (M2M).

Steps performed atomically:
  1. Add new M2M fields (managers, finance_users)
  2. Data migration: copy existing FK values into the new M2M tables
  3. Remove old FK fields (manager, finance)
"""
from django.conf import settings
from django.db import migrations, models


def copy_fk_to_m2m(apps, schema_editor):
    """Copy single FK values into the new M2M fields before removing FKs."""
    ReimbursementApproverMapping = apps.get_model(
        "reimbursement", "ReimbursementApproverMapping"
    )
    for mapping in ReimbursementApproverMapping.objects.all():
        if mapping.manager_id:
            mapping.managers.add(mapping.manager_id)
        if mapping.finance_id:
            mapping.finance_users.add(mapping.finance_id)


def reverse_m2m_to_fk(apps, schema_editor):
    """Reverse: restore first M2M entry back into the FK field."""
    ReimbursementApproverMapping = apps.get_model(
        "reimbursement", "ReimbursementApproverMapping"
    )
    for mapping in ReimbursementApproverMapping.objects.prefetch_related(
        "managers", "finance_users"
    ).all():
        first_mgr = mapping.managers.first()
        first_fin = mapping.finance_users.first()
        update_fields = []
        if first_mgr:
            mapping.manager_id = first_mgr.pk
            update_fields.append("manager")
        if first_fin:
            mapping.finance_id = first_fin.pk
            update_fields.append("finance")
        if update_fields:
            mapping.save(update_fields=update_fields)


class Migration(migrations.Migration):

    dependencies = [
        ("reimbursement", "0016_add_submission_notify_emails"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # ── Step 1: Add new M2M fields (old FK fields still exist here) ──
        migrations.AddField(
            model_name="reimbursementapprovermapping",
            name="managers",
            field=models.ManyToManyField(
                blank=True,
                related_name="reimbursement_managers_for",
                to=settings.AUTH_USER_MODEL,
                verbose_name="Managers",
                help_text="One or more managers who can approve this employee's requests.",
            ),
        ),
        migrations.AddField(
            model_name="reimbursementapprovermapping",
            name="finance_users",
            field=models.ManyToManyField(
                blank=True,
                related_name="reimbursement_finance_users_for",
                to=settings.AUTH_USER_MODEL,
                verbose_name="Finance Users",
                help_text="One or more finance users for this employee's requests.",
            ),
        ),
        # ── Step 2: Copy FK data into the new M2M tables ─────────────────
        migrations.RunPython(copy_fk_to_m2m, reverse_code=reverse_m2m_to_fk),
        # ── Step 3: Remove old FK fields ──────────────────────────────────
        migrations.RemoveField(
            model_name="reimbursementapprovermapping",
            name="manager",
        ),
        migrations.RemoveField(
            model_name="reimbursementapprovermapping",
            name="finance",
        ),
    ]