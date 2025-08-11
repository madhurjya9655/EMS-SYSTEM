from django.db import models

class AuthorizedNumber(models.Model):
    label = models.CharField(max_length=100, help_text="A friendly name for this number")
    number = models.CharField(max_length=20, help_text="Phone number or code")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.label} ({self.number})"


class Holiday(models.Model):
    date = models.DateField(unique=True)
    name = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date']

    def __str__(self):
        return f"{self.date:%Y-%m-%d} – {self.name}"


class SystemSetting(models.Model):
    whatsapp_vendor      = models.CharField(max_length=100, blank=True)
    whatsapp_api_key     = models.CharField(max_length=255, blank=True)
    whatsapp_sender_id   = models.CharField(max_length=100, blank=True)
    whatsapp_webhook_url = models.URLField(blank=True)

    authorized_phones = models.TextField(
        blank=True,
        help_text="Comma-separated phone numbers allowed for WhatsApp."
    )
    authorized_emails = models.TextField(
        blank=True,
        help_text="Comma-separated emails allowed for notifications."
    )

    send_daily_doer    = models.BooleanField(default=False)
    send_daily_admin   = models.BooleanField(default=False)
    send_weekly_doer   = models.BooleanField(default=False)
    send_weekly_admin  = models.BooleanField(default=False)
    send_monthly_doer  = models.BooleanField(default=False)
    send_monthly_admin = models.BooleanField(default=False)

    notify_wapp_pending_checklist    = models.BooleanField(default=False)
    notify_wapp_pending_delegation   = models.BooleanField(default=False)
    notify_email_pending_checklist   = models.BooleanField(default=False)
    notify_email_pending_delegation  = models.BooleanField(default=False)

    notify_wapp_checklist            = models.BooleanField(default=False)
    notify_wapp_fms                  = models.BooleanField(default=False)
    notify_email_checklist           = models.BooleanField(default=False)
    notify_email_delegation          = models.BooleanField(default=False)
    notify_email_helpticket          = models.BooleanField(default=False)
    notify_email_helpticket_reminder = models.BooleanField(default=False)
    all_doer_report_generate         = models.BooleanField(default=False)

    MIS_MODES = [('equal','Equal'),('weighted','Weighted')]
    mis_performance_mode = models.CharField(max_length=20, choices=MIS_MODES, default='equal')
    checklist_weightage  = models.PositiveIntegerField(default=1)
    delegation_weightage = models.PositiveIntegerField(default=1)
    fms_weightage        = models.PositiveIntegerField(default=1)
    weight_low           = models.PositiveIntegerField(default=1)
    weight_medium        = models.PositiveIntegerField(default=1)
    weight_high          = models.PositiveIntegerField(default=1)

    smtp_from_name = models.CharField(max_length=100, blank=True)
    smtp_username  = models.EmailField(blank=True)
    smtp_password  = models.CharField(max_length=255, blank=True)

    high_stock_notification_freq = models.PositiveIntegerField(default=7)
    low_stock_notification_freq  = models.PositiveIntegerField(default=7)
    stockout_notification_freq   = models.PositiveIntegerField(default=7)
    max_fast_flowing_product     = models.PositiveIntegerField(default=50)
    max_slow_flowing_product     = models.PositiveIntegerField(default=50)

    MARKETING_MODES = [('random','Random'),('sequential','Sequential')]
    marketing_mode         = models.CharField(max_length=20, choices=MARKETING_MODES, default='random')
    marketing_freeze_min   = models.PositiveIntegerField(default=1)
    marketing_freeze_max   = models.PositiveIntegerField(default=5)
    marketing_after_sending = models.PositiveIntegerField(default=10)
    marketing_sleep_min    = models.PositiveIntegerField(default=1)
    marketing_sleep_max    = models.PositiveIntegerField(default=3)

    logo = models.ImageField(upload_to='system_logos/', blank=True, null=True)

    def __str__(self):
        return "System Settings"

    class Meta:
        verbose_name = "System Setting"
        verbose_name_plural = "System Settings"
