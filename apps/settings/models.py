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
