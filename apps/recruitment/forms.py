#D:\CLIENT PROJECT\employee management system bos\employee_management_system\apps\recruitment\forms.py
from django import forms
from django.contrib.auth import get_user_model
from .models import Employee, Candidate, InterviewSchedule, InterviewFeedback

class EmployeeForm(forms.ModelForm):
    class Meta:
        model = Employee
        fields = ["first_name", "last_name", "email", "department", "reporting_officer", "is_active"]
        widgets = {
            "first_name": forms.TextInput(attrs={"class":"form-control","placeholder":"First Name"}),
            "last_name": forms.TextInput(attrs={"class":"form-control","placeholder":"Last Name"}),
            "email": forms.EmailInput(attrs={"class":"form-control","placeholder":"Email"}),
            "department": forms.TextInput(attrs={"class":"form-control","placeholder":"Department"}),
            "reporting_officer": forms.Select(attrs={"class":"form-select"}),
            "is_active": forms.CheckboxInput(attrs={"class":"form-check-input"}),
        }
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.fields["reporting_officer"].queryset=get_user_model().objects.filter(is_active=True).order_by("first_name","last_name","email")

class CandidateForm(forms.ModelForm):
    class Meta: model=Candidate; fields=["name","email","resume"]
class CandidateStatusForm(forms.ModelForm):
    class Meta: model=Candidate; fields=["status"]
class InterviewScheduleForm(forms.ModelForm):
    scheduled_at=forms.DateTimeField(widget=forms.DateTimeInput(attrs={"type":"datetime-local"}))
    class Meta: model=InterviewSchedule; fields=["candidate","scheduled_at","interviewer","location"]
class InterviewFeedbackForm(forms.ModelForm):
    class Meta: model=InterviewFeedback; fields=["interview","reviewer","feedback","rating"]