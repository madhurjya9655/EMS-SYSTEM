from django import forms
from .models import Employee, Candidate, InterviewSchedule, InterviewFeedback

class EmployeeForm(forms.ModelForm):
    class Meta:
        model = Employee
        fields = ['first_name','last_name','email','department','is_active']
        widgets = {
            'first_name': forms.TextInput(attrs={'class':'form-control','placeholder':'First Name'}),
            'last_name': forms.TextInput(attrs={'class':'form-control','placeholder':'Last Name'}),
            'email': forms.EmailInput(attrs={'class':'form-control','placeholder':'Email'}),
            'department': forms.TextInput(attrs={'class':'form-control','placeholder':'Department'}),
            'is_active': forms.CheckboxInput(attrs={'class':'form-check-input'}),
        }

class CandidateForm(forms.ModelForm):
    class Meta:
        model = Candidate
        fields = ['name','email','resume']

class CandidateStatusForm(forms.ModelForm):
    class Meta:
        model = Candidate
        fields = ['status']

class InterviewScheduleForm(forms.ModelForm):
    scheduled_at = forms.DateTimeField(widget=forms.DateTimeInput(attrs={'type':'datetime-local'}))
    class Meta:
        model = InterviewSchedule
        fields = ['candidate','scheduled_at','interviewer','location']

class InterviewFeedbackForm(forms.ModelForm):
    class Meta:
        model = InterviewFeedback
        fields = ['interview','reviewer','feedback','rating']
