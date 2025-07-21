from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required, user_passes_test
from django.views.generic import ListView, CreateView, DetailView, UpdateView, DeleteView
from django.urls import reverse, reverse_lazy
from django.template.loader import render_to_string
from django.http import HttpResponse
from datetime import date
from .models import Employee, Candidate, InterviewSchedule, InterviewFeedback
from .forms import EmployeeForm, CandidateForm, CandidateStatusForm, InterviewScheduleForm, InterviewFeedbackForm
from apps.users.permissions import has_permission

class EmployeeListView(LoginRequiredMixin, ListView):
    login_url = 'login'
    model = Employee
    template_name = 'recruitment/employee_list.html'
    context_object_name = 'employees'

class EmployeeCreateView(LoginRequiredMixin, CreateView):
    login_url = 'login'
    model = Employee
    form_class = EmployeeForm
    template_name = 'recruitment/employee_form.html'
    def get_success_url(self):
        return reverse('employee_detail', args=[self.object.pk])

class EmployeeDetailView(LoginRequiredMixin, DetailView):
    login_url = 'login'
    model = Employee
    template_name = 'recruitment/employee_detail.html'
    context_object_name = 'employee'

class EmployeeUpdateView(LoginRequiredMixin, UpdateView):
    login_url = 'login'
    model = Employee
    form_class = EmployeeForm
    template_name = 'recruitment/employee_form.html'
    success_url = reverse_lazy('employee_list')

class EmployeeDeleteView(LoginRequiredMixin, DeleteView):
    login_url = 'login'
    model = Employee
    template_name = 'recruitment/employee_confirm_delete.html'
    success_url = reverse_lazy('employee_list')

@login_required
def candidate_list(request):
    qs = Candidate.objects.all()
    return render(request, 'recruitment/candidate_list.html', {'candidates': qs})

@login_required
def candidate_detail(request, pk):
    obj = get_object_or_404(Candidate, pk=pk)
    return render(request, 'recruitment/candidate_detail.html', {'candidate': obj})

hr_or_manager_or_super = lambda u: u.is_superuser or u.groups.filter(name__in=['HR', 'Manager']).exists()

@login_required
@user_passes_test(hr_or_manager_or_super)
def update_status(request, pk):
    obj = get_object_or_404(Candidate, pk=pk)
    if request.method == 'POST':
        form = CandidateStatusForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            return redirect('candidate_detail', pk=pk)
    else:
        form = CandidateStatusForm(instance=obj)
    return render(request, 'recruitment/update_status.html', {'form': form, 'candidate': obj})

@login_required
@user_passes_test(hr_or_manager_or_super)
def schedule_interview(request):
    if request.method == 'POST':
        form = InterviewScheduleForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('interview_list')
    else:
        form = InterviewScheduleForm()
    return render(request, 'recruitment/schedule_interview.html', {'form': form})

@login_required
def interview_list(request):
    qs = InterviewSchedule.objects.all()
    return render(request, 'recruitment/interview_list.html', {'interviews': qs})

@login_required
def interview_feedback(request, pk):
    interview = get_object_or_404(InterviewSchedule, pk=pk)
    if request.method == 'POST':
        form = InterviewFeedbackForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('interview_list')
    else:
        form = InterviewFeedbackForm(initial={'interview': interview, 'reviewer': request.user})
    return render(request, 'recruitment/interview_feedback.html', {'form': form, 'interview': interview})

@login_required
def offer_letter(request, pk):
    candidate = get_object_or_404(Candidate, pk=pk)
    context = {'candidate': candidate, 'date': date.today()}
    html = render_to_string('recruitment/offer_letter.html', context)
    resp = HttpResponse(html, content_type='application/msword')
    resp['Content-Disposition'] = f'attachment; filename="OfferLetter_{candidate.name}.doc"'
    return resp
