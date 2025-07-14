from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from apps.users.views import CustomLoginView

urlpatterns = [
    path('admin/', admin.site.urls),

    # Custom login with remember me and dashboard redirect
    path(
        'accounts/login/',
        CustomLoginView.as_view(),
        name='login'
    ),
    path('accounts/', include('django.contrib.auth.urls')),

    # App includes
    path('leave/',         include(('apps.leave.urls',          'leave'),        namespace='leave')),
    path('petty_cash/',    include(('apps.petty_cash.urls',     'petty_cash'),   namespace='petty_cash')),
    path('sales/',         include(('apps.sales.urls',          'sales'),        namespace='sales')),
    path('reimbursement/', include(('apps.reimbursement.urls',  'reimbursement'),namespace='reimbursement')),
    path('tasks/',         include(('apps.tasks.urls',          'tasks'),        namespace='tasks')),
    path('reports/',       include(('apps.reports.urls',        'reports'),      namespace='reports')),
    path('users/',         include(('apps.users.urls',          'users'),        namespace='users')),
    path('dashboard/',     include(('dashboard.urls',           'dashboard'),    namespace='dashboard')),
    path('',               include(('apps.recruitment.urls',    'recruitment'),  namespace='recruitment')),
    path('settings/',      include(('apps.settings.urls',       'settings'),     namespace='settings')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

# --- ADD THIS AT THE END! ---
# Custom handler for 403 PermissionDenied
handler403 = 'apps.users.views.custom_permission_denied_view'
