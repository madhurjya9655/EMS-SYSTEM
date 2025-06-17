# employee_management_system/settings.py

from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
SECRET_KEY = 'django-insecure-vgrup&h)hqen77^t@siu1m6*(^^510i$yspng(i&&n%$n*#!&_'
DEBUG = True
ALLOWED_HOSTS = []


# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # your apps
    'apps.recruitment',
    'apps.leave',
    'apps.core',
    'apps.sales',
    'apps.reimbursement',
    'apps.petty_cash',
    'apps.tasks',
    'apps.reports',
    'apps.users',
    'dashboard',
    'widget_tweaks',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'employee_management.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],   # ← your global templates folder
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'employee_management.wsgi.application'


# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]


# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE     = 'UTC'
USE_I18N      = True
USE_TZ        = True


# Static files (CSS, JavaScript, Images)
STATIC_URL          = 'static/'
STATICFILES_DIRS    = [BASE_DIR / 'static']


# ───────────────────────────────────────────────────────────
# Authentication redirects & email backend for password-reset
LOGIN_URL           = 'login'
LOGIN_REDIRECT_URL  = 'dashboard:home'
LOGOUT_REDIRECT_URL = 'login'

# During development, password-reset emails go to the console
EMAIL_BACKEND       = 'django.core.mail.backends.console.EmailBackend'
DEFAULT_FROM_EMAIL  = 'no-reply@example.com'
# ───────────────────────────────────────────────────────────


# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
