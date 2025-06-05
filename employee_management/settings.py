import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# ------------------------------------------------------------------------------
# PRODUCTION CONFIGURATION VIA ENVIRONMENT VARIABLES
# ------------------------------------------------------------------------------

# Read SECRET_KEY from the environment (or fall back to a dummy for dev).
# In production (Render), you must set SECRET_KEY as an environment variable.
SECRET_KEY = os.getenv(
    "SECRET_KEY",
    "django-insecure-__dev-only-use-this__",
)

# Read DEBUG from the environment. Default to True only when not set.
# On Render, set DEBUG=False in your Environment Variables.
DEBUG = os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")

# Allow only these hosts in production.
# You can still add localhost for local testing.
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "ems-system-v944.onrender.com,localhost,127.0.0.1") \
    .split(",")


# ------------------------------------------------------------------------------
# APPLICATION DEFINITION (unchanged)
# ------------------------------------------------------------------------------

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "apps.recruitment",
    "apps.leave",
    "apps.core",
    "apps.sales",
    "apps.reimbursement",
    "apps.petty_cash",
    "apps.tasks",
    "apps.reports",
    "apps.users",
    "dashboard",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",

    # ────────────────────────────────────────────────────────────────────────────
    # WhiteNoise middleware must come right after SecurityMiddleware in production
    # ────────────────────────────────────────────────────────────────────────────
    "whitenoise.middleware.WhiteNoiseMiddleware",

    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "employee_management.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "employee_management.wsgi.application"


# ------------------------------------------------------------------------------
# DATABASE (unchanged from your local SQLite; you can switch to Postgres later)
# ------------------------------------------------------------------------------

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}


# ------------------------------------------------------------------------------
# PASSWORD VALIDATION (unchanged)
# ------------------------------------------------------------------------------

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",},
]


# ------------------------------------------------------------------------------
# INTERNATIONALIZATION (unchanged)
# ------------------------------------------------------------------------------

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True


# ------------------------------------------------------------------------------
# STATIC FILES CONFIGURATION (UPDATED)
# ------------------------------------------------------------------------------

# URL to use when referring to static files (where WhiteNoise will serve them)
STATIC_URL = "/static/"

# Local folders you use during development (e.g. your “static/” folder in the repo).
# Django’s collectstatic will look in these directories for any “static/” subfolders.
STATICFILES_DIRS = [BASE_DIR / "static"]

# Directory where ‘collectstatic’ will copy all static files for production.
# WhiteNoise (in your middleware) will serve files from here when DEBUG=False.
STATIC_ROOT = BASE_DIR / "staticfiles"

# Tell WhiteNoise to create compressed versions and add cache‐busting hashes
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"


# ------------------------------------------------------------------------------
# DEFAULT PRIMARY KEY FIELD TYPE (unchanged)
# ------------------------------------------------------------------------------

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
