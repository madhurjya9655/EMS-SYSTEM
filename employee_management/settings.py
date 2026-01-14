# employee_management/settings.py
import os
import sqlite3
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from django.db.backends.signals import connection_created  # type: ignore

load_dotenv()

# -----------------------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # …/employee_management_system
ON_RENDER = bool(os.environ.get("RENDER"))

# We will default to Render's disk mount path. You don't *need* to set DISK_ROOT.
DEFAULT_DISK_ROOT = "/opt/render/project/src/db"

# If MEDIA_ROOT is set in env (you have it), we’ll honor it.
MEDIA_ROOT_ENV = os.getenv("MEDIA_ROOT", "").strip()

# -----------------------------------------------------------------------------
# ENV HELPERS
# -----------------------------------------------------------------------------
def env_list(name: str, default_csv: str = "") -> List[str]:
    raw = os.getenv(name, default_csv) or ""
    return [part.strip() for part in raw.split(",") if part.strip()]

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, str(default))
    return str(raw).lower() in ("1", "true", "yes", "on")

def env_int(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default

# -----------------------------------------------------------------------------
# CORE
# -----------------------------------------------------------------------------
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-__dev-only-use-this__")
DEBUG = env_bool("DEBUG", False)

# Both are read by reimbursement code; SITE_BASE_URL falls back to SITE_URL.
SITE_URL = os.getenv("SITE_URL", "https://ems-system-d26q.onrender.com")
SITE_BASE_URL = os.getenv("SITE_BASE_URL", SITE_URL)

CRON_SECRET = os.getenv("CRON_SECRET", "")

# Serve MEDIA behind Django (kept ON by default on Render)
SERVE_MEDIA = env_bool("SERVE_MEDIA", True if ON_RENDER else False)

# Hardened admin path (used in urls.py)
ADMIN_URL = os.getenv("ADMIN_URL", "super-secret-admin/")

ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    "ems-system-d26q.onrender.com,.onrender.com,localhost,127.0.0.1,0.0.0.0",
)

CSRF_TRUSTED_ORIGINS = env_list(
    "CSRF_TRUSTED_ORIGINS",
    "https://ems-system-d26q.onrender.com",
)

if DEBUG:
    for local_origin in (
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://0.0.0.0:8000",
        "http://testserver",
    ):
        if local_origin not in CSRF_TRUSTED_ORIGINS:
            CSRF_TRUSTED_ORIGINS.append(local_origin)

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True

CSRF_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_HTTPONLY = True
SESSION_EXPIRE_AT_BROWSER_CLOSE = False
APPEND_SLASH = True

SESSION_COOKIE_SECURE = True if (ON_RENDER or not DEBUG) else False
CSRF_COOKIE_SECURE = True if (ON_RENDER or not DEBUG) else False
SECURE_CONTENT_TYPE_NOSNIFF = True

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.Argon2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher",
    "django.contrib.auth.hashers.BCryptSHA256PasswordHasher",
]

# -----------------------------------------------------------------------------
# APPS
# -----------------------------------------------------------------------------
DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
]
THIRD_PARTY_APPS = [
    "widget_tweaks",
    "crispy_forms",
    "crispy_bootstrap5",
]
LOCAL_APPS = [
    "apps.common",
    "apps.recruitment",
    "apps.leave.apps.LeaveConfig",
    "apps.core",
    "apps.sales",
    "apps.reimbursement",
    "apps.petty_cash",
    "apps.tasks.apps.TasksConfig",
    "apps.reports",
    "apps.users.apps.UsersConfig",
    "dashboard.apps.DashboardConfig",
    "apps.settings.apps.SettingsConfig",
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

if "django_celery_beat" not in INSTALLED_APPS:
    INSTALLED_APPS.append("django_celery_beat")

# -----------------------------------------------------------------------------
# MIDDLEWARE
# -----------------------------------------------------------------------------
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.middleware.gzip.GZipMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "apps.users.middleware.PermissionEnforcementMiddleware",
    "apps.users.middleware.PermissionDebugMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "employee_management.urls"
WSGI_APPLICATION = "employee_management.wsgi.application"

# -----------------------------------------------------------------------------
# TEMPLATES
# -----------------------------------------------------------------------------
_template_options = {
    "context_processors": [
        "django.template.context_processors.debug",
        "django.template.context_processors.request",
        "django.contrib.auth.context_processors.auth",
        "django.contrib.messages.context_processors.messages",
        "apps.users.permissions.permissions_context",
    ],
    "builtins": [
        "dashboard.templatetags.dashboard_extras",
        "apps.reports.templatetags.reports_extras",
    ],
    "libraries": {
        "common_filters": "apps.common.templatetags.common_filters",
        "user_filters": "apps.users.templatetags.user_filters",
        "users_filters": "apps.users.templatetags.user_filters",
        "users_permissions": "apps.users.templatetags.users_permissions",  # ✅ added
        "group_tags": "apps.common.templatetags.group_tags",
        "model_extras": "apps.common.templatetags.model_extras",
    },
    "string_if_invalid": "",
}

if DEBUG:
    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [BASE_DIR / "templates"],
            "APP_DIRS": True,
            "OPTIONS": _template_options,
        }
    ]
else:
    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [BASE_DIR / "templates"],
            "APP_DIRS": False,
            "OPTIONS": {
                **_template_options,
                "loaders": [
                    (
                        "django.template.loaders.cached.Loader",
                        [
                            "django.template.loaders.filesystem.Loader",
                            "django.template.loaders.app_directories.Loader",
                        ],
                    ),
                ],
            },
        }
    ]

# -----------------------------------------------------------------------------
# DATABASE
# -----------------------------------------------------------------------------
try:
    import dj_database_url  # type: ignore
except Exception:
    dj_database_url = None

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# ✅ Single source of truth for connection reuse (used for both Postgres + SQLite)
CONN_MAX_AGE = env_int("CONN_MAX_AGE", 0)

if DATABASE_URL and dj_database_url:
    DATABASES = {
        "default": dj_database_url.parse(
            DATABASE_URL,
            conn_max_age=CONN_MAX_AGE,
            ssl_require=env_bool("DB_SSL_REQUIRE", True),
        ),
    }
else:
    # Prefer explicit SQLITE_PATH if provided (you have it set)
    sqlite_path = os.getenv("SQLITE_PATH", "").strip()
    if not sqlite_path:
        # Fallback to disk mount
        disk_root = os.getenv("DISK_ROOT", DEFAULT_DISK_ROOT).strip() or DEFAULT_DISK_ROOT
        sqlite_dir = Path(disk_root) / "sqlite"
        sqlite_dir.mkdir(parents=True, exist_ok=True)
        sqlite_path = str(sqlite_dir / "db.sqlite3")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)

    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": sqlite_path,
            # ✅ THIS is the missing piece that caused Django to show CONN_MAX_AGE: 0
            "CONN_MAX_AGE": CONN_MAX_AGE,
            "OPTIONS": {
                "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                "timeout": 60,
            },
        },
    }

DATABASE_CONNECTION_POOLING = False

# -----------------------------------------------------------------------------
# SQLITE ROBUSTNESS
# -----------------------------------------------------------------------------
def _robust_sqlite_decoder(val):
    if val is None:
        return None
    if isinstance(val, memoryview):
        try:
            val = val.tobytes()
        except Exception:
            return str(val)
    if isinstance(val, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1", "ascii", "cp1252"):
            try:
                s = val.decode(enc).strip().replace("\x00", "")
                if s:
                    return s
            except Exception:
                continue
        try:
            return val.decode("utf-8", errors="ignore").strip()
        except Exception:
            return str(val)
    if isinstance(val, str):
        return val.strip().replace("\x00", "") or None
    try:
        return str(val)
    except Exception:
        return None

try:
    for dt_type in [
        "timestamp","datetime","timestamptz","timestamp with time zone",
        "date","time","TIMESTAMP","DATETIME","DATE","TIME",
    ]:
        sqlite3.register_converter(dt_type, _robust_sqlite_decoder)
except Exception:
    pass

def _configure_sqlite_connection(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    try:
        def universal_text_factory(data):
            if data is None:
                return None
            if isinstance(data, memoryview):
                try:
                    data = data.tobytes()
                except Exception:
                    return str(data)
            if isinstance(data, (bytes, bytearray)):
                for enc in ("utf-8","latin-1","ascii","cp1252"):
                    try:
                        res = data.decode(enc).strip().replace("\x00","")
                        if res:
                            return res
                    except Exception:
                        continue
                try:
                    return data.decode("utf-8", errors="ignore").strip()
                except Exception:
                    return str(data)
            if isinstance(data, str):
                return data.strip().replace("\x00","")
            try:
                return str(data)
            except Exception:
                return ""
        connection.connection.text_factory = universal_text_factory
    except Exception:
        pass

    try:
        with connection.cursor() as cur:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA cache_size=50000;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA mmap_size=536870912;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=60000;")
            cur.execute("PRAGMA wal_autocheckpoint=1000;")
            cur.execute("PRAGMA optimize;")
    except Exception:
        pass

connection_created.connect(_configure_sqlite_connection)

try:
    from django.db.backends.sqlite3.operations import DatabaseOperations  # type: ignore
    _orig_convert = DatabaseOperations.convert_datetimefield_value

    def safe_convert_datetimefield_value(self, value, expression, connection):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = _robust_sqlite_decoder(value)
        if isinstance(value, str):
            value = value.strip().replace("\x00", "") or None
            if value is None:
                return None
        try:
            return _orig_convert(self, value, expression, connection)
        except (TypeError, ValueError) as e:
            if "fromisoformat" in str(e) or "argument must be str" in str(e):
                try:
                    if hasattr(value, "decode"):
                        fixed = value.decode("utf-8", errors="ignore").strip()
                        return _orig_convert(self, fixed, expression, connection)
                except Exception:
                    return None
                return None
            raise

    DatabaseOperations.convert_datetimefield_value = safe_convert_datetimefield_value
except Exception:
    pass

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
LOGS_DIR = BASE_DIR / "logs"
if not DEBUG and ON_RENDER:
    LOGS_DIR = Path("/tmp/logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {"format": "{levelname} {asctime} {module} {process:d} {thread:d} {message}", "style": "{"},
        "simple": {"format": "{levelname} {asctime} {message}", "style": "{"},
        "detailed": {"format": "[{asctime}] {levelname} {name} {module}.{funcName}:{lineno} - {message}", "style": "{"},
    },
    "handlers": {
        "console": {"level": "DEBUG" if DEBUG else "INFO","class": "logging.StreamHandler","formatter": "simple"},
        "file": {"level": "INFO","class": "logging.FileHandler","filename": str(LOGS_DIR / "django.log"),"formatter": "verbose","encoding": "utf-8"},
        "permissions_file": {"level": "DEBUG" if DEBUG else "INFO","class": "logging.FileHandler","filename": str(LOGS_DIR / "permissions.log"),"formatter": "detailed","encoding": "utf-8"},
        "tasks_file": {"level": "DEBUG" if DEBUG else "INFO","class": "logging.FileHandler","filename": str(LOGS_DIR / "tasks.log"),"formatter": "detailed","encoding": "utf-8"},
        "bulk_upload_file": {"level": "DEBUG" if DEBUG else "INFO","class": "logging.FileHandler","filename": str(LOGS_DIR / "bulk_upload.log"),"formatter": "detailed","encoding": "utf-8"},
        "mail_admins": {"level": "ERROR","class": "django.utils.log.AdminEmailHandler"},
    },
    "root": {"handlers": ["console"], "level": "WARNING"},
    "loggers": {
        "django": {"handlers": ["file"], "level": "INFO", "propagate": False},
        "django.request": {"handlers": ["console", "file", "mail_admins"], "level": "ERROR", "propagate": False},
        "django.db.backends": {"handlers": ["file"], "level": "WARNING", "propagate": False},
        "apps.users.permissions": {"handlers": ["permissions_file"] + (["console"] if DEBUG else []),"level": "DEBUG" if DEBUG else "INFO","propagate": False},
        "apps.users.middleware": {"handlers": ["permissions_file"] + (["console"] if DEBUG else []),"level": "DEBUG" if DEBUG else "INFO","propagate": False},
        "apps.tasks": {"handlers": ["tasks_file", "console"], "level": "DEBUG" if DEBUG else "INFO","propagate": False},
        "apps.tasks.views": {"handlers": ["bulk_upload_file", "console"], "level": "INFO","propagate": False},
        "apps.tasks.signals": {"handlers": ["tasks_file"], "level": "INFO", "propagate": False},
        "apps.leave": {"handlers": ["file", "console"], "level": "INFO", "propagate": False},
        "apps.leave.services.notifications": {"handlers": ["file", "console"], "level": "INFO", "propagate": False},
    },
}

ADMINS = [("Ops", os.getenv("ADMIN_EMAIL", "ops@example.com"))]
SERVER_EMAIL = os.getenv("SERVER_EMAIL", os.getenv("DEFAULT_FROM_EMAIL", "no-reply@example.com"))

# -----------------------------------------------------------------------------
# I18N / TZ
# -----------------------------------------------------------------------------
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Kolkata"
USE_I18N = True
USE_TZ = True

# -----------------------------------------------------------------------------
# STATIC & MEDIA
# -----------------------------------------------------------------------------
STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

WHITENOISE_MANIFEST_STRICT = False
WHITENOISE_MAX_AGE = 60 * 60 * 24 * 365
WHITENOISE_AUTOREFRESH = DEBUG
WHITENOISE_USE_FINDERS = DEBUG

STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
]

STORAGES = {
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
}

MEDIA_URL = "/media/"
# Use your explicit MEDIA_ROOT if provided; otherwise default to the Render disk.
MEDIA_ROOT = MEDIA_ROOT_ENV or os.getenv("DISK_ROOT", DEFAULT_DISK_ROOT)
Path(MEDIA_ROOT).mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# AUTH
# -----------------------------------------------------------------------------
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "/dashboard/"
LOGOUT_REDIRECT_URL = "login"

# -----------------------------------------------------------------------------
# EMAIL
# -----------------------------------------------------------------------------
EMAIL_BACKEND = os.getenv("EMAIL_BACKEND", "django.core.mail.backends.smtp.EmailBackend")
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = env_int("EMAIL_PORT", 587)
EMAIL_USE_TLS = env_bool("EMAIL_USE_TLS", True)
EMAIL_USE_SSL = env_bool("EMAIL_USE_SSL", False)
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", EMAIL_HOST_USER or "BOS Lakshya <no-reply@example.com>")

REIMBURSEMENT_SENDER_EMAIL = os.getenv("REIMBURSEMENT_SENDER_EMAIL", "amreen@blueoceansteels.com")
REIMBURSEMENT_SENDER_NAME = os.getenv("REIMBURSEMENT_SENDER_NAME", "Amreen")
REIMBURSEMENT_EMAIL_FROM = os.getenv(
    "REIMBURSEMENT_EMAIL_FROM",
    f"{REIMBURSEMENT_SENDER_NAME} <{REIMBURSEMENT_SENDER_EMAIL}>",
)

# Cap for outbound attachments (bytes) — used by reimbursement notifications.
REIMBURSEMENT_EMAIL_ATTACHMENTS_MAX_BYTES = env_int(
    "REIMBURSEMENT_EMAIL_ATTACHMENTS_MAX_BYTES",
    20 * 1024 * 1024,  # 20 MB
)

EMAIL_TIMEOUT = env_int("EMAIL_TIMEOUT", 10)
EMAIL_FAIL_SILENTLY = env_bool("EMAIL_FAIL_SILENTLY", False if DEBUG else True)
SEND_EMAILS_FOR_AUTO_RECUR = env_bool("SEND_EMAILS_FOR_AUTO_RECUR", True)
SEND_DELEGATION_IMMEDIATE_EMAIL = env_bool("SEND_DELEGATION_IMMEDIATE_EMAIL", False)

EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "[BOS Lakshya] ")
LEAVE_EMAIL_FROM = os.getenv("LEAVE_EMAIL_FROM", DEFAULT_FROM_EMAIL)
LEAVE_EMAIL_REPLY_TO_EMPLOYEE = env_bool("LEAVE_EMAIL_REPLY_TO_EMPLOYEE", True)
LEAVE_DECISION_TOKEN_SALT = os.getenv("LEAVE_DECISION_TOKEN_SALT", "leave-action-v1")
LEAVE_DECISION_TOKEN_MAX_AGE = env_int("LEAVE_DECISION_TOKEN_MAX_AGE", 60 * 60 * 24 * 7)

ASSIGNER_CC_FOR_DELEGATION = {
    "emails": env_list("DELEGATION_CC_ASSIGNER_EMAILS", ""),
    "usernames": env_list("DELEGATION_CC_ASSIGNER_USERNAMES", ""),
}

if ON_RENDER and not os.getenv("EMAIL_HOST_USER"):
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

# -----------------------------------------------------------------------------
# SECURITY (Prod)
# -----------------------------------------------------------------------------
if not DEBUG:
    SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", True)
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = env_int("SECURE_HSTS_SECONDS", 31536000)
    SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", True)
    SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", True)
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = "DENY"
    SECURE_REFERRER_POLICY = os.getenv("SECURE_REFERRER_POLICY", "strict-origin-when-cross-origin")

# -----------------------------------------------------------------------------
# THIRD-PARTY
# -----------------------------------------------------------------------------
CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

# -----------------------------------------------------------------------------
# REIMBURSEMENT / GOOGLE INTEGRATIONS
# -----------------------------------------------------------------------------
REIMBURSEMENT_SHEET_ID = os.getenv("REIMBURSEMENT_SHEET_ID", "1LOVDkTVMGdEPOP9CQx-WVDv7ZY1TpqiQD82FFCc3t4A")
REIMBURSEMENT_DRIVE_FOLDER_ID = os.getenv("REIMBURSEMENT_DRIVE_FOLDER_ID")
REIMBURSEMENT_DRIVE_LINK_SHARING = os.getenv("REIMBURSEMENT_DRIVE_LINK_SHARING", "anyone")
REIMBURSEMENT_DRIVE_DOMAIN = os.getenv("REIMBURSEMENT_DRIVE_DOMAIN")
REIMBURSEMENT_DETAIL_URL_TEMPLATE = os.getenv("REIMBURSEMENT_DETAIL_URL_TEMPLATE")

# For Google credentials, keep them in env as JSON or file path:
# GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_FILE

# -----------------------------------------------------------------------------
# TASK SYSTEM
# -----------------------------------------------------------------------------
BULK_UPLOAD_BATCH_SIZE = env_int("BULK_UPLOAD_BATCH_SIZE", 200)
BULK_UPLOAD_MAX_ROWS = env_int("BULK_UPLOAD_MAX_ROWS", 5000)
EMAIL_BATCH_SIZE = env_int("EMAIL_BATCH_SIZE", 20)
EMAIL_SEND_DELAY = float(os.getenv("EMAIL_SEND_DELAY", "0.01"))
TASK_PROCESSING_TIMEOUT = env_int("TASK_PROCESSING_TIMEOUT", 600)
RECURRING_TASK_BATCH_SIZE = env_int("RECURRING_TASK_BATCH_SIZE", 100)
AUTO_CREATE_RECURRING_TASKS = env_bool("AUTO_CREATE_RECURRING_TASKS", True)
RECURRING_TASK_LOOKAHEAD_DAYS = env_int("RECURRING_TASK_LOOKAHEAD_DAYS", 30)
DASHBOARD_CACHE_TIMEOUT = env_int("DASHBOARD_CACHE_TIMEOUT", 300)
TASK_LIST_PAGE_SIZE = env_int("TASK_LIST_PAGE_SIZE", 50)

# -----------------------------------------------------------------------------
# PERFORMANCE / CACHING
# -----------------------------------------------------------------------------
SESSION_ENGINE = "django.contrib.sessions.backends.db"
SESSION_COOKIE_AGE = 60 * 60 * 24 * 7
FILE_UPLOAD_MAX_MEMORY_SIZE = env_int("FILE_UPLOAD_MAX_MEMORY_SIZE", 10 * 1024 * 1024)
DATA_UPLOAD_MAX_MEMORY_SIZE = env_int("DATA_UPLOAD_MAX_MEMORY_SIZE", 10 * 1024 * 1024)
DATA_UPLOAD_MAX_NUMBER_FIELDS = env_int("DATA_UPLOAD_MAX_NUMBER_FIELDS", 2000)

REDIS_URL = os.getenv("REDIS_URL", "").strip()
if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": REDIS_URL,
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "COMPRESSOR": "django_redis.compressors.zlib.ZlibCompressor",
                "PARSER_CLASS": "redis.connection.PythonParser",
            },
            "TIMEOUT": env_int("CACHE_TIMEOUT", 300),
        },
    }
    SESSION_ENGINE = "django.contrib.sessions.backends.cache"
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "ems-fast-cache",
            "TIMEOUT": 300,
            "OPTIONS": {"MAX_ENTRIES": 2000, "CULL_FREQUENCY": 3},
        },
    }

JSON_DUMPS_PARAMS = {"ensure_ascii": False}

# -----------------------------------------------------------------------------
# RENDER.COM SPECIFIC
# -----------------------------------------------------------------------------
if ON_RENDER:
    if not REDIS_URL:
        SESSION_ENGINE = "django.contrib.sessions.backends.db"
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    WEB_CONCURRENCY = env_int("WEB_CONCURRENCY", 2)
    MESSAGE_STORAGE = "django.contrib.messages.storage.session.SessionStorage"
    FILE_UPLOAD_MAX_MEMORY_SIZE = min(FILE_UPLOAD_MAX_MEMORY_SIZE, 5 * 1024 * 1024)
    DATA_UPLOAD_MAX_MEMORY_SIZE = min(DATA_UPLOAD_MAX_MEMORY_SIZE, 5 * 1024 * 1024)
    EMAIL_FAIL_SILENTLY = True

# -----------------------------------------------------------------------------
# DEVELOPMENT CONVENIENCES
# -----------------------------------------------------------------------------
if DEBUG:
    INTERNAL_IPS = ["127.0.0.1", "localhost"]
    if not os.getenv("EMAIL_HOST_USER"):
        EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
    SECURE_SSL_REDIRECT = False
    SESSION_COOKIE_SECURE = False
    CSRF_COOKIE_SECURE = False

# -----------------------------------------------------------------------------
# VALIDATION / REQUIRED DIRS
# -----------------------------------------------------------------------------
def validate_email_settings():
    if (not DEBUG and EMAIL_BACKEND == "django.core.mail.backends.smtp.EmailBackend"):
        if not EMAIL_HOST_USER or not EMAIL_HOST_PASSWORD:
            import warnings
            warnings.warn(
                "Email credentials not configured. Email functionality may not work.",
                RuntimeWarning,
            )

def validate_required_dirs():
    required_dirs = [
        MEDIA_ROOT,
        STATIC_ROOT,
    ]
    for dir_path in required_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

validate_email_settings()
validate_required_dirs()

# -----------------------------------------------------------------------------
# FEATURE FLAGS
# -----------------------------------------------------------------------------
FEATURE_EMAIL_NOTIFICATIONS = env_bool("FEATURE_EMAIL_NOTIFICATIONS", True)

FEATURES = {
    "BULK_UPLOAD_ENABLED": env_bool("FEATURE_BULK_UPLOAD", True),
    "EMAIL_NOTIFICATIONS": env_bool("FEATURE_EMAIL_NOTIFICATIONS", True),
    "RECURRING_TASKS": env_bool("FEATURE_RECURRING_TASKS", True),
    "TASK_REMINDERS": env_bool("FEATURE_TASK_REMINDERS", True),
    "ADVANCED_REPORTING": env_bool("FEATURE_ADVANCED_REPORTING", True),
    "AUDIT_LOGGING": env_bool("FEATURE_AUDIT_LOGGING", True),
}

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------
TASK_PRIORITIES = [("Low", "Low"), ("Medium", "Medium"), ("High", "High")]
TASK_STATUSES = [("Pending", "Pending"), ("Completed", "Completed")]
RECURRING_MODES = [("Daily", "Daily"), ("Weekly", "Weekly"), ("Monthly", "Monthly"), ("Yearly", "Yearly")]
HELP_TICKET_STATUSES = [("Open", "Open"), ("In Progress", "In Progress"), ("Closed", "Closed")]

# -----------------------------------------------------------------------------
# LEAVE ROUTING FILE
# -----------------------------------------------------------------------------
LEAVE_ROUTING_FILE = str(BASE_DIR / "apps" / "users" / "data" / "leave_routing.json")

# -----------------------------------------------------------------------------
# PERMISSION SYSTEM SETTINGS
# -----------------------------------------------------------------------------
PERMISSION_DENIED_REDIRECT = "dashboard:home"
PERMISSION_DEBUG_ENABLED = env_bool("PERMISSION_DEBUG_ENABLED", DEBUG and not ON_RENDER)

# -----------------------------------------------------------------------------
# CELERY
# -----------------------------------------------------------------------------
try:
    from celery.schedules import crontab  # type: ignore
except Exception:  # pragma: no cover
    def crontab(*args, **kwargs):  # type: ignore
        return {"__crontab__": True, "args": args, "kwargs": kwargs}

ENABLE_CELERY_EMAIL = env_bool("ENABLE_CELERY_EMAIL", False)

def _redis_db(url: str, db: int) -> str:
    u = (url or "").strip()
    if not u:
        return u
    parts = u.rsplit("/", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return f"{parts[0]}/{db}"
    if u.endswith("/"):
        return f"{u}{db}"
    return f"{u}/{db}"

REDIS_URL_FOR_CELERY = os.getenv("REDIS_URL", "").strip()

CELERY_BROKER_URL = (
    os.getenv("CELERY_BROKER_URL", "").strip()
    or (_redis_db(REDIS_URL_FOR_CELERY, 0) if REDIS_URL_FOR_CELERY else "redis://127.0.0.1:6379/0")
)
CELERY_RESULT_BACKEND = (
    os.getenv("CELERY_RESULT_BACKEND", "").strip()
    or (_redis_db(REDIS_URL_FOR_CELERY, 1) if REDIS_URL_FOR_CELERY else "redis://127.0.0.1:6379/1")
)

CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE
CELERY_ENABLE_UTC = True
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 30 * 60
CELERY_TASK_SOFT_TIME_LIMIT = 60
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

CELERY_BEAT_SCHEDULE = {
    "pre10am_unblock_and_generate_0955": {
        "task": "apps.tasks.tasks.run_pre10am_unblock_and_generate",
        "schedule": crontab(hour=9, minute=55),
    },
    "tasks_due_today_10am_fanout": {
        "task": "apps.tasks.tasks.send_due_today_assignments",
        "schedule": crontab(hour=10, minute="0-10/5"),
    },
    "delegation_reminders_every_5_minutes": {
        "task": "apps.tasks.tasks.dispatch_delegation_reminders",
        "schedule": crontab(minute="*/5"),
    },
    "generate_recurring_checklists_hourly": {
        "task": "apps.tasks.tasks.generate_recurring_checklists",
        "schedule": crontab(minute=15, hour="*/1"),
        "args": (),
    },
    "audit_recurring_health_daily": {
        "task": "apps.tasks.tasks.audit_recurring_health",
        "schedule": crontab(hour=2, minute=30),
    },
    "daily_employee_pending_digest_7pm_mon_sat": {
        "task": "apps.tasks.pending_digest.send_daily_employee_pending_digest",
        "schedule": crontab(hour=19, minute=0, day_of_week="1-6"),
    },
    "daily_admin_all_pending_digest_7pm_mon_sat": {
        "task": "apps.tasks.pending_digest.send_admin_all_pending_digest",
        "schedule": crontab(hour=19, minute=0, day_of_week="1-6"),
    },
}

# -----------------------------------------------------------------------------
# REIMBURSEMENT CONTENT RULES
# -----------------------------------------------------------------------------
REIMBURSEMENT_ALLOWED_EXTENSIONS = env_list(
    "REIMBURSEMENT_ALLOWED_EXTENSIONS",
    ".jpg,.jpeg,.png,.pdf,.xls,.xlsx",
)
REIMBURSEMENT_MAX_RECEIPT_MB = env_int("REIMBURSEMENT_MAX_RECEIPT_MB", 8)
