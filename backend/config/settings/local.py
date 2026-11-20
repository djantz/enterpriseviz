from .base import *
from .base import env, configure_databases

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#debug
DEBUG = True

# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
# .envs/.local ships the var empty; treat blank as unset so local dev always
# has a working key while a real value in .env.django still takes effect
SECRET_KEY = (env("DJANGO_SECRET_KEY", default="").strip("'\" ")
              or "3wneAnw6zljumgH1mbWBEgPOlls2U02p3Hgduu82reVJFkVpfY12KA6qgkKMn9uL")
# https://docs.djangoproject.com/en/dev/ref/settings/#allowed-hosts
ALLOWED_HOSTS = ["localhost", "0.0.0.0", "127.0.0.1"]
URL_PREFIX = None
# DATABASES
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#databases
DATABASES = configure_databases({"default": env.db("DATABASE_URL")})
# https://docs.djangoproject.com/en/stable/ref/settings/#std:setting-DEFAULT_AUTO_FIELD
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# CACHES
# ------------------------------------------------------------------------------
# Matches production: the database, so the credential handoff between the web
# process and run_worker is exercised the same way in development.
# Requires `manage.py createcachetable`.
# https://docs.djangoproject.com/en/dev/ref/settings/#caches
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "django_cache",
        # See production.py: the default ceiling of 300 culls by key order and
        # would take credential tokens out from under a running refresh.
        "OPTIONS": {"MAX_ENTRIES": 50000},
    }
}

# WhiteNoise
# ------------------------------------------------------------------------------
# http://whitenoise.evans.io/en/latest/django.html#using-whitenoise-in-development
INSTALLED_APPS = ["whitenoise.runserver_nostatic"] + INSTALLED_APPS

# https://django-debug-toolbar.readthedocs.io/en/latest/installation.html#internal-ips
INTERNAL_IPS = ["127.0.0.1", "10.0.2.2"]
if env("USE_DOCKER") == "yes":
    import socket

    hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
    INTERNAL_IPS += [".".join(ip.split(".")[:-1] + ["1"]) for ip in ips]

# django-extensions
# ------------------------------------------------------------------------------
# https://django-extensions.readthedocs.io/en/latest/installation_instructions.html#configuration
INSTALLED_APPS += ["django_extensions"]

# BACKGROUND JOBS
# ------------------------------------------------------------------------------
# Poll faster in development so a queued job starts more or less immediately.
JOB_POLL_INTERVAL = env.float("JOB_POLL_INTERVAL", default=1.0)

# Your stuff...
# ------------------------------------------------------------------------------
# Dev-only default for encrypting the temporary credential cache; override via
# .env.django (blank/quoted-empty values there are treated as unset). In DEBUG
# CredentialManager can also derive a key from SECRET_KEY, so this default
# exists only to silence that derivation warning locally.
CREDENTIAL_ENCRYPTION_KEY = (env("CREDENTIAL_ENCRYPTION_KEY", default="").strip("'\" ")
                             or "jEOgHEl4Q11XJ1xK7DokYZG5LRhDjErs76uuMjWwFTs=")
