# Deploying EnterpriseViz with Docker Compose

Docker Compose runs EnterpriseViz for development and for production. Both
compose files start the same two processes against PostgreSQL, and differ in the
web server, where configuration comes from, and whether the database is a
container.

| Component        | `local.yml`                          | `production.yml`                     |
|------------------|--------------------------------------|--------------------------------------|
| Web process      | `runserver`, container 8000 → host 80 | waitress, container 8080 → host 8080 |
| Worker           | `run_worker` under watchfiles        | `run_worker`                         |
| PostgreSQL       | `postgres:15` container              | an existing server                   |
| Settings module  | `config.settings.local`              | `config.settings.production`         |
| Configuration    | `.envs/.local/backend/`              | the `environment:` block in the file |
| Application code | `./backend` mounted, reloads on edit | copied into the image                |

The job queue is a PostgreSQL table and the cache is another, so neither stack
needs a broker or a Redis instance.

Windows Server + IIS is the other production option; see
[../iis/README.md](../iis/README.md).

---

## Prerequisites

Docker with Compose v2 — every command here uses `docker compose`, spelled as
two words. Python, PostgreSQL and the dependencies live in the images.

ArcGIS sign-in is configured from inside the running application under
**Settings > ArcGIS Sign-In**, so registering an OAuth application is not needed
before the first start. When you do register one, its redirect URI is the site
URL plus `/enterpriseviz/oauth/complete/arcgis/`, which for the development
stack is:

```
http://localhost/enterpriseviz/oauth/complete/arcgis/
```

See [Registering ArcGIS applications](../../README.md#registering-arcgis-applications).

---

## Development

### Environment files

Two env files, both read by `local.yml`.

**`.envs/.local/backend/.postgres`**

```ini
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=enterpriseviz
POSTGRES_USER=<any name except "postgres">
POSTGRES_PASSWORD=<password>
```

The container entrypoint assembles `DATABASE_URL` from these five, so set them
and leave `DATABASE_URL` alone. Pick a `POSTGRES_USER` other than `postgres`;
the backup script refuses to run as the superuser.

**`.envs/.local/backend/.django`**

```ini
DJANGO_SETTINGS_MODULE=config.settings.local
USE_DOCKER=yes
IPYTHONDIR=/app/.ipython

DJANGO_SECRET_KEY=<50+ random characters>
DJANGO_ADMIN_URL=admin/

# Read by `createsuperuser --noinput`. The password must clear the validators
# in base.py, including a 12-character minimum.
DJANGO_SUPERUSER_USERNAME=
DJANGO_SUPERUSER_PASSWORD=
DJANGO_SUPERUSER_EMAIL=

USE_SERVICE_USAGE_REPORT=True

CREDENTIAL_ENCRYPTION_KEY=<Fernet key>

# Leave unset to verify against the image's trust store. Mount a CA and give
# the path here for an Enterprise portal using an internal CA:
# ARCGIS_VERIFY_TLS=/etc/ssl/certs/my-enterprise-ca.pem
ARCGIS_VERIFY_TLS=
```

`DJANGO_SETTINGS_MODULE` is set here because `manage.py`, `wsgi.py` and
`asgi.py` all default to `config.settings.production`; development opts in to
local settings through this file.

`CREDENTIAL_ENCRYPTION_KEY` must be the exact output of `Fernet.generate_key()`.
A blank value is treated as unset, and `local.py` then falls back to a fixed
development key. That is fine on a laptop; generate a real one for any
environment holding portal credentials you care about:

```bash
docker compose -f local.yml run --rm django python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

The repository README's [Configuration](../../README.md#configuration) section
lists every variable with its default.

### Start the stack

```bash
docker compose -f local.yml up -d
```

The `django` container runs `migrate` and `createcachetable` before starting the
server, and the worker waits on its healthcheck so both tables exist before it
claims anything. Browse to `http://localhost/enterpriseviz/`.

`createcachetable` matters beyond caching: the web process hands encrypted
portal credentials to the worker through the `django_cache` table, so a refresh
that prompts for credentials depends on it.

### First sign-in

No start script creates an administrator, and there is no way to sign in until
one exists. Fill in `DJANGO_SUPERUSER_USERNAME`, `DJANGO_SUPERUSER_PASSWORD` and
`DJANGO_SUPERUSER_EMAIL` in `.django` — the password needs 12 characters or more
— then:

```bash
docker compose -f local.yml run --rm django python manage.py createsuperuser --noinput
```

To be prompted for the values instead, leave those variables empty and drop
`--noinput`.

Sign in at `http://localhost/enterpriseviz/login/` with that username and
password. ArcGIS sign-in, once configured, creates accounts without staff status
— they can browse but not administer — so promoting them is a separate step. See
[First sign-in](../../README.md#first-sign-in).

From here, [Usage](../../README.md#usage) in the repository README covers adding
a portal and refreshing its data.

### Working in the stack

Run any management command through `run --rm`:

```bash
docker compose -f local.yml run --rm django python manage.py <command>
docker compose -f local.yml run --rm django python manage.py test
```

Watch the two processes:

```bash
docker compose -f local.yml logs -f django
docker compose -f local.yml logs -f worker
```

Both containers mount `./backend` as `/app`, so edits land immediately.
`runserver` reloads on change, and the worker runs under `watchfiles`, which
restarts it when anything under `app/` or `config/` changes. The worker finishes
in-flight jobs on SIGINT before exiting, so a reload leaves a running refresh
alone.

Static assets are the exception. `local.py` prepends
`whitenoise.runserver_nostatic`, so WhiteNoise serves `/static` from
`STATIC_ROOT` the same way it does in production, and edits to `app/static/`
appear once collected:

```bash
docker compose -f local.yml run --rm django python manage.py collectstatic --noinput
docker compose -f local.yml restart django
```

Stop the stack:

```bash
docker compose -f local.yml down        # keeps the database volume
docker compose -f local.yml down -v     # deletes it
```

The worker gets a 60-second stop grace period to land on a clean job boundary.
Anything cut short sits in RUNNING until `JOB_HEARTBEAT_TIMEOUT` (default 300s)
passes, after which the next worker requeues it.

### Migrations

Generate migrations here, in development, and commit them. Both start scripts
run `migrate` alone.

```bash
docker compose -f local.yml run --rm django python manage.py makemigrations

# Models and migrations agree when this prints "No changes detected":
docker compose -f local.yml run --rm django python manage.py makemigrations --check --dry-run
```

### Database backups

The `postgres` image carries three scripts on its `PATH`:

```bash
docker compose -f local.yml exec postgres backup
docker compose -f local.yml exec postgres backups          # list them
docker compose -f local.yml exec postgres restore <filename>
```

Backups land in the `enterpriseviz_local_postgres_data_backups` volume, which
survives `down` and is removed by `down -v`.

---

## Production

`production.yml` builds one image from `compose/production/django/Dockerfile`
and runs it twice: waitress serving `config.wsgi:application` on port 8080, and
`manage.py run_worker`. Both carry `restart: unless-stopped`. There is no
database container — production expects an existing PostgreSQL server.

### Build the image

`production.yml` names the image `<imageName>`. Set it to your registry path
first, then:

```bash
docker compose -f production.yml build
docker compose -f production.yml up -d
```

Build with `--build-arg ENABLE_AZURE_SSH=1` only for Azure App Service. It
installs sshd on port 2222 with a documented root password, which App Service
reaches over an internal tunnel. Every other target should leave it off, and
`production.yml` never publishes that port.

### Environment variables

`production.yml` passes configuration through an `environment:` block rather
than an env file. It forwards `DJANGO_SECRET_KEY`, `DJANGO_SECURE_SSL_REDIRECT`,
`DJANGO_SETTINGS_MODULE`, `DJANGO_ADMIN_URL`, `DJANGO_ALLOWED_HOSTS`,
`DJANGO_CSRF_TRUSTED_ORIGINS`, `DJANGO_USE_X_FORWARDED_PROTO`,
`USE_SERVICE_USAGE_REPORT` and a set of `AZURE_POSTGRESQL_*` variables from the
shell that runs `docker compose`. Forwarding a variable is not the same as
setting one: each still needs a value, and an unset variable expands to empty.

Add these before the stack will start or behave correctly:

| Variable | Why |
|---|---|
| `DATABASE_URL` | `production.py` reads this, and `production.yml` does not forward it. The `AZURE_POSTGRESQL_*` variables in the file are not read by any setting, and the production entrypoint does not assemble a URL from them the way the development one does. |
| `DJANGO_WEBHOOK_URL` | Read with no default and not forwarded, so the container raises `ImproperlyConfigured` without it. |
| `CREDENTIAL_ENCRYPTION_KEY` | Not forwarded. Defaults to `None`, leaving credential-prompting refreshes with no key to encrypt the handoff. |
| `TRUSTED_PROXY_COUNT` | Not forwarded. Only needed behind a proxy — see below. |

And give these forwarded variables a value:

| Variable | Why |
|---|---|
| `DJANGO_ALLOWED_HOSTS` | Empty falls back to `localhost` and `127.0.0.1`, which rejects every other hostname with a 400. |
| `DJANGO_CSRF_TRUSTED_ORIGINS` | Scheme and host of the site, including a non-default port. Required whenever a proxy terminates TLS, or every form submission fails CSRF. |
| `DJANGO_USE_X_FORWARDED_PROTO` | Leave `False` unless a proxy overwrites the header. Left `False` *behind* a TLS-terminating proxy, `SECURE_SSL_REDIRECT` never sees a secure request and the site redirects to itself forever. |

See [TLS and the reverse proxy](#tls-and-the-reverse-proxy) for what
`DJANGO_USE_X_FORWARDED_PROTO` and `TRUSTED_PROXY_COUNT` each require of the
proxy before they are safe to turn on.

ArcGIS sign-in is stored in the database, so no container needs the OAuth
secret in its environment. See
[Registering ArcGIS applications](../../README.md#registering-arcgis-applications).

### Static files

Static files must be collected as part of the build or the start-up. Add this to
`compose/production/django/start`, above the waitress line:

```bash
python /app/manage.py collectstatic --noinput
```

Nothing does this today: neither the Dockerfile nor the production start script
runs `collectstatic`, and `backend/staticfiles/` is git-ignored, so a clean clone
builds an image with no static manifest. `production.py` uses WhiteNoise's
`CompressedManifestStaticFilesStorage`, which raises instead of falling back, and
every page then returns 500 with `Missing staticfiles manifest entry`.

Once the manifest exists WhiteNoise serves `/static` from inside Django, so
nothing in front needs a static handler.

### TLS and the reverse proxy

waitress serves plain HTTP on 8080. Terminate TLS at a reverse proxy and give
Django the two settings that go with that arrangement:

```ini
DJANGO_SECURE_SSL_REDIRECT=False
DJANGO_CSRF_TRUSTED_ORIGINS=https://enterpriseviz.example.org
```

The first stops a redirect loop, since Django sees plain HTTP on the container
port. The second is what CSRF falls back to when the browser's origin and
Django's computed origin disagree.

`production.py` sets `SESSION_COOKIE_SECURE` and `CSRF_COOKIE_SECURE`, so a
browser sends neither cookie over plain HTTP. Serving the site itself over HTTP
makes sign-in post successfully and bounce straight back to the sign-in page,
with nothing logged, because the session never persisted.

A proxy that sets `X-Forwarded-Proto` lets `SECURE_SSL_REDIRECT` and HSTS stay
on instead. Two things are needed, and neither works without the other:

```ini
DJANGO_USE_X_FORWARDED_PROTO=True
```

and a proxy rule that **overwrites** `X-Forwarded-Proto` on every request rather
than forwarding whatever arrived. Django cannot tell the two apart, so a proxy
that passes the client's header through lets any caller declare its own request
secure — satisfying `SECURE_SSL_REDIRECT`, collecting an HSTS header, and making
`build_absolute_uri()` (which composes the OAuth `redirect_uri`) write `https`.
The setting defaults to off for that reason.

The same applies to `X-Forwarded-For`. Set `TRUSTED_PROXY_COUNT` to the number
of proxies that append to it — `1` behind a single reverse proxy — so the
sign-in throttle and `LogEntry.client_ip` read the address your proxy saw. At
the default of `0` the header is ignored and the peer address is used, which is
right when the container is reached directly. Every entry you claim to trust is
one an attacker can supply.

### First administrator

No start script creates an administrator, and `production.yml` passes no
`DJANGO_SUPERUSER_*` variables, so create one interactively once the stack is
up:

```bash
docker compose -f production.yml run --rm django python manage.py createsuperuser
```

Sign in with it at `/enterpriseviz/login/`. Accounts created by ArcGIS sign-in
arrive without staff status and have to be promoted in the Django admin before
they can administer anything. See
[First sign-in](../../README.md#first-sign-in).

### Backups

`production.yml` has no database container and no backup scripts, so the
database is backed up by whatever covers your PostgreSQL server. Store
`DJANGO_SECRET_KEY` and `CREDENTIAL_ENCRYPTION_KEY` with every dump you intend
to restore — a database restored under a different secret key has unreadable
portal credentials. See [Encryption keys](../../README.md#encryption-keys).

---

## When something is wrong

| Symptom | Cause and fix |
|---|---|
| **Jobs sit at QUEUED forever** | The worker is down or crash-looping. Check `logs worker`. |
| **Jobs stuck at RUNNING** | The worker died mid-job. It recovers once the heartbeat passes `JOB_HEARTBEAT_TIMEOUT`: refreshes are requeued up to `JOB_MAX_ATTEMPTS`, and portal-modifying tools are failed instead of rerun. |
| **Credential-prompt refreshes fail** | The `django_cache` table is missing. Run `manage.py createcachetable`. |
| **The worker crash-loops on startup** | It started before `migrate` created `app_job`. `depends_on` waits for the django container's healthcheck; check that container first. |
| **`ImproperlyConfigured`, e.g. `DJANGO_SECRET_KEY`** | The variable is absent, or the file was added after the containers started. Recreate them with `up -d --force-recreate`. |
| **500 on every page, `Missing staticfiles manifest entry`** | `collectstatic` has not run. See [Static files](#static-files). |
| **No ArcGIS button on the login page** | No organization URL is configured. Sign in with the superuser and set it under Settings > ArcGIS Sign-In. |
| **Login bounces back to login, no error** | Secure cookies over plain HTTP. See [TLS and the reverse proxy](#tls-and-the-reverse-proxy). |
| **Port 80 already in use** | Change the `django` port mapping in `local.yml`, set `DJANGO_CSRF_TRUSTED_ORIGINS` to match, and update the portal's registered redirect URI. |
| **Portal connections fail on TLS** | The portal's certificate does not chain to a root in the image's trust store. Mount the issuing CA and point `ARCGIS_VERIFY_TLS` at it. |
| **Static changes do not show in development** | Run `collectstatic` and restart the `django` container. |
| Anything else | The in-app log viewer at `/enterpriseviz/logs/`, which carries both web and worker logs tied together by request id. |
