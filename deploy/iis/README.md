# Deploying EnterpriseViz on Windows Server + IIS

EnterpriseViz runs on a single Windows host. IIS serves the web process, a
scheduled task runs the worker, and PostgreSQL sits on this host or another.

```
IIS  (site: EnterpriseViz, app pool: AlwaysRunning, idleTimeout=0)
  ├─ TLS termination
  └─ HttpPlatformHandler ──spawns──▶ waitress ──▶ config.wsgi:application
                                     127.0.0.1:%HTTP_PLATFORM_PORT%

Windows Task Scheduler ("At startup", restart on failure)
  └─ python manage.py run_worker      background jobs + schedules

PostgreSQL
  └─ app tables + app_job (job queue) + django_cache + PeriodicTask (schedules)
```

Two things stay running: the IIS application pool and one scheduled task. The
job queue is a PostgreSQL table, so there is no message broker to install.

The worker runs outside IIS so that an app-pool recycle cannot interrupt a
running portal refresh, and so scheduled refreshes still run when nobody is
browsing.

**Conventions used below.** Application root `D:\apps\enterpriseviz`, logs
`D:\logs\enterpriseviz`, service account `DOMAIN\svc_enterpriseviz`, hostname
`enterpriseviz.example.org`. Change these consistently; the `web.config` in this
directory carries the same paths.

Run every PowerShell block in this document from an elevated session.

---

## 1. Prerequisites

| Component               | Notes                                                                |
|-------------------------|----------------------------------------------------------------------|
| Python 3.11 (64-bit)    | See the note below.                                                  |
| IIS                     | With the Application Initialization feature, used for preload.       |
| HttpPlatformHandler 1.2 | Starts and supervises the Python process. The only IIS extension this deployment requires. [Download](https://www.iis.net/downloads/microsoft/httpplatformhandler) |
| PostgreSQL              | Reachable from this host, on it or elsewhere.                        |
| Service account         | Runs both the app pool and the worker task. See [Service account](#8-service-account). |

Python 3.11 is the version the container images use, so behaviour matches what
is tested; `arcgis` 2.4.2 accepts 3.10 through 3.13. Install it for all users
and tick "Add python.exe to PATH". Use a standalone Python — installing into
ArcGIS Pro's conda environment can break Pro.

Enable the IIS features. On Windows Server this is `Install-WindowsFeature`;
`Enable-WindowsOptionalFeature` is the client-OS equivalent and uses different
feature names:

```powershell
Install-WindowsFeature -Name `
    Web-Server, Web-AppInit, Web-Http-Errors, Web-Http-Logging, `
    Web-Filtering, Web-Static-Content, Web-Mgmt-Console
```

Install HttpPlatformHandler from the link above, then confirm it registered:

```powershell
Get-WebGlobalModule | Where-Object Name -eq 'httpPlatformHandler'
```

Empty output means it is unavailable to IIS, and the `<httpPlatform>` section in
`web.config` will be an unrecognised section — IIS starts no Python process and
serves a 500.19 for every request. An MSI that appears to succeed but registers
nothing usually ran against a machine where the IIS role was added afterwards;
reinstall it.

> **Why HttpPlatformHandler.** It treats Python as an ordinary reverse-proxied
> process: IIS owns its lifetime, restarts it, and forwards requests. waitress
> therefore runs unchanged, and nothing in the application has to know it is
> behind IIS.

---

## 2. Application files and virtual environment

```powershell
New-Item -ItemType Directory -Force D:\apps\enterpriseviz, D:\logs\enterpriseviz

# Copy the repository to D:\apps\enterpriseviz, then:
Set-Location D:\apps\enterpriseviz
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r backend\requirements\production.txt
```

The install pulls in `arcgis`, which brings numpy and pandas — expect a few
minutes and roughly 1.5 GB.

`tzdata` comes in with those requirements and is required on Windows. Windows
has no system time zone database, so without it `zoneinfo` cannot resolve
`TIME_ZONE = "America/Los_Angeles"` and Django raises `ZoneInfoNotFoundError`
before serving anything.

Confirm the stack imports:

```powershell
.\.venv\Scripts\python.exe -c "import waitress, django, arcgis, django_celery_beat; print('ok')"
```

> **If that fails with `OSError: Could not find KfW installation`**, see
> [Kerberos / KfW](troubleshooting.md#kerberos--kfw-oserror-could-not-find-kfw-installation).
> It is common on a host that also runs ArcGIS Server or Portal for ArcGIS, and
> it breaks `manage.py`, the web application and the worker alike.

---

## 3. Configuration

Create `D:\apps\enterpriseviz\.env`, **one level above the site directory**.
That is the only place `config/settings/base.py` looks, and it reads it without
needing an environment variable set first, so a Scheduled Task, a `cmd` prompt
and your own shell all behave the same way.

> **Why above the site directory, and only there.** IIS is pointed at
> `backend\` as the site's physical path, so a `.env` inside it would sit under
> the web root, where several pieces of configuration have to stay correct to
> keep it from being served. The file holds the database password, the ArcGIS
> client secret and the `SECRET_KEY` that decrypts every stored portal
> credential. There is no in-`backend` fallback, so a file put there is silently
> ignored rather than quietly working — if settings appear unset, check the path
> first.

```ini
DJANGO_SECRET_KEY=<50+ random characters>
DJANGO_ALLOWED_HOSTS=enterpriseviz.example.org
DJANGO_CSRF_TRUSTED_ORIGINS=https://enterpriseviz.example.org
DJANGO_ADMIN_URL=some-non-obvious-path/
DJANGO_WEBHOOK_URL=webhook/

DATABASE_URL=postgres://user:password@dbhost:5432/enterpriseviz

CREDENTIAL_ENCRYPTION_KEY=<Fernet key, see below>

# IIS terminates TLS and forwards over plain HTTP on loopback, so Django cannot
# see the original scheme. This pairs with CSRF_TRUSTED_ORIGINS above, which
# must carry the scheme and any non-default port. See section 6.
DJANGO_SECURE_SSL_REDIRECT=False

# Leave both of these alone under the layout in this guide.
#
# HttpPlatformHandler forwards the client's headers as it received them and adds
# none of its own, so an X-Forwarded-Proto or X-Forwarded-For arriving here came
# from the client and is worth exactly nothing. Turning either on without a
# proxy rule that OVERWRITES the header lets a caller declare its own request
# secure, and pick its own address for the sign-in throttle and the audit log.
#
# DJANGO_USE_X_FORWARDED_PROTO=True   # only with the URL Rewrite rule in section 6
# TRUSTED_PROXY_COUNT=1               # only with ARR, see the appendix

# TLS verification for every connection to a portal. Python does not read the
# Windows certificate store, so a portal issued by an internal CA needs the
# issuing CA as a PEM bundle here even when the host itself trusts it:
#   ARCGIS_VERIFY_TLS=D:\apps\enterpriseviz\internal-ca.pem
ARCGIS_VERIFY_TLS=

# Optional tuning — defaults shown
# JOB_CONCURRENCY=2
# JOB_BATCH_CONCURRENCY=8
# JOB_POLL_INTERVAL=3.0
# JOB_HEARTBEAT_TIMEOUT=300
# JOB_MAX_ATTEMPTS=3
```

The repository README's [Configuration](../../README.md#configuration) section
lists every variable with its default, and the settings that are stored in the
database and edited from inside the application.

Generate the two keys:

```powershell
.\.venv\Scripts\python.exe -c "from django.core.management.utils import get_random_secret_key as k; print(k())"
.\.venv\Scripts\python.exe -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> Set both keys once and back them up with the database. `DJANGO_SECRET_KEY`
> encrypts every stored credential as well as signing sessions, so rotating it
> makes portal passwords, OAuth tokens and the webhook secret unreadable.
> `CREDENTIAL_ENCRYPTION_KEY` must be the exact output of
> `Fernet.generate_key()`: 32 bytes, url-safe base64. See
> [Encryption keys](../../README.md#encryption-keys).

Lock the file down. Both the app-pool identity and the worker task read it, so
the service account needs read access explicitly:

```powershell
icacls D:\apps\enterpriseviz\.env /inheritance:r `
    /grant:r "Administrators:(R)" "SYSTEM:(R)" "DOMAIN\svc_enterpriseviz:(R)"
```

Every process reads `.env` once at startup. After editing it, restart the app
pool and the worker task, as in
[Redeploying a new version](#10-redeploying-a-new-version).

---

## 4. Initialise the database

```powershell
Set-Location D:\apps\enterpriseviz\backend
$env:DJANGO_SETTINGS_MODULE = "config.settings.production"

..\.venv\Scripts\python.exe manage.py migrate
..\.venv\Scripts\python.exe manage.py createcachetable
..\.venv\Scripts\python.exe manage.py collectstatic --noinput
..\.venv\Scripts\python.exe manage.py createsuperuser
```

`createcachetable` creates the table the cache backend uses. The cache is how
the web process hands encrypted portal credentials to the worker: a refresh that
prompts for credentials stores them encrypted in `django_cache` and passes the
background job only a token. Without the table, those refreshes fail.

`createsuperuser` is the only way in to a new deployment. The password needs 12
characters or more. Accounts created later by ArcGIS sign-in arrive without
staff status and have to be promoted in the Django admin before they can
administer anything — see [First sign-in](../../README.md#first-sign-in).

Migrations are committed to the repository. Run `migrate` to apply them; do not
generate new ones here.

> **Upgrading an existing deployment.** Run `migrate` while the current `.env`
> is still in place. Migration `0032` reads `SOCIAL_AUTH_ARCGIS_URL`,
> `SOCIAL_AUTH_ARCGIS_KEY`, `SOCIAL_AUTH_ARCGIS_SECRET` and `ARCGIS_USER_ROLE`
> from the environment once and writes them to the database, after which they
> are edited under Settings > ArcGIS Sign-In. Those four lines can then be
> deleted from `.env`, along with `WEBHOOK_SECRET`, `REDIS_URL`, `CACHE_URL`
> and `DJANGO_ADMIN_FORCE_ALLAUTH`, none of which are read any more.
>
> `WEBHOOK_SECRET` needs no migration: nothing ever read it. The setting was
> defined in `base.py` but the webhook has always authenticated against
> `SiteSettings.webhook_secret`, which is edited under Settings > Webhook.

---

## 5. Prove Python works before involving IIS

Running the stack by hand first separates application failures from IIS ones.

```powershell
Set-Location D:\apps\enterpriseviz\backend
$env:DJANGO_SETTINGS_MODULE = "config.settings.production"

..\.venv\Scripts\python.exe -m waitress `
    --listen=127.0.0.1:8000 --threads=16 --channel-timeout=1200 `
    config.wsgi:application
```

This is the command IIS will run, with a fixed port instead of
`%HTTP_PLATFORM_PORT%`. A failure here points at settings, the database or the
virtual environment, and the traceback in the console says which.

> **Keep `DJANGO_SECURE_SSL_REDIRECT=False` in `.env` for this check.** With it
> on and no TLS in front of anything yet, Django answers a plain-HTTP request
> with `301 Moved Permanently → https://127.0.0.1:8000/`, and nothing serves
> HTTPS on that port. Browsers cache a 301, so after the setting is fixed the
> browser keeps redirecting on its own; test in a private window, or clear the
> cached redirect for that host.

Verify without a browser in the way:

```powershell
Invoke-WebRequest -Uri http://127.0.0.1:8000/enterpriseviz/ -MaximumRedirection 0 `
    -SkipHttpErrorCheck | Select-Object StatusCode, @{n='Location';e={$_.Headers.Location}}
```

- `302` to `/enterpriseviz/login/?next=...` — **correct.** The application is
  serving and sending you to sign in.
- `301` to `https://...` — `DJANGO_SECURE_SSL_REDIRECT` is still on.

`Ctrl+C` to stop waitress.

Use waitress for this check. `runserver` refuses to serve static files when
`DEBUG=False`, so the page would render unstyled and send you after a second
problem that does not exist.

---

## 6. IIS site

Copy `deploy\iis\web.config` to `D:\apps\enterpriseviz\backend\` and correct the
three `D:\` paths in it. The site's physical path is the **backend** directory,
because that is where `config/wsgi.py` is importable from.

```powershell
Import-Module WebAdministration

New-WebAppPool -Name "EnterpriseViz"
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name managedRuntimeVersion -Value ""
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name startMode -Value AlwaysRunning
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name processModel.idleTimeout -Value "00:00:00"
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name recycling.periodicRestart.time -Value "00:00:00"
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name processModel.identityType -Value SpecificUser
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name processModel.userName -Value "DOMAIN\svc_enterpriseviz"
Set-ItemProperty IIS:\AppPools\EnterpriseViz -Name processModel.password -Value "<password>"

New-Website -Name "EnterpriseViz" `
    -PhysicalPath "D:\apps\enterpriseviz\backend" `
    -ApplicationPool "EnterpriseViz" `
    -HostHeader "enterpriseviz.example.org" -Port 443 -Ssl

New-NetFirewallRule -DisplayName "EnterpriseViz HTTPS" `
    -Direction Inbound -Protocol TCP -LocalPort 443 -Action Allow
```

Bind the TLS certificate (IIS Manager, or `New-WebBinding` plus
`netsh http add sslcert` for the thumbprint), then enable preload on the site's
root application:

```powershell
Set-WebConfigurationProperty -PSPath 'MACHINE/WEBROOT/APPHOST' `
    -Filter "system.applicationHost/sites/site[@name='EnterpriseViz']/application[@path='/']" `
    -Name preloadEnabled -Value True
```

If that filter is fiddly to get right, IIS Manager is equivalent and less error
prone: *Sites → EnterpriseViz → Advanced Settings → Preload Enabled → True*.

Each of those app-pool settings matters:

| Setting               | Value           | Why                                                                                                   |
|-----------------------|-----------------|-------------------------------------------------------------------------------------------------------|
| .NET CLR version      | No Managed Code | Nothing .NET is hosted.                                                                               |
| Start Mode            | AlwaysRunning   | Otherwise nothing starts until the first request.                                                     |
| Idle Time-out         | `0`             | The default 20 minutes kills the Python process; the next visitor then waits out a cold `import arcgis`. |
| Regular Time Interval | `0`             | Disables the default 29-hour recycle.                                                                 |
| Identity              | Domain account  | See [Service account](#8-service-account).                                                            |
| Preload Enabled       | True            | Warms the process after a recycle or reboot.                                                          |

### Telling Django the request was HTTPS

HttpPlatformHandler forwards to `127.0.0.1` over plain HTTP, so Django cannot
see that the original request was HTTPS. Left unaddressed this causes two
things: `SECURE_SSL_REDIRECT` redirects to HTTPS forever, and CSRF rejects every
POST because the browser's `Origin` says `https://` while Django computes
`http://`.

The fix needs no additional IIS module. Both of these belong in `.env`:

```ini
DJANGO_SECURE_SSL_REDIRECT=False
DJANGO_CSRF_TRUSTED_ORIGINS=https://enterpriseviz.example.org
```

The first stops the redirect loop. The second is what CSRF falls back to when
the browser's origin and Django's computed origin disagree — it must carry the
scheme and, on a non-default port, the port.

IIS still terminates TLS, and the browser still refuses to send the Secure
session and CSRF cookies over plain HTTP, so this weakens nothing the TLS
binding provides. The cost is the HSTS header, which Django emits only on a
request it believes is secure. Add it at the IIS end if you want it.

> `manage.py check --deploy` ([Verify](#9-verify)) flags `SECURE_SSL_REDIRECT`
> and `SECURE_HSTS_SECONDS` under this arrangement. Both are expected: IIS owns
> the redirect and the header.

<details>
<summary>The URL Rewrite alternative, and when to choose it</summary>

A rewrite rule can set `HTTP_X_FORWARDED_PROTO`, which `SECURE_PROXY_SSL_HEADER`
picks up, making `request.is_secure()` correct and letting `SECURE_SSL_REDIRECT`
and HSTS stay on. Choose this if you want Django to own the redirect and emit
HSTS itself.

Both halves are required. Django ignores the header unless
`DJANGO_USE_X_FORWARDED_PROTO=True` is in `.env`, and that setting is unsafe
without the rule below — so set the two together, or neither.

Two costs. URL Rewrite becomes a second required IIS extension, and IIS will not
let a rule set a server variable unless it is allow-listed first —
**`allowedServerVariables` can only be set at server level**, because a site
granting itself server variables would be a privilege escalation. Attempting it
per-site fails with:

> This configuration section cannot be used at this path. This happens when the
> section is locked at a parent level.

So the `-PSPath` must be `MACHINE/WEBROOT/APPHOST` rather than the site:

```powershell
Add-WebConfiguration -Filter "/system.webServer/rewrite/allowedServerVariables" `
    -PSPath "MACHINE/WEBROOT/APPHOST" `
    -Value @{name="HTTP_X_FORWARDED_PROTO"}
```

Or in IIS Manager: select the **server** node, then *URL Rewrite → View Server
Variables → Add*.

That is a server-wide change affecting every site on the machine, and is often
refused outright on a shared or locked-down host — which is why the `.env`
approach above is the default here. Add a `<rewrite>` block to `web.config` only
after the variable is allow-listed; a section IIS cannot resolve makes the whole
file fail to load.

The rule must **set** the variable rather than pass one through, and it must do
so on *every* request, including the plain-HTTP ones. HttpPlatformHandler
forwards client headers as it receives them, so a rule that only forwards leaves
the value under the caller's control: a request arriving over HTTP with
`X-Forwarded-Proto: https` would satisfy `SECURE_SSL_REDIRECT`, get HSTS, and
make `build_absolute_uri()` — which composes the OAuth `redirect_uri` — write
`https`. Overwriting unconditionally is what makes the header evidence.

</details>

### Running on a non-default port, alongside another site

To keep EnterpriseViz clear of an existing site, give it **its own site and its
own application pool** on a distinct port. Adding it as an application or
virtual directory under an existing site makes it inherit that site's
`web.config`, and its `<httpPlatform>` handler would then apply to the parent's
URLs too.

Only the binding changes — `web.config` is identical, because
`%HTTP_PLATFORM_PORT%` is the *internal* loopback port IIS assigns to waitress
and is unrelated to the port users connect to.

```powershell
# HTTPS on 8001 (see the warning below)
New-Website -Name "EnterpriseViz" `
    -PhysicalPath "D:\apps\enterpriseviz\backend" `
    -ApplicationPool "EnterpriseViz" `
    -Port 8001 -Ssl

# Bind the certificate to that port
$cert = Get-ChildItem Cert:\LocalMachine\My | Where-Object Subject -match 'enterpriseviz'
New-Item -Path "IIS:\SslBindings\0.0.0.0!8001" -Value $cert

New-NetFirewallRule -DisplayName "EnterpriseViz 8001" `
    -Direction Inbound -Protocol TCP -LocalPort 8001 -Action Allow
```

Check nothing else already holds the port — including non-IIS listeners and
stale HTTP.SYS reservations, which `netstat` alone will not show:

```powershell
Get-NetTCPConnection -LocalPort 8001 -ErrorAction SilentlyContinue
netsh http show urlacl | Select-String ":8001"
Get-WebBinding | Where-Object bindingInformation -match ":8001:"
```

Then set the origin in `.env`. **`CSRF_TRUSTED_ORIGINS` must carry the scheme
and the port**; `ALLOWED_HOSTS` must not, because Django strips the port before
matching:

```ini
DJANGO_ALLOWED_HOSTS=enterpriseviz.example.org
DJANGO_CSRF_TRUSTED_ORIGINS=https://enterpriseviz.example.org:8001
```

The redirect URIs registered in ArcGIS carry the port as well, for example
`https://enterpriseviz.example.org:8001/enterpriseviz/oauth/complete/arcgis/`.
See [Registering ArcGIS applications](../../README.md#registering-arcgis-applications).

> **Serving that port over plain HTTP will look like a broken login.**
>
> `production.py` sets `SESSION_COOKIE_SECURE` and `CSRF_COOKIE_SECURE` to True,
> so over HTTP the browser accepts neither cookie. Sign-in then posts,
> succeeds, and bounces straight back to the sign-in page — with no error
> anywhere, because nothing failed. The session simply never persisted.
>
> Use HTTPS. This application stores portal administrator credentials and OAuth
> refresh tokens; plain HTTP puts a session that can read them on the wire. To
> run HTTP on an isolated network anyway, you also have to set
> `SESSION_COOKIE_SECURE = False` and `CSRF_COOKIE_SECURE = False` — and know
> that you have made that trade.

### Static files

WhiteNoise serves `/static` from inside Django, so IIS needs no static handler
and no MIME map — make sure `collectstatic` has run. Moving static serving to
IIS is a later optimisation.

---

## 7. The worker

The worker runs as a native Scheduled Task, which keeps the deployment to
Microsoft-signed IIS modules plus Python.

First grant the service account the **Log on as a batch job** right
(`SeBatchLogonRight`), through *Local Security Policy → Local Policies → User
Rights Assignment*, or by Group Policy for a domain account. Without it,
`Register-ScheduledTask` rejects the credentials, or the task registers and then
fails to start with `2147943785` / *"The user has not been granted the requested
logon type"*.

```powershell
$action = New-ScheduledTaskAction `
    -Execute 'D:\apps\enterpriseviz\.venv\Scripts\python.exe' `
    -Argument 'D:\apps\enterpriseviz\backend\manage.py run_worker' `
    -WorkingDirectory 'D:\apps\enterpriseviz\backend'

# The 1-minute delay lets the network and PostgreSQL come up first. A worker
# that starts too early exits and is restarted by the settings below, so this
# only keeps the event log quiet after a reboot.
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = 'PT1M'

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999 `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName 'EnterpriseViz Worker' `
    -Action $action -Trigger $trigger -Settings $settings `
    -User 'DOMAIN\svc_enterpriseviz' -Password (Read-Host -AsSecureString) `
    -RunLevel Limited

Start-ScheduledTask -TaskName 'EnterpriseViz Worker'
```

Three of those settings matter:

- **`-ExecutionTimeLimit Zero`** — the default stops the task after three days.
- **`-MultipleInstances IgnoreNew`** — a restart must not leave two workers.
  Two would be *safe* (the queue claim uses `FOR UPDATE SKIP LOCKED`), but they
  would double the database connections for no gain.
- **`-AtStartup`** — the worker must come back after a reboot without a login.

A Scheduled Task action has no environment-variable field, so `manage.py`
defaults `DJANGO_SETTINGS_MODULE` to `config.settings.production`. The `.env`
file supplies the rest.

Confirm it is alive:

```powershell
Get-ScheduledTask 'EnterpriseViz Worker' | Get-ScheduledTaskInfo
```

The worker logs to the application's own log table — visible in the UI at
`/enterpriseviz/logs/`, filtered to the `enterpriseviz.worker` and
`enterpriseviz.jobs` loggers.

---

## 8. Service account

Run **both** the app pool and the scheduled task as the same domain account,
and grant it:

- **Log on as a batch job**, for the [scheduled task](#7-the-worker).
- **Read** on `D:\apps\enterpriseviz`, including `.env` and `.venv`.
- **Modify** on `D:\logs\enterpriseviz`. HttpPlatformHandler fails silently when
  it cannot create its stdout log.
- **Read** on the ArcGIS Server directories share.

Read access on the share enables per-layer index tracking.
`utils.extract_msd_from_manifest` reads `.msd` files off the ArcGIS Server file
system to resolve a service's layers, and logs *"must be run on a machine with
access to the ArcGIS Server file system"* when the path is unreachable, falling
back to service manifest parsing.

Setting `processModel.userName` through the IIS APIs adds the account to
`IIS_IUSRS` for you; if you set the identity by editing
`applicationHost.config` directly, add it yourself.

---

## 9. Verify

```powershell
Set-Location D:\apps\enterpriseviz\backend
..\.venv\Scripts\python.exe manage.py check --deploy
```

Expect warnings about `SECURE_SSL_REDIRECT` and `SECURE_HSTS_SECONDS`; IIS owns
both under the arrangement in [IIS site](#6-iis-site).

Then, in order:

- [ ] `https://enterpriseviz.example.org/enterpriseviz/` loads.
- [ ] The Django superuser signs in at `/enterpriseviz/login/`.
- [ ] Sign-in through the portal completes and returns to the application. A
      first-time ArcGIS account lands read-only until it is promoted.
- [ ] No redirect loop and no mixed-content warnings.
- [ ] A POST succeeds — saving a setting is enough. This confirms
      `CSRF_TRUSTED_ORIGINS` carries the right scheme and port.
- [ ] Refresh a portal: the progress bar advances and reaches completion.
- [ ] **Refresh a portal that prompts for credentials.** This exercises the
      credential handoff from the web process to the worker through the database
      cache, and is the single most likely thing to be misconfigured.
- [ ] Create a schedule a minute out; confirm it fires exactly once.
- [ ] Run a tool (Pro License / Inactive User / Public Sharing).
- [ ] Open a service whose layers come from a `.msd` and confirm they resolve.
- [ ] Reboot. The site answers and the worker task restarts on its own.
- [ ] Recycle the app pool during a running refresh — the job still finishes,
      because the worker is independent of IIS.

---

## 10. Redeploying a new version

```powershell
Stop-ScheduledTask -TaskName 'EnterpriseViz Worker'
Stop-WebAppPool -Name 'EnterpriseViz'

# update the files in D:\apps\enterpriseviz\backend (keep .venv and .env)

Set-Location D:\apps\enterpriseviz\backend
$env:DJANGO_SETTINGS_MODULE = "config.settings.production"
..\.venv\Scripts\python.exe -m pip install -r requirements\production.txt
..\.venv\Scripts\python.exe manage.py migrate
..\.venv\Scripts\python.exe manage.py collectstatic --noinput

Start-WebAppPool -Name 'EnterpriseViz'
Start-ScheduledTask -TaskName 'EnterpriseViz Worker'
```

Stop the worker **first** and start it **last**. Stopping it lets in-flight jobs
reach a checkpoint and exit cleanly; anything still running when the process
dies is requeued by the next worker after `JOB_HEARTBEAT_TIMEOUT` (default
300s), so nothing is lost either way.

The same two commands restart both processes after an `.env` edit.

---

## Appendix: running waitress as its own service instead

The layout above has IIS own the Python process. The alternative is to run
waitress independently and put IIS in front of it as a pure reverse proxy with
[Application Request Routing](https://www.iis.net/downloads/microsoft/application-request-routing).

It is worth it if you need the web process to survive IIS restarts, or want
several waitress instances behind one site. The costs: ARR becomes a second IIS
extension, you supervise waitress yourself (another Scheduled Task, the same
shape as [The worker](#7-the-worker), on a fixed port), and you set
`X-Forwarded-Proto` on the proxy rule.

On that route waitress can interpret the header itself rather than leaving it to
Django:

```
--trusted-proxy=127.0.0.1 --trusted-proxy-headers=x-forwarded-proto
```

which sets `wsgi.url_scheme` directly, so `request.is_secure()` is correct
regardless of `SECURE_PROXY_SSL_HEADER`, and a client-supplied header from
anywhere other than the trusted proxy is discarded.

If you also configure ARR to append `X-Forwarded-For`, set `TRUSTED_PROXY_COUNT`
to the number of proxies in the chain — `1` for a single ARR instance. That is
what makes the sign-in throttle and `LogEntry.client_ip` read the address ARR
saw rather than the leftmost entry, which is whatever the client put there.
Leave it at `0` if nothing appends the header.
