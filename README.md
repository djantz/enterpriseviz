# EnterpriseViz

EnterpriseViz maps the dependencies between layers, services, web maps and apps in an ArcGIS Enterprise Portal or
an ArcGIS Online organization. It helps administrators and developers understand the relationships within
their ArcGIS ecosystem so you can see what a service feeds before you rename, republish or delete it.

## Features

* **Dependency graph** A Cytoscape.js graph on every layer, service and web map detail page, showing what the
  item depends on and what depends on it.
* **Portal and ArcGIS Online** Both register the same way, and one deployment can track several of either.
* **Two ways to deploy** Docker Compose, or Windows Server with IIS. See [Deployment](#deployment).

## Layer tracking

A service does not report which dataset sits behind each of its layers, so EnterpriseViz works it out from one of
two sources, depending on what the application host can reach.

### Layer discovery methods

1. **MSD parsing (Recommended)**
   - Reads the .msd (Map Service Definition) file ArcGIS Server writes for each map service
   - Records each layer's index within the service, so MapServer/0 and MapServer/5 stay distinct
   - Resolves the datasource of each layer
   - Needs the application host to be able to read the ArcGIS Server directories fileshare

2. **Service manifest parsing (Secondary)**
   - Reads datasources from the database connection strings in the service manifest
   - EnterpriseViz drops to this wherever it cannot read an .msd: a disconnected environment, a
     service type that has none, or a host without access to the share

    **Limitations:**
   - Datasources land at the service level, not per layer
   - Layers match by name rather than by service index
   - Two references to the same service cannot be told apart by the sublayer they use
   - Layer detail pages list every service holding that layer's datasource

### Finding layer usage

"Find Layer Usage" works under either method. Each result carries a `usage_type`:

* `specific_layer` — the item references one sublayer, e.g. MapServer/5
* `full_service` — the item references the service as a whole

Without MSD parsing every result is `full_service`, because nothing records which sublayer a reference points at.
The set of maps and apps found is the same either way.

## Architecture

EnterpriseViz runs as two processes against one PostgreSQL database.

| Process | Command                   | Role                                          |
|---------|---------------------------|-----------------------------------------------|
| Web     | `config.wsgi:application` | Serves the UI and the webhook endpoint        |
| Worker  | `manage.py run_worker`    | Drains the job queue and fires due schedules  |

PostgreSQL holds the application tables, the job queue (`app_job`) and the cache
(`django_cache`), so there is no message broker or Redis instance to run.

Both deployments run those same two processes. They differ in what starts them
and where configuration comes from.

| Component     | Docker Compose                                         | Windows + IIS                   |
|---------------|--------------------------------------------------------|---------------------------------|
| Web process   | `docker compose`                                       | IIS via HttpPlatformHandler     |
| WSGI server   | `runserver` (`local.yml`), waitress (`production.yml`) | waitress                        |
| Worker        | `docker compose`                                       | Task Scheduler, at startup      |
| Configuration | `.envs/.local/backend/`                                | `.env` beside the app root      |
| Launch files  | `compose/`, `local.yml`, `production.yml`              | `deploy/iis/web.config`         |

## Deployment

| Target | Guide |
|---|---|
| **Windows Server + IIS** — install on Windows | [deploy/iis/README.md](deploy/iis/README.md) |
| **Docker Compose** — install on Linux or Azure App Service | [deploy/docker/README.md](deploy/docker/README.md) |

Both guides assume you have filled in the [configuration](#configuration) below.
You set up ArcGIS sign-in afterwards, from inside the running application. The
Django superuser you create during deployment signs in without it.

## Configuration

Settings come from the environment, and from a `.env` file if one is present.
`config/settings/base.py` looks for `.env` in exactly one place: **one level
above the `backend` directory**. That is deliberate — `backend` is the directory
IIS serves as the site root, and the file holds the database password, the
ArcGIS client secret and the key that decrypts every stored portal credential.
Real environment variables work too, and take precedence over the file. The two
deployment guides say where to put it.

### Required

| Variable | Notes                                                                                                                    |
|---|--------------------------------------------------------------------------------------------------------------------------|
| `DJANGO_SECRET_KEY` | 50+ random characters. **Also an encryption key.** See below.                                                           |
| `DJANGO_ALLOWED_HOSTS` | Comma separated hostnames, without ports. Defaults to `localhost,127.0.0.1`.                                             |
| `DJANGO_ADMIN_URL` | Path to the Django admin, with a trailing slash. Pick something non-obvious.                                             |
| `DJANGO_WEBHOOK_URL` | Path the Portal posts webhooks to, with a trailing slash, e.g. `webhook/`.                                               |
| `DATABASE_URL` | `postgres://user:password@host:5432/dbname`. The container entrypoint assembles this from `POSTGRES_*` instead.          |
| `CREDENTIAL_ENCRYPTION_KEY` | A Fernet key. Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. |

### Optional

| Variable | Default | Notes |
|---|---|---|
| `DJANGO_CSRF_TRUSTED_ORIGINS` | *(empty)* | Scheme + host, and the port when it is not the default. Needed whenever a proxy terminates TLS. |
| `DJANGO_SECURE_SSL_REDIRECT` | `True` | Set to `False` when the front-end web server owns the HTTPS redirect. |
| `DJANGO_USE_X_FORWARDED_PROTO` | `False` | Trust `X-Forwarded-Proto` to decide whether a request was HTTPS. Turn on **only** where a proxy rule overwrites that header; otherwise any client can send it and be treated as secure. |
| `TRUSTED_PROXY_COUNT` | `0` | How many reverse proxies in front of this deployment append to `X-Forwarded-For`. `0` ignores the header and uses the peer address. Each proxy you claim is one entry an attacker gets to choose, so set it to the number you actually run. |
| `DJANGO_SECURE_HSTS_SECONDS` | `31536000` | Also `DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS` and `DJANGO_SECURE_HSTS_PRELOAD`. |
| `DJANGO_SECURE_CONTENT_TYPE_NOSNIFF` | `True` | |
| `CONN_MAX_AGE` | `60` | Seconds a database connection is reused. |
| `DJANGO_READ_DOT_ENV_FILE` | `True` | Whether to read the `.env` file described above. Set to `False` where configuration is injected as real environment variables only. |
| `ARCGIS_VERIFY_TLS` | verify | `True`/`False`, or a path to a CA bundle. Point it at the issuing CA for a portal using an internal CA, because Python does not read the Windows certificate store. |
| `USE_SERVICE_USAGE_REPORT` | `True` | Real-time service usage graphs on detail pages. |
| `CREDENTIAL_TOKEN_TTL` | `900` | Seconds a prompted portal credential stays readable in the cache. The window slides on each read, and the job that was given it releases the credential when it finishes. |
| `OAUTH_LOCK_TIMEOUT` | `5` | Seconds the OAuth refresh waits for the portal row before giving up. Refreshes run on their own database connection so a rotated refresh token is committed even if the request that triggered it rolls back. |
| `JOB_CONCURRENCY` | `2` | Jobs one worker runs at once. |
| `JOB_BATCH_CONCURRENCY` | `8` | Batches one job may fan out to. Peak database connections are roughly the product of the two. |
| `JOB_POLL_INTERVAL` | `3.0` | Seconds between queue polls. |
| `JOB_HEARTBEAT_TIMEOUT` | `300` | A RUNNING job with no heartbeat for this long is treated as orphaned and requeued. |
| `JOB_MAX_ATTEMPTS` | `3` | Claims of the same job before it is failed instead of requeued. |
| `LOGIN_RATELIMIT_ATTEMPTS` | `10` | Failed sign-ins allowed per identity before further attempts are refused. |
| `LOGIN_RATELIMIT_WINDOW` | `900` | Seconds the failed sign-in counter covers. |
| `WEBHOOK_RATELIMIT_ATTEMPTS` | `20` | Rejected webhook requests allowed per identity before further ones are refused. |
| `WEBHOOK_RATELIMIT_WINDOW` | `300` | Seconds the webhook counter covers. |

### Development only

`DJANGO_SETTINGS_MODULE=config.settings.local` (the entry points otherwise
default to `config.settings.production`), `USE_DOCKER`, `IPYTHONDIR`,
`POSTGRES_*` for the entrypoint's `DATABASE_URL`, and `DJANGO_SUPERUSER_USERNAME`
/ `_PASSWORD` / `_EMAIL` for `createsuperuser --noinput`.

### Settings that live in the database

These are edited in the running application, not in `.env`. A change applies to
the next request or the next job — nothing has to be restarted.

| Setting | Edited under | Formerly |
|---|---|---|
| ArcGIS sign-in: organization URL, App ID, App Secret, required role | **Settings > ArcGIS Sign-In** *(superuser)* | `SOCIAL_AUTH_ARCGIS_URL`, `SOCIAL_AUTH_ARCGIS_KEY`, `SOCIAL_AUTH_ARCGIS_SECRET`, `ARCGIS_USER_ROLE`. Migration `0032` copies them out of the environment on upgrade. |
| Webhook shared secret | **Settings > Webhook** *(superuser)* | Already in the database. A `WEBHOOK_SECRET` variable existed but nothing ever read it. |
| SMTP host, port, encryption, username, password, From, Reply-To, admin address | **Settings > Email** *(superuser)* | Already in the database. |
| Log level | **Settings > Retention** *(staff)* | Already in the database. |
| Retention windows for log entries, finished jobs and Replace Service backups | **Settings > Retention** *(staff)* | Not configurable. Logs and finished jobs were never trimmed; the backup window was unreachable. |
| Per-run caps for the Inactive User tool | each portal's **Tools** page | Not configurable. |

The four ArcGIS sign-in variables were read with no default, so registering an
OAuth application used to be a precondition for the first `migrate`. Blank now
means local Django sign-in only, and the button appears once an organization URL
is set.

Removed entirely, and safe to delete from an existing `.env`: `REDIS_URL`,
`CACHE_URL`, `RABBIT_URL` and `CELERY_BROKER_URL` (PostgreSQL holds the queue
and the cache), `WEB_CONCURRENCY` (waitress is configured on its command line)
and `DJANGO_ADMIN_FORCE_ALLAUTH` (never used).

### Encryption keys

`DJANGO_SECRET_KEY` encrypts data as well as signing sessions.
`django-cryptography` derives its key from it, so it protects every stored
credential: portal passwords and tokens, portal OAuth client secrets and refresh
tokens, the SMTP password, the ArcGIS App Secret and the webhook secret.
Rotating it makes all of them unreadable, which shows up later as
`Signature "..." does not match` when something reads a portal. To rotate it,
decrypt and re-save every encrypted field under the old key first.

`CREDENTIAL_ENCRYPTION_KEY` encrypts the short-lived credential handoff from the
web process to the worker. Losing it breaks credential-prompting refreshes until
you set a new one. Nothing already stored becomes unreadable.

Back up both keys with any database dump you intend to restore.

### Registering ArcGIS applications

Two separate things use OAuth, and each needs its own application registered in
ArcGIS with its own redirect URI. You configure both from inside EnterpriseViz,
once it is deployed and reachable at its final URL, so neither has to exist
before the first start.

#### Signing in to EnterpriseViz

Controls who may log in. Entered under **Settings → ArcGIS Sign-In**.

1. In ArcGIS Portal or ArcGIS Online, as an administrator, add an application
   item (**Content → New item → Application**) and register it from its
   **Settings** tab.
2. Give it a **Redirect URI** of the site URL plus
   `/enterpriseviz/oauth/complete/arcgis/`, trailing slash included, and the
   port when it is not the default:

   ```
   https://enterpriseviz.example.org/enterpriseviz/oauth/complete/arcgis/
   https://enterpriseviz.example.org:8001/enterpriseviz/oauth/complete/arcgis/
   ```

3. In EnterpriseViz, enter the organization URL, **App ID**, **App Secret** and
   **Required Role**.

**Required Role** is the portal role id a member must hold to be admitted;
members without it are rejected during sign-in, and a blank role rejects
everybody. Clearing the organization URL and App ID turns ArcGIS sign-in off and
removes the button from the login page, leaving local Django accounts.

#### Connecting to a portal for data collection

Optional, and set per portal. This is how EnterpriseViz authenticates to a
portal it inventories, in place of a stored username and password. Entered on
the portal's own Add or Update form.

1. Register a second application, in the portal being inventoried.
2. Give it a **Redirect URI** of the site URL plus
   `/enterpriseviz/portal/oauth/callback/`.
3. On the portal in EnterpriseViz, set **Authentication method** to *OAuth 2.0
   (ArcGIS sign in)*, enter the Client ID and Client Secret, and consent once as
   an administrator.

EnterpriseViz stores an encrypted refresh token in place of a password. Refresh
tokens expire, after 90 days at most on ArcGIS Online and after a configurable
period on Enterprise. Once the stored token stops working, the portal form shows
a re-consent prompt. See [Adding portals](#adding-portals).

## Usage

### First sign-in

EnterpriseViz has two kinds of account, and only one of them can be created without a shell.

**Local Django accounts** come from `manage.py createsuperuser`, run during deployment. A superuser
is staff, and staff is what every administrative view requires. This is the only way in to a new
deployment: sign in at `/enterpriseviz/login/` with that username and password.

**ArcGIS accounts** are created by the sign-in pipeline the first time someone signs in through the
portal, once ArcGIS sign-in is configured. They arrive **without staff status**, so they can browse
portals, items and the dependency graph, but cannot register a portal, run a refresh, open the logs
or change settings.

Holding the Required Role admits a user; on its own it does not make them an administrator.

The order on a new deployment is:

1. Create a Django superuser as part of [deployment](#deployment).
2. Sign in with it at `/enterpriseviz/login/`.
3. Configure ArcGIS sign-in under **Settings → ArcGIS Sign-In**, if you want portal sign-in at all.
4. Grant administrative access to whoever needs it.

### Granting administrative access

Sign in as a superuser, open the Django admin at the path set in `DJANGO_ADMIN_URL`, and tick
**Staff status** on a user under **Authentication and Authorization → Users**. That user must have
signed in through ArcGIS at least once for their row to exist.

This is deliberately a manual step rather than something the portal decides. **Required Role**
answers who may sign in, and is usually broad. Staff is a different question: it is what lets
someone register portals, run refreshes against a live portal, read the logs — which carry other
people's usernames, IP addresses and tracebacks — and run the tools that disable accounts and
unshare items. Sourcing that from a portal group would mean whoever administers the portal also
decides who administers EnterpriseViz.

### Three levels, and what each one reaches

| | Browse | Register portals, refresh, tools, logs | Email, webhook and ArcGIS sign-in settings |
|---|---|---|---|
| Signed in | ✅ | | |
| Staff | ✅ | ✅ | |
| Superuser | ✅ | ✅ | ✅ |

The last column is one database row, and it holds the SMTP password, the webhook secret that
authenticates every inbound webhook, and the ArcGIS OAuth client secret. The Django admin already
refuses to show that row to anyone but a superuser, so the pages that edit it are gated the same
way — they are hidden from the settings panel for plain staff and refused with a 403 if reached
directly. Log level and retention windows hold no shared credential and stay staff-editable.

### Adding portals

**Manage → Add Portal** in the left-hand menu.

* **Name/Alias:** Short identifier for the portal, up to 20 characters. It is the key
  EnterpriseViz stores the portal under, and it appears in this application's URLs.
* **URL:** The base URL of the ArcGIS Portal or ArcGIS Online instance.
* **Type:** Enterprise Portal or ArcGIS Online.
* **Authentication method:** One of
    * *Prompt for credentials* An administrator enters them each time a refresh needs them.
    * *Stored username and password* Encrypted in the database, which lets
      scheduled refreshes and unattended tools run.
    * *OAuth 2.0 (ArcGIS sign in)* Register a client id and secret for the portal and consent
      once. EnterpriseViz then stores an encrypted refresh token instead of a password. See
      [Registering ArcGIS applications](#registering-arcgis-applications).

### Portal pages

Each registered portal appears in the "Portals" menu and has its own page, with tables of the
web maps, services, layers, apps and users it holds.

### Refreshing portal data

Refresh each data type from the portal page, in this order:

1. Users
2. Services
3. Web Maps
4. Apps

### Managing portals

**Manage**, then the portal:

* **Delete:** Remove the portal from EnterpriseViz. The portal itself is untouched.
* **Update:** Change its name, URL, type or credentials.
* **Schedule Refreshes:** Set up recurring data refreshes. The schedule window also shows
  results from refreshes run in the past 24 hours.
* **Run Tools:** The [portal tools](#portal-tools) below.

### Details pages

The "Details" button on any row:

* **Web Map:** The contents of that web map.
* **Service:** The web maps holding the service, and the apps using either the service
  directly or one of those maps.
* **Layer:** The services including the layer (matched by layer name), the web maps using
  those services, and the apps using either.

### Settings

* **Logging:** Read the application logs from the Settings panel.
    * Set the log level here: INFO, WARNING, ERROR, DEBUG or CRITICAL
    * Each entry includes a timestamp, level, message, and the request or job it came from
* **Retention:** Opens the same dialog as the Logs page's **Logging & Retention** button.
    * Sets the log level, and how long the nightly purge keeps log entries, finished background
      jobs and Replace Service backups
    * Once a replacement's backups are purged, that replacement can never be reverted. Raise this
      window if you may need reverts months later. `0` keeps everything
* **ArcGIS Sign-In:** *(superuser only)* The organization users sign in against and the role
  required to be admitted. See
  [Registering ArcGIS applications](#registering-arcgis-applications) and
  [Granting administrative access](#granting-administrative-access).
    * The App Secret is encrypted at rest and never rendered back. Leave it blank when editing
      to keep the one already saved
    * Required Role accepts a built-in role such as `org_admin`, or the ID of a custom role
    * Admission is not administration: every ArcGIS account arrives without staff status
* **Email configuration:** Set to allow notification emails.
    * SMTP server settings (host, port, encryption type)
    * Authentication credentials
    * Default From and Reply-To addresses
* **Theme:** Light or dark mode.
    * EnterpriseViz saves the choice on your account
* **Service usage:** Shows or hides the real-time usage graphs on detail pages.
    * Sometimes usage graphs can take a while to retrieve from ArcGIS Server, especially when multiple services or portals are involved
* **Webhooks:** The shared secret that validates incoming webhook requests, which is how portal
  events reach EnterpriseViz as they happen rather than at the next refresh.
    * The Webhook Secret is required. Set or rotate it in the Webhook Settings form.
    * Copy the same secret into your ArcGIS webhook configuration so EnterpriseViz can verify
      requests.
    * Organization webhooks are not signed. Esri's `createWebhook` reference says only that
      "the secret will be added to the header of the webhook payload" and gives no way to
      choose that header, so EnterpriseViz reads whichever header the portal sends and there
      is nothing else to configure. When a webhook is rejected, the application log names the
      headers that did arrive.

### Portal tools

Three tools that act on the portal itself. Each runs on demand or on a schedule.

* **ArcGIS Pro License Removal:** Reclaims Pro licenses from users who have stopped using them.
    * Remove licenses from inactive users based on configurable inactivity duration
    * Send warning notifications before license removal
* **Inactive User Management:** Acts on accounts that have not signed in for a configurable period.
    * Identify users based on configurable inactivity duration
    * Choose actions: notify only, disable user, delete user, or transfer content
    * Send warning notifications before taking action
    * A per-run ceiling on how many accounts one unattended run may act on, as a count and as a
      percentage of the portal
* **Public Item Unsharing:** Removes public sharing from items with incomplete metadata.
    * Unshare publicly shared items that don't meet metadata score requirements
    * Configure minimum metadata score threshold (50%, 75%, 90%, 100%)
    * Choose between immediate (webhook) or daily processing

### Replace Service

Repoints the web maps and apps that consume a service at a replacement service, in place, after a
republish, rename or split. Staff users reach it from the **Replace** button on a service's details
page. Every run starts with a dry run and backs up each item before changing it, so you can revert
the whole job or a single item afterwards.

The dry run also checks that the replacement service actually publishes the layers being repointed
at it, and warns when a layer number is missing there or names a different layer, since a layer
number carries over unchanged unless you renumber it.

Operator guide: [docs/replace-service.md](docs/replace-service.md).
Implementation notes: [docs/replace-service-internals.md](docs/replace-service-internals.md).

## Limitations

**App-to-app dependencies are not tracked.** The graph covers four kinds of edge:

- App → Service
- App → Web Map
- Web Map → Service
- Service → Layer

An app that embeds or links to another app produces no edge. A Map Series holding Web AppBuilder
apps, or a Hub site pointing at a dashboard, looks like an unrelated node here, so deleting the
inner app breaks the outer one with no warning from EnterpriseViz.

## Changelog

### August 2026 - 3.0 Database-Backed Job Runner & Windows/IIS Deployment
* **Job Runner** - Background work is queued into the `app_job` table and run by a single `manage.py run_worker` process that also fires due schedules. Jobs orphaned by a dead worker are requeued once their heartbeat passes `JOB_HEARTBEAT_TIMEOUT`, up to `JOB_MAX_ATTEMPTS`; portal-modifying tools are failed rather than rerun.
* **PostgreSQL Cache** - `django_cache` replaces Redis, including for the encrypted credential handoff between the web process and the worker.
* **waitress on Both Platforms** - One WSGI server for containers and IIS, so concurrency behaves the same in each.
* **Windows Server + IIS Deployment** - IIS hosts the web process through HttpPlatformHandler and the worker runs as a Task Scheduler task. Setup runbook in [deploy/iis/README.md](deploy/iis/README.md), diagnostics in [deploy/iis/troubleshooting.md](deploy/iis/troubleshooting.md). MSD parsing works on this host when the service account can read the ArcGIS Server directories share.
* **Policy Settings Moved into the Database** - Job, log and replacement-backup retention and the inactive-user action limits are edited in the application instead of the environment. The backup window was previously unreachable from anywhere.
* **Replacing a Whole-Service Reference** - A web map that had an entire map image service added records its layer numbers as bare integers next to the service URL, not as `/0`, `/1` suffixes, so the advanced per-layer mapping matched nothing and the dry run reported the map as having no references at all. Those numbers are now rewritten directly when one replacement service covers the map, and named in the dry run when a split means the map cannot be repointed without restructuring it. This also closes a silent case: renumbering with a single replacement service previously swapped the URL while leaving the old layer numbers behind, pointing the map at the wrong layers with no warning.
* **Layer Coverage Warnings** - A layer number survives a replacement untouched unless the mapping renumbers it, which is the default in both simple and advanced mode. The dry run now warns when the replacement service does not publish that layer, and when it publishes a differently named one at the same number - the second breaks nothing visibly and would otherwise leave items quietly showing the wrong data. The check is scoped to the layers consumers actually reference, and each item's row names the specific layers it points at that the replacement lacks. An item a split cannot repoint is recorded as skipped, with the reason, so the note survives execution.
* **Live Layer Lists for Replacement** - The replacement dialog and dry run read a service's layer numbers and names from the service itself rather than from the last sync, falling back to the database when the service cannot be reached or the portal has not been authenticated yet. Referenced services whose publishing document could not be read have datasets recorded with no layer numbers at all, which left the advanced mapping table empty and coverage checks unable to say anything; they now work for every service, and reflect the service as it stands rather than as of the last refresh.
* **Hosted Layer Numbers Recorded** - The service sync stored a hosted layer's name and dropped its sublayer number, so hosted services had no layer ids anywhere: the advanced replacement mapping table was empty for them, a web map that added a whole hosted service could not be linked to its individual layers, and layer coverage checks could only report "unknown". The number is now recorded from the layer definition; links written by the previous code are retired on the next service refresh.
* **Repeated Layers in One Service** - The uniqueness key on the layer-service relationship now includes the service layer id. A feature class published twice in the same service, same source but a different definition query, symbology or scale range, is two layers with two ids, and the second no longer collides with the first.
* **Default-Closed Authentication** - Middleware protects every view unless it is explicitly exempt, matched by URL namespace so the rule holds regardless of how `DJANGO_ADMIN_URL` is set.
* **Sign-In and Webhook Throttling** - Failed attempts are counted against every identity involved, failing open on a cache error so an outage cannot become a total sign-in outage.
* **Export Escaping** - Exported cells beginning with `=`, `+`, `-`, `@`, tab or CR are prefixed so a spreadsheet treats portal-supplied titles as text.
* **Portal TLS Verification** - Certificates are verified on every portal connection by default, with `ARCGIS_VERIFY_TLS` for an internal CA bundle.

### August 2026 - Authentication Methods
* **Per-Portal Authentication Method** - A portal now carries an explicit `auth_method`, one of prompt for credentials, stored username and password, or OAuth 2.0, in place of the `store_password` flag. Templates and the unattended-refresh path read the method, so a portal that cannot run unattended is gated consistently instead of by inference.
* **Portal OAuth 2.0** - A portal can authenticate with its own registered OAuth application and an encrypted refresh token rather than a stored password. Consent runs through an authorization-code flow with a `state` parameter, and both consent views are staff-only. The client secret and refresh token are encrypted at rest, and neither is logged.
* **Silent Token Refresh** - Scheduled and unattended work exchanges the refresh token for an access token with no operator present. The exchange is serialized per portal under `select_for_update`, so concurrent workers reuse one refreshed token instead of racing, and it runs on a database connection of its own: the portal invalidates the old refresh token the moment it issues the new one, so storing the new one inside the request's transaction would lose it to any later rollback and lock the portal out until someone consented again.
* **Re-Consent Prompt** - A refresh token that has expired or been revoked marks `oauth_refresh_expiration` on the portal, which drives a re-consent banner on the portal form. Refresh tokens on ArcGIS Online expire within 90 days, so this is a routine event rather than a failure.
* **Privilege Verification** - Connecting checks the account holds the privileges the work needs: `portal:admin:viewItems` for items, `portal:admin:viewUsers` for members, plus `portal:admin:manageServers` on Enterprise. A member without them gets no error from the portal, only their own content, so an inventory would otherwise report success while silently omitting everyone else's. Adding or updating a portal warns and still saves; a content refresh returns early rather than writing a partial inventory over good data.
* **Site Sign-In Moved into the Database** - The organization URL, App ID, App Secret and required role for signing in to EnterpriseViz are columns on the settings row, edited under Settings > ArcGIS Sign-In. They were four environment variables read with no default, which made registering an OAuth application a precondition for the first `migrate`.
* **Custom Roles Admitted** - Required Role is matched against the portal's `roleId` as well as its `role`. A portal reports anyone holding a custom role as `org_user` with the role's id in `roleId`, so a deployment that entered a custom role id was refusing every sign-in with nothing to say why.
* **Shared Credentials Are Superuser-Only** - The Email, Webhook and ArcGIS Sign-In pages edit the one settings row that holds the SMTP password, the webhook secret and the OAuth client secret. The Django admin already restricted that row to superusers; the application's own pages now match, rather than leaving the restriction to be walked around. Log level and retention windows stay staff-editable.

### July 2026 - Replace Service Tool
* **Service Replacement** - New staff-only tool on service detail pages that repoints consuming web maps and apps to a replacement service via string-level replacement of service URLs and portal item IDs in item JSON, URL properties, and app resources (Web Experience configs, StoryMap draft/published resources).
* **Simple & Split Modes** - Swap a whole service one-for-one, or map individual sublayers to different replacement services when a service has been split apart.
* **Dry Run Preview** - Every replacement starts with a dry run showing per-item replacement counts and the exact string pairs before an explicit, confirmed execution.
* **Backups & Revert** - Each modified item's pre-change state is snapshotted to the database before updating; executed jobs (including failed runs with partially-applied changes) can be reverted one-click from the results view or job history, and individual items can be reverted from the replacement report without undoing the whole job. Items edited after the replacement are never overwritten without confirmation: full-job reverts skip them, and per-item reverts prompt with the option to download the backup for manual restoration instead. Backups are retained for 90 days by default, configurable under Settings > Retention.
* **Replacement Report** - Portal-wide report page (with the portal navigation sidebar) listing every job's source and replacement services with one row per affected map/app (owner, counts, status). Service, replacement, and item titles link to their portal pages; Copy/CSV/Excel/PDF exports add the service, replacement, and item URLs as separate columns. Rows support per-item revert and a per-item JSON *Backup* download for manual restoration (e.g. via ArcGIS Online Assistant).
* **Credential Prompt** - Portals without stored credentials prompt for admin credentials inline; validated credentials are cached encrypted for up to an hour, scoped to the submitting user, so multi-step flows can reuse them without re-prompting while that cache entry is still valid.
* **Safety Guards** - Per-item error isolation, JSON validity checks on every change, stale-item detection between dry run and execution, case- and encoding-tolerant URL matching (JSON-escaped and percent-encoded forms), digit-boundary-safe sublayer URL matching, one-job-per-portal locking with automatic cleanup of abandoned analyses, and automatic database resync of affected items after execution or revert. Execute and revert confirmations use Calcite confirmation sheets (not browser-native dialogs), and reverts that would overwrite post-replacement edits require an explicit second confirmation.

### July 2026 - Calcite Design System 5.1 Upgrade
* **Calcite Components Upgraded to 5.1** - Updated CDN from 3.3.3 to 5.1, picking up two major versions of design system improvements and bug fixes.
* **Design Token Migration** - Migrated deprecated `--calcite-color-foreground-*` and `--calcite-color-background` tokens to the new `--calcite-color-surface-*` naming convention for v5 compatibility.
* **Component API Updates** - Updated `calcite-block` icon slot (`slot="icon"` → `slot="content-start"`) and `calcite-combobox-item` label attribute (`text-label` → `heading`) to conform to v5 breaking changes.
* **Modal → Dialog Migration** - Replaced removed `calcite-modal` component with `calcite-dialog`; restored intended dialog widths (`60rem` for Add/Schedule, `400px` for Login) using the v5 `--calcite-dialog-size-x` token.
* **Settings & Portal Panel Refactor** - Settings and portal panels rebuilt on `calcite-list` and `calcite-list-item`, which carry the list semantics screen readers need.

### June 2026 - Multi-Server Federated Portal Support
* **Per-Server Folder Processing** - Batch tasks are now scoped to a specific (server, folder) pair instead of re-querying all servers for each folder, eliminating duplicate work and incorrect cross-server service associations.
* **Per-Server Usage Reporting** - Usage reports are now fetched from each server individually rather than from the last server in the loop.
* **Correct Service URLs for Federated Servers** - Service URLs are now built from each server's own public URL rather than always using the first hosting server's URL. Fixes broken webmap/app linkage for services hosted on non-hosting federated servers.

### April 2026 - Map Layer Linking & Parsing Improvements
* **Layer-Service Linking Update** - Layer-service links now use `service_layer_id`, so one feature class associates correctly with several service layers, including those carrying definition queries.
* **Map Image Service Layers** - All layers of a map image service are linked to the map where available, and full-service inclusion is distinguished from partial-layer inclusion.
* **MSD Parsing** - Identifies the primary MSD file, the one carrying all layer information.

### February 2026 - Layer Parsing, Webhook, and UI Improvements
* **MSD Layer Parsing** - Handles standalone layers, layers split across folders, and nested groups in ArcGIS Server .msd files.
* **Webhook Workflow** - Simpler event processing, better error handling.
* **Service Tracking Updates** - Added `service_created` and `service_modified` fields.
* **UI** - Warning state styling for progress bars and chips; chart icons render per mode.

### December 2025 - Dependency Tracking & Layer Management
* **MSD Parser** - Reads layer detail from .msd files, giving per-index tracking (MapServer/0 vs MapServer/5)
* **Service Manifest Fallback** - Falls back to service manifest parsing in disconnected environments
* **Layer DataSource Tracking** - Added `service_layer_id` and `webmap_layer_id` fields for precise dependency tracking
* **Experience Builder Extraction** - Layer-level granularity for Experience Builder apps, with widget type detection (search, filter, table, map)
* **App Type Context** - Dependency extraction for Web AppBuilder, Instant Apps, Dashboards and StoryMaps records the context each service is used in

### November 2025 - Visualization Improvements
* **New Dependency Graph** - Cytoscape.js with a Dagre layout, replacing D3
* **Layer Details** - Layer location shows server, database and version
* **Graph Navigation Controls** - Added action bar with zoom, pan, and layout controls
* **App Type Display** - Show specific app item types in dependency graphs

### October 2025 - Accessibility & Security
* **Accessibility** - ARIA labels, semantic HTML, keyboard navigation
* **Content Security Policy** - Removed inline styles and `unsafe-eval` requirements
* **Form Accessibility** - Calcite form components given labels and validation messages
* **Table Navigation** - Accessible column sorting and pagination without eval()

### September 2025 - Webhooks
* **Webhook Integration** - Real-time processing of ArcGIS Portal events
* **Webhook Secret Management** - Secure validation of incoming webhook requests
* **Immediate Event Processing** - Process item updates, sharing changes, and deletions in real-time
* **Public Unshare Automation** - Webhook-triggered enforcement of metadata quality standards

### August 2025 - Portal Management Tools
* **ArcGIS Pro License Management** - Automated removal from inactive users with configurable grace periods
* **Inactive User Management** - Identify, notify, disable, or delete inactive users with content transfer options
* **Public Item Unsharing** - Enforce metadata score requirements (50%, 75%, 90%, 100% thresholds)
* **Admin Email Notifications** - Configurable email alerts for portal administrators
* **Tool Scheduling** - Schedule automatic runs or execute on-demand

### June 2025 - Logging & Monitoring
* **Database Logging** - Log entries stored in the database, with the level changeable at runtime (INFO, WARNING, ERROR, DEBUG, CRITICAL)
* **Log Viewer** - Web interface for viewing and filtering application logs
* **Request Context** - Log entries carry the request and Celery task they came from
* **Settings Panel** - Centralized configuration for logging, email, theme, and service usage

### May 2025 - Security & Performance
* **Credential Encryption** - Encrypted storage of portal credentials using configurable encryption keys
* **Credential Manager** - Temporary credential handling with Redis cache
* **Form Validation** - Schedule and settings forms validated before saving
* **Celery Task Improvements** - Parallel processing with configurable concurrency

### Version 2.0 (April 2025) - Major Architecture Update
* **Docker Compose Deployment** - Simplified deployment with containerization
* **ArcGIS API Update** - Updated to arcgis-python-api 2.x
* **WebMap Processing Refactor** - Using operationalLayers instead of deprecated WebMap class
* **Service Tracking** - Service and layer relationship tracking reworked
* **Schedule Management** - Configurable recurring data refreshes with cron scheduling

### Pre-2.0 (2021-2025) - Initial Development
* **Core Functionality** - Initial implementation of dependency visualization
* **Portal Integration** - Support for ArcGIS Enterprise Portal and ArcGIS Online
* **Multi-Item Type Support** - Services, Web Maps, Applications, Layers
* **Basic Refresh** - Manual refresh capabilities for portal data
* **User Authentication** - OAuth integration with ArcGIS Portal/AGOL

---

## Screenshots

![Home](images/home_page.png)
![Dark Mode](images/dark_mode.png)
![Layer Details](images/layer_details.png)
![Layer Details Cont](images/layer_details2.png)
![Schedule Refresh](images/schedule.png)
![Logs](images/logs.png)
![Portal Tools](images/portal_tools.png)
![Notify](images/notify.png)

## ERD (Entity-Relationship Diagram)

![Model Diagram](images/graphviz.png)

## Credits & Attributions

This project was inspired by **Mapping Item Dependencies Across ArcGIS Enterprise with Python and d3**
by **Seth Lewis, Ayan Mitra, Stephanie Deitrick**
(https://community.esri.com/t5/devsummit-past-user-presentations/mapping-item-dependencies-across-arcgis-enterprise/ta-p/909500)

Data extraction patterns for various ArcGIS application types reference approaches from **Esri's ArcGIS API for Python**
[Esri's ArcGIS API for Python](https://github.com/Esri/arcgis-python-api)
(licensed under the Apache License 2.0).

Portions of this project originally used the Gentelella template by **Giri Bhatnagar**
(https://github.com/GiriB/django-gentelella)
(licensed under the MIT License). Modifications have been made.

**Original License:**
The MIT License (MIT)

Copyright (c) 2018 Giri Bhatnagar

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

## License

This project is licensed under the **MIT License**.

You are free to use, modify, and distribute this software under the terms of the MIT License. See the [LICENSE](LICENSE)
file for details.
