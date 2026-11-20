
# define an alias for the specific python version used in this file.
FROM python:3.11-slim-bullseye as python

# Python build stage
FROM python as python-build-stage

ARG BUILD_ENVIRONMENT=production

RUN apt-get -y update; apt-get -y install curl gnupg gnupg2 gnupg1

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# optional: for unixODBC development headers
RUN apt install unixodbc
# optional: kerberos library for debian-slim distributions
RUN apt-get install -y libgssapi-krb5-2

# Install apt packages
RUN apt-get update && apt-get install --no-install-recommends -y \
  # dependencies for building Python packages
  build-essential \
  # psycopg2 dependencies
  libpq-dev

# Requirements are installed here to ensure they will be cached.
COPY ./backend/requirements .

# Create Python Dependency and Sub-Dependency Wheels.
RUN pip wheel --wheel-dir /usr/src/app/wheels  \
  -r ${BUILD_ENVIRONMENT}.txt


# Python 'run' stage
FROM python as python-run-stage

ARG BUILD_ENVIRONMENT=production
ARG APP_HOME=/app

# Azure App Service reaches a container's SSH shell over an internal tunnel and
# requires the well-known root password "Docker!". That is only safe when port
# 2222 is never published, so it is off unless a build explicitly asks for it:
#   docker build --build-arg ENABLE_AZURE_SSH=1 ...
# Leave it at 0 for every other target (Kubernetes, Compose, plain Docker),
# where publishing 2222 would expose a root login with a documented password.
ARG ENABLE_AZURE_SSH=0

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV BUILD_ENV ${BUILD_ENVIRONMENT}
ENV ENABLE_AZURE_SSH ${ENABLE_AZURE_SSH}

WORKDIR ${APP_HOME}

RUN apt-get -y update; apt-get -y install curl gnupg gnupg2 gnupg1

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# optional: for unixODBC development headers
RUN apt install unixodbc
# optional: kerberos library for debian-slim distributions
RUN apt-get install -y libgssapi-krb5-2

# Install required system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
  # psycopg2 dependencies
  libpq-dev \
  libkrb5-dev \
  gcc \
  # Translations dependencies
  gettext \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

# SSH server, only when the image is built for Azure App Service.
RUN if [ "${ENABLE_AZURE_SSH}" = "1" ]; then \
      apt-get update && apt-get install --no-install-recommends -y \
        dialog openssh-server sudo \
      && rm -rf /var/lib/apt/lists/*; \
    fi

# All absolute dir copies ignore workdir instruction. All relative dir copies are wrt to the workdir instruction
# copy python dependency wheels from python-build-stage
COPY --from=python-build-stage /usr/src/app/wheels  /wheels/

# use wheels to install python dependencies
RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/* \
  && rm -rf /wheels/


# Make non-root user
RUN addgroup --system django \
    && adduser --system --ingroup django django

# Setting up SSH, only for Azure App Service builds.
# The root password must be exactly Docker! as it is used by App Service to let
# you access the SSH session with the container. App Service exposes 2222 only
# on its internal tunnel; never publish that port from a host or orchestrator.
# https://learn.microsoft.com/en-us/azure/app-service/configure-custom-container?pivots=container-linux&tabs=debian#enable-ssh
COPY ./compose/production/django/sshd_config /etc/ssh_config_azure/sshd_config
RUN if [ "${ENABLE_AZURE_SSH}" = "1" ]; then \
      cp /etc/ssh_config_azure/sshd_config /etc/ssh/sshd_config \
      && echo "root:Docker!" | chpasswd \
      && adduser django sudo \
      # Allow the non-root user to start sshd, and nothing else.
      && echo 'django ALL=(ALL) NOPASSWD: /usr/sbin/sshd -f /etc/ssh/sshd_config' >> /etc/sudoers \
      # The entrypoint exports the environment into /etc/profile for the SSH session.
      && chown django:django /etc/profile \
      # Create the privilege separation directory for sshd
      && mkdir /run/sshd; \
    fi \
    && rm -rf /etc/ssh_config_azure

COPY --chown=django:django ./compose/production/django/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint


COPY --chown=django:django ./compose/production/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start
COPY --chown=django:django ./compose/production/django/worker/start /start-worker
RUN sed -i 's/\r$//g' /start-worker
RUN chmod +x /start-worker

# copy application code to WORKDIR
COPY --chown=django:django backend/ ${APP_HOME}

# make django owner of the WORKDIR directory as well.
RUN chown django:django ${APP_HOME}

USER django

# RUN DATABASE_URL="" \
  #
  # CELERY_BROKER_URL="" \
  #
  # DJANGO_SETTINGS_MODULE="config.settings.test" \
  # python manage.py compilemessages

ENTRYPOINT ["/entrypoint"]
