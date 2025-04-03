
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

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV BUILD_ENV ${BUILD_ENVIRONMENT}

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
  # Enable SSH1
  dialog \
  openssh-server \
  sudo \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

# All absolute dir copies ignore workdir instruction. All relative dir copies are wrt to the workdir instruction
# copy python dependency wheels from python-build-stage
COPY --from=python-build-stage /usr/src/app/wheels  /wheels/

# use wheels to install python dependencies
RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/* \
  && rm -rf /wheels/


# Setting up SSH 
# Copy sshd_config file
COPY ./compose/production/django/sshd_config /etc/ssh/
# The root password must be exactly Docker! as it is used by App Service to let you access the SSH session with the container
RUN echo "root:Docker!" | chpasswd

# Make non-root user
RUN addgroup --system django \
    && adduser --system --ingroup django django \
    && adduser django sudo

# Allow non-root user 'django' to run ssh srvice 
RUN echo 'django ALL=(ALL) NOPASSWD: /usr/sbin/sshd -f /etc/ssh/sshd_config' >> /etc/sudoers
# Allow non-root user 'django' to modify the profile file 
RUN chown -R django:django /etc/profile
# For better understanting why i did do that check this links
# https://learn.microsoft.com/en-us/azure/app-service/configure-custom-container?pivots=container-linux&tabs=debian#enable-ssh
# https://stackoverflow.com/questions/69215797/docker-user-as-root-for-azure-app-service

# Create the privilege separation directory for sshd
RUN mkdir /run/sshd

COPY --chown=django:django ./compose/production/django/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint


COPY --chown=django:django ./compose/production/django/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start
COPY --chown=django:django ./compose/production/django/celery/worker/start /start-celeryworker
RUN sed -i 's/\r$//g' /start-celeryworker
RUN chmod +x /start-celeryworker


COPY --chown=django:django ./compose/production/django/celery/beat/start /start-celerybeat
RUN sed -i 's/\r$//g' /start-celerybeat
RUN chmod +x /start-celerybeat

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
