# ----------------------------------------------------------------------
# Enterpriseviz
# Copyright (C) 2025 David C Jantz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ----------------------------------------------------------------------
"""
Failure counters for the endpoints an attacker can guess against.

Counters live in the Django cache and expire on their own, so a quiet period
clears them without any cleanup task. Accuracy therefore depends on the cache
being shared between processes: with a per-process backend such as LocMemCache,
each gunicorn worker keeps its own count and the effective limit is multiplied
by the number of workers. It still bounds the attack, but a shared backend
(Redis) is what makes the configured number the real one.
"""
import hashlib
import logging

from django.conf import settings
from django.core.cache import cache

from .middleware import client_ip as middleware_client_ip

logger = logging.getLogger('enterpriseviz')


def client_ip(request):
    """
    Client address for rate limiting.

    One definition, shared with the request-context logger: app.middleware
    resolves this against settings.TRUSTED_PROXY_COUNT so an untrusted
    X-Forwarded-For cannot be used to pick a fresh counter every attempt.
    Returns "unknown" rather than None so a request with no usable address still
    counts against something.
    """
    return middleware_client_ip(request) or 'unknown'


class FailureLimiter:
    """A sliding count of failures per identity, over a fixed window."""

    def __init__(self, name, limit_setting, window_setting, default_limit, default_window):
        self.name = name
        self.limit_setting = limit_setting
        self.window_setting = window_setting
        self.default_limit = default_limit
        self.default_window = default_window

    @property
    def limit(self):
        return getattr(settings, self.limit_setting, self.default_limit)

    @property
    def window(self):
        return getattr(settings, self.window_setting, self.default_window)

    def _key(self, identity):
        # Identities include usernames and IPs; hash them so the value is a
        # safe cache key and the plaintext isn't sitting in the cache backend.
        digest = hashlib.sha256(str(identity).encode('utf-8', 'replace')).hexdigest()[:32]
        return f"throttle:{self.name}:{digest}"

    def is_blocked(self, *identities):
        """
        True when any identity has reached the limit.

        A broken or unreachable cache fails open. Failing closed would turn a
        cache outage into a total sign-in outage, which is a worse result than
        briefly losing the limiter; the failure is logged so it is visible.
        """
        if self.limit <= 0:
            return False
        try:
            return any(cache.get(self._key(i), 0) >= self.limit for i in identities if i)
        except Exception as e:
            logger.error(f"Rate limiter '{self.name}' could not read the cache, "
                         f"allowing the request: {e}")
            return False

    def record_failure(self, *identities):
        """
        Count a failure against every identity involved.

        The cache backend is DatabaseCache, whose incr() is a read-modify-write
        rather than an atomic operation, so two failures landing at the same
        moment can be counted once. That undercounts the limiter but never
        overcounts it, so a legitimate user is not locked out early; the effect
        on an attacker is at most a few extra attempts per window, which is not
        worth a SELECT FOR UPDATE on the sign-in path.
        """
        for identity in identities:
            if not identity:
                continue
            key = self._key(identity)
            try:
                # add() only sets when absent, so the window starts at the
                # first failure and is not extended by later ones.
                cache.add(key, 0, self.window)
                cache.incr(key)
            except ValueError:
                # The entry expired between add() and incr(); the next failure
                # starts a fresh window.
                pass
            except Exception as e:
                logger.error(f"Rate limiter '{self.name}' could not record a failure: {e}")

    def reset(self, *identities):
        """Clear the counters after a success."""
        for identity in identities:
            if identity:
                try:
                    cache.delete(self._key(identity))
                except Exception as e:
                    logger.error(f"Rate limiter '{self.name}' could not clear a counter: {e}")


login_limiter = FailureLimiter(
    "login", "LOGIN_RATELIMIT_ATTEMPTS", "LOGIN_RATELIMIT_WINDOW",
    default_limit=10, default_window=900)

webhook_limiter = FailureLimiter(
    "webhook", "WEBHOOK_RATELIMIT_ATTEMPTS", "WEBHOOK_RATELIMIT_WINDOW",
    default_limit=20, default_window=300)
