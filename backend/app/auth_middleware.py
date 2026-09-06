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
Authentication middleware.

Kept out of app.middleware because that module sits in the import chain of the
``combined_context_filter`` logging filter, which Django configures at the very
top of ``django.setup()``. Importing django.contrib.auth there runs before the
app registry is ready and aborts startup.
"""
from django.contrib.auth.middleware import LoginRequiredMiddleware


class AppLoginRequiredMiddleware(LoginRequiredMiddleware):
    """
    Require authentication for every view unless it opts out.

    This makes the default closed: a view added without a decorator is
    protected rather than public. Views that must stay reachable anonymously
    declare it with @login_not_required.

    Two URL trees can't be decorated because they belong to other packages, so
    they are exempted by namespace rather than by path — which keeps the rule
    correct regardless of how ADMIN_URL or URL_PREFIX are configured:

    * ``admin``  - the admin authenticates on its own and needs its login page
                   reachable; every view behind it is already staff-gated.
    * ``social`` - social_django's OAuth entry and callback URLs are how an
                   anonymous user signs in.
    """

    EXEMPT_APP_NAMES = frozenset({"admin", "social"})

    def process_view(self, request, view_func, view_args, view_kwargs):
        match = request.resolver_match
        if match and match.app_name in self.EXEMPT_APP_NAMES:
            return None
        return super().process_view(request, view_func, view_args, view_kwargs)
