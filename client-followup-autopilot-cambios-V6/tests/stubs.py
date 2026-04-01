"""
Stub modules for testing without network access.
Creates fake modules for anthropic, google*, slack_sdk, schedule, pytz
so that the tool modules can be imported without errors.
"""

import sys
import types
from unittest.mock import MagicMock


def _create_module(name, attrs=None):
    mod = types.ModuleType(name)
    if attrs:
        for k, v in attrs.items():
            setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# ─── anthropic ────────────────────────────────────────────────────────────────
class _FakeAnthropicError(Exception):
    pass

class _FakeAPIError(_FakeAnthropicError):
    pass

class _FakeRateLimitError(_FakeAnthropicError):
    pass

class _FakeAnthropicClient:
    def __init__(self, **kw):
        pass
    class messages:
        @staticmethod
        def create(**kw):
            raise NotImplementedError("Stub: no real API calls in tests")

if "anthropic" not in sys.modules:
    anthropic_mod = _create_module("anthropic", {
        "Anthropic": _FakeAnthropicClient,
        "APIError": _FakeAPIError,
        "RateLimitError": _FakeRateLimitError,
    })

# ─── pytz ─────────────────────────────────────────────────────────────────────
if "pytz" not in sys.modules:
    import datetime as _dt

    class _FakeUTC(_dt.tzinfo):
        def utcoffset(self, d):
            return _dt.timedelta(0)
        def tzname(self, d):
            return "UTC"
        def dst(self, d):
            return _dt.timedelta(0)

    class _FakeTZ(_dt.tzinfo):
        def __init__(self, name="UTC"):
            self._name = name
            # Rough offset map for common timezones
            offsets = {
                "America/Mexico_City": -6, "America/Bogota": -5, "America/Lima": -5,
                "America/Santiago": -3, "America/Argentina/Buenos_Aires": -3,
                "America/Sao_Paulo": -3, "America/Panama": -5, "America/Guayaquil": -5,
                "America/Santo_Domingo": -4, "America/Montevideo": -3,
                "America/Costa_Rica": -6, "Europe/Madrid": 1, "UTC": 0,
            }
            self._offset = _dt.timedelta(hours=offsets.get(name, 0))

        def utcoffset(self, d):
            return self._offset
        def tzname(self, d):
            return self._name
        def dst(self, d):
            return _dt.timedelta(0)

    def _fake_timezone(name):
        return _FakeTZ(name)

    pytz_mod = _create_module("pytz", {
        "UTC": _FakeUTC(),
        "timezone": _fake_timezone,
    })

# ─── schedule ─────────────────────────────────────────────────────────────────
if "schedule" not in sys.modules:
    class _FakeJob:
        def seconds(self):
            return self
        def do(self, fn, *a, **k):
            return self

    class _FakeScheduler:
        def every(self, *a, **k):
            return _FakeEvery()

    class _FakeEvery:
        def __getattr__(self, name):
            return self
        def __call__(self, *a, **k):
            return self
        def do(self, fn, *a, **k):
            return self
        def at(self, *a, **k):
            return self

    sched_mod = _create_module("schedule", {
        "every": _FakeEvery(),
        "run_pending": lambda: None,
    })

# ─── slack_sdk ────────────────────────────────────────────────────────────────
if "slack_sdk" not in sys.modules:
    class _FakeWebClient:
        def __init__(self, **kw):
            pass
        def __getattr__(self, name):
            return lambda **kw: MagicMock()

    class _FakeSlackApiError(Exception):
        pass

    slack_mod = _create_module("slack_sdk", {
        "WebClient": _FakeWebClient,
    })
    _create_module("slack_sdk.web", {"WebClient": _FakeWebClient})
    _create_module("slack_sdk.errors", {"SlackApiError": _FakeSlackApiError})

# ─── google modules ───────────────────────────────────────────────────────────

class _FakeRequest:
    def __init__(self):
        pass
    def __call__(self):
        return self

class _FakeCredentials:
    valid = True
    expired = False
    refresh_token = "fake-refresh"
    token = "fake-token"
    def refresh(self, *a):
        pass
    @classmethod
    def from_authorized_user_file(cls, *a, **k):
        return cls()
    @classmethod
    def from_authorized_user_info(cls, *a, **k):
        return cls()
    def with_subject(self, *a, **k):
        return self
    def with_scopes(self, *a, **k):
        return self

class _FakeSACredentials(_FakeCredentials):
    @classmethod
    def from_service_account_file(cls, *a, **k):
        return cls()

class _FakeFlow:
    @classmethod
    def from_client_secrets_file(cls, *a, **k):
        return cls()
    def run_local_server(self, *a, **k):
        return _FakeCredentials()

for mod_name in [
    "google", "google.oauth2", "google.oauth2.credentials", "google.oauth2.service_account",
    "google.auth", "google.auth.transport", "google.auth.transport.requests",
    "google_auth_oauthlib", "google_auth_oauthlib.flow",
    "googleapiclient", "googleapiclient.discovery", "googleapiclient.errors",
]:
    if mod_name not in sys.modules:
        _create_module(mod_name)

sys.modules["google.oauth2.credentials"].Credentials = _FakeCredentials
sys.modules["google.oauth2.service_account"].Credentials = _FakeSACredentials
sys.modules["google.auth.transport.requests"].Request = _FakeRequest
sys.modules["googleapiclient.discovery"].build = lambda *a, **k: MagicMock()
sys.modules["google_auth_oauthlib.flow"].InstalledAppFlow = _FakeFlow
sys.modules["googleapiclient.errors"].HttpError = type("HttpError", (Exception,), {})
