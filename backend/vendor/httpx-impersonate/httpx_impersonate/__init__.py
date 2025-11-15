"""Local fallback implementation of the :mod:`httpx_impersonate` API.

The original `httpx-impersonate` package is no longer publicly available on
PyPI.  To keep the rest of the codebase decoupled from that distribution we
provide a tiny wrapper around :mod:`tls_client` that exposes a compatible
``Client`` type.  Only the subset of the interface that is used by our
scrapers is implemented (``get`` requests with optional proxies and headers).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

import tls_client

logger = logging.getLogger(__name__)

__all__ = ["Client"]


def _normalize_identifier(name: Optional[str]) -> str:
    """Convert browser impersonation labels into the format tls_client expects."""

    if not name:
        return "chrome_120"

    normalized = name.strip().lower().replace("-", "_")

    if normalized.startswith("chrome"):
        suffix = normalized.split("chrome", 1)[1].strip(" _") or "120"
        return f"chrome_{suffix}"

    if normalized.startswith("safari"):
        suffix = normalized.split("safari", 1)[1].strip(" _") or "17_0"
        suffix = suffix.replace(".", "_")
        return f"safari_{suffix}"

    if normalized.startswith("firefox"):
        suffix = normalized.split("firefox", 1)[1].strip(" _") or "120"
        return f"firefox_{suffix}"

    return normalized


def _normalize_proxies(proxies: Optional[Mapping[str, str]]) -> Optional[Dict[str, str]]:
    if not proxies:
        return None

    normalized: Dict[str, str] = {}
    for key, value in proxies.items():
        if not value:
            continue
        cleaned = key.rstrip(":/")
        normalized[cleaned] = value
    return normalized or None


def _merge_headers(defaults: Optional[Mapping[str, str]], overrides: Optional[Mapping[str, str]]) -> Optional[Dict[str, str]]:
    if not defaults and not overrides:
        return None

    merged: Dict[str, str] = {}
    if defaults:
        merged.update(defaults)
    if overrides:
        merged.update(overrides)
    return merged


class Client:
    """Thin wrapper around :class:`tls_client.Session` mimicking httpx' API."""

    def __init__(
        self,
        *,
        proxies: Optional[Mapping[str, str]] = None,
        impersonate: str = "chrome120",
        timeout: float = 30.0,
        follow_redirects: bool = True,
        headers: Optional[Mapping[str, str]] = None,
        **_: Any,
    ) -> None:
        identifier = _normalize_identifier(impersonate)
        self._session = tls_client.Session(client_identifier=identifier)
        self._proxies = _normalize_proxies(proxies)
        self._timeout = timeout
        self._follow_redirects = follow_redirects
        self._default_headers = headers

        if self._proxies:
            self._session.proxies = self._proxies

        try:
            self._session.timeout_seconds = timeout
        except Exception:  # pragma: no cover - attribute may not exist on old builds
            logger.debug("tls_client session does not expose timeout_seconds attribute")

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # Public API ---------------------------------------------------------
    def close(self) -> None:
        self._session.close()

    def get(self, url: str, *, headers: Optional[Mapping[str, str]] = None, **kwargs: Any):
        return self.request("GET", url, headers=headers, **kwargs)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        cookies: Optional[Mapping[str, str]] = None,
        follow_redirects: Optional[bool] = None,
        timeout: Optional[float] = None,
        proxy: Optional[Mapping[str, str]] = None,
    ):
        final_headers = _merge_headers(self._default_headers, headers)
        allow_redirects = self._follow_redirects if follow_redirects is None else follow_redirects
        timeout_seconds = timeout if timeout is not None else self._timeout
        proxy_mapping = proxy or self._proxies

        if proxy_mapping and not isinstance(proxy_mapping, Mapping):
            raise TypeError("proxy must be a mapping of scheme to URL")

        response = self._session.execute_request(
            method=method.upper(),
            url=url,
            params=dict(params) if params else None,
            data=data,
            json=json,
            cookies=dict(cookies) if cookies else None,
            headers=final_headers,
            allow_redirects=allow_redirects,
            timeout_seconds=timeout_seconds,
            proxy=dict(proxy_mapping) if proxy_mapping else None,
        )

        return response

    # Convenience for compatibility ------------------------------------
    def stream(self, method: str, url: str, **kwargs: Any):
        return self.request(method, url, **kwargs)

    def post(self, url: str, **kwargs: Any):
        return self.request("POST", url, **kwargs)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        identifier = getattr(self._session, "client_identifier", "unknown")
        return f"<httpx_impersonate.Client impersonate={identifier!r}>"
