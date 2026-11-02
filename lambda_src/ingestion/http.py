from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable

from .errors import SourceAPIError

LOGGER = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class HttpClient:
    """Small stdlib HTTP client with bounded retries and Retry-After support."""

    def __init__(self, timeout_seconds: int = 20, max_retries: int = 4, user_agent: str | None = None):
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.user_agent = user_agent or "financial-data-lakehouse-ingestor/0.1"

    def get_json(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        min_interval_seconds: float = 0.0,
        before_attempt: Callable[[], None] | None = None,
    ) -> Any:
        request_headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        if headers:
            request_headers.update(headers)

        for attempt in range(self.max_retries + 1):
            # Invoked for every attempt, including 429/5xx/network retries.
            if before_attempt is not None:
                before_attempt()
            if min_interval_seconds > 0:
                host = urllib.parse.urlsplit(url).netloc
                now = time.monotonic()
                previous = getattr(self, "_last_request_by_host", {}).get(host, 0.0)
                wait_seconds = min_interval_seconds - (now - previous)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                if not hasattr(self, "_last_request_by_host"):
                    self._last_request_by_host = {}
                self._last_request_by_host[host] = time.monotonic()
            request = urllib.request.Request(url, headers=request_headers, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    raw = response.read()
                    try:
                        return json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise SourceAPIError(f"Provider returned invalid JSON for {self._safe_url(url)}") from exc
            except urllib.error.HTTPError as exc:
                body = exc.read(2048).decode("utf-8", errors="replace")
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    delay = self._retry_delay(attempt, exc.headers.get("Retry-After"))
                    LOGGER.warning(
                        "Retryable HTTP response status=%s attempt=%s delay_seconds=%.2f url=%s",
                        exc.code, attempt + 1, delay, self._safe_url(url),
                    )
                    time.sleep(delay)
                    continue
                # Do not include request headers or credentials in error messages.
                raise SourceAPIError(
                    f"Provider request failed with HTTP {exc.code}; url={self._safe_url(url)}; "
                    f"response_excerpt={body[:500]}"
                ) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.max_retries:
                    delay = self._retry_delay(attempt, None)
                    LOGGER.warning(
                        "Transient network failure attempt=%s delay_seconds=%.2f url=%s error=%s",
                        attempt + 1, delay, self._safe_url(url), type(exc).__name__,
                    )
                    time.sleep(delay)
                    continue
                raise SourceAPIError(
                    f"Network request failed after {self.max_retries + 1} attempts; "
                    f"url={self._safe_url(url)}; error={type(exc).__name__}"
                ) from exc

        raise SourceAPIError(f"Request exhausted retries; url={self._safe_url(url)}")


    def get_bytes(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        before_attempt: Callable[[], None] | None = None,
    ) -> bytes:
        """Fetch a binary resource (for example Upstox's gzip instrument master)."""
        request_headers = {"Accept": "application/octet-stream, application/json", "User-Agent": self.user_agent}
        if headers:
            request_headers.update(headers)
        for attempt in range(self.max_retries + 1):
            if before_attempt is not None:
                before_attempt()
            request = urllib.request.Request(url, headers=request_headers, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                body = exc.read(1024).decode("utf-8", errors="replace")
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    delay = self._retry_delay(attempt, exc.headers.get("Retry-After"))
                    LOGGER.warning("Retryable binary HTTP response status=%s attempt=%s delay_seconds=%.2f url=%s",
                                   exc.code, attempt + 1, delay, self._safe_url(url))
                    time.sleep(delay)
                    continue
                raise SourceAPIError(
                    f"Provider binary request failed with HTTP {exc.code}; url={self._safe_url(url)}; "
                    f"response_excerpt={body[:300]}"
                ) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.max_retries:
                    delay = self._retry_delay(attempt, None)
                    LOGGER.warning("Transient binary network failure attempt=%s delay_seconds=%.2f url=%s error=%s",
                                   attempt + 1, delay, self._safe_url(url), type(exc).__name__)
                    time.sleep(delay)
                    continue
                raise SourceAPIError(
                    f"Binary request failed after {self.max_retries + 1} attempts; "
                    f"url={self._safe_url(url)}; error={type(exc).__name__}"
                ) from exc
        raise SourceAPIError(f"Binary request exhausted retries; url={self._safe_url(url)}")

    @staticmethod
    def build_url(base_url: str, path: str = "", query: dict[str, Any] | None = None) -> str:
        url = base_url.rstrip("/") + ("/" + path.lstrip("/") if path else "")
        if query:
            filtered = {key: value for key, value in query.items() if value is not None}
            if filtered:
                url += "?" + urllib.parse.urlencode(filtered, doseq=True)
        return url

    @staticmethod
    def _retry_delay(attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(60.0, max(0.0, float(retry_after)))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return min(60.0, max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds()))
                except (TypeError, ValueError, OverflowError):
                    pass
        return min(30.0, (2 ** attempt) + random.uniform(0, 1))

    @staticmethod
    def _safe_url(url: str) -> str:
        # Query parameters can contain non-secret filters. Strip them from logs/errors.
        parsed = urllib.parse.urlsplit(url)
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
