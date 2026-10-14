"""Page results from a completed Splunk search job."""

import logging
import os
from dataclasses import dataclass
from typing import Any

from splunklib.results import JSONResultsReader, Message

from src.core.list_paging import (
    PaginationError,
    PaginationParams,
    build_paging,
    validate_pagination,
)

SEARCH_PAGE_MAX = 100
JOB_TTL_SECONDS = 1800
SEARCH_MAX_TIME_ENV = "SPLUNK_SEARCH_MAX_TIME"
logger = logging.getLogger(__name__)


def search_time_limit_kwargs() -> dict[str, int]:
    """Splunk ``max_time`` for new search jobs, read from SPLUNK_SEARCH_MAX_TIME.

    The value is in seconds; Splunk finalizes the job when it is reached and
    keeps the results gathered so far. Unset, empty, non-positive or
    non-integer values mean no limit.
    """
    raw = os.getenv(SEARCH_MAX_TIME_ENV, "").strip()
    if not raw:
        return {}
    try:
        seconds = int(raw)
    except ValueError:
        logger.warning("Ignoring %s=%r: not an integer", SEARCH_MAX_TIME_ENV, raw)
        return {}
    return {"max_time": seconds} if seconds > 0 else {}


def apply_job_ttl(job: Any, ttl_seconds: int = JOB_TTL_SECONDS) -> None:
    """Best-effort TTL so later pages can reuse the sid."""
    if not hasattr(job, "set_ttl"):
        return
    try:
        job.set_ttl(ttl_seconds)
    except Exception as exc:
        logger.debug("Could not set search job TTL: %s", exc)


class JobResultsError(RuntimeError):
    """Search job is not ready to page or failed."""


@dataclass(frozen=True)
class JobResultsPage:
    results: list[dict[str, Any]]
    params: PaginationParams
    paging: dict[str, int | bool | None]


def fetch_job_results_page(
    job: Any,
    *,
    count: int = 50,
    offset: int = 0,
) -> JobResultsPage:
    if hasattr(job, "refresh"):
        job.refresh()
    content = getattr(job, "content", {}) or {}
    if str(content.get("isFailed", "0")) == "1":
        raise JobResultsError("Search job failed")
    if str(content.get("isDone", "0")) != "1":
        raise JobResultsError("Search job is not done")
    try:
        params = validate_pagination(count=count, offset=offset, max_count=SEARCH_PAGE_MAX)
    except PaginationError:
        raise
    stream = job.results(output_mode="json", count=params.count, offset=params.offset)
    rows: list[dict[str, Any]] = []
    reader = stream if isinstance(stream, list) else JSONResultsReader(stream)
    for result in reader:
        if isinstance(result, Message):
            continue
        if isinstance(result, dict):
            rows.append(result)
    if hasattr(job, "touch"):
        job.touch()
    total = int(float(content.get("resultCount", len(rows)) or 0))
    return JobResultsPage(
        results=rows,
        params=params,
        paging=build_paging(
            returned=len(rows),
            total_available=total,
            offset=params.offset,
            count=params.count,
        ),
    )
