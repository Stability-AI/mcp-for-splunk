"""SPLUNK_SEARCH_MAX_TIME caps new search jobs with Splunk's max_time."""

from unittest.mock import AsyncMock, Mock

import pytest

from src.core.splunk_job_results_page import search_time_limit_kwargs
from src.tools.search.job_search import JobSearch
from src.tools.search.oneshot_search import OneshotSearch


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, {}),
        ("", {}),
        ("0", {}),
        ("-5", {}),
        ("abc", {}),
        ("120", {"max_time": 120}),
        (" 30 ", {"max_time": 30}),
    ],
)
def test_search_time_limit_kwargs(monkeypatch: pytest.MonkeyPatch, raw, expected) -> None:
    if raw is None:
        monkeypatch.delenv("SPLUNK_SEARCH_MAX_TIME", raising=False)
    else:
        monkeypatch.setenv("SPLUNK_SEARCH_MAX_TIME", raw)
    assert search_time_limit_kwargs() == expected


def _context() -> Mock:
    ctx = Mock()
    ctx.info = AsyncMock()
    ctx.error = AsyncMock()
    ctx.report_progress = AsyncMock()
    return ctx


def _service(done: bool) -> Mock:
    job = Mock()
    job.sid = "sid_1"
    job.is_done.return_value = done
    job.content = {"isDone": "1" if done else "0", "isFailed": "0", "resultCount": "0"}
    job.results.return_value = []
    service = Mock()
    service.host = "splunk.example"
    service.scheme = "https"
    service.port = 8089
    service.jobs.create.return_value = job
    return service


@pytest.mark.asyncio
@pytest.mark.parametrize(("env", "expected"), [("90", 90), (None, None)])
async def test_oneshot_search_passes_max_time(
    monkeypatch: pytest.MonkeyPatch, env, expected
) -> None:
    if env is None:
        monkeypatch.delenv("SPLUNK_SEARCH_MAX_TIME", raising=False)
    else:
        monkeypatch.setenv("SPLUNK_SEARCH_MAX_TIME", env)
    tool = OneshotSearch("run_oneshot_search", "oneshot")
    service = _service(done=True)
    tool.check_splunk_available = Mock(return_value=(True, service, None))
    tool.get_client_config_from_context = AsyncMock(return_value={})

    await tool.execute(_context(), query="index=main")

    assert service.jobs.create.call_args.kwargs.get("max_time") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("env", "expected"), [("45", 45), (None, None)])
async def test_job_search_passes_max_time(monkeypatch: pytest.MonkeyPatch, env, expected) -> None:
    monkeypatch.setenv("MCP_SEARCH_WAIT_SECONDS", "0")
    if env is None:
        monkeypatch.delenv("SPLUNK_SEARCH_MAX_TIME", raising=False)
    else:
        monkeypatch.setenv("SPLUNK_SEARCH_MAX_TIME", env)
    tool = JobSearch("run_splunk_search", "search")
    service = _service(done=False)
    tool.check_splunk_available = Mock(return_value=(True, service, None))
    tool.get_client_config_from_context = AsyncMock(return_value={})

    await tool.execute(_context(), query="index=main")

    assert service.jobs.create.call_args.kwargs.get("max_time") == expected
