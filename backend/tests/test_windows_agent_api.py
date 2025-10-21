from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.windows_agent import (
    WindowsAgentJobRequest,
    WindowsAgentJobStatusUpdate,
    WindowsAgentTaskPayload,
    acquire_next_job,
    create_windows_agent_job,
    list_windows_agent_jobs,
    reset_windows_agent_state,
    update_windows_agent_job_status,
)


def setup_function() -> None:
    reset_windows_agent_state()


def test_windows_agent_job_lifecycle() -> None:
    payload = WindowsAgentJobRequest(
        name="Test job",
        tasks=[
            WindowsAgentTaskPayload(kind="ohlc_history", symbols=["CDR.WA", "PKO"], start_date="2023-01-01"),
            WindowsAgentTaskPayload(kind="company_news", limit=5),
        ],
    )
    created = create_windows_agent_job(payload)
    assert created.status == "pending"

    jobs = list_windows_agent_jobs()
    assert any(job.id == created.id for job in jobs.jobs)

    acquired = acquire_next_job(agent_id="tester")
    assert hasattr(acquired, "id")
    assert getattr(acquired, "status", None) == "assigned"

    running = update_windows_agent_job_status(
        created.id,
        WindowsAgentJobStatusUpdate(status="running", agent_id="tester", progress="start"),
    )
    assert running.status == "running"

    completed = update_windows_agent_job_status(
        created.id,
        WindowsAgentJobStatusUpdate(
            status="completed",
            agent_id="tester",
            progress="done",
            details={"files": 2},
        ),
    )
    assert completed.status == "completed"
    assert completed.result_details == {"files": 2}


def test_windows_agent_no_pending_jobs_returns_response() -> None:
    result = acquire_next_job(agent_id="tester")
    # When there are no jobs we expect a bare Response with HTTP 204
    from fastapi import Response

    assert isinstance(result, Response)
    assert result.status_code == 204

