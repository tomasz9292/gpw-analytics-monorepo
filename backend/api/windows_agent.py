"""FastAPI endpoints coordinating Windows desktop agents."""

from __future__ import annotations

from collections import deque
from datetime import date, datetime
import threading
from typing import Any, Deque, Dict, List, Optional, Tuple
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator


class WindowsAgentTaskPayload(BaseModel):
    kind: str = Field(pattern=r"^[a-z_]+$")
    symbols: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: Optional[int] = Field(default=None, ge=1, le=20000)
    notes: Optional[str] = Field(default=None, max_length=500)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:  # noqa: D401 - short validator description
        """Ensure the task kind is supported."""

        allowed = {"ohlc_history", "company_profiles", "company_news"}
        if value not in allowed:
            raise ValueError(f"Nieobsługiwany typ zadania: {value}")
        return value

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, str):
            raw_values = [part.strip() for part in value.replace("\n", ",").split(",")]
        else:
            raw_values = [str(part).strip() for part in value]
        cleaned = [item.upper() for item in raw_values if item]
        return cleaned or None


class WindowsAgentJobEvent(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str
    message: Optional[str] = None


class WindowsAgentJob(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: Optional[str] = Field(default=None, max_length=160)
    description: Optional[str] = Field(default=None, max_length=500)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    assigned_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    status: str = Field(default="pending")
    assigned_to: Optional[str] = None
    progress_message: Optional[str] = None
    result_details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    tasks: List[WindowsAgentTaskPayload]
    events: List[WindowsAgentJobEvent] = Field(default_factory=list)

class WindowsAgentJobRequest(BaseModel):
    name: Optional[str] = Field(default=None, max_length=160)
    description: Optional[str] = Field(default=None, max_length=500)
    tasks: List[WindowsAgentTaskPayload]

    @field_validator("tasks")
    @classmethod
    def validate_tasks(
        cls, value: List[WindowsAgentTaskPayload]
    ) -> List[WindowsAgentTaskPayload]:
        if not value:
            raise ValueError("Wymagane jest co najmniej jedno zadanie")
        return value


class WindowsAgentJobStatusUpdate(BaseModel):
    status: str
    agent_id: str = Field(min_length=1, max_length=120)
    progress: Optional[str] = Field(default=None, max_length=500)
    details: Optional[Dict[str, Any]] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        allowed = {"running", "completed", "failed"}
        if value not in allowed:
            raise ValueError(f"Nieobsługiwany status: {value}")
        return value


class WindowsAgentJobListResponse(BaseModel):
    jobs: List[WindowsAgentJob]


router = APIRouter(prefix="/windows-agent", tags=["windows-agent"])

_JOBS: Dict[str, WindowsAgentJob] = {}
_QUEUE: Deque[str] = deque()
_LOCK = threading.Lock()


def _snapshot_job(job: WindowsAgentJob) -> WindowsAgentJob:
    return job.model_copy(deep=True)


def _create_job(payload: WindowsAgentJobRequest) -> WindowsAgentJob:
    job_id = uuid4().hex
    job = WindowsAgentJob(
        id=job_id,
        name=payload.name,
        description=payload.description,
        tasks=payload.tasks,
        events=[WindowsAgentJobEvent(status="created", message="Zlecenie oczekuje w kolejce")],
    )
    with _LOCK:
        _JOBS[job_id] = job
        _QUEUE.append(job_id)
    return job


def _pop_next_job(agent_id: str) -> Optional[WindowsAgentJob]:
    with _LOCK:
        while _QUEUE:
            job_id = _QUEUE.popleft()
            job = _JOBS.get(job_id)
            if not job or job.status != "pending":
                continue
            job.status = "assigned"
            job.assigned_to = agent_id
            job.assigned_at = datetime.utcnow()
            job.events.append(WindowsAgentJobEvent(status="assigned", message=f"Przypisano agentowi {agent_id}"))
            return job
        # Fallback in case queue desynchronised
        for job in _JOBS.values():
            if job.status == "pending":
                job.status = "assigned"
                job.assigned_to = agent_id
                job.assigned_at = datetime.utcnow()
                job.events.append(WindowsAgentJobEvent(status="assigned", message=f"Przypisano agentowi {agent_id}"))
                return job
    return None


def _get_job(job_id: str) -> WindowsAgentJob:
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nie znaleziono zlecenia")
    return job


def _ensure_agent(job: WindowsAgentJob, agent_id: str) -> None:
    if job.assigned_to and job.assigned_to != agent_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Zlecenie jest obsługiwane przez innego agenta ({job.assigned_to})",
        )
    if not job.assigned_to:
        job.assigned_to = agent_id
        job.assigned_at = job.assigned_at or datetime.utcnow()


def _update_job_state(job: WindowsAgentJob, payload: WindowsAgentJobStatusUpdate) -> WindowsAgentJob:
    now = datetime.utcnow()
    job.events.append(WindowsAgentJobEvent(status=payload.status, message=payload.progress))
    if payload.status == "running":
        if job.status not in {"assigned", "running"}:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Zlecenie jest w stanie nieaktywnym")
        job.status = "running"
        job.started_at = job.started_at or now
        job.progress_message = payload.progress
    elif payload.status == "completed":
        if job.status not in {"assigned", "running"}:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Nie można zakończyć zlecenia w tym stanie")
        job.status = "completed"
        job.started_at = job.started_at or now
        job.finished_at = now
        job.progress_message = payload.progress
        job.result_details = payload.details
    else:  # failed
        job.status = "failed"
        job.started_at = job.started_at or now
        job.finished_at = now
        job.progress_message = payload.progress
        if payload.details:
            job.result_details = payload.details
        job.error_message = payload.progress or "Zlecenie zakończyło się niepowodzeniem"
    return job


@router.post("/jobs", response_model=WindowsAgentJob, status_code=status.HTTP_201_CREATED)
def create_windows_agent_job(payload: WindowsAgentJobRequest) -> WindowsAgentJob:
    job = _create_job(payload)
    return _snapshot_job(job)


@router.get("/jobs", response_model=WindowsAgentJobListResponse)
def list_windows_agent_jobs() -> WindowsAgentJobListResponse:
    with _LOCK:
        jobs = sorted(_JOBS.values(), key=lambda job: job.created_at, reverse=True)
        return WindowsAgentJobListResponse(jobs=[_snapshot_job(job) for job in jobs])


@router.get(
    "/jobs/next",
    response_model=WindowsAgentJob,
    responses={status.HTTP_204_NO_CONTENT: {"description": "Brak oczekujących zleceń"}},
)
def acquire_next_job(agent_id: str) -> Response | WindowsAgentJob:
    cleaned_agent = agent_id.strip()
    if not cleaned_agent:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Wymagany identyfikator agenta")
    job = _pop_next_job(cleaned_agent)
    if not job:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return _snapshot_job(job)


@router.get("/jobs/{job_id}", response_model=WindowsAgentJob)
def get_windows_agent_job(job_id: str) -> WindowsAgentJob:
    job = _get_job(job_id)
    return _snapshot_job(job)


@router.post("/jobs/{job_id}/status", response_model=WindowsAgentJob)
def update_windows_agent_job_status(job_id: str, payload: WindowsAgentJobStatusUpdate) -> WindowsAgentJob:
    job = _get_job(job_id)
    _ensure_agent(job, payload.agent_id)
    with _LOCK:
        updated = _update_job_state(job, payload)
    return _snapshot_job(updated)


def reset_windows_agent_state() -> None:
    """Utility for tests to clear in-memory job queue."""

    with _LOCK:
        _JOBS.clear()
        _QUEUE.clear()


__all__ = [
    "WindowsAgentJob",
    "WindowsAgentJobListResponse",
    "WindowsAgentJobRequest",
    "WindowsAgentJobStatusUpdate",
    "WindowsAgentTaskPayload",
    "acquire_next_job",
    "create_windows_agent_job",
    "list_windows_agent_jobs",
    "reset_windows_agent_state",
    "router",
    "update_windows_agent_job_status",
]

