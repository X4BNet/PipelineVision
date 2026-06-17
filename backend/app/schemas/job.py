from pydantic import BaseModel, computed_field, field_validator
from typing import Optional, List
from datetime import datetime


class RunnerInfo(BaseModel):
    id: int
    name: str
    runner_id: str
    labels: Optional[List[str]] = None

    @field_validator("labels", mode="before")
    @classmethod
    def normalize_labels(cls, labels):
        if not labels:
            return []

        normalized_labels = []
        for label in labels:
            if isinstance(label, str):
                normalized_labels.append(label)
            elif isinstance(label, dict) and label.get("name"):
                normalized_labels.append(label["name"])

        return normalized_labels

    model_config = {"from_attributes": True}


class RepositoryInfo(BaseModel):
    id: int
    name: str
    full_name: str
    owner: str

    model_config = {"from_attributes": True}


class JobResponse(BaseModel):
    id: int
    job_id: str
    repository: Optional[RepositoryInfo] = None
    runner: Optional[RunnerInfo] = None
    workflow_name: Optional[str] = None
    job_name: str
    status: str
    conclusion: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    url: str

    model_config = {"from_attributes": True}

    @computed_field
    @property
    def duration(self) -> Optional[int]:
        if self.started_at and self.completed_at:
            return int((self.completed_at - self.started_at).total_seconds())
        return None

    @computed_field
    @property
    def labels(self) -> List[str]:
        return self.runner.labels if self.runner and self.runner.labels else []


class JobListResponse(BaseModel):
    jobs: List[JobResponse]
    total: int
    offset: int
    limit: int


class JobLogResponse(BaseModel):
    id: int
    job_id: int
    step_number: Optional[int] = None
    line_number: int
    timestamp: datetime
    content: str
    created_at: datetime

    model_config = {"from_attributes": True}


class JobLogStreamResponse(BaseModel):
    logs: List[JobLogResponse]
    total_lines: int
    from_line: int
    is_complete: bool
    job_status: str

    model_config = {"from_attributes": True}
