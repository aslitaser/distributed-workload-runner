from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Job(BaseModel):
    job_id: UUID = Field(default_factory=uuid4)
    docker_image: str
    command: List[str]
    cpu_cores: int = Field(ge=1)
    memory_gb: int = Field(ge=1)
    gpu_type: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
    status_date: datetime = Field(default_factory=datetime.utcnow)
    executor_ip: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def update_status(self, new_status: JobStatus, executor_ip: Optional[str] = None):
        self.status = new_status
        self.status_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        if executor_ip is not None:
            self.executor_ip = executor_ip

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: str
        }


class ExecutorResources(BaseModel):
    ip: str
    total_cpu_cores: int
    available_cpu_cores: int
    total_memory_gb: int
    available_memory_gb: int
    gpu_types: List[str] = Field(default_factory=list)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }