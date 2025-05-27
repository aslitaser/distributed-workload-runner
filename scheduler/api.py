from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime
import uvicorn
import asyncio
import logging

from shared.models import Job, JobStatus
from shared.config import SchedulerConfig
from scheduler.redis_client import SchedulerRedisClient
from scheduler.job_manager import JobManager
from scheduler.resource_manager import ResourceManager


class JobCreateRequest(BaseModel):
    docker_image: str = Field(..., description="Docker image to run")
    command: List[str] = Field(..., description="Command to execute")
    cpu_cores: int = Field(..., ge=1, description="Number of CPU cores required")
    memory_gb: int = Field(..., ge=1, description="Memory in GB required")
    gpu_type: Optional[str] = Field(None, description="GPU type required (optional)")
    gpu_count: Optional[int] = Field(None, ge=0, description="Number of GPUs required")
    allocation_timeout: Optional[int] = Field(None, ge=30, description="Allocation timeout in seconds (default: 300)")
    eligible_regions: Optional[List[str]] = Field(None, description="List of eligible regions for execution")
    eligible_datacenters: Optional[List[str]] = Field(None, description="List of eligible datacenters for execution")


class JobResponse(BaseModel):
    job_id: UUID
    docker_image: str
    command: List[str]
    cpu_cores: int
    memory_gb: int
    gpu_type: Optional[str]
    gpu_count: int
    allocation_timeout: int
    eligible_regions: List[str]
    eligible_datacenters: List[str]
    status: JobStatus
    status_date: str
    executor_ip: Optional[str]
    created_at: str
    updated_at: str


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Distributed Workload Runner - Scheduler",
    description="API for managing distributed job execution",
    version="1.0.0"
)

# Global job manager for background tasks
_job_manager = None


def get_redis_client() -> SchedulerRedisClient:
    return SchedulerRedisClient()


def get_job_manager() -> JobManager:
    return JobManager(get_redis_client())


def get_resource_manager() -> ResourceManager:
    return ResourceManager(get_redis_client())


async def background_scheduler():
    """Background task that continuously schedules pending jobs."""
    global _job_manager
    
    if not _job_manager:
        _job_manager = JobManager(get_redis_client())
    
    logger.info("Starting background job scheduler")
    
    while True:
        try:
            scheduled_jobs = _job_manager.schedule_pending_jobs()
            if scheduled_jobs:
                logger.info(f"Scheduled {len(scheduled_jobs)} jobs: {scheduled_jobs}")
            
            # Clean up stale jobs every 10 cycles (5 minutes)
            if hasattr(background_scheduler, 'cycle_count'):
                background_scheduler.cycle_count += 1
            else:
                background_scheduler.cycle_count = 1
                
            if background_scheduler.cycle_count % 10 == 0:
                cleaned = _job_manager.cleanup_stale_jobs()
                if cleaned > 0:
                    logger.info(f"Cleaned up {cleaned} stale jobs")
            
        except Exception as e:
            logger.error(f"Error in background scheduler: {e}")
        
        await asyncio.sleep(30)  # Schedule every 30 seconds


@app.on_event("startup")
async def startup_event():
    """Start background tasks when the application starts."""
    global _job_manager
    _job_manager = JobManager(get_redis_client())
    
    # Start background scheduler
    asyncio.create_task(background_scheduler())
    logger.info("Scheduler API started with background job scheduling")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up when the application shuts down."""
    logger.info("Scheduler API shutting down")


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Internal server error", "detail": str(exc)}
    )


def job_to_response(job: Job) -> JobResponse:
    return JobResponse(
        job_id=job.job_id,
        docker_image=job.docker_image,
        command=job.command,
        cpu_cores=job.cpu_cores,
        memory_gb=job.memory_gb,
        gpu_type=job.gpu_type,
        gpu_count=job.gpu_count,
        allocation_timeout=job.allocation_timeout,
        eligible_regions=job.eligible_regions,
        eligible_datacenters=job.eligible_datacenters,
        status=job.status,
        status_date=job.status_date.isoformat(),
        executor_ip=job.executor_ip,
        created_at=job.created_at.isoformat(),
        updated_at=job.updated_at.isoformat()
    )


@app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    job_request: JobCreateRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    try:
        # Determine GPU count based on request
        gpu_count = job_request.gpu_count if job_request.gpu_count is not None else (1 if job_request.gpu_type else 0)
        
        job = Job(
            docker_image=job_request.docker_image,
            command=job_request.command,
            cpu_cores=job_request.cpu_cores,
            memory_gb=job_request.memory_gb,
            gpu_type=job_request.gpu_type,
            gpu_count=gpu_count,
            allocation_timeout=job_request.allocation_timeout if job_request.allocation_timeout else 300,
            eligible_regions=job_request.eligible_regions if job_request.eligible_regions else [],
            eligible_datacenters=job_request.eligible_datacenters if job_request.eligible_datacenters else []
        )
        
        success = job_manager.submit_job(job)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create job in database"
            )
        
        return job_to_response(job)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating job: {str(e)}"
        )


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    redis_client: SchedulerRedisClient = Depends(get_redis_client)
):
    try:
        job = redis_client.get_job(str(job_id))
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found"
            )
        
        return job_to_response(job)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving job: {str(e)}"
        )


@app.delete("/jobs/{job_id}", response_model=JobResponse)
async def abort_job(
    job_id: UUID,
    job_manager: JobManager = Depends(get_job_manager)
):
    try:
        job = job_manager.redis_client.get_job(str(job_id))
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found"
            )
        
        if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot abort job in status: {job.status.value}. Only pending or running jobs can be aborted."
            )
        
        success = job_manager.abort_job(str(job_id))
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to abort job"
            )
        
        updated_job = job_manager.redis_client.get_job(str(job_id))
        if not updated_job:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Job aborted but could not retrieve updated status"
            )
        
        return job_to_response(updated_job)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error aborting job: {str(e)}"
        )


@app.get("/jobs", response_model=List[JobResponse])
async def list_jobs(
    status_filter: Optional[JobStatus] = None,
    limit: int = 100,
    redis_client: SchedulerRedisClient = Depends(get_redis_client)
):
    try:
        if limit <= 0 or limit > 1000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Limit must be between 1 and 1000"
            )
        
        jobs = []
        
        if status_filter == JobStatus.PENDING:
            jobs = redis_client.get_pending_jobs(limit)
        elif status_filter == JobStatus.RUNNING:
            jobs = redis_client.get_running_jobs()
        else:
            pending_jobs = redis_client.get_pending_jobs(limit // 2)
            running_jobs = redis_client.get_running_jobs()
            
            all_jobs = pending_jobs + running_jobs
            
            pattern = "job:*"
            redis_keys = redis_client.redis_client.keys(pattern)
            
            for key in redis_keys[:limit]:
                job_id = key.split(":")[-1]
                job = redis_client.get_job(job_id)
                if job and job not in all_jobs:
                    all_jobs.append(job)
                    if len(all_jobs) >= limit:
                        break
            
            jobs = all_jobs[:limit]
        
        if status_filter and status_filter not in [JobStatus.PENDING, JobStatus.RUNNING]:
            jobs = [job for job in jobs if job.status == status_filter]
        
        response_jobs = [job_to_response(job) for job in jobs]
        return sorted(response_jobs, key=lambda x: x.created_at, reverse=True)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing jobs: {str(e)}"
        )


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "scheduler"}


@app.get("/executors")
async def list_executors(resource_manager: ResourceManager = Depends(get_resource_manager)):
    try:
        executors = resource_manager.get_available_executors()
        
        executor_details = []
        for executor in executors:
            utilization = resource_manager.get_executor_utilization(executor.ip)
            executor_info = executor.model_dump()
            executor_info.update(utilization)
            executor_details.append(executor_info)
        
        cluster_stats = resource_manager.get_cluster_stats()
        
        return {
            "executors": executor_details,
            "cluster_stats": cluster_stats
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing executors: {str(e)}"
        )


@app.post("/schedule")
async def trigger_scheduling(job_manager: JobManager = Depends(get_job_manager)):
    """Manually trigger job scheduling (useful for testing or manual operation)."""
    try:
        scheduled_jobs = job_manager.schedule_pending_jobs()
        
        return {
            "scheduled_jobs": len(scheduled_jobs),
            "assignments": [{"job_id": job_id, "executor_ip": executor_ip} 
                          for job_id, executor_ip in scheduled_jobs]
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during scheduling: {str(e)}"
        )


@app.get("/stats")
async def get_scheduler_stats(
    job_manager: JobManager = Depends(get_job_manager),
    resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Get comprehensive scheduler statistics."""
    try:
        queue_stats = job_manager.get_job_queue_stats()
        cluster_stats = resource_manager.get_cluster_stats()
        
        return {
            "queue": queue_stats,
            "cluster": cluster_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting stats: {str(e)}"
        )


@app.post("/cleanup")
async def cleanup_stale_jobs(
    timeout_minutes: int = 60,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Clean up jobs that have been running too long without updates."""
    try:
        cleaned_count = job_manager.cleanup_stale_jobs(timeout_minutes)
        
        return {
            "cleaned_jobs": cleaned_count,
            "timeout_minutes": timeout_minutes
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during cleanup: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(
        "scheduler.api:app",
        host=SchedulerConfig.API_HOST,
        port=SchedulerConfig.API_PORT,
        reload=True
    )