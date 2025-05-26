from typing import List, Optional, Tuple
from datetime import datetime, timedelta

from shared.models import Job, JobStatus
from scheduler.redis_client import SchedulerRedisClient
from scheduler.resource_manager import ResourceManager


class JobManager:
    def __init__(self, redis_client: SchedulerRedisClient):
        self.redis_client = redis_client
        self.resource_manager = ResourceManager(redis_client)

    def submit_job(self, job: Job) -> bool:
        """Submit a new job to the pending queue."""
        try:
            return self.redis_client.create_job(job)
        except Exception as e:
            print(f"Error submitting job: {e}")
            return False

    def schedule_pending_jobs(self) -> List[Tuple[str, str]]:
        """
        Schedule pending jobs to available executors using first-fit algorithm.
        Returns list of (job_id, executor_ip) pairs for successfully scheduled jobs.
        """
        scheduled_jobs = []
        
        try:
            # Get pending jobs (sorted by creation time - FIFO)
            pending_jobs = self.redis_client.get_pending_jobs(limit=50)
            
            if not pending_jobs:
                return scheduled_jobs
            
            for job in pending_jobs:
                # Check if job has exceeded allocation timeout
                time_since_creation = (datetime.utcnow() - job.created_at).total_seconds()
                if time_since_creation > job.allocation_timeout:
                    print(f"Job {job.job_id} exceeded allocation timeout ({job.allocation_timeout}s)")
                    # Mark job as failed due to allocation timeout
                    self.redis_client.update_job_status(str(job.job_id), JobStatus.FAILED)
                    continue
                
                # Find suitable executor using first-fit algorithm
                suitable_executor = self.resource_manager.find_suitable_executor(job)
                
                if suitable_executor:
                    # Try to assign job to executor
                    success = self.assign_job_to_executor(job, suitable_executor.ip)
                    if success:
                        scheduled_jobs.append((str(job.job_id), suitable_executor.ip))
                        print(f"Scheduled job {job.job_id} to executor {suitable_executor.ip}")
                    else:
                        print(f"Failed to assign job {job.job_id} to executor {suitable_executor.ip}")
                else:
                    # Log why no suitable executor was found
                    regions_msg = f", regions: {job.eligible_regions}" if job.eligible_regions else ""
                    datacenters_msg = f", datacenters: {job.eligible_datacenters}" if job.eligible_datacenters else ""
                    print(f"No suitable executor found for job {job.job_id} "
                          f"(CPU: {job.cpu_cores}, Memory: {job.memory_gb}GB, GPU: {job.gpu_type}"
                          f"{regions_msg}{datacenters_msg})")
        
        except Exception as e:
            print(f"Error during job scheduling: {e}")
        
        return scheduled_jobs

    def assign_job_to_executor(self, job: Job, executor_ip: str) -> bool:
        """Assign a specific job to a specific executor."""
        try:
            # Allocate resources
            if not self.resource_manager.allocate_resources(job, executor_ip):
                return False
            
            # Update job status to running
            success = self.redis_client.update_job_status(
                str(job.job_id), 
                JobStatus.RUNNING, 
                executor_ip
            )
            
            if not success:
                # If job status update fails, release the allocated resources
                self.resource_manager.release_resources(job, executor_ip)
                return False
            
            return True
            
        except Exception as e:
            print(f"Error assigning job to executor: {e}")
            return False

    def complete_job(self, job_id: str, status: JobStatus) -> bool:
        """Mark a job as completed and release its resources."""
        try:
            job = self.redis_client.get_job(job_id)
            if not job:
                print(f"Job {job_id} not found")
                return False
            
            if job.status != JobStatus.RUNNING:
                print(f"Job {job_id} is not running (status: {job.status})")
                return False
            
            # Update job status
            success = self.redis_client.update_job_status(job_id, status)
            if not success:
                return False
            
            # Release resources if job was running on an executor
            if job.executor_ip:
                self.resource_manager.release_resources(job, job.executor_ip)
                print(f"Released resources for job {job_id} on executor {job.executor_ip}")
            
            return True
            
        except Exception as e:
            print(f"Error completing job: {e}")
            return False

    def abort_job(self, job_id: str) -> bool:
        """Abort a job and release its resources."""
        try:
            job = self.redis_client.get_job(job_id)
            if not job:
                return False
            
            if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
                return False
            
            # If job is running, release its resources
            if job.status == JobStatus.RUNNING and job.executor_ip:
                self.resource_manager.release_resources(job, job.executor_ip)
            
            # Update job status to failed
            return self.redis_client.update_job_status(job_id, JobStatus.FAILED)
            
        except Exception as e:
            print(f"Error aborting job: {e}")
            return False

    def get_job_queue_stats(self) -> dict:
        """Get statistics about the job queue."""
        try:
            pending_jobs = self.redis_client.get_pending_jobs(limit=1000)
            running_jobs = self.redis_client.get_running_jobs()
            
            # Group pending jobs by resource requirements
            pending_by_size = {"small": 0, "medium": 0, "large": 0}
            
            for job in pending_jobs:
                if job.cpu_cores <= 2 and job.memory_gb <= 4:
                    pending_by_size["small"] += 1
                elif job.cpu_cores <= 8 and job.memory_gb <= 16:
                    pending_by_size["medium"] += 1
                else:
                    pending_by_size["large"] += 1
            
            return {
                "pending_jobs": len(pending_jobs),
                "running_jobs": len(running_jobs),
                "pending_by_size": pending_by_size,
                "oldest_pending_job": pending_jobs[0].created_at.isoformat() if pending_jobs else None
            }
            
        except Exception as e:
            print(f"Error getting queue stats: {e}")
            return {}

    def cleanup_stale_jobs(self, timeout_minutes: int = 60) -> int:
        """Clean up jobs that have been running too long without updates."""
        try:
            running_jobs = self.redis_client.get_running_jobs()
            cleaned_count = 0
            
            cutoff_time = datetime.utcnow().timestamp() - (timeout_minutes * 60)
            
            for job in running_jobs:
                if job.status_date.timestamp() < cutoff_time:
                    print(f"Cleaning up stale job {job.job_id}")
                    if self.complete_job(str(job.job_id), JobStatus.FAILED):
                        cleaned_count += 1
            
            return cleaned_count
            
        except Exception as e:
            print(f"Error cleaning up stale jobs: {e}")
            return 0