import asyncio
import signal
import sys
import time
import threading
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from shared.models import Job, JobStatus
from shared.config import ExecutorConfig
from executor.redis_client import ExecutorRedisClient
from executor.resource_monitor import ResourceMonitor
from executor.docker_manager import DockerManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('executor.log')
    ]
)
logger = logging.getLogger(__name__)


class ExecutorService:
    def __init__(self):
        self.resource_monitor = ResourceMonitor()
        
        self.executor_ip = self.resource_monitor.ip_address
        self.heartbeat_interval = ExecutorConfig.HEARTBEAT_INTERVAL
        self.job_poll_interval = ExecutorConfig.JOB_POLL_INTERVAL
        
        self.redis_client = ExecutorRedisClient(self.executor_ip)
        self.docker_manager = DockerManager()
        
        self.running = False
        self.shutdown_event = threading.Event()
        self.current_jobs: Dict[str, Any] = {}
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown()

    def startup(self) -> bool:
        """Initialize executor and register with scheduler."""
        try:
            logger.info(f"Starting executor service on {self.executor_ip}")
            
            # Get initial resource information
            resources = self.resource_monitor.get_current_resources()
            logger.info(f"Detected resources: {resources.total_cpu_cores} CPU cores, "
                  f"{resources.total_memory_gb}GB memory, GPUs: {resources.gpu_types}")
            
            # Register with scheduler
            success = self.redis_client.register_resources(resources)
            if not success:
                logger.error("Failed to register with scheduler")
                return False
            
            logger.info("Successfully registered with scheduler")
            
            # Initialize docker manager
            if not self.docker_manager.initialize():
                logger.error("Failed to initialize Docker manager")
                return False
            
            self.running = True
            return True
            
        except Exception as e:
            logger.error(f"Error during startup: {e}")
            return False

    def shutdown(self):
        """Gracefully shutdown the executor."""
        if not self.running:
            return
            
        logger.info("Shutting down executor service...")
        self.running = False
        self.shutdown_event.set()
        
        try:
            # Stop all running jobs
            for job_id in list(self.current_jobs.keys()):
                logger.info(f"Stopping job {job_id}")
                self.stop_job(job_id)
            
            # Clean up Docker resources
            self.docker_manager.cleanup()
            
            logger.info("Executor shutdown complete")
            
        except Exception as e:
            print(f"Error during shutdown: {e}")

    def send_heartbeat(self):
        """Send heartbeat to scheduler with current resource status."""
        try:
            # Update available resources based on current usage
            resources = self.resource_monitor.get_current_resources()
            
            # Adjust available resources based on running jobs
            for job_info in self.current_jobs.values():
                if job_info.get("status") == "running":
                    job = job_info["job"]
                    resources.available_cpu_cores -= job.cpu_cores
                    resources.available_memory_gb -= job.memory_gb
            
            # Ensure available resources don't go negative
            resources.available_cpu_cores = max(0, resources.available_cpu_cores)
            resources.available_memory_gb = max(0, resources.available_memory_gb)
            
            success = self.redis_client.register_resources(resources)
            if success:
                print(f"Heartbeat sent - Available: {resources.available_cpu_cores} CPU, "
                      f"{resources.available_memory_gb}GB memory")
            else:
                print("Failed to send heartbeat")
                
        except Exception as e:
            print(f"Error sending heartbeat: {e}")

    def poll_for_jobs(self):
        """Poll for new jobs assigned to this executor."""
        try:
            my_jobs = self.redis_client.get_my_jobs()
            
            for job_id in my_jobs:
                if job_id not in self.current_jobs:
                    job = self.redis_client.get_job(job_id)
                    if job and job.status == JobStatus.RUNNING:
                        print(f"Found new job {job_id}")
                        self.start_job(job)
                        
        except Exception as e:
            print(f"Error polling for jobs: {e}")

    def start_job(self, job: Job):
        """Start executing a job with full Docker integration."""
        try:
            job_id = str(job.job_id)
            print(f"Starting job {job_id}: {job.docker_image} {' '.join(job.command)}")
            
            # Update job status to running
            self.redis_client.update_job_status(job_id, JobStatus.RUNNING)
            self.redis_client.add_job_log(job_id, f"Starting job on executor {self.executor_ip}")
            
            # Create log callback to stream container logs to Redis
            def log_callback(log_line: str):
                self.redis_client.add_job_log(job_id, log_line)
            
            # Start job in Docker with enhanced configuration
            container_id = self.docker_manager.start_container(
                image=job.docker_image,
                command=job.command,
                cpu_cores=job.cpu_cores,
                memory_gb=job.memory_gb,
                gpu_type=job.gpu_type,
                environment={
                    "JOB_ID": job_id,
                    "EXECUTOR_IP": self.executor_ip
                },
                log_callback=log_callback
            )
            
            if container_id:
                self.current_jobs[job_id] = {
                    "job": job,
                    "container_id": container_id,
                    "status": "running",
                    "started_at": datetime.utcnow()
                }
                self.redis_client.add_job_log(job_id, f"Container {container_id[:12]} started")
                
                # Start monitoring thread for this job
                monitor_thread = threading.Thread(
                    target=self._monitor_job,
                    args=(job_id,),
                    daemon=True
                )
                monitor_thread.start()
                
            else:
                print(f"Failed to start container for job {job_id}")
                self.redis_client.update_job_status(job_id, JobStatus.FAILED)
                self.redis_client.add_job_log(job_id, "Failed to start container")
                
        except Exception as e:
            print(f"Error starting job {job.job_id}: {e}")
            self.redis_client.update_job_status(str(job.job_id), JobStatus.FAILED)
            self.redis_client.add_job_log(str(job.job_id), f"Error starting job: {e}")

    def stop_job(self, job_id: str, force: bool = False):
        """Stop a running job with proper cleanup."""
        try:
            if job_id in self.current_jobs:
                job_info = self.current_jobs[job_id]
                container_id = job_info.get("container_id")
                
                if container_id:
                    if force:
                        print(f"Force killing container {container_id[:12]} for job {job_id}")
                        self.docker_manager.kill_container(container_id)
                        self.redis_client.add_job_log(job_id, f"Container {container_id[:12]} force killed")
                    else:
                        print(f"Stopping container {container_id[:12]} for job {job_id}")
                        self.docker_manager.stop_container(container_id)
                        self.redis_client.add_job_log(job_id, f"Container {container_id[:12]} stopped")
                    
                    # Clean up container
                    self.docker_manager.remove_container(container_id)
                    self.redis_client.add_job_log(job_id, f"Container {container_id[:12]} removed")
                
                del self.current_jobs[job_id]
                
        except Exception as e:
            print(f"Error stopping job {job_id}: {e}")

    def abort_job(self, job_id: str):
        """Abort a job and update its status to failed."""
        try:
            print(f"Aborting job {job_id}")
            self.redis_client.add_job_log(job_id, "Job abort requested")
            
            # Stop the job forcefully
            self.stop_job(job_id, force=True)
            
            # Update job status to failed
            self.redis_client.update_job_status(job_id, JobStatus.FAILED)
            self.redis_client.add_job_log(job_id, "Job aborted")
            
        except Exception as e:
            print(f"Error aborting job {job_id}: {e}")
            self.redis_client.add_job_log(job_id, f"Error during abort: {e}")

    def _monitor_job(self, job_id: str):
        """Monitor a running job and update its status with enhanced tracking."""
        try:
            monitoring_interval = 5  # seconds
            resource_log_interval = 30  # seconds
            last_resource_log = 0
            
            while job_id in self.current_jobs and self.running:
                job_info = self.current_jobs[job_id]
                container_id = job_info["container_id"]
                current_time = time.time()
                
                # Check container status
                container_status = self.docker_manager.get_container_status(container_id)
                
                # Log resource usage periodically
                if current_time - last_resource_log >= resource_log_interval:
                    try:
                        resources = self.docker_manager.monitor_container_resources(container_id)
                        if "error" not in resources:
                            self.redis_client.add_job_log(
                                job_id, 
                                f"Resource usage - CPU: {resources['cpu_percent']:.1f}%, "
                                f"Memory: {resources['memory_percent']:.1f}%"
                            )
                        last_resource_log = current_time
                    except Exception:
                        pass  # Don't fail monitoring for resource logging errors
                
                if container_status == "exited":
                    # Get exit code and final logs
                    exit_code = self.docker_manager.get_container_exit_code(container_id)
                    
                    if exit_code == 0:
                        print(f"Job {job_id} completed successfully")
                        self.redis_client.update_job_status(job_id, JobStatus.SUCCEEDED)
                        self.redis_client.add_job_log(job_id, "Job completed successfully")
                    else:
                        print(f"Job {job_id} failed with exit code {exit_code}")
                        self.redis_client.update_job_status(job_id, JobStatus.FAILED)
                        self.redis_client.add_job_log(job_id, f"Job failed with exit code {exit_code}")
                    
                    # Get final container logs
                    try:
                        final_logs = self.docker_manager.get_container_logs(container_id, tail=50)
                        if final_logs and "error" not in final_logs.lower():
                            self.redis_client.add_job_log(job_id, "=== Final container logs ===")
                            for line in final_logs.split('\n')[-10:]:  # Last 10 lines
                                if line.strip():
                                    self.redis_client.add_job_log(job_id, line)
                    except Exception:
                        pass  # Don't fail for log retrieval errors
                    
                    # Clean up container
                    self.docker_manager.remove_container(container_id)
                    del self.current_jobs[job_id]
                    break
                    
                elif container_status in ["dead", "removing"]:
                    print(f"Job {job_id} container died or is being removed")
                    self.redis_client.update_job_status(job_id, JobStatus.FAILED)
                    self.redis_client.add_job_log(job_id, f"Container {container_status}")
                    self.docker_manager.remove_container(container_id)
                    del self.current_jobs[job_id]
                    break
                
                elif container_status == "not_found":
                    print(f"Job {job_id} container not found")
                    self.redis_client.update_job_status(job_id, JobStatus.FAILED)
                    self.redis_client.add_job_log(job_id, "Container not found")
                    if job_id in self.current_jobs:
                        del self.current_jobs[job_id]
                    break
                
                # Check for abort requests by looking at job status in Redis
                try:
                    current_job = self.redis_client.get_job(job_id)
                    if current_job and current_job.status == JobStatus.FAILED:
                        print(f"Job {job_id} marked as failed externally, stopping container")
                        self.stop_job(job_id, force=True)
                        break
                except Exception:
                    pass  # Don't fail monitoring for Redis check errors
                
                # Sleep before next check
                time.sleep(monitoring_interval)
                
        except Exception as e:
            print(f"Error monitoring job {job_id}: {e}")
            if job_id in self.current_jobs:
                self.redis_client.update_job_status(job_id, JobStatus.FAILED)
                self.redis_client.add_job_log(job_id, f"Monitoring error: {e}")
                # Try to clean up the container
                try:
                    container_id = self.current_jobs[job_id]["container_id"]
                    self.docker_manager.remove_container(container_id)
                except Exception:
                    pass
                del self.current_jobs[job_id]

    def run(self):
        """Main executor event loop."""
        if not self.startup():
            print("Failed to start executor")
            return 1
        
        print("Executor service started. Waiting for jobs...")
        
        last_heartbeat = 0
        last_job_poll = 0
        
        try:
            while self.running:
                current_time = time.time()
                
                # Send heartbeat
                if current_time - last_heartbeat >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat = current_time
                
                # Poll for jobs
                if current_time - last_job_poll >= self.job_poll_interval:
                    self.poll_for_jobs()
                    last_job_poll = current_time
                
                # Check for shutdown
                if self.shutdown_event.wait(1):
                    break
                    
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received")
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            self.shutdown()
        
        return 0


def main():
    """Entry point for the executor service."""
    executor = ExecutorService()
    exit_code = executor.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()