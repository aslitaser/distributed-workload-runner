from typing import List, Optional, Dict
from datetime import datetime, timedelta

from shared.models import Job, ExecutorResources
from scheduler.redis_client import SchedulerRedisClient


class ResourceManager:
    def __init__(self, redis_client: SchedulerRedisClient):
        self.redis_client = redis_client

    def can_fit_job(self, job: Job, executor: ExecutorResources) -> bool:
        """Check if a job can fit on an executor based on resource requirements."""
        # Check CPU requirements
        if job.cpu_cores > executor.available_cpu_cores:
            return False
        
        # Check memory requirements
        if job.memory_gb > executor.available_memory_gb:
            return False
        
        # Check GPU requirements
        if job.gpu_type and job.gpu_count > 0:
            # Check if executor has enough available GPUs
            if job.gpu_count > executor.available_gpus:
                return False
            
            # Check GPU type compatibility
            if job.gpu_type != "any":
                if not executor.gpu_types or job.gpu_type not in executor.gpu_types:
                    return False
            else:
                # For "any" GPU type, just need to have some GPU types available
                if not executor.gpu_types:
                    return False
        
        # Check region requirements
        if job.eligible_regions:
            if executor.region not in job.eligible_regions:
                return False
        
        # Check datacenter requirements
        if job.eligible_datacenters:
            if executor.datacenter not in job.eligible_datacenters:
                return False
        
        return True

    def allocate_resources(self, job: Job, executor_ip: str) -> bool:
        """Allocate resources for a job on a specific executor."""
        try:
            executor = self.redis_client.get_executor_resources(executor_ip)
            if not executor:
                return False
            
            if not self.can_fit_job(job, executor):
                return False
            
            # Calculate new available resources
            new_cpu = executor.available_cpu_cores - job.cpu_cores
            new_memory = executor.available_memory_gb - job.memory_gb
            new_gpus = executor.available_gpus - job.gpu_count
            
            # Update executor resources
            pipe = self.redis_client.redis_client.pipeline()
            executor_key = f"executor:{executor_ip}:resources"
            
            pipe.hset(executor_key, "available_cpu_cores", new_cpu)
            pipe.hset(executor_key, "available_memory_gb", new_memory)
            pipe.hset(executor_key, "available_gpus", new_gpus)
            pipe.hset(executor_key, "last_heartbeat", datetime.utcnow().isoformat())
            pipe.expire(executor_key, 120)
            
            pipe.execute()
            return True
            
        except Exception as e:
            print(f"Error allocating resources: {e}")
            return False

    def release_resources(self, job: Job, executor_ip: str) -> bool:
        """Release resources when a job completes."""
        try:
            executor = self.redis_client.get_executor_resources(executor_ip)
            if not executor:
                return False
            
            # Calculate restored available resources
            new_cpu = min(
                executor.available_cpu_cores + job.cpu_cores,
                executor.total_cpu_cores
            )
            new_memory = min(
                executor.available_memory_gb + job.memory_gb,
                executor.total_memory_gb
            )
            new_gpus = min(
                executor.available_gpus + job.gpu_count,
                executor.total_gpus
            )
            
            # Update executor resources
            pipe = self.redis_client.redis_client.pipeline()
            executor_key = f"executor:{executor_ip}:resources"
            
            pipe.hset(executor_key, "available_cpu_cores", new_cpu)
            pipe.hset(executor_key, "available_memory_gb", new_memory)
            pipe.hset(executor_key, "available_gpus", new_gpus)
            pipe.hset(executor_key, "last_heartbeat", datetime.utcnow().isoformat())
            pipe.expire(executor_key, 120)
            
            pipe.execute()
            return True
            
        except Exception as e:
            print(f"Error releasing resources: {e}")
            return False

    def get_available_executors(self) -> List[ExecutorResources]:
        """Get list of available executors (those that sent heartbeat recently)."""
        try:
            all_executors = self.redis_client.get_all_executors()
            available_executors = []
            
            cutoff_time = datetime.utcnow() - timedelta(minutes=2)
            
            for executor in all_executors:
                # Check if executor is recent (heartbeat within last 2 minutes)
                if isinstance(executor.last_heartbeat, str):
                    heartbeat_time = datetime.fromisoformat(executor.last_heartbeat.replace('Z', '+00:00').replace('+00:00', ''))
                else:
                    heartbeat_time = executor.last_heartbeat
                
                if heartbeat_time >= cutoff_time:
                    available_executors.append(executor)
            
            return available_executors
            
        except Exception as e:
            print(f"Error getting available executors: {e}")
            return []

    def find_suitable_executor(self, job: Job) -> Optional[ExecutorResources]:
        """Find the first executor that can accommodate the job (first-fit algorithm)."""
        available_executors = self.get_available_executors()
        
        # Sort executors by available resources (prefer executors with more available resources)
        available_executors.sort(
            key=lambda x: (x.available_cpu_cores, x.available_memory_gb, x.available_gpus),
            reverse=True
        )
        
        for executor in available_executors:
            if self.can_fit_job(job, executor):
                return executor
        
        return None

    def get_executor_utilization(self, executor_ip: str) -> Dict[str, float]:
        """Get resource utilization percentages for an executor."""
        try:
            executor = self.redis_client.get_executor_resources(executor_ip)
            if not executor:
                return {}
            
            cpu_utilization = ((executor.total_cpu_cores - executor.available_cpu_cores) / 
                             executor.total_cpu_cores * 100) if executor.total_cpu_cores > 0 else 0
            
            memory_utilization = ((executor.total_memory_gb - executor.available_memory_gb) / 
                                executor.total_memory_gb * 100) if executor.total_memory_gb > 0 else 0
            
            return {
                "cpu_utilization": round(cpu_utilization, 2),
                "memory_utilization": round(memory_utilization, 2),
                "running_jobs": len(self.redis_client.get_executor_jobs(executor_ip))
            }
            
        except Exception as e:
            print(f"Error calculating utilization: {e}")
            return {}

    def get_cluster_stats(self) -> Dict[str, any]:
        """Get cluster-wide resource statistics."""
        try:
            executors = self.get_available_executors()
            
            if not executors:
                return {
                    "total_executors": 0,
                    "total_cpu_cores": 0,
                    "available_cpu_cores": 0,
                    "total_memory_gb": 0,
                    "available_memory_gb": 0,
                    "cluster_cpu_utilization": 0,
                    "cluster_memory_utilization": 0
                }
            
            total_cpu = sum(e.total_cpu_cores for e in executors)
            available_cpu = sum(e.available_cpu_cores for e in executors)
            total_memory = sum(e.total_memory_gb for e in executors)
            available_memory = sum(e.available_memory_gb for e in executors)
            
            cpu_utilization = ((total_cpu - available_cpu) / total_cpu * 100) if total_cpu > 0 else 0
            memory_utilization = ((total_memory - available_memory) / total_memory * 100) if total_memory > 0 else 0
            
            return {
                "total_executors": len(executors),
                "total_cpu_cores": total_cpu,
                "available_cpu_cores": available_cpu,
                "total_memory_gb": total_memory,
                "available_memory_gb": available_memory,
                "cluster_cpu_utilization": round(cpu_utilization, 2),
                "cluster_memory_utilization": round(memory_utilization, 2)
            }
            
        except Exception as e:
            print(f"Error calculating cluster stats: {e}")
            return {}