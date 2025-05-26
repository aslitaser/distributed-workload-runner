import json
import redis
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID

from shared.models import Job, JobStatus, ExecutorResources
from shared.config import RedisConfig, RedisKeys


class SchedulerRedisClient:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=RedisConfig.HOST,
            port=RedisConfig.PORT,
            db=RedisConfig.DB,
            password=RedisConfig.PASSWORD,
            decode_responses=RedisConfig.DECODE_RESPONSES
        )

    def create_job(self, job: Job) -> bool:
        try:
            pipe = self.redis_client.pipeline()
            
            job_key = RedisKeys.job(str(job.job_id))
            job_data = job.model_dump_json()
            
            pipe.hset(job_key, mapping={"data": job_data})
            
            pipe.zadd(RedisKeys.jobs_pending(), {str(job.job_id): job.created_at.timestamp()})
            
            pipe.execute()
            return True
        except Exception as e:
            print(f"Error creating job: {e}")
            return False

    def get_job(self, job_id: str) -> Optional[Job]:
        try:
            job_key = RedisKeys.job(job_id)
            job_data = self.redis_client.hget(job_key, "data")
            
            if job_data:
                return Job.model_validate_json(job_data)
            return None
        except Exception as e:
            print(f"Error getting job {job_id}: {e}")
            return None

    def update_job_status(self, job_id: str, status: JobStatus, executor_ip: Optional[str] = None) -> bool:
        try:
            job = self.get_job(job_id)
            if not job:
                return False

            job.update_status(status, executor_ip)
            
            pipe = self.redis_client.pipeline()
            
            job_key = RedisKeys.job(job_id)
            pipe.hset(job_key, "data", job.model_dump_json())
            
            if status == JobStatus.RUNNING:
                pipe.zrem(RedisKeys.jobs_pending(), job_id)
                pipe.sadd(RedisKeys.jobs_running(), job_id)
                if executor_ip:
                    pipe.sadd(RedisKeys.executor_jobs(executor_ip), job_id)
            elif status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
                pipe.srem(RedisKeys.jobs_running(), job_id)
                if job.executor_ip:
                    pipe.srem(RedisKeys.executor_jobs(job.executor_ip), job_id)
            
            pipe.execute()
            return True
        except Exception as e:
            print(f"Error updating job status: {e}")
            return False

    def get_pending_jobs(self, limit: int = 10) -> List[Job]:
        try:
            job_ids = self.redis_client.zrange(RedisKeys.jobs_pending(), 0, limit - 1)
            jobs = []
            
            for job_id in job_ids:
                job = self.get_job(job_id)
                if job:
                    jobs.append(job)
            
            return jobs
        except Exception as e:
            print(f"Error getting pending jobs: {e}")
            return []

    def get_running_jobs(self) -> List[Job]:
        try:
            job_ids = self.redis_client.smembers(RedisKeys.jobs_running())
            jobs = []
            
            for job_id in job_ids:
                job = self.get_job(job_id)
                if job:
                    jobs.append(job)
            
            return jobs
        except Exception as e:
            print(f"Error getting running jobs: {e}")
            return []

    def register_executor(self, resources: ExecutorResources) -> bool:
        try:
            executor_key = RedisKeys.executor_resources(resources.ip)
            self.redis_client.hset(executor_key, mapping=resources.model_dump())
            self.redis_client.expire(executor_key, 120)
            return True
        except Exception as e:
            print(f"Error registering executor: {e}")
            return False

    def get_executor_resources(self, ip: str) -> Optional[ExecutorResources]:
        try:
            executor_key = RedisKeys.executor_resources(ip)
            data = self.redis_client.hgetall(executor_key)
            
            if data:
                # Convert Redis data back to proper types
                converted_data = {
                    "ip": data["ip"],
                    "total_cpu_cores": int(data["total_cpu_cores"]),
                    "available_cpu_cores": int(data["available_cpu_cores"]),
                    "total_memory_gb": int(data["total_memory_gb"]),
                    "available_memory_gb": int(data["available_memory_gb"]),
                    "gpu_types": data["gpu_types"].split(",") if data["gpu_types"] else [],
                    "region": data.get("region", "unknown"),
                    "datacenter": data.get("datacenter", "unknown"),
                    "last_heartbeat": datetime.fromisoformat(data["last_heartbeat"])
                }
                return ExecutorResources(**converted_data)
            return None
        except Exception as e:
            print(f"Error getting executor resources: {e}")
            return None

    def get_all_executors(self) -> List[ExecutorResources]:
        try:
            pattern = RedisKeys.executor_resources("*")
            keys = self.redis_client.keys(pattern)
            executors = []
            
            for key in keys:
                data = self.redis_client.hgetall(key)
                if data:
                    try:
                        # Convert Redis data back to proper types
                        converted_data = {
                            "ip": data["ip"],
                            "total_cpu_cores": int(data["total_cpu_cores"]),
                            "available_cpu_cores": int(data["available_cpu_cores"]),
                            "total_memory_gb": int(data["total_memory_gb"]),
                            "available_memory_gb": int(data["available_memory_gb"]),
                            "gpu_types": data["gpu_types"].split(",") if data["gpu_types"] else [],
                            "region": data.get("region", "unknown"),
                            "datacenter": data.get("datacenter", "unknown"),
                            "last_heartbeat": datetime.fromisoformat(data["last_heartbeat"])
                        }
                        executors.append(ExecutorResources(**converted_data))
                    except Exception as e:
                        print(f"Error converting executor data: {e}")
                        continue
            
            return executors
        except Exception as e:
            print(f"Error getting all executors: {e}")
            return []

    def get_executor_jobs(self, executor_ip: str) -> List[str]:
        try:
            return list(self.redis_client.smembers(RedisKeys.executor_jobs(executor_ip)))
        except Exception as e:
            print(f"Error getting executor jobs: {e}")
            return []