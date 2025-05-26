import redis
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from shared.models import Job, JobStatus, ExecutorResources
from shared.config import RedisConfig, RedisKeys


class ExecutorRedisClient:
    def __init__(self, executor_ip: str):
        self.executor_ip = executor_ip
        self.redis_client = redis.Redis(
            host=RedisConfig.HOST,
            port=RedisConfig.PORT,
            db=RedisConfig.DB,
            password=RedisConfig.PASSWORD,
            decode_responses=RedisConfig.DECODE_RESPONSES
        )

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

    def update_job_status(self, job_id: str, status: JobStatus) -> bool:
        try:
            job = self.get_job(job_id)
            if not job:
                return False

            job.update_status(status, self.executor_ip)
            
            pipe = self.redis_client.pipeline()
            
            job_key = RedisKeys.job(job_id)
            pipe.hset(job_key, "data", job.model_dump_json())
            
            if status == JobStatus.RUNNING:
                pipe.zrem(RedisKeys.jobs_pending(), job_id)
                pipe.sadd(RedisKeys.jobs_running(), job_id)
                pipe.sadd(RedisKeys.executor_jobs(self.executor_ip), job_id)
            elif status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
                pipe.srem(RedisKeys.jobs_running(), job_id)
                pipe.srem(RedisKeys.executor_jobs(self.executor_ip), job_id)
            
            pipe.execute()
            return True
        except Exception as e:
            print(f"Error updating job status: {e}")
            return False

    def get_next_pending_job(self) -> Optional[Job]:
        try:
            job_ids = self.redis_client.zrange(RedisKeys.jobs_pending(), 0, 0)
            
            if job_ids:
                job_id = job_ids[0]
                job = self.get_job(job_id)
                return job
            
            return None
        except Exception as e:
            print(f"Error getting next pending job: {e}")
            return None

    def claim_job(self, job_id: str) -> bool:
        try:
            pipe = self.redis_client.pipeline()
            pipe.watch(RedisKeys.jobs_pending())
            
            if self.redis_client.zscore(RedisKeys.jobs_pending(), job_id) is not None:
                pipe.multi()
                pipe.zrem(RedisKeys.jobs_pending(), job_id)
                pipe.sadd(RedisKeys.jobs_running(), job_id)
                pipe.sadd(RedisKeys.executor_jobs(self.executor_ip), job_id)
                
                result = pipe.execute()
                return result is not None
            else:
                pipe.unwatch()
                return False
                
        except Exception as e:
            print(f"Error claiming job {job_id}: {e}")
            return False

    def register_resources(self, resources: ExecutorResources) -> bool:
        try:
            executor_key = RedisKeys.executor_resources(self.executor_ip)
            resources.last_heartbeat = datetime.utcnow()
            
            # Convert to Redis-compatible format
            resource_data = {
                "ip": resources.ip,
                "total_cpu_cores": resources.total_cpu_cores,
                "available_cpu_cores": resources.available_cpu_cores,
                "total_memory_gb": resources.total_memory_gb,
                "available_memory_gb": resources.available_memory_gb,
                "gpu_types": ",".join(resources.gpu_types),  # Convert list to comma-separated string
                "region": resources.region,
                "datacenter": resources.datacenter,
                "last_heartbeat": resources.last_heartbeat.isoformat()
            }
            
            self.redis_client.hset(executor_key, mapping=resource_data)
            self.redis_client.expire(executor_key, 120)
            return True
        except Exception as e:
            print(f"Error registering resources: {e}")
            return False

    def update_available_resources(self, available_cpu: int, available_memory: int) -> bool:
        try:
            executor_key = RedisKeys.executor_resources(self.executor_ip)
            
            pipe = self.redis_client.pipeline()
            pipe.hset(executor_key, "available_cpu_cores", available_cpu)
            pipe.hset(executor_key, "available_memory_gb", available_memory)
            pipe.hset(executor_key, "last_heartbeat", datetime.utcnow().isoformat())
            pipe.expire(executor_key, 120)
            pipe.execute()
            
            return True
        except Exception as e:
            print(f"Error updating available resources: {e}")
            return False

    def get_my_jobs(self) -> List[str]:
        try:
            return list(self.redis_client.smembers(RedisKeys.executor_jobs(self.executor_ip)))
        except Exception as e:
            print(f"Error getting my jobs: {e}")
            return []

    def add_job_log(self, job_id: str, log_line: str) -> bool:
        try:
            log_key = RedisKeys.job_logs(job_id)
            timestamp = datetime.utcnow().isoformat()
            log_entry = f"[{timestamp}] {log_line}"
            
            self.redis_client.lpush(log_key, log_entry)
            self.redis_client.ltrim(log_key, 0, 999)
            self.redis_client.expire(log_key, 86400)
            
            return True
        except Exception as e:
            print(f"Error adding job log: {e}")
            return False

    def heartbeat(self) -> bool:
        try:
            executor_key = RedisKeys.executor_resources(self.executor_ip)
            
            if self.redis_client.exists(executor_key):
                self.redis_client.hset(executor_key, "last_heartbeat", datetime.utcnow().isoformat())
                self.redis_client.expire(executor_key, 120)
                return True
            return False
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
            return False