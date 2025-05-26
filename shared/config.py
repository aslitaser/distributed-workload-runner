import os
from typing import Optional


class RedisConfig:
    HOST = os.getenv("REDIS_HOST", "localhost")
    PORT = int(os.getenv("REDIS_PORT", "6379"))
    DB = int(os.getenv("REDIS_DB", "0"))
    PASSWORD = os.getenv("REDIS_PASSWORD")
    DECODE_RESPONSES = True


class RedisKeys:
    @staticmethod
    def job(job_id: str) -> str:
        return f"job:{job_id}"
    
    @staticmethod
    def jobs_pending() -> str:
        return "jobs:pending"
    
    @staticmethod
    def jobs_running() -> str:
        return "jobs:running"
    
    @staticmethod
    def executor_resources(ip: str) -> str:
        return f"executor:{ip}:resources"
    
    @staticmethod
    def executor_jobs(ip: str) -> str:
        return f"executor:{ip}:jobs"
    
    @staticmethod
    def job_logs(job_id: str) -> str:
        return f"job:{job_id}:logs"


class SchedulerConfig:
    API_HOST = os.getenv("SCHEDULER_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("SCHEDULER_PORT", "8000"))
    REDIS_CONFIG = RedisConfig


class ExecutorConfig:
    EXECUTOR_IP = os.getenv("EXECUTOR_IP", "localhost")
    HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "30"))
    JOB_POLL_INTERVAL = int(os.getenv("JOB_POLL_INTERVAL", "5"))
    REGION_NAME = os.getenv("EXECUTOR_REGION", "us-east-1")
    DATACENTER_NAME = os.getenv("EXECUTOR_DATACENTER", "dc1")
    REDIS_CONFIG = RedisConfig