#!/usr/bin/env python3
"""
Test script for the complete job execution flow.
This script tests the end-to-end workflow from job submission to completion.
"""

import time
import json
import requests
from uuid import uuid4
from shared.models import Job, JobStatus
from scheduler.redis_client import SchedulerRedisClient

def test_job_execution_flow():
    """Test the complete job execution flow."""
    print("🧪 Testing job execution flow...")
    
    # Test configuration
    scheduler_api = "http://localhost:8000"
    redis_client = SchedulerRedisClient()
    
    print("\n1️⃣ Testing scheduler health...")
    try:
        response = requests.get(f"{scheduler_api}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Scheduler is healthy")
        else:
            print("❌ Scheduler health check failed")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to scheduler: {e}")
        print("💡 Make sure to start the scheduler with: python -m scheduler.api")
        return False
    
    print("\n2️⃣ Checking available executors...")
    try:
        response = requests.get(f"{scheduler_api}/executors", timeout=5)
        if response.status_code == 200:
            data = response.json()
            executors = data.get("executors", [])
            if executors:
                print(f"✅ Found {len(executors)} executor(s):")
                for executor in executors:
                    print(f"   - {executor['ip']}: {executor['available_cpu_cores']} CPU, "
                          f"{executor['available_memory_gb']}GB RAM")
            else:
                print("⚠️  No executors available")
                print("💡 Start an executor with: python -m executor.main")
                return False
        else:
            print("❌ Failed to get executor list")
            return False
    except Exception as e:
        print(f"❌ Error checking executors: {e}")
        return False
    
    print("\n3️⃣ Submitting a test job...")
    test_job = {
        "docker_image": "alpine:latest",
        "command": ["echo", "Hello from distributed workload runner!"],
        "cpu_cores": 1,
        "memory_gb": 1,
        "gpu_type": None
    }
    
    try:
        response = requests.post(f"{scheduler_api}/jobs", json=test_job, timeout=10)
        if response.status_code == 201:
            job_data = response.json()
            job_id = job_data["job_id"]
            print(f"✅ Job submitted successfully: {job_id}")
        else:
            print(f"❌ Job submission failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error submitting job: {e}")
        return False
    
    print("\n4️⃣ Monitoring job execution...")
    max_wait_time = 120  # 2 minutes
    check_interval = 3   # 3 seconds
    elapsed_time = 0
    
    while elapsed_time < max_wait_time:
        try:
            response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
            if response.status_code == 200:
                job_data = response.json()
                status = job_data["status"]
                executor_ip = job_data.get("executor_ip")
                
                print(f"⏳ Job status: {status}" + (f" (executor: {executor_ip})" if executor_ip else ""))
                
                if status == "succeeded":
                    print("✅ Job completed successfully!")
                    break
                elif status == "failed":
                    print("❌ Job failed")
                    break
                elif status == "running":
                    print(f"🏃 Job is running on {executor_ip}")
                
            else:
                print(f"❌ Error checking job status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error monitoring job: {e}")
        
        time.sleep(check_interval)
        elapsed_time += check_interval
    
    if elapsed_time >= max_wait_time:
        print(f"⏰ Test timed out after {max_wait_time} seconds")
        return False
    
    print("\n5️⃣ Checking job logs...")
    try:
        # Get job logs from Redis directly
        log_key = f"job_logs:{job_id}"
        logs = redis_client.redis_client.lrange(log_key, 0, -1)
        
        if logs:
            print("📄 Job logs:")
            for log in reversed(logs[-10:]):  # Show last 10 logs in chronological order
                print(f"   {log}")
        else:
            print("📄 No job logs found")
            
    except Exception as e:
        print(f"❌ Error retrieving job logs: {e}")
    
    print("\n6️⃣ Testing job list API...")
    try:
        response = requests.get(f"{scheduler_api}/jobs?limit=5", timeout=5)
        if response.status_code == 200:
            jobs = response.json()
            print(f"✅ Retrieved {len(jobs)} jobs from API")
        else:
            print(f"❌ Error listing jobs: {response.status_code}")
    except Exception as e:
        print(f"❌ Error testing job list: {e}")
    
    print("\n🎉 Job execution flow test completed!")
    return True


def test_resource_scheduling():
    """Test resource-aware job scheduling."""
    print("\n🔬 Testing resource-aware scheduling...")
    
    scheduler_api = "http://localhost:8000"
    
    # Submit multiple jobs with different resource requirements
    test_jobs = [
        {"docker_image": "alpine", "command": ["sleep", "10"], "cpu_cores": 1, "memory_gb": 1},
        {"docker_image": "alpine", "command": ["sleep", "10"], "cpu_cores": 2, "memory_gb": 2},
        {"docker_image": "alpine", "command": ["sleep", "10"], "cpu_cores": 1, "memory_gb": 1},
    ]
    
    submitted_jobs = []
    
    print("📤 Submitting multiple test jobs...")
    for i, job in enumerate(test_jobs):
        try:
            response = requests.post(f"{scheduler_api}/jobs", json=job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                submitted_jobs.append(job_data["job_id"])
                print(f"✅ Job {i+1} submitted: {job_data['job_id']}")
            else:
                print(f"❌ Job {i+1} submission failed")
        except Exception as e:
            print(f"❌ Error submitting job {i+1}: {e}")
    
    if submitted_jobs:
        print(f"\n📊 Submitted {len(submitted_jobs)} jobs for parallel execution")
        
        # Wait a bit and check scheduler stats
        time.sleep(5)
        try:
            response = requests.get(f"{scheduler_api}/stats", timeout=5)
            if response.status_code == 200:
                stats = response.json()
                print("📈 Scheduler stats:")
                print(f"   Queue: {json.dumps(stats['queue'], indent=4)}")
                print(f"   Cluster: {json.dumps(stats['cluster'], indent=4)}")
        except Exception as e:
            print(f"❌ Error getting stats: {e}")


if __name__ == "__main__":
    print("🚀 Distributed Workload Runner - Test Suite")
    print("=" * 50)
    
    # Test basic job execution flow
    success = test_job_execution_flow()
    
    if success:
        # Test resource scheduling
        test_resource_scheduling()
    
    print("\n" + "=" * 50)
    print("✨ Test suite completed!")