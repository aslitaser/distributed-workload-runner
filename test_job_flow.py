#!/usr/bin/env python3
"""
Test script for the complete job execution flow.
This script tests the end-to-end workflow from job submission to completion.
"""

import time
import json
import requests
import os
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
                    region = executor.get('region', 'unknown')
                    datacenter = executor.get('datacenter', 'unknown')
                    gpu_info = ""
                    if executor.get('gpu_types') and executor.get('total_gpus', 0) > 0:
                        gpu_types = ", ".join(executor['gpu_types'])
                        gpu_count = f"{executor.get('available_gpus', 0)}/{executor.get('total_gpus', 0)}"
                        gpu_info = f", GPUs: {gpu_count} ({gpu_types})"
                    print(f"   - {executor['ip']}: {executor['available_cpu_cores']} CPU, "
                          f"{executor['available_memory_gb']}GB RAM{gpu_info}, "
                          f"Region: {region}, DC: {datacenter}")
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


def test_region_datacenter_scheduling():
    """Test region and datacenter aware job scheduling."""
    print("\n🌍 Testing region/datacenter aware scheduling...")
    
    scheduler_api = "http://localhost:8000"
    redis_client = SchedulerRedisClient()
    
    print("\n1️⃣ Checking executor regions and datacenters...")
    try:
        response = requests.get(f"{scheduler_api}/executors", timeout=5)
        if response.status_code == 200:
            data = response.json()
            executors = data.get("executors", [])
            
            if not executors:
                print("⚠️  No executors available for region/datacenter testing")
                return False
            
            # Group executors by region and datacenter
            regions = {}
            datacenters = {}
            
            for executor in executors:
                region = executor.get('region', 'unknown')
                datacenter = executor.get('datacenter', 'unknown')
                
                if region not in regions:
                    regions[region] = []
                regions[region].append(executor['ip'])
                
                if datacenter not in datacenters:
                    datacenters[datacenter] = []
                datacenters[datacenter].append(executor['ip'])
            
            print(f"📍 Available regions: {list(regions.keys())}")
            print(f"🏢 Available datacenters: {list(datacenters.keys())}")
            
            # Test 1: Submit job with specific region requirement
            print("\n2️⃣ Testing job with region constraint...")
            test_region = list(regions.keys())[0]
            region_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", f"Running in region {test_region}"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "eligible_regions": [test_region]
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=region_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ Region-constrained job submitted: {job_id}")
                print(f"   Required region: {test_region}")
                
                # Wait and check if job was assigned to correct region
                time.sleep(5)
                response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    job_info = response.json()
                    if job_info.get("executor_ip"):
                        print(f"✅ Job assigned to executor: {job_info['executor_ip']}")
                        # Verify it's in the correct region
                        for executor in executors:
                            if executor['ip'] == job_info['executor_ip']:
                                actual_region = executor.get('region', 'unknown')
                                if actual_region == test_region:
                                    print(f"✅ Job correctly placed in region: {actual_region}")
                                else:
                                    print(f"❌ Job placed in wrong region: {actual_region} (expected: {test_region})")
                                break
                    else:
                        print("⏳ Job not yet assigned to an executor")
            else:
                print(f"❌ Failed to submit region-constrained job: {response.text}")
            
            # Test 2: Submit job with datacenter constraint
            print("\n3️⃣ Testing job with datacenter constraint...")
            test_datacenter = list(datacenters.keys())[0]
            dc_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", f"Running in datacenter {test_datacenter}"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "eligible_datacenters": [test_datacenter]
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=dc_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ Datacenter-constrained job submitted: {job_id}")
                print(f"   Required datacenter: {test_datacenter}")
            
            # Test 3: Submit job with allocation timeout
            print("\n4️⃣ Testing job with custom allocation timeout...")
            timeout_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", "Testing allocation timeout"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "allocation_timeout": 60  # 60 seconds
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=timeout_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                print(f"✅ Job with 60s allocation timeout submitted: {job_data['job_id']}")
                print(f"   Allocation timeout: {job_data['allocation_timeout']} seconds")
            
            # Test 4: Submit job that cannot be scheduled (impossible constraints)
            print("\n5️⃣ Testing job with impossible constraints...")
            impossible_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", "This should not run"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "eligible_regions": ["non-existent-region"],
                "allocation_timeout": 30  # Will timeout quickly
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=impossible_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ Impossible job submitted: {job_id}")
                print(f"   This job should fail due to allocation timeout...")
                
                # Wait for timeout
                print("   Waiting 35 seconds for allocation timeout...")
                time.sleep(35)
                
                response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    job_info = response.json()
                    if job_info['status'] == 'failed':
                        print(f"✅ Job correctly failed due to allocation timeout")
                    else:
                        print(f"❌ Job status is {job_info['status']}, expected 'failed'")
            
            return True
            
    except Exception as e:
        print(f"❌ Error in region/datacenter testing: {e}")
        return False


def test_gpu_scheduling():
    """Test GPU-aware job scheduling."""
    print("\n🎮 Testing GPU-aware scheduling...")
    
    scheduler_api = "http://localhost:8000"
    
    print("\n1️⃣ Checking GPU availability...")
    try:
        response = requests.get(f"{scheduler_api}/executors", timeout=5)
        if response.status_code == 200:
            data = response.json()
            executors = data.get("executors", [])
            
            gpu_executors = [e for e in executors if e.get('gpu_types') and e.get('available_gpus', 0) > 0]
            
            if not gpu_executors:
                print("⚠️  No GPU executors available - skipping GPU tests")
                print("💡 To test GPU scheduling, start executors with GPU configuration")
                return False
                
            print(f"✅ Found {len(gpu_executors)} GPU executor(s):")
            for executor in gpu_executors:
                gpu_info = f"{executor.get('available_gpus', 0)}/{executor.get('total_gpus', 0)} GPUs"
                gpu_types = ", ".join(executor.get('gpu_types', []))
                print(f"   - {executor['ip']}: {gpu_info} ({gpu_types})")
            
            # Test 1: Job requiring "any" GPU
            print("\n2️⃣ Testing job with 'any' GPU requirement...")
            any_gpu_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", "Running with any GPU"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "gpu_type": "any",
                "gpu_count": 1
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=any_gpu_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ 'Any' GPU job submitted: {job_id}")
                
                # Wait and check assignment
                time.sleep(5)
                response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    job_info = response.json()
                    if job_info.get("executor_ip"):
                        print(f"✅ Job assigned to GPU executor: {job_info['executor_ip']}")
                    else:
                        print("⏳ Job not yet assigned")
            else:
                print(f"❌ 'Any' GPU job submission failed: {response.text}")
            
            # Test 2: Job requiring specific GPU type
            if gpu_executors[0].get('gpu_types'):
                print("\n3️⃣ Testing job with specific GPU type...")
                specific_gpu_type = gpu_executors[0]['gpu_types'][0]
                specific_gpu_job = {
                    "docker_image": "alpine:latest",
                    "command": ["echo", f"Running with {specific_gpu_type}"],
                    "cpu_cores": 1,
                    "memory_gb": 1,
                    "gpu_type": specific_gpu_type,
                    "gpu_count": 1
                }
                
                response = requests.post(f"{scheduler_api}/jobs", json=specific_gpu_job, timeout=10)
                if response.status_code == 201:
                    job_data = response.json()
                    print(f"✅ Specific GPU job submitted: {job_data['job_id']}")
                    print(f"   Required GPU: {specific_gpu_type}")
                else:
                    print(f"❌ Specific GPU job submission failed: {response.text}")
            
            # Test 3: Job requiring more GPUs than available
            print("\n4️⃣ Testing job with excessive GPU requirements...")
            max_available_gpus = max(e.get('available_gpus', 0) for e in gpu_executors)
            excessive_gpu_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", "This should not run"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "gpu_type": "any",
                "gpu_count": max_available_gpus + 10,  # More than available
                "allocation_timeout": 30
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=excessive_gpu_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ Excessive GPU job submitted: {job_id}")
                print(f"   Required GPUs: {max_available_gpus + 10} (should fail)")
                
                # Wait for allocation timeout
                print("   Waiting for allocation timeout...")
                time.sleep(35)
                
                response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    job_info = response.json()
                    if job_info['status'] == 'failed':
                        print("✅ Job correctly failed due to insufficient GPUs")
                    else:
                        print(f"❌ Job status is {job_info['status']}, expected 'failed'")
            
            # Test 4: Job requiring unsupported GPU type
            print("\n5️⃣ Testing job with unsupported GPU type...")
            unsupported_gpu_job = {
                "docker_image": "alpine:latest",
                "command": ["echo", "This should not run"],
                "cpu_cores": 1,
                "memory_gb": 1,
                "gpu_type": "unsupported-gpu-type",
                "gpu_count": 1,
                "allocation_timeout": 30
            }
            
            response = requests.post(f"{scheduler_api}/jobs", json=unsupported_gpu_job, timeout=10)
            if response.status_code == 201:
                job_data = response.json()
                job_id = job_data["job_id"]
                print(f"✅ Unsupported GPU job submitted: {job_id}")
                print("   This should fail due to unsupported GPU type...")
                
                time.sleep(35)
                response = requests.get(f"{scheduler_api}/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    job_info = response.json()
                    if job_info['status'] == 'failed':
                        print("✅ Job correctly failed due to unsupported GPU type")
                    else:
                        print(f"❌ Job status is {job_info['status']}, expected 'failed'")
            
            return True
        
    except Exception as e:
        print(f"❌ Error in GPU scheduling test: {e}")
        return False


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
        
        # Test GPU scheduling
        test_gpu_scheduling()
        
        # Test region/datacenter aware scheduling
        test_region_datacenter_scheduling()
    
    print("\n" + "=" * 50)
    print("✨ Test suite completed!")