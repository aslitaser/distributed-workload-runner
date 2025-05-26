# Distributed Workload Runner

A distributed job execution system that schedules and runs Docker containers across multiple executor nodes with resource-aware scheduling.

## Architecture

- **Scheduler**: FastAPI service that receives job requests, manages the job queue, and assigns jobs to available executors
- **Executor**: Worker service that polls for assigned jobs, runs Docker containers, and reports back status
- **Redis**: Central data store for job queue, executor registration, and job logs

## Features

- ✅ Resource-aware job scheduling (CPU, memory, GPU)
- ✅ Docker container execution with resource limits
- ✅ Real-time job monitoring and logging
- ✅ GPU support detection and allocation
- ✅ Graceful job abortion and cleanup
- ✅ Executor heartbeat and health monitoring
- ✅ First-fit scheduling algorithm
- ✅ Comprehensive REST API
- ✅ Background job scheduling

## Quick Start

### Prerequisites

1. **Redis server**:
   ```bash
   # Install Redis (macOS)
   brew install redis
   redis-server
   
   # Or using Docker
   docker run -d -p 6379:6379 redis:alpine
   ```

2. **Docker** (for running job containers)

3. **Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Services

1. **Start the Scheduler**:
   ```bash
   python -m scheduler.api
   ```
   
   The scheduler API will be available at `http://localhost:8000`

2. **Start an Executor** (in a new terminal):
   ```bash
   python -m executor.main
   ```
   
   Start multiple executors on different machines by setting `EXECUTOR_IP` environment variable.

3. **Test the System**:
   ```bash
   python test_job_flow.py
   ```

## API Usage

### Submit a Job

```bash
curl -X POST "http://localhost:8000/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "docker_image": "alpine:latest",
    "command": ["echo", "Hello World!"],
    "cpu_cores": 1,
    "memory_gb": 1,
    "gpu_type": null
  }'
```

### Check Job Status

```bash
curl "http://localhost:8000/jobs/{job_id}"
```

### List All Jobs

```bash
curl "http://localhost:8000/jobs"
```

### View Executors

```bash
curl "http://localhost:8000/executors"
```

### Get System Stats

```bash
curl "http://localhost:8000/stats"
```

### Manual Job Scheduling

```bash
curl -X POST "http://localhost:8000/schedule"
```

## Configuration

### Environment Variables

**Scheduler**:
- `SCHEDULER_API_HOST` (default: "0.0.0.0")
- `SCHEDULER_API_PORT` (default: 8000)

**Executor**:
- `EXECUTOR_IP` (default: auto-detected local IP)
- `HEARTBEAT_INTERVAL` (default: 30 seconds)
- `JOB_POLL_INTERVAL` (default: 10 seconds)

**Redis**:
- `REDIS_HOST` (default: "localhost")
- `REDIS_PORT` (default: 6379)
- `REDIS_PASSWORD` (default: None)
- `REDIS_DB` (default: 0)

### GPU Support

The system automatically detects available GPUs using `nvidia-smi`. To run GPU jobs:

```json
{
  "docker_image": "tensorflow/tensorflow:latest-gpu",
  "command": ["python", "-c", "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"],
  "cpu_cores": 2,
  "memory_gb": 4,
  "gpu_type": "any"
}
```

## Job Lifecycle

1. **PENDING**: Job submitted and waiting for assignment
2. **RUNNING**: Job assigned to executor and container started
3. **SUCCEEDED**: Job completed successfully (exit code 0)
4. **FAILED**: Job failed (non-zero exit code or error)

## Monitoring

### Job Logs

Job execution logs are stored in Redis and can be retrieved via the job details API or directly:

```python
import redis
r = redis.Redis()
logs = r.lrange(f"job_logs:{job_id}", 0, -1)
```

### Resource Monitoring

The system continuously monitors:
- CPU and memory usage per container
- System resources per executor
- Job queue statistics
- Cluster utilization

### Health Checks

- **Scheduler**: `GET /health`
- **Executor heartbeat**: Automatic every 30 seconds
- **Stale job cleanup**: Automatic cleanup of jobs running >60 minutes

## Scaling

### Horizontal Scaling

Add more executors by starting the executor service on additional machines:

```bash
EXECUTOR_IP=192.168.1.100 python -m executor.main
```

### Resource Management

The scheduler automatically:
- Tracks available resources per executor
- Assigns jobs using first-fit algorithm
- Prevents resource over-allocation
- Balances load across executors

## Troubleshooting

### Common Issues

1. **"No executors available"**
   - Ensure at least one executor is running
   - Check executor logs for connection issues
   - Verify Redis connectivity

2. **"Failed to start container"**
   - Check Docker daemon is running
   - Verify image exists and is accessible
   - Check resource constraints

3. **Jobs stuck in PENDING**
   - Check if any executors have sufficient resources
   - Manually trigger scheduling: `POST /schedule`
   - Review executor heartbeat status

### Logs

- Scheduler logs: Console output and `scheduler.log`
- Executor logs: Console output and `executor.log`
- Job logs: Stored in Redis with key `job_logs:{job_id}`

## Development

### Project Structure

```
distributed-workload-runner/
├── scheduler/           # Scheduler service
│   ├── api.py          # FastAPI application
│   ├── job_manager.py  # Job scheduling logic
│   ├── redis_client.py # Redis operations
│   └── resource_manager.py # Resource tracking
├── executor/           # Executor service
│   ├── main.py         # Main executor loop
│   ├── redis_client.py # Redis operations
│   ├── resource_monitor.py # System resource detection
│   └── docker_manager.py # Docker container management
├── shared/             # Shared models and config
│   ├── models.py       # Pydantic models
│   └── config.py       # Configuration classes
└── test_job_flow.py    # End-to-end tests
```



