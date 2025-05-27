import docker
import threading
import time
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime
import json


class DockerManager:
    def __init__(self):
        self.client = None
        self.containers: Dict[str, Any] = {}
        self.log_streams: Dict[str, threading.Thread] = {}
        self.log_callbacks: Dict[str, Callable] = {}

    def initialize(self) -> bool:
        """Initialize Docker client connection."""
        try:
            self.client = docker.from_env()
            
            # Test connection
            self.client.ping()
            print("Docker connection established")
            return True
            
        except docker.errors.DockerException as e:
            print(f"Failed to connect to Docker: {e}")
            return False
        except Exception as e:
            print(f"Error initializing Docker manager: {e}")
            return False

    def start_container(
        self, 
        image: str, 
        command: List[str], 
        cpu_cores: int = 1, 
        memory_gb: int = 1,
        gpu_type: Optional[str] = None,
        gpu_count: int = 0,
        environment: Optional[Dict[str, str]] = None,
        volumes: Optional[Dict[str, Dict[str, str]]] = None,
        working_dir: Optional[str] = None,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """Start a Docker container for job execution with full resource control."""
        try:
            if not self.client:
                return None
            
            # Pull image if not available locally
            self._pull_image_if_needed(image)
            
            # Prepare container configuration
            container_config = self._build_container_config(
                image, command, cpu_cores, memory_gb, gpu_type, gpu_count,
                environment, volumes, working_dir
            )
            
            # Start container
            container = self.client.containers.run(**container_config)
            
            container_id = container.id
            self.containers[container_id] = {
                "container": container,
                "started_at": datetime.utcnow(),
                "image": image,
                "command": command,
                "cpu_cores": cpu_cores,
                "memory_gb": memory_gb,
                "gpu_type": gpu_type,
                "gpu_count": gpu_count,
                "status": "running"
            }
            
            # Start log streaming if callback provided
            if log_callback:
                self.log_callbacks[container_id] = log_callback
                self._start_log_streaming(container_id)
            
            gpu_info = f"{gpu_count}x {gpu_type}" if gpu_type and gpu_count > 0 else "none"
            print(f"Started container {container_id[:12]} for image {image} "
                  f"(CPU: {cpu_cores}, Memory: {memory_gb}GB, GPU: {gpu_info})")
            return container_id
            
        except docker.errors.ContainerError as e:
            print(f"Container failed to start: {e}")
            return None
        except docker.errors.ImageNotFound as e:
            print(f"Image not found: {e}")
            return None
        except docker.errors.APIError as e:
            print(f"Docker API error: {e}")
            return None
        except Exception as e:
            print(f"Error starting container: {e}")
            return None

    def _pull_image_if_needed(self, image: str):
        """Pull Docker image if not available locally."""
        try:
            self.client.images.get(image)
        except docker.errors.ImageNotFound:
            print(f"Pulling image {image}...")
            try:
                self.client.images.pull(image)
                print(f"Successfully pulled image {image}")
            except docker.errors.APIError as e:
                print(f"Failed to pull image {image}: {e}")
                raise

    def _build_container_config(
        self, 
        image: str, 
        command: List[str], 
        cpu_cores: int, 
        memory_gb: int, 
        gpu_type: Optional[str] = None,
        gpu_count: int = 0,
        environment: Optional[Dict[str, str]] = None,
        volumes: Optional[Dict[str, Dict[str, str]]] = None,
        working_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build container configuration with resource limits and GPU support."""
        
        config = {
            "image": image,
            "command": command,
            "detach": True,
            "remove": False,
            "auto_remove": False,
            "network_mode": "bridge",
            "tty": True,
            "stdin_open": True
        }
        
        # CPU limits (using nano_cpus for resource limits)
        config["nano_cpus"] = int(cpu_cores * 1_000_000_000)  # Convert to nanocpus
        
        # Memory limits
        config["mem_limit"] = f"{memory_gb}g"
        config["memswap_limit"] = f"{memory_gb}g"  # Prevent swap usage
        
        # GPU support
        if gpu_type and gpu_count > 0:
            # For nvidia-docker runtime - allocate specific number of GPUs
            config["device_requests"] = [
                docker.types.DeviceRequest(count=gpu_count, capabilities=[["gpu"]])
            ]
            # Alternative: use runtime if nvidia-docker2 is installed
            # config["runtime"] = "nvidia"
        
        # Environment variables
        if environment:
            config["environment"] = environment
        
        # Volume mounts
        if volumes:
            config["volumes"] = volumes
        
        # Working directory
        if working_dir:
            config["working_dir"] = working_dir
        
        # Security options
        config["security_opt"] = ["no-new-privileges:true"]
        
        # Ulimits for better resource control
        config["ulimits"] = [
            docker.types.Ulimit(name="nofile", soft=1024, hard=1024),
            docker.types.Ulimit(name="nproc", soft=512, hard=512)
        ]
        
        return config

    def _start_log_streaming(self, container_id: str):
        """Start streaming container logs in a separate thread."""
        def stream_logs():
            try:
                container = self.containers[container_id]["container"]
                log_stream = container.logs(stream=True, follow=True, timestamps=True)
                
                for log_line in log_stream:
                    if container_id not in self.containers:
                        break
                        
                    log_text = log_line.decode('utf-8').strip()
                    if log_text and container_id in self.log_callbacks:
                        self.log_callbacks[container_id](log_text)
                        
            except Exception as e:
                print(f"Error streaming logs for container {container_id[:12]}: {e}")
        
        log_thread = threading.Thread(target=stream_logs, daemon=True)
        log_thread.start()
        self.log_streams[container_id] = log_thread

    def stop_container(self, container_id: str, timeout: int = 10) -> bool:
        """Stop a running container and clean up resources."""
        try:
            # Stop log streaming
            self._stop_log_streaming(container_id)
            
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.stop(timeout=timeout)
                self.containers[container_id]["status"] = "stopped"
                print(f"Stopped container {container_id[:12]}")
                return True
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    container.stop(timeout=timeout)
                    return True
                except docker.errors.NotFound:
                    print(f"Container {container_id[:12]} not found")
                    return False
                    
        except Exception as e:
            print(f"Error stopping container {container_id[:12]}: {e}")
            return False

    def _stop_log_streaming(self, container_id: str):
        """Stop log streaming thread for a container."""
        if container_id in self.log_callbacks:
            del self.log_callbacks[container_id]
        
        if container_id in self.log_streams:
            # Note: The thread will terminate naturally when the container stops
            del self.log_streams[container_id]

    def remove_container(self, container_id: str) -> bool:
        """Remove a container and clean up all associated resources."""
        try:
            # Stop log streaming first
            self._stop_log_streaming(container_id)
            
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.remove(force=True)
                del self.containers[container_id]
                print(f"Removed container {container_id[:12]}")
                return True
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    container.remove(force=True)
                    return True
                except docker.errors.NotFound:
                    print(f"Container {container_id[:12]} not found")
                    return False
                    
        except Exception as e:
            print(f"Error removing container {container_id[:12]}: {e}")
            return False

    def kill_container(self, container_id: str, signal: str = "SIGKILL") -> bool:
        """Forcefully kill a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.kill(signal=signal)
                self.containers[container_id]["status"] = "killed"
                print(f"Killed container {container_id[:12]} with {signal}")
                return True
            else:
                try:
                    container = self.client.containers.get(container_id)
                    container.kill(signal=signal)
                    return True
                except docker.errors.NotFound:
                    print(f"Container {container_id[:12]} not found")
                    return False
                    
        except Exception as e:
            print(f"Error killing container {container_id[:12]}: {e}")
            return False

    def get_container_status(self, container_id: str) -> str:
        """Get the current status of a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.reload()
                status = container.status
                # Update our local tracking
                self.containers[container_id]["status"] = status
                return status
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    return container.status
                except docker.errors.NotFound:
                    return "not_found"
                    
        except Exception as e:
            print(f"Error getting container status {container_id[:12]}: {e}")
            return "error"

    def get_container_details(self, container_id: str) -> Dict[str, Any]:
        """Get detailed information about a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.reload()
                
                info = self.containers[container_id].copy()
                info.update({
                    "current_status": container.status,
                    "container_id": container_id,
                    "short_id": container_id[:12],
                    "name": container.name,
                    "labels": container.labels,
                    "created": container.attrs.get("Created"),
                    "state": container.attrs.get("State", {}),
                    "network_settings": container.attrs.get("NetworkSettings", {}),
                    "host_config": container.attrs.get("HostConfig", {})
                })
                
                # Remove the container object from the copy to make it JSON serializable
                if "container" in info:
                    del info["container"]
                
                return info
            else:
                return {"error": "Container not managed by this instance"}
                
        except Exception as e:
            return {"error": str(e)}

    def monitor_container_resources(self, container_id: str) -> Dict[str, Any]:
        """Get real-time resource usage stats for a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                stats = container.stats(stream=False)
                
                # Parse CPU usage
                cpu_stats = stats.get("cpu_stats", {})
                precpu_stats = stats.get("precpu_stats", {})
                
                cpu_percent = 0.0
                if cpu_stats and precpu_stats:
                    cpu_delta = cpu_stats.get("cpu_usage", {}).get("total_usage", 0) - \
                               precpu_stats.get("cpu_usage", {}).get("total_usage", 0)
                    system_delta = cpu_stats.get("system_cpu_usage", 0) - \
                                  precpu_stats.get("system_cpu_usage", 0)
                    
                    if system_delta > 0:
                        cpu_count = len(cpu_stats.get("cpu_usage", {}).get("percpu_usage", [1]))
                        cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
                
                # Parse memory usage
                memory_stats = stats.get("memory_stats", {})
                memory_usage = memory_stats.get("usage", 0)
                memory_limit = memory_stats.get("limit", 0)
                memory_percent = (memory_usage / memory_limit * 100) if memory_limit > 0 else 0
                
                # Parse network I/O
                networks = stats.get("networks", {})
                rx_bytes = sum(net.get("rx_bytes", 0) for net in networks.values())
                tx_bytes = sum(net.get("tx_bytes", 0) for net in networks.values())
                
                # Parse block I/O
                blkio_stats = stats.get("blkio_stats", {})
                read_bytes = sum(entry.get("value", 0) 
                               for entry in blkio_stats.get("io_service_bytes_recursive", [])
                               if entry.get("op") == "Read")
                write_bytes = sum(entry.get("value", 0) 
                                for entry in blkio_stats.get("io_service_bytes_recursive", [])
                                if entry.get("op") == "Write")
                
                return {
                    "cpu_percent": round(cpu_percent, 2),
                    "memory_usage_bytes": memory_usage,
                    "memory_limit_bytes": memory_limit,
                    "memory_percent": round(memory_percent, 2),
                    "network_rx_bytes": rx_bytes,
                    "network_tx_bytes": tx_bytes,
                    "block_read_bytes": read_bytes,
                    "block_write_bytes": write_bytes,
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                return {"error": "Container not managed by this instance"}
                
        except Exception as e:
            print(f"Error getting container resources {container_id[:12]}: {e}")
            return {"error": str(e)}

    def get_container_exit_code(self, container_id: str) -> Optional[int]:
        """Get the exit code of a finished container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                container.reload()
                return container.attrs.get("State", {}).get("ExitCode")
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    return container.attrs.get("State", {}).get("ExitCode")
                except docker.errors.NotFound:
                    return None
                    
        except Exception as e:
            print(f"Error getting container exit code {container_id[:12]}: {e}")
            return None

    def get_container_logs(self, container_id: str, tail: int = 100) -> str:
        """Get logs from a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                logs = container.logs(tail=tail, timestamps=True)
                return logs.decode('utf-8')
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    logs = container.logs(tail=tail, timestamps=True)
                    return logs.decode('utf-8')
                except docker.errors.NotFound:
                    return "Container not found"
                    
        except Exception as e:
            print(f"Error getting container logs {container_id[:12]}: {e}")
            return f"Error retrieving logs: {e}"

    def get_container_stats(self, container_id: str) -> Dict[str, Any]:
        """Get resource usage stats for a container."""
        try:
            if container_id in self.containers:
                container = self.containers[container_id]["container"]
                stats = container.stats(stream=False)
                return stats
            else:
                # Try to find container by ID
                try:
                    container = self.client.containers.get(container_id)
                    stats = container.stats(stream=False)
                    return stats
                except docker.errors.NotFound:
                    return {}
                    
        except Exception as e:
            print(f"Error getting container stats {container_id[:12]}: {e}")
            return {}

    def list_containers(self) -> List[Dict[str, Any]]:
        """List all containers managed by this instance."""
        try:
            container_info = []
            for container_id, info in self.containers.items():
                container = info["container"]
                container.reload()
                
                container_info.append({
                    "id": container_id,
                    "short_id": container_id[:12],
                    "image": info["image"],
                    "command": info["command"],
                    "status": container.status,
                    "started_at": info["started_at"].isoformat()
                })
                
            return container_info
            
        except Exception as e:
            print(f"Error listing containers: {e}")
            return []

    def cleanup(self):
        """Clean up all containers managed by this instance."""
        try:
            print("Cleaning up Docker containers...")
            
            for container_id in list(self.containers.keys()):
                print(f"Cleaning up container {container_id[:12]}")
                self.stop_container(container_id)
                self.remove_container(container_id)
            
            self.containers.clear()
            print("Docker cleanup complete")
            
        except Exception as e:
            print(f"Error during Docker cleanup: {e}")

    def cleanup_all_stopped_containers(self):
        """Remove all stopped containers."""
        try:
            if not self.client:
                return
                
            containers = self.client.containers.list(all=True, filters={"status": "exited"})
            
            for container in containers:
                try:
                    container.remove()
                    print(f"Removed stopped container {container.id[:12]}")
                except Exception as e:
                    print(f"Failed to remove container {container.id[:12]}: {e}")
                    
        except Exception as e:
            print(f"Error cleaning up stopped containers: {e}")

    def prune_images(self):
        """Remove unused Docker images to free up space."""
        try:
            if not self.client:
                return
                
            result = self.client.images.prune()
            freed_space = result.get('SpaceReclaimed', 0)
            
            if freed_space > 0:
                print(f"Freed {freed_space / (1024*1024):.1f} MB by pruning images")
            else:
                print("No unused images to prune")
                
        except Exception as e:
            print(f"Error pruning images: {e}")