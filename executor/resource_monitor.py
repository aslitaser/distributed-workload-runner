import psutil
import subprocess
import socket
from typing import List, Dict, Optional
from datetime import datetime

from shared.models import ExecutorResources
from shared.config import ExecutorConfig


class ResourceMonitor:
    def __init__(self):
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip_address()
        self.region = ExecutorConfig.REGION_NAME
        self.datacenter = ExecutorConfig.DATACENTER_NAME
        
    def _get_ip_address(self) -> str:
        """Get the IP address of this machine."""
        try:
            # Connect to a remote server to determine local IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    def get_cpu_info(self) -> Dict[str, int]:
        """Get CPU information including total and available cores."""
        try:
            total_cores = psutil.cpu_count(logical=True)
            
            # Get current CPU usage and calculate available cores
            cpu_percent = psutil.cpu_percent(interval=1)
            available_cores = max(1, int(total_cores * (100 - cpu_percent) / 100))
            
            return {
                "total_cores": total_cores,
                "available_cores": available_cores,
                "cpu_percent": cpu_percent
            }
        except Exception as e:
            print(f"Error getting CPU info: {e}")
            return {"total_cores": 1, "available_cores": 1, "cpu_percent": 0}

    def get_memory_info(self) -> Dict[str, int]:
        """Get memory information in GB."""
        try:
            memory = psutil.virtual_memory()
            
            total_gb = int(memory.total / (1024**3))
            available_gb = int(memory.available / (1024**3))
            
            return {
                "total_memory_gb": total_gb,
                "available_memory_gb": available_gb,
                "memory_percent": memory.percent
            }
        except Exception as e:
            print(f"Error getting memory info: {e}")
            return {"total_memory_gb": 1, "available_memory_gb": 1, "memory_percent": 0}

    def get_gpu_info(self) -> List[str]:
        """Detect available GPUs using nvidia-smi."""
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader,nounits"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                gpu_names = [name.strip() for name in result.stdout.strip().split('\n') if name.strip()]
                return gpu_names
            else:
                return []
                
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            # nvidia-smi not available or timeout
            return []
        except Exception as e:
            print(f"Error detecting GPUs: {e}")
            return []

    def get_disk_info(self) -> Dict[str, float]:
        """Get disk usage information."""
        try:
            disk = psutil.disk_usage('/')
            
            return {
                "total_gb": disk.total / (1024**3),
                "available_gb": disk.free / (1024**3),
                "used_percent": (disk.used / disk.total) * 100
            }
        except Exception as e:
            print(f"Error getting disk info: {e}")
            return {"total_gb": 0, "available_gb": 0, "used_percent": 0}

    def get_network_info(self) -> Dict[str, any]:
        """Get network information."""
        try:
            net_io = psutil.net_io_counters()
            
            return {
                "bytes_sent": net_io.bytes_sent,
                "bytes_recv": net_io.bytes_recv,
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv
            }
        except Exception as e:
            print(f"Error getting network info: {e}")
            return {}

    def get_system_load(self) -> Dict[str, float]:
        """Get system load averages."""
        try:
            load1, load5, load15 = psutil.getloadavg()
            
            return {
                "load_1min": load1,
                "load_5min": load5,
                "load_15min": load15
            }
        except Exception as e:
            print(f"Error getting system load: {e}")
            return {"load_1min": 0, "load_5min": 0, "load_15min": 0}

    def get_current_resources(self) -> ExecutorResources:
        """Get current resource status as ExecutorResources object."""
        try:
            cpu_info = self.get_cpu_info()
            memory_info = self.get_memory_info()
            gpu_info = self.get_gpu_info()
            
            return ExecutorResources(
                ip=self.ip_address,
                total_cpu_cores=cpu_info["total_cores"],
                available_cpu_cores=cpu_info["available_cores"],
                total_memory_gb=memory_info["total_memory_gb"],
                available_memory_gb=memory_info["available_memory_gb"],
                gpu_types=gpu_info,
                region=self.region,
                datacenter=self.datacenter,
                last_heartbeat=datetime.utcnow()
            )
        except Exception as e:
            print(f"Error getting current resources: {e}")
            # Return minimal resources as fallback
            return ExecutorResources(
                ip=self.ip_address,
                total_cpu_cores=1,
                available_cpu_cores=1,
                total_memory_gb=1,
                available_memory_gb=1,
                gpu_types=[],
                region=self.region,
                datacenter=self.datacenter,
                last_heartbeat=datetime.utcnow()
            )

    def get_detailed_system_info(self) -> Dict[str, any]:
        """Get comprehensive system information for monitoring."""
        try:
            cpu_info = self.get_cpu_info()
            memory_info = self.get_memory_info()
            gpu_info = self.get_gpu_info()
            disk_info = self.get_disk_info()
            network_info = self.get_network_info()
            load_info = self.get_system_load()
            
            # Get running processes count
            process_count = len(psutil.pids())
            
            # Get boot time
            boot_time = datetime.fromtimestamp(psutil.boot_time())
            
            return {
                "hostname": self.hostname,
                "ip_address": self.ip_address,
                "region": self.region,
                "datacenter": self.datacenter,
                "timestamp": datetime.utcnow().isoformat(),
                "uptime_hours": (datetime.utcnow() - boot_time).total_seconds() / 3600,
                "process_count": process_count,
                "cpu": cpu_info,
                "memory": memory_info,
                "gpus": gpu_info,
                "disk": disk_info,
                "network": network_info,
                "load": load_info
            }
        except Exception as e:
            print(f"Error getting detailed system info: {e}")
            return {
                "hostname": self.hostname,
                "ip_address": self.ip_address,
                "error": str(e)
            }

    def check_resource_health(self) -> Dict[str, any]:
        """Check if system resources are healthy."""
        try:
            cpu_info = self.get_cpu_info()
            memory_info = self.get_memory_info()
            disk_info = self.get_disk_info()
            load_info = self.get_system_load()
            
            warnings = []
            critical = []
            
            # Check CPU usage
            if cpu_info["cpu_percent"] > 90:
                critical.append(f"High CPU usage: {cpu_info['cpu_percent']:.1f}%")
            elif cpu_info["cpu_percent"] > 80:
                warnings.append(f"High CPU usage: {cpu_info['cpu_percent']:.1f}%")
            
            # Check memory usage
            if memory_info["memory_percent"] > 95:
                critical.append(f"Critical memory usage: {memory_info['memory_percent']:.1f}%")
            elif memory_info["memory_percent"] > 85:
                warnings.append(f"High memory usage: {memory_info['memory_percent']:.1f}%")
            
            # Check disk usage
            if disk_info["used_percent"] > 95:
                critical.append(f"Critical disk usage: {disk_info['used_percent']:.1f}%")
            elif disk_info["used_percent"] > 85:
                warnings.append(f"High disk usage: {disk_info['used_percent']:.1f}%")
            
            # Check load average
            cpu_count = cpu_info["total_cores"]
            if load_info["load_1min"] > cpu_count * 2:
                critical.append(f"Very high load: {load_info['load_1min']:.2f}")
            elif load_info["load_1min"] > cpu_count * 1.5:
                warnings.append(f"High load: {load_info['load_1min']:.2f}")
            
            # Determine overall health status
            if critical:
                health_status = "critical"
            elif warnings:
                health_status = "warning"
            else:
                health_status = "healthy"
            
            return {
                "status": health_status,
                "warnings": warnings,
                "critical": critical,
                "checked_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "checked_at": datetime.utcnow().isoformat()
            }