"""
Performance monitoring and metrics collection for the chess analytics pipeline.
Tracks throughput, latency, and data quality metrics.
"""
import time
import json
from datetime import datetime
from collections import defaultdict
import threading

class PipelineMetrics:
    """Singleton class to track pipeline metrics across components."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.start_time = time.time()
        self.metrics = defaultdict(lambda: defaultdict(int))
        self.latencies = defaultdict(list)
        self._initialized = True
    
    def record_event(self, component, event_type, count=1):
        """Record a countable event (games processed, errors, etc.)"""
        self.metrics[component][event_type] += count
    
    def record_latency(self, component, operation, duration_ms):
        """Record operation latency in milliseconds"""
        self.latencies[f"{component}.{operation}"].append(duration_ms)
    
    def get_throughput(self, component, event_type):
        """Calculate events per second"""
        elapsed = time.time() - self.start_time
        total_events = self.metrics[component][event_type]
        return total_events / elapsed if elapsed > 0 else 0
    
    def get_avg_latency(self, component, operation):
        """Calculate average latency in ms"""
        key = f"{component}.{operation}"
        latencies = self.latencies[key]
        return sum(latencies) / len(latencies) if latencies else 0
    
    def get_summary(self):
        """Get complete metrics summary"""
        elapsed = time.time() - self.start_time
        
        summary = {
            'uptime_seconds': round(elapsed, 2),
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        for component, events in self.metrics.items():
            summary['components'][component] = {
                'events': dict(events),
                'throughput': {
                    event: f"{self.get_throughput(component, event):.2f}/sec"
                    for event in events.keys()
                }
            }
        
        # Add latency stats
        summary['latencies'] = {
            key: {
                'avg_ms': round(self.get_avg_latency(*key.split('.')), 2),
                'samples': len(latencies)
            }
            for key, latencies in self.latencies.items()
        }
        
        return summary
    
    def print_summary(self):
        """Print formatted metrics summary"""
        summary = self.get_summary()
        print(f"\n{'='*60}")
        print(f"Pipeline Metrics Summary")
        print(f"{'='*60}")
        print(f"Uptime: {summary['uptime_seconds']}s")
        print(f"Timestamp: {summary['timestamp']}")
        
        for component, data in summary['components'].items():
            print(f"\n{component.upper()}:")
            for event, count in data['events'].items():
                throughput = data['throughput'][event]
                print(f"  {event}: {count:,} ({throughput})")
        
        if summary['latencies']:
            print(f"\nLATENCIES:")
            for op, stats in summary['latencies'].items():
                print(f"  {op}: {stats['avg_ms']}ms avg ({stats['samples']} samples)")
        
        print(f"{'='*60}\n")
    
    def export_json(self, filepath):
        """Export metrics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_summary(), f, indent=2)

# Global metrics instance
metrics = PipelineMetrics()
