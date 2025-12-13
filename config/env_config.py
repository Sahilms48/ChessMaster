"""
Configuration management for different deployment environments.
Supports development, staging, and production configurations.
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class KafkaConfig:
    """Kafka cluster configuration."""
    bootstrap_servers: list
    auto_offset_reset: str = 'latest'
    enable_auto_commit: bool = True
    session_timeout_ms: int = 30000
    max_poll_records: int = 500
    compression_type: str = 'snappy'

@dataclass
class SparkConfig:
    """Spark cluster configuration."""
    app_name: str
    master: str
    executor_memory: str = '2g'
    executor_cores: int = 2
    num_executors: int = 2
    driver_memory: str = '1g'
    shuffle_partitions: int = 200
    
@dataclass
class StorageConfig:
    """Storage paths and settings."""
    raw_data_dir: str
    processed_data_dir: str
    checkpoint_dir: str
    metrics_dir: str
    compression: str = 'snappy'
    partition_by: list = None

class EnvironmentConfig:
    """Environment-specific configurations."""
    
    def __init__(self, env: str = None):
        self.env = env or os.getenv('CHESS_ENV', 'development')
        self._load_config()
    
    def _load_config(self):
        """Load configuration based on environment."""
        if self.env == 'production':
            self._load_production()
        elif self.env == 'staging':
            self._load_staging()
        else:
            self._load_development()
    
    def _load_development(self):
        """Development configuration (local laptop)."""
        # Use external storage if configured via environment variable
        external_storage = os.environ.get('CHESS_DATA_DIR', './data')
        base_path = external_storage if os.path.exists(external_storage) else './data'
        
        self.kafka = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            max_poll_records=100,  # Lower for development
        )
        
        self.spark = SparkConfig(
            app_name='ChessAnalytics-Dev',
            master='local[*]',  # Use all cores
            executor_memory='1g',
            driver_memory='512m',
            shuffle_partitions=50,  # Lower for small datasets
        )
        
        self.storage = StorageConfig(
            raw_data_dir=os.path.join(base_path, 'raw'),
            processed_data_dir=os.path.join(base_path, 'processed'),
            checkpoint_dir=os.path.join(base_path, 'checkpoints'),
            metrics_dir=os.path.join(base_path, 'metrics'),
            partition_by=['date', 'game_type'],
        )
    
    def _load_staging(self):
        """Staging configuration (testing cluster)."""
        self.kafka = KafkaConfig(
            bootstrap_servers=[
                'kafka-staging-1:9092',
                'kafka-staging-2:9092',
            ],
            max_poll_records=500,
        )
        
        self.spark = SparkConfig(
            app_name='ChessAnalytics-Staging',
            master='spark://staging-master:7077',
            executor_memory='4g',
            executor_cores=4,
            num_executors=4,
            driver_memory='2g',
            shuffle_partitions=200,
        )
        
        self.storage = StorageConfig(
            raw_data_dir='/mnt/storage/chess/raw',
            processed_data_dir='/mnt/storage/chess/processed',
            checkpoint_dir='/mnt/storage/chess/checkpoints',
            metrics_dir='/mnt/storage/chess/metrics',
            partition_by=['year', 'month', 'game_type'],
        )
    
    def _load_production(self):
        """Production configuration (full cluster)."""
        self.kafka = KafkaConfig(
            bootstrap_servers=[
                'kafka-prod-1:9092',
                'kafka-prod-2:9092',
                'kafka-prod-3:9092',
            ],
            max_poll_records=1000,
            compression_type='lz4',  # Better compression for production
        )
        
        self.spark = SparkConfig(
            app_name='ChessAnalytics-Prod',
            master='spark://prod-master:7077',
            executor_memory='8g',
            executor_cores=8,
            num_executors=10,
            driver_memory='4g',
            shuffle_partitions=500,  # More partitions for large datasets
        )
        
        self.storage = StorageConfig(
            raw_data_dir='s3a://chess-data/raw',
            processed_data_dir='s3a://chess-data/processed',
            checkpoint_dir='s3a://chess-data/checkpoints',
            metrics_dir='s3a://chess-data/metrics',
            compression='zstd',  # Better compression
            partition_by=['year', 'month', 'day', 'game_type'],
        )
    
    def get_kafka_topics(self):
        """Get Kafka topic names."""
        return {
            'casual': f'chess.games.casual.{self.env}',
            'professional': f'chess.games.professional.{self.env}',
        }
    
    def print_config(self):
        """Print current configuration."""
        print(f"\n{'='*60}")
        print(f"Environment: {self.env.upper()}")
        print(f"{'='*60}")
        print(f"Kafka: {self.kafka.bootstrap_servers}")
        print(f"Spark Master: {self.spark.master}")
        print(f"Spark Resources: {self.spark.num_executors} executors × {self.spark.executor_cores} cores")
        print(f"Storage: {self.storage.processed_data_dir}")
        print(f"{'='*60}\n")

# Global config instance
config = EnvironmentConfig()
