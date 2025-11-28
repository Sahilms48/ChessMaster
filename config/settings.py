import os

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_CASUAL = 'chess.games.casual'
TOPIC_PROFESSIONAL = 'chess.games.professional'

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

# Spark Settings
SPARK_APP_NAME = "ChessAnalytics"
SPARK_MASTER = "local[*]"

