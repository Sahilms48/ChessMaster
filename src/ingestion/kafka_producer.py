import os
import sys
import json
import time
from datetime import datetime
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor

# Add project root to Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from kafka import KafkaProducer
from src.ingestion.pgn_parser import parse_pgn_file
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, 
    TOPIC_CASUAL, 
    TOPIC_PROFESSIONAL,
    RAW_DATA_DIR,
    DATA_DIR
)

# Thread-safe counters
counts_lock = Lock()
casual_count = 0
professional_count = 0

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Optimize for throughput
        batch_size=16384,
        linger_ms=100,
        compression_type='gzip'
    )

def determine_source_and_topic(filename):
    filename = filename.lower()
    if 'lichess' in filename or 'chesscom' in filename:
        return 'chesscom' if 'chesscom' in filename else 'lichess', 'casual', TOPIC_CASUAL
    elif 'twic' in filename:
        return 'twic', 'professional', TOPIC_PROFESSIONAL
    elif 'pgnmentor' in filename or 'mentor' in filename or 'pgnmento' in filename:
        return 'pgnmentor', 'professional', TOPIC_PROFESSIONAL
    else:
        return 'unknown', 'unknown', None

def process_file(filename):
    """Process a single file - runs in its own thread"""
    global casual_count, professional_count
    
    file_path = os.path.join(RAW_DATA_DIR, filename)
    source, game_type, topic = determine_source_and_topic(filename)
    
    if not topic:
        print(f"[{filename}] Skipping: Could not determine topic")
        return 0
    
    print(f"[{filename}] Starting -> {topic}")
    
    # Each thread gets its own producer for thread safety
    try:
        producer = create_producer()
    except Exception as e:
        print(f"[{filename}] Failed to create producer: {e}")
        return 0
    
    count = 0
    try:
        # Use skip_moves=True for faster processing (we only need metadata for analytics)
        for game_data in parse_pgn_file(file_path, skip_moves=True):
            # Add source info
            game_data['source'] = source
            game_data['game_type'] = game_type
            
            producer.send(topic, game_data)
            
            count += 1
            if count % 500 == 0:
                print(f"[{filename}] Sent {count:,} games...")
                
    except Exception as e:
        print(f"[{filename}] Error: {e}")
    finally:
        producer.flush()
        producer.close()
    
    # Update global counts thread-safely
    with counts_lock:
        if game_type == 'casual':
            casual_count += count
        else:
            professional_count += count
    
    print(f"[{filename}] FINISHED: {count:,} games sent.")
    return count

def produce_games():
    global casual_count, professional_count
    casual_count = 0
    professional_count = 0
    
    print(f"Looking for files in: {RAW_DATA_DIR}")
    if not os.path.exists(RAW_DATA_DIR):
        print(f"Directory {RAW_DATA_DIR} does not exist.")
        return

    files = [f for f in os.listdir(RAW_DATA_DIR) if not f.startswith('.') and os.path.isfile(os.path.join(RAW_DATA_DIR, f))]
    
    if not files:
        print(f"No data files found in {RAW_DATA_DIR}")
        print("Make sure PGN files are placed directly in the 'data/raw/' folder (not in subfolders)")
        return

    print(f"Found {len(files)} files: {files}")
    print(f"Processing ALL files in PARALLEL...\n")
    
    # Remove old completion marker if exists
    completion_file = os.path.join(DATA_DIR, "streaming_complete.json")
    if os.path.exists(completion_file):
        os.remove(completion_file)
    
    # Process all files in parallel using ThreadPoolExecutor
    # Each file gets its own thread
    with ThreadPoolExecutor(max_workers=len(files)) as executor:
        results = list(executor.map(process_file, files))
    
    total_games = sum(results)
    
    # Write completion marker
    completion_data = {
        "status": "complete",
        "casual_games": casual_count,
        "professional_games": professional_count,
        "total_games": total_games,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(completion_file, "w") as f:
        json.dump(completion_data, f, indent=2)
    
    print(f"\n{'='*50}")
    print(f"STREAMING COMPLETE!")
    print(f"{'='*50}")
    print(f"Casual games:       {casual_count:,}")
    print(f"Professional games: {professional_count:,}")
    print(f"Total games:        {total_games:,}")
    print(f"{'='*50}")

if __name__ == "__main__":
    produce_games()
