import os
import json
import time
import threading
from kafka import KafkaProducer
from src.ingestion.pgn_parser import parse_pgn_file
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, 
    TOPIC_CASUAL, 
    TOPIC_PROFESSIONAL,
    RAW_DATA_DIR
)

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

def process_files(files, thread_name):
    """
    Worker function to process a list of files and send to Kafka.
    """
    try:
        producer = create_producer()
        print(f"[{thread_name}] Producer created. Processing {len(files)} files.")
    except Exception as e:
        print(f"[{thread_name}] Failed to create Kafka producer: {e}")
        return

    total_games = 0
    
    for filename in files:
        file_path = os.path.join(RAW_DATA_DIR, filename)
        source, game_type, topic = determine_source_and_topic(filename)
        
        if not topic:
            print(f"[{thread_name}] Skipping {filename}: Could not determine topic")
            continue
            
        print(f"[{thread_name}] Processing {filename} -> {topic}...")
        
        count = 0
        for game_data in parse_pgn_file(file_path):
            # Add source info
            game_data['source'] = source
            game_data['game_type'] = game_type
            
            producer.send(topic, game_data)
            
            count += 1
            if count % 1000 == 0:
                # Use simple print to avoid thread output collision
                print(f"[{thread_name}] {filename}: Sent {count} games...")
                
        print(f"[{thread_name}] Finished {filename}: {count} games sent.")
        total_games += count
        
    producer.flush()
    producer.close()
    print(f"[{thread_name}] Completed! Total games sent: {total_games}")

def produce_games():
    print(f"Looking for files in: {RAW_DATA_DIR}")
    if not os.path.exists(RAW_DATA_DIR):
        print(f"Directory {RAW_DATA_DIR} does not exist.")
        return

    all_files = [f for f in os.listdir(RAW_DATA_DIR) if not f.startswith('.') and os.path.isfile(os.path.join(RAW_DATA_DIR, f))]
    
    if not all_files:
        print(f"No files found in {RAW_DATA_DIR}")
        return

    # Group files by type
    casual_files = []
    professional_files = []
    
    for f in all_files:
        _, game_type, _ = determine_source_and_topic(f)
        if game_type == 'casual':
            casual_files.append(f)
        elif game_type == 'professional':
            professional_files.append(f)
            
    print(f"Found {len(casual_files)} Casual files and {len(professional_files)} Professional files.")
    
    threads = []
    
    # Start Casual Thread
    if casual_files:
        t1 = threading.Thread(target=process_files, args=(casual_files, "Casual-Worker"))
        t1.start()
        threads.append(t1)
        
    # Start Professional Thread
    if professional_files:
        t2 = threading.Thread(target=process_files, args=(professional_files, "Professional-Worker"))
        t2.start()
        threads.append(t2)
        
    # Wait for all to finish
    for t in threads:
        t.join()
        
    print("\nAll processing threads finished.")

if __name__ == "__main__":
    produce_games()
