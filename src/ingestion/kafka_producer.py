import os
import json
import time
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

def produce_games():
    print(f"Looking for files in: {RAW_DATA_DIR}")
    if not os.path.exists(RAW_DATA_DIR):
        print(f"Directory {RAW_DATA_DIR} does not exist.")
        return

    files = [f for f in os.listdir(RAW_DATA_DIR) if not f.startswith('.') and os.path.isfile(os.path.join(RAW_DATA_DIR, f))]
    
    if not files:
        print(f"No .zst files found in {RAW_DATA_DIR}")
        return

    print(f"Found files: {files}")
    
    try:
        producer = create_producer()
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return
    
    total_games = 0
    
    for filename in files:
        file_path = os.path.join(RAW_DATA_DIR, filename)
        source, game_type, topic = determine_source_and_topic(filename)
        
        if not topic:
            print(f"Skipping {filename}: Could not determine topic")
            continue
            
        print(f"Processing {filename} -> {topic}...")
        
        count = 0
        for game_data in parse_pgn_file(file_path):
            # if count >= 2000:
            #    break

            # Add source info
            game_data['source'] = source
            game_data['game_type'] = game_type
            
            producer.send(topic, game_data)
            
            count += 1
            if count % 1000 == 0:
                print(f"  Sent {count} games...", end='\r')
                
        print(f"  Finished {filename}: {count} games sent.")
        total_games += count
        
    producer.flush()
    producer.close()
    print(f"\nTotal games sent: {total_games}")

if __name__ == "__main__":
    produce_games()

