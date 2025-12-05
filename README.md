# Chess Analytics - Real-Time Streaming Dashboard

A real-time streaming analytics platform comparing **Professional vs Casual** chess games using Apache Kafka, Apache Spark, and Streamlit.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PGN Files     │────▶│  Kafka Producer │────▶│  Kafka Topics   │
│  (Chess Games)  │     │  (Parallel)     │     │  casual/pro     │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Streamlit     │◀────│  Parquet Files  │◀────│  Spark Consumer │
│   Dashboard     │     │  (Processed)    │     │  (Streaming)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Prerequisites

### 1. Java 11+
Download and install JDK 11 from [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or [Adoptium](https://adoptium.net/).

Set `JAVA_HOME` environment variable.

### 2. Apache Kafka
1. Download Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
2. Extract to `C:\kafka\kafka_2.12-3.9.0` (or your preferred location)

### 3. Python 3.12
Download from [python.org](https://www.python.org/downloads/)

## Installation

### 1. Clone the Repository
```cmd
git clone https://github.com/YOUR_USERNAME/ChessMaster.git
cd ChessMaster
```

### 2. Create Virtual Environment
```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Download Chess Data
Place PGN files in `data/raw/`:
- **Casual games**: Download from [Chess.com](https://www.chess.com/games/archive) or [Lichess](https://database.lichess.org/)
- **Professional games**: Download from [PGN Mentor](https://www.pgnmentor.com/) or [TWIC](https://theweekinchess.com/)

Name files with keywords for automatic classification:
- Files containing `chesscom` or `lichess` → Casual
- Files containing `twic` or `pgnmentor` → Professional

## Running the Application

Open **5 separate terminal windows** and run in order:

### Terminal 1: Zookeeper
```cmd
cd C:\kafka\kafka_2.12-3.9.0
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

### Terminal 2: Kafka Broker
```cmd
cd C:\kafka\kafka_2.12-3.9.0
bin\windows\kafka-server-start.bat config\server.properties
```

### Terminal 3: Spark Consumer
```cmd
cd ChessMaster
venv\Scripts\activate
python src\processing\spark_consumer.py
```

### Terminal 4: Kafka Producer
```cmd
cd ChessMaster
venv\Scripts\activate
python src\ingestion\kafka_producer.py
```

### Terminal 5: Streamlit Dashboard
```cmd
cd ChessMaster
venv\Scripts\activate
streamlit run src\dashboard\app.py
```

### Open Dashboard
Navigate to **http://localhost:8501** in your browser.

## Project Structure

```
ChessMaster/
├── config/
│   └── settings.py          # Kafka, Spark, path configurations
├── data/
│   ├── raw/                  # Place PGN files here
│   └── processed/            # Generated Parquet output
├── scripts/
│   └── verify_setup.py       # Environment verification
├── src/
│   ├── dashboard/
│   │   └── app.py            # Streamlit dashboard
│   ├── ingestion/
│   │   ├── kafka_producer.py # Parallel file streaming
│   │   └── pgn_parser.py     # PGN file parser
│   └── processing/
│       ├── spark_consumer.py # Spark streaming consumer
│       └── feature_extraction.py
├── requirements.txt
└── README.md
```

## Dashboard Features

1. **Live Streaming Stats** - Games/sec, progress bars
2. **Game Length Distribution** - Histogram comparison
3. **Game Results** - Win/Draw/Loss rates
4. **Game Phase Breakdown** - Opening/Middlegame/Endgame
5. **Opening Analysis** - ECO code statistics
6. **Top Openings** - Most popular openings by type

## Troubleshooting

### "No module named 'config'"
Run from the `ChessMaster` directory with venv activated.

### Kafka connection errors
Ensure Zookeeper and Kafka broker are running before starting the consumer.

### Spark errors with spaces in path
The project handles Windows paths with spaces using 8.3 short paths.

## Tech Stack

- **Apache Kafka** - Message streaming
- **Apache Spark** - Stream processing
- **Streamlit** - Dashboard UI
- **Plotly** - Interactive charts
- **Python-Chess** - PGN parsing

## License

MIT License

