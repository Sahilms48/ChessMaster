import zstandard as zstd
import chess.pgn
import io
import json
from datetime import datetime

def parse_pgn_file(file_path):
    """
    Generator that reads a .zst compressed PGN file OR a plain text PGN file
    and yields parsed game dictionaries.
    """
    try:
        # Try opening as ZSTD first
        is_zstd = False
        with open(file_path, 'rb') as f:
            magic = f.read(4)
            # Zstandard magic number: 0xFD2FB528
            if magic == b'\x28\xb5\x2f\xfd':
                is_zstd = True
        
        if is_zstd:
            dctx = zstd.ZstdDecompressor()
            fh = open(file_path, 'rb')
            reader = dctx.stream_reader(fh)
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
        else:
            # Fallback to plain text
            fh = open(file_path, 'r', encoding='utf-8', errors='replace')
            text_stream = fh

        while True:
            try:
                game = chess.pgn.read_game(text_stream)
            except ValueError:
                continue
                
            if game is None:
                break
                
            headers = game.headers
            
            # Extract basic metadata
            game_data = {
                "event": headers.get("Event", "?"),
                "white": headers.get("White", "?"),
                "black": headers.get("Black", "?"),
                "result": headers.get("Result", "*"),
                "eco": headers.get("ECO", "?"),
                "opening": headers.get("Opening", "?"),
                "time_control": headers.get("TimeControl", "?"),
                "date": headers.get("Date", "?"),
                "white_elo": headers.get("WhiteElo", "?"),
                "black_elo": headers.get("BlackElo", "?"),
                "termination": headers.get("Termination", "?")
            }
            
            # Extract moves
            moves = []
            node = game
            while node.variations:
                node = node.variations[0]
                moves.append(node.san())
            
            game_data["moves"] = " ".join(moves)
            game_data["num_moves"] = len(moves)
            
            yield game_data
        
        # Clean up
        if is_zstd:
            reader.close()
        fh.close()
                    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
                    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    print("This module provides parse_pgn_file(file_path) to be used by the producer.")

