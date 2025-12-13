import zstandard as zstd
import chess.pgn
import io
import re

def parse_pgn_file(file_path, skip_moves=False):
    """
    Generator that reads a .zst compressed PGN file OR a plain text PGN file
    and yields parsed game dictionaries.
    
    Uses headers-only parsing when skip_moves=True for 10x faster processing.
    
    Args:
        file_path: Path to PGN file (.pgn or .pgn.zst)
        skip_moves: If True, use headers-only parsing for maximum speed
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
            text_stream = io.TextIOWrapper(reader, encoding='utf-8', errors='replace')
        else:
            # Fallback to plain text
            fh = open(file_path, 'r', encoding='utf-8', errors='replace')
            text_stream = fh
            reader = None

        if skip_moves:
            # FAST MODE: Headers only (10x faster)
            while True:
                try:
                    headers = chess.pgn.read_headers(text_stream)
                except Exception:
                    continue
                    
                if headers is None:
                    break
                
                # Get PlyCount if available, otherwise estimate from time control
                ply_count = headers.get("PlyCount", "")
                if ply_count and ply_count.isdigit():
                    num_moves = (int(ply_count) + 1) // 2
                else:
                    # Estimate based on result - completed games average ~40 moves
                    result = headers.get("Result", "*")
                    num_moves = 40 if result in ('1-0', '0-1', '1/2-1/2') else 20
                    
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
                    "termination": headers.get("Termination", "?"),
                    "moves": "",  # Skip moves for speed
                    "num_moves": num_moves
                }
                
                yield game_data
        else:
            # FULL MODE: Parse complete games
            while True:
                try:
                    game = chess.pgn.read_game(text_stream)
                except Exception:
                    continue
                    
                if game is None:
                    break
                    
                headers = game.headers
                
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
                
                # Get actual move count from board position
                try:
                    board = game.end().board()
                    num_moves = (board.ply() + 1) // 2
                    moves = [move.san() for move in game.mainline()]
                    moves_str = " ".join(moves)
                except Exception:
                    moves_str = ""
                    num_moves = 0
                
                game_data["moves"] = moves_str
                game_data["num_moves"] = num_moves
                
                yield game_data
        
        # Clean up
        if reader:
            reader.close()
        fh.close()
                    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    print("This module provides parse_pgn_file(file_path) to be used by the producer.")
