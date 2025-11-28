from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_opening_family(eco):
    if not eco or len(eco) < 1:
        return "Unknown"
    return eco[0] # First letter A-E

def get_game_phase(moves, num_moves):
    # Simple heuristic:
    # < 15 moves: Opening
    # 15-40 moves: Middlegame
    # > 40 moves: Endgame
    if not num_moves:
        return "Unknown"
    if num_moves < 15:
        return "Opening"
    elif num_moves < 40:
        return "Middlegame"
    else:
        return "Endgame"

# Register UDFs
get_opening_family_udf = udf(get_opening_family, StringType())
get_game_phase_udf = udf(get_game_phase, StringType())

