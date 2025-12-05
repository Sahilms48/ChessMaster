import pandas as pd
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def inspect_data():
    # Path to your processed data
    # Note: Spark writes to nested folders inside 'games', so we target the directory
    games_path = os.path.join("data", "processed", "games")

    print(f"🔍 Checking data in: {games_path}")
    
    if not os.path.exists(games_path):
        print("❌ Data directory does not exist yet.")
        return

    try:
        # Read the parquet file
        # pyarrow engine is usually better for folder datasets
        df = pd.read_parquet(games_path, engine='pyarrow')
        
        print(f"\n✅ Successfully loaded {len(df)} rows")
        
        print("\n📋 Columns Available:")
        for col in df.columns:
            print(f" - {col}")
            
        print("\n📊 Sample Data (First 3 rows):")
        pd.set_option('display.max_columns', None)
        print(df.head(3))
        
        print("\n♟️ Opening Data Check:")
        if 'opening' in df.columns:
            nulls = df['opening'].isnull().sum()
            print(f" - Null openings: {nulls}")
            print(f" - Unique openings: {df['opening'].nunique()}")
            print(" - Top 5 Openings:")
            print(df['opening'].value_counts().head(5))
        else:
            print("❌ 'opening' column is MISSING")

        print("\n🏷️ ECO Data Check:")
        if 'eco' in df.columns:
             print(f" - Unique ECOs: {df['eco'].nunique()}")
             print(" - Top 5 ECOs:")
             print(df['eco'].value_counts().head(5))
        else:
             print("❌ 'eco' column is MISSING")
             
        print("\nℹ️ Game Types:")
        if 'game_type' in df.columns:
            print(df['game_type'].value_counts())

    except Exception as e:
        print(f"\n❌ Error reading data: {e}")
        print("Tip: If the error is 'No such file or directory', check if Spark has actually written any batches yet.")

if __name__ == "__main__":
    inspect_data()

