import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# --- Config & Constants ---
API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://api.football-data.org/v4/"

# Database Connection String (SQLAlchemy format)
DB_URL = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"

def get_engine():
    """
    Creates a SQLAlchemy Connection Engine.
    Senior Tip: Engines manage connection pooling, which optimizes DB performance.
    """
    return create_engine(DB_URL)

def fetch_matches(competition_code='PL'):
    """
    Fetches match data for a specific competition from the API.
    """
    headers = {"X-Auth-Token": API_KEY}
    url = f"{BASE_URL}competitions/{competition_code}/matches"
    
    print(f"[FETCH] Requesting data from API: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        return response.json().get("matches", [])
    except Exception as e:
        print(f"[ERROR] API Error: {e}")
        return []

def transform_matches(matches_raw):
    """
    Normalizes raw JSON data into a clean Pandas DataFrame.
    Senior Tip: Flattening nested JSON structures is a key part of the 'Bronze' layer preparation.
    """
    if not matches_raw:
        return pd.DataFrame()

    print(f"[PROCESS] Cleaning and flattening data... (Total matches: {len(matches_raw)})")
    
    # Flattening nested dictionaries into columns
    df = pd.json_normalize(matches_raw)

    # Mapping API field names to our localized DB schema
    cols_map = {
        'id': 'match_id',
        'competition.name': 'competition_name',
        'season.startDate': 'season',
        'matchday': 'matchday',
        'utcDate': 'utc_date',
        'status': 'status',
        'homeTeam.name': 'home_team',
        'awayTeam.name': 'away_team',
        'score.fullTime.home': 'score_full_time_home',
        'score.fullTime.away': 'score_full_time_away',
        'score.winner': 'winner'
    }

    # Keep only necessary columns
    available_cols = [c for c in cols_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=cols_map)

    if 'utc_date' in df.columns:
        df['utc_date'] = pd.to_datetime(df['utc_date'])
    
    # Metadata for auditing
    df['extracted_at'] = datetime.now()

    return df

def load_to_postgres(df):
    """
    Loads DataFrame to PostgreSQL using Upsert logic for idempotency.
    Senior Tip: Idempotency ensures that running the script multiple times 
    doesn't create duplicate records.
    """
    if df.empty:
        print("[INFO] No data to load. Check API key and connectivity.")
        return

    table_name = "raw_matches"
    print(f"[LOAD] Loading data into '{table_name}' table...")
    
    engine = get_engine()
    try:
        with engine.begin() as connection:
            # Write to a temporary table
            df.to_sql("temp_matches", connection, if_exists="replace", index=False)

            # Perform Upsert (Insert or Update on conflict)
            upsert_query = text("""
                INSERT INTO raw_matches 
                SELECT * FROM temp_matches
                ON CONFLICT (match_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    score_full_time_home = EXCLUDED.score_full_time_home,
                    score_full_time_away = EXCLUDED.score_full_time_away,
                    winner = EXCLUDED.winner,
                    extracted_at = EXCLUDED.extracted_at;
            """)
            connection.execute(upsert_query)
            
            # Clean up temp table
            connection.execute(text("DROP TABLE temp_matches;"))

        print("[SUCCESS] Data load completed successfully!")
    except Exception as e:
        print(f"[ERROR] Database Loading Error: {e}")

if __name__ == "__main__":
    if not API_KEY or API_KEY == "your_api_key_here":
        print("[WARNING] Please add a valid FOOTBALL_API_KEY to the .env file and save it.")
    else:
        # Step 1: Extract
        raw_json = fetch_matches('PL') 
        
        # Step 2: Transform
        clean_df = transform_matches(raw_json)
        
        # Step 3: Load
        load_to_postgres(clean_df)
        
        print("\n[FINISH] Pipeline process finished. You can now check 'raw_matches' in pgAdmin.")
