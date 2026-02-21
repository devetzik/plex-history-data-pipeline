import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import logging
import os

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


TAUTULLI_IP = os.getenv("TAUTULLI_IP")
TAUTULLI_API_KEY = os.getenv("TAUTULLI_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Connection String
db_uri = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}'
engine = create_engine(db_uri)

def fetch_history():
    
    logger.info("--- Starting ETL Job ---")
    
    # 1. EXTRACT
    # We fetch the last 1000 items from Tautulli history
    url = f"http://{TAUTULLI_IP}:8181/api/v2?apikey={TAUTULLI_API_KEY}&cmd=get_history&length=1000"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Error fetching from Tautulli: {e}")
        return

    history_data = data['response']['data']['data']
    if not history_data:
        logger.warning("No data found.")
        return

    # 2. TRANSFORM
    df = pd.DataFrame(history_data)

    logging.info(f"Fetched {len(df)} records from Tautulli.")
    
    # Select only useful columns
    cols = ['reference_id', 'date', 'friendly_name', 'full_title', 'media_type', 'duration', 'ip_address', 'platform']
    # Handle missing columns gracefully
    df = df[[c for c in cols if c in df.columns]]
    
    # Convert Unix timestamp to datetime
    df['watched_at'] = pd.to_datetime(df['date'], unit='s')
    df = df.drop(columns=['date'])
    
    # 3. LOAD
    # We will use the 'reference_id' from Tautulli as our Primary Key to prevent duplicates.
    
    logger.info(f"Processing {len(df)} records for database insertion...")
    
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS plex_history (
                reference_id INT PRIMARY KEY,
                watched_at TIMESTAMP,
                friendly_name TEXT,
                full_title TEXT,
                media_type TEXT,
                duration INT,
                ip_address TEXT,
                platform TEXT
            );
        """))
        conn.commit()



        df = df.dropna(subset=['reference_id'])
        df['reference_id'] = df['reference_id'].astype(int)


        # Insert row by row (If ID exists, do nothing)
        count = 0
        for index, row in df.iterrows():
            insert_sql = text("""
                INSERT INTO plex_history (reference_id, watched_at, friendly_name, full_title, media_type, duration, ip_address, platform)
                VALUES (:ref_id, :watched, :name, :title, :type, :dur, :ip, :plat)
                ON CONFLICT (reference_id) DO NOTHING;
            """)
            
            result = conn.execute(insert_sql, {
                'ref_id': row['reference_id'],
                'watched': row['watched_at'],
                'name': row['friendly_name'],
                'title': row['full_title'],
                'type': row['media_type'],
                'dur': row['duration'],
                'ip': row['ip_address'],
                'plat': row['platform']
            })
            if result.rowcount > 0:
                count += 1
        
        conn.commit()
        logger.info(f"Successfully inserted {count} new records.")

if __name__ == "__main__":

    logger.info("Service started. Entering main loop.")  
    while True:
        try:
            logger.info("Starting ETL job...")
            fetch_history()
            logger.info("Job finished successfully.")
        except Exception as e:
            logger.error(f"Critical loop error: {e}")
        
        # Sleep for 1 hour (3600 seconds)
        #timeout=60; # 1 minute
        timeout=3600; # 1 hour
        #timeout=3600*24; # 24 hours
        logger.info(f"Sleeping for {timeout} seconds...")
        time.sleep(timeout)