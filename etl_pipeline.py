import pandas as pd
from sqlalchemy import create_engine
import logging
import os
from datetime import datetime

# 1. Setup Logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"etl_log_{datetime.now().strftime('%Y%m%d')}.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. Database Connection
DB_NAME = 'healthcare_db.sqlite'
engine = create_engine(f'sqlite:///{DB_NAME}')

def extract_data(file_path):
    """Reads data from CSV source."""
    try:
        logging.info(f"Starting extraction from {file_path}")
        df = pd.read_csv(file_path)
        logging.info(f"Extracted {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Extraction Failed: {e}")
        raise

def transform_data(df):
    """Cleans and standardizes the data."""
    logging.info("Starting transformation...")
    
    # Handle Missing Diagnosis Codes
    df['diagnosis_code'] = df['diagnosis_code'].fillna('UNKNOWN')
    
    # Standardize Date Formats
    df['date_of_service'] = pd.to_datetime(df['date_of_service'])
    
    # Filter out 'ERROR' status claims
    valid_df = df[df['status'] != 'ERROR'].copy()
    
    # Add timestamp
    valid_df['processed_at'] = datetime.now()
    
    logging.info("Transformation complete.")
    return valid_df

def load_data(df):
    """Loads data into SQLite database."""
    try:
        logging.info("Starting load process...")
        df.to_sql('claims_fact_table', engine, if_exists='replace', index=False)
        logging.info("Data successfully loaded.")
    except Exception as e:
        logging.error(f"Load Failed: {e}")
        raise

if __name__ == "__main__":
    # Mimic an Airflow Task execution
    print("Starting ETL Job...")
    # Ideally this path comes from an Airflow variable or config
    raw_data = extract_data('data/raw_claims_data.csv')
    clean_data = transform_data(raw_data)
    load_data(clean_data)
    print("Job Finished.")