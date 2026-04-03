# ============================================
# Hospital Readmission Data Ingestion Script
# Author: Awanish Kumar
# Tools: Python, Snowflake, AWS S3
# ============================================

import snowflake.connector
import boto3
import pandas as pd
import logging
from datetime import datetime

# ============================================
# Logging Setup
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# Snowflake Connection Config
# ============================================
SNOWFLAKE_CONFIG = {
    'account'   : 'your_account',
    'user'      : 'your_username',
    'password'  : 'your_password',
    'warehouse' : 'COMPUTE_WH',
    'database'  : 'HOSPITAL_DB',
    'schema'    : 'STAGING'
}

# ============================================
# AWS S3 Config
# ============================================
S3_CONFIG = {
    'bucket_name' : 'your-s3-bucket',
    'prefix'      : 'hospital-readmission/',
    'region'      : 'us-east-1'
}

# ============================================
# Connect to Snowflake
# ============================================
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info("✅ Snowflake connection successful")
        return conn
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {e}")
        raise

# ============================================
# Read Data from AWS S3
# ============================================
def read_from_s3(bucket, key):
    try:
        s3 = boto3.client('s3', region_name=S3_CONFIG['region'])
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj['Body'])
        logger.info(f"✅ Read {len(df)} records from S3: {key}")
        return df
    except Exception as e:
        logger.error(f"❌ S3 read failed: {e}")
        raise

# ============================================
# Data Validation
# ============================================
def validate_data(df):
    logger.info("🔍 Running data validation checks...")
    
    # Check for nulls in critical columns
    critical_cols = ['patient_id', 'admission_date', 'department_name']
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.warning(f"⚠️ {null_count} nulls found in {col}")
    
    # Check for duplicate patient records
    duplicates = df.duplicated(subset=['patient_id', 'admission_date']).sum()
    if duplicates > 0:
        logger.warning(f"⚠️ {duplicates} duplicate records found")
    
    # Check date format
    try:
        pd.to_datetime(df['admission_date'])
        logger.info("✅ Date format validation passed")
    except Exception:
        logger.error("❌ Invalid date format in admission_date")
        raise
    
    logger.info(f"✅ Validation complete — {len(df)} records passed")
    return df

# ============================================
# Load Data into Snowflake Staging
# ============================================
def load_to_snowflake(df, conn, table_name):
    try:
        cursor = conn.cursor()
        
        # Truncate staging table
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        logger.info(f"✅ Truncated {table_name}")
        
        # Insert records in batches
        batch_size = 10000
        total_records = len(df)
        loaded = 0
        
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [tuple(row) for row in batch.values]
            
            placeholders = ','.join(['%s'] * len(df.columns))
            insert_sql = f"""
                INSERT INTO {table_name} 
                VALUES ({placeholders})
            """
            cursor.executemany(insert_sql, values)
            loaded += len(batch)
            logger.info(f"✅ Loaded {loaded}/{total_records} records")
        
        conn.commit()
        logger.info(f"✅ Successfully loaded {total_records} records into {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Load failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

# ============================================
# Main Pipeline
# ============================================
def run_pipeline():
    logger.info("🚀 Starting Hospital Readmission Pipeline")
    start_time = datetime.now()
    
    try:
        # Step 1 — Read from S3
        df = read_from_s3(
            bucket=S3_CONFIG['bucket_name'],
            key=f"{S3_CONFIG['prefix']}patient_records.csv"
        )
        
        # Step 2 — Validate Data
        df = validate_data(df)
        
        # Step 3 — Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Step 4 — Load to Staging
        load_to_snowflake(df, conn, 'staging.stg_patient_records')
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        logger.info(f"✅ Pipeline completed in {duration} seconds")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("✅ Snowflake connection closed")

# ============================================
# Entry Point
# ============================================
if __name__ == "__main__":
    run_pipeline()