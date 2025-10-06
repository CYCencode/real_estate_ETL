import requests
import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from psycopg2 import OperationalError 
from io import StringIO
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
import traceback

# --- 1. 設定與連線 ---
# 日誌寫入 MongoDB 的輔助函數
def log_to_mongo(log_level: str, message: str, details=None):
    """
    從環境變數獲取 MongoDB 連線資訊，並將日誌寫入指定的 Collection。
    在連線失敗時，退回到標準輸出 (stderr) 進行緊急輸出。
    """
    MONGO_URI = os.environ.get("MONGO_URI")
    MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "etl_monitoring")
    MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "pipeline_logs")
    PIPELINE_NAME = os.environ.get("JOB_NAME", "unknown-cloud-run-job")    

    log_entry = {
        "timestamp": datetime.now(timezone.utc),
        "level": log_level,
        "message": message,
        "pipeline_name": PIPELINE_NAME,
        "details": details if details is not None else {}
    }

    # 確保在 MongoDB 寫入失敗時，緊急輸出到 stderr
    fallback_message = f"[{log_level}] {message}"
    if details:
        fallback_message += f" | Details: {details.get('error_message', 'N/A')}"
        if log_level in ('ERROR', 'CRITICAL') and 'traceback' in details:
            fallback_message += f"\n--- TRACEBACK ---\n{details['traceback']}\n--- END TRACEBACK ---"
    
    if not MONGO_URI:
        print(f"[Fallback Log] {fallback_message}", file=sys.stderr)
        return

    try:
        # 使用 tlsInsecure=True 繞過 SSL 握手錯誤，用於測試
        client = MongoClient(
            MONGO_URI, 
            serverSelectionTimeoutMS=5000, 
            w=0,
            tlsInsecure=True
        ) 
        db = client[MONGO_DB_NAME]
        db[MONGO_COLLECTION].insert_one(log_entry)
        client.close()
    except Exception as e:
        print(f"[MONGO_FAILOVER - {log_level}] {fallback_message} (Mongo Write Error: {e})", file=sys.stderr)


# 地區代碼對應表
area_name_mapping = {
    'A': 'Taipei', 'F': 'NewTaipei', 'H': 'Taoyuan',
    'C': 'Keelung', 'J': 'HsinchuCounty', 'O': 'HsinchuCity'
}


# 定義 MVP 階段要從原始 CSV 中提取的欄位 (中文名稱)
COLUMNS_MAPPING = {
    "土地移轉總面積平方公尺": "land_total_sqm",
    "建物移轉總面積平方公尺": "building_total_sqm",
    "建物現況格局-房": "room_count",  
    "主要用途": "use_zone",
}
# 額外需要 "交易標的" 欄位進行篩選
FILTER_COLUMN = "交易標的"

def get_pg_engine():
    """使用 Public IP + Authorized Networks 連線。"""
    DB_USER = os.environ.get("PG_USER", "postgres")
    DB_PASSWORD = os.environ.get("PG_PASSWORD")
    
    # 使用 Public IP
    DB_HOST_PUBLIC = os.environ.get("PG_HOST_PUBLIC") 
    
    DB_PORT = os.environ.get("PG_PORT", "5432")
    DB_NAME = os.environ.get("PG_DATABASE", "postgres")

    if not DB_PASSWORD or not DB_HOST_PUBLIC:
        raise ValueError("PG_PASSWORD 或 PG_HOST_PUBLIC 環境變數未設定。")

    
    # 直接連線到 Public IP
    DATABASE_URL = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST_PUBLIC}:{DB_PORT}/{DB_NAME}"
    )
    
    print(f"DEBUG (Public IP): Final PG URL: Targeting {DB_HOST_PUBLIC}:{DB_PORT} (Authorized Networks)", file=sys.stderr)
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        # 嘗試建立一個實際連線
        with engine.connect() as connection:
            print("DEBUG: Successfully connected to PostgreSQL using Public IP.", file=sys.stderr)
            return engine
    except OperationalError as e:
        print(f"CRITICAL SYSTEM ERROR: Failed to connect using Public IP: {e}", file=sys.stderr)
        raise e

# --- 2. 核心 ETL 邏輯 (保持不變) ---

def real_estate_pipeline(year, season, area, trade_type, pg_engine):

    if year > 1000:
        year -= 1911

    url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&fileName={area}_lvr_land_{trade_type}.csv"
    
    # 【優化】動態生成 TARGET_TABLE_NAME
    trade_type_name = 'used' if trade_type == 'A' else 'presale'
    area_name = area_name_mapping.get(area, 'unknown').lower()
    TARGET_TABLE_NAME = f"real_estate_{trade_type_name}_{area_name}" 

    log_to_mongo('INFO',f"Starting ETL for {area_name.capitalize()}, {year}S{season}, {trade_type_name.capitalize()}",
                 details={"table": TARGET_TABLE_NAME, "url": url})

    res = requests.get(url)

    if res.status_code != 200:
        log_to_mongo('ERROR',f"Error: HTTP status code {res.status_code} for {url}",
                      details={"http_status": res.status_code, "table": TARGET_TABLE_NAME})
        return

    csv_data = StringIO(res.text, newline=None)
    df = pd.read_csv(csv_data, on_bad_lines='skip', engine='python', encoding='utf-8', encoding_errors='ignore', skip_blank_lines=True)
    
    # 檢查數據是否有效
    if df.empty or len(df) < 2:
        log_to_mongo('WARNING',f"File for {TARGET_TABLE_NAME} is empty or contains only headers. Skipping.")
        return

    # --- E (提取/轉換) 階段 ---

    # 1. 檢查關鍵欄位是否存在
    required_cols = list(COLUMNS_MAPPING.keys()) + [FILTER_COLUMN]
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        log_to_mongo('ERROR',f"Missing critical columns for {TARGET_TABLE_NAME}", 
                      details={"missing_columns": missing, "table": TARGET_TABLE_NAME})
        return

    # 2. 篩選：只保留 '交易標的' 包含 "建物" 的資料
    df = df[df[FILTER_COLUMN].astype(str).str.contains('建物', na=False, case=False)].copy()

    if df.empty:
        log_to_mongo('WARNING',f"No '建物' records found after filtering for {TARGET_TABLE_NAME}. Skipping load.")
        return

    # 3. 選擇並重命名欄位到目標 Schema (英文名稱)
    df_clean = df[list(COLUMNS_MAPPING.keys())].rename(columns=COLUMNS_MAPPING).copy()

    # 4. 數據清洗：處理空字串和基本類型轉換
    df_clean = df_clean.replace(r'^\s*$', np.nan, regex=True)

    # 5. 類型轉換 (符合目標 Schema: NUMERIC, INTEGER)
    numeric_cols = ["land_total_sqm", "building_total_sqm"]
    integer_cols = ["room_count"]
    
    for col in numeric_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    for col in integer_cols:
        # 使用 Int64 允許 NaN 整數
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').astype(float).astype('Int64')
    
    # 6. 將所有 Pandas NaN 替換為 Python 的 None (SQL NULL)
    df_clean = df_clean.where(pd.notnull(df_clean), None)


    # --- 3. L (載入) 階段 - 寫入 PostgreSQL ---
    start_time = time.time()
    try:
        df_clean.to_sql(
            TARGET_TABLE_NAME,
            con=pg_engine,
            if_exists='append',
            index=False
        )
        end_time = time.time()
        load_time = end_time - start_time
        
        log_to_mongo('INFO',f"Successfully loaded {len(df_clean)} rows to {TARGET_TABLE_NAME}.",
                      details={"rows": len(df_clean), "time_seconds": f"{load_time:.2f}", "table": TARGET_TABLE_NAME})
        
        print(f"  -> Successfully loaded {len(df_clean)} rows to {TARGET_TABLE_NAME}. Time: {load_time:.2f}s", file=sys.stderr)
        
    except Exception as e:
        tb = traceback.format_exc()
        log_to_mongo(
            'ERROR', 
            f"DB Load ERROR for {TARGET_TABLE_NAME}: {type(e).__name__}",
            details={
                "error_type": type(e).__name__,
                "error_message": str(e), 
                "traceback": tb, 
                "table_name": TARGET_TABLE_NAME,
                "data_count": len(df_clean) if 'df_clean' in locals() else 0
            }
        )
        print(f"  -> CRITICAL DB ERROR for {TARGET_TABLE_NAME}: {e}\nTraceback:\n{tb}", file=sys.stderr)


# --- 主執行邏輯 ---
if __name__ == "__main__":
    log_to_mongo('INFO',"Starting MVP ETL Data Pipeline")

    try:
        # 1. 建立 PostgreSQL 連線引擎
        engine = get_pg_engine()
        log_to_mongo('INFO',"PostgreSQL Engine created successfully.")

        # 2. 測試連線
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            log_to_mongo('INFO',"Successfully connected to PostgreSQL.")

        # 3. 執行 ETL
        log_to_mongo('INFO',"Starting MVP Data Ingestion...")

        # 測試範圍：112 年第一季 (2023 Q1) 的基隆和桃園
        for area in ['C', 'H']: # 基隆(C), 桃園(H)
            for trade_type in ['A', 'B']: # 中古屋(A), 預售屋(B)
                real_estate_pipeline(112, 1, area, trade_type, engine)

        log_to_mongo('INFO',"Pipeline execution finished successfully.")

    except ValueError as ve:
        tb = traceback.format_exc()
        log_to_mongo('CRITICAL',f"CRITICAL CONFIGURATION ERROR: {ve}", 
                      details={"error_message": str(ve), "recommendation": "請確認您在 Docker 執行命令中設定了 PG_PASSWORD。", "traceback": tb})
        print(f"CRITICAL CONFIGURATION ERROR: {ve}\nTraceback:\n{tb}", file=sys.stderr)

    except OperationalError as oe:
        tb = traceback.format_exc()
        log_to_mongo('CRITICAL',f"CRITICAL DATABASE CONNECTION ERROR: {oe}", 
                      details={"error_message": str(oe), "recommendation": "請檢查 Cloud SQL IP、Proxy設定、授權網路和密碼是否正確。", "traceback": tb})
        print(f"CRITICAL DATABASE CONNECTION ERROR: {oe}\nTraceback:\n{tb}", file=sys.stderr)
        sys.exit(1) # 連線失敗直接退出，避免浪費資源

    except Exception as e:
        tb = traceback.format_exc()
        log_to_mongo(
            'CRITICAL',
            f"SYSTEM FAIL: Main pipeline execution failed. Error Type: {type(e).__name__}",
            details={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": tb,
                "recommendation": "請檢查 Cloud SQL IP、連線密碼、防火牆規則以及 Mongo URI 是否正確。"
            }
        )
        print(f"CRITICAL SYSTEM ERROR: {e}\nTraceback:\n{tb}", file=sys.stderr)