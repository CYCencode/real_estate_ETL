import requests
import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from io import StringIO
import numpy as np
from pymongo import MongoClient
from datetime import datetime
import sys
import traceback # 引入 traceback 模組，用於捕捉詳細錯誤堆疊

# --- 1. 設定與連線 ---
# 日誌寫入 MongoDB 的輔助函數
def log_to_mongo(log_level: str, message: str, details=None):
    """
    從環境變數獲取 MongoDB 連線資訊，並將日誌寫入指定的 Collection。
    在連線失敗時，退回到標準輸出 (stdout) 進行緊急輸出。
    """
    MONGO_URI = os.environ.get("MONGO_URI")
    MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "etl_monitoring")
    MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "pipeline_logs")
    
    log_entry = {
        "timestamp": datetime.utcnow(),
        "level": log_level,
        "message": message,
        "pipeline_name": "real_estate_mvp",
        "details": details if details is not None else {}
    }

    # 確保在 MongoDB 寫入失敗時，緊急輸出到 stderr
    fallback_message = f"[{log_level}] {message}"
    if details:
        # 將詳細資訊也輸出到 stderr (格式化)
        fallback_message += f" | Details: {details.get('error_message', 'N/A')}"
        if log_level in ('ERROR', 'CRITICAL') and 'traceback' in details:
            fallback_message += f"\n--- TRACEBACK ---\n{details['traceback']}\n--- END TRACEBACK ---"
    
    if not MONGO_URI:
        # 如果 MONGO_URI 沒有設定，退回到標準錯誤輸出
        print(f"[Fallback Log] {fallback_message}", file=sys.stderr)
        return

    try:
        # 使用 w=0 確保非同步寫入，減少 ETL 流程的延遲 (MVP 適用)
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, w=0) 
        db = client[MONGO_DB_NAME]
        db[MONGO_COLLECTION].insert_one(log_entry)
        client.close()
    except Exception as e:
        # 如果 MongoDB 寫入失敗，則退回到標準輸出進行緊急日誌記錄
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
    """從環境變數中取得 PostgreSQL 連線資訊並建立 SQLAlchemy 引擎。"""
    DB_USER = os.environ.get("PG_USER", "postgres")
    DB_PASSWORD = os.environ.get("PG_PASSWORD")
    DB_HOST = os.environ.get("PG_HOST", "localhost")
    DB_PORT = os.environ.get("PG_PORT", "5432")
    DB_NAME = os.environ.get("PG_DATABASE", "postgres")

    if not DB_PASSWORD:
        # 如果密碼沒有設定，無法連線
        raise ValueError("PG_PASSWORD 環境變數未設定，請檢查 Docker 運行參數。")

    if DB_HOST.startswith("/cloudsql/"):
        # Unix Socket 連線格式： host 填寫為 '' 或 'localhost'，
        # 並且將 socket 路徑作為連線參數 (query string) 傳遞
        # 這是 Cloud SQL/psycopg2 處理 Unix Socket 連線的標準做法。
        # 讓 SQLAlchemy 將 DB_HOST 視為 socket 參數
        DATABASE_URL = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@/{DB_NAME}?"
            f"host={DB_HOST}"
        )
    else:
        # 傳統的 TCP/IP 連線
        DATABASE_URL = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

    print(f"DEBUG: Final PG URL: {DATABASE_URL}", file=sys.stderr) 
    return create_engine(DATABASE_URL, echo=False)


# --- 2. 核心 ETL 邏輯 ---

def real_estate_pipeline(year, season, area, trade_type, pg_engine):

    if year > 1000:
        year -= 1911

    url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&fileName={area}_lvr_land_{trade_type}.csv"
    
    # 【優化】動態生成 TARGET_TABLE_NAME
    trade_type_name = 'used' if trade_type == 'A' else 'presale'
    area_name = area_name_mapping.get(area, 'unknown').lower()
    TARGET_TABLE_NAME = f"real_estate_{trade_type_name}_{area_name}" # 將表名改為小寫以符合PostgreSQL慣例

    log_to_mongo('INFO',f"Starting ETL for {area_name.capitalize()}, {year}S{season}, {trade_type_name.capitalize()}",
                 details={"table": TARGET_TABLE_NAME, "url": url})

    res = requests.get(url)

    if res.status_code != 200:
        log_to_mongo('ERROR',f"Error: HTTP status code {res.status_code} for {url}",
                     details={"http_status": res.status_code, "table": TARGET_TABLE_NAME})
        return

    csv_data = StringIO(res.text, newline=None)
    df = pd.read_csv(csv_data, on_bad_lines='skip', engine='python', encoding='utf-8', encoding_errors='ignore', skip_blank_lines=True)

    if df.empty or len(df) < 2:
        log_to_mongo('WARNING',f"File for {TARGET_TABLE_NAME} is empty or contains only headers. Skipping.")
        return

    # --- E (提取/轉換) 階段 ---

    # 1. 檢查關鍵欄位是否存在
    required_cols = list(COLUMNS_MAPPING.keys()) + [FILTER_COLUMN]
    # 使用原始的 df.columns 進行檢查 (中文名稱)
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
    
    # 使用 Int64 允許 NaN 整數
    for col in integer_cols:
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
        
        print(f"  -> Successfully loaded {len(df_clean)} rows to {TARGET_TABLE_NAME}. Time: {load_time:.2f}s")
        
    except Exception as e:
        # 將錯誤訊息和 traceback 寫入 MongoDB
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
        # 緊急備援日誌 : 輸出到 stderr
        print(f"  -> CRITICAL DB ERROR for {TARGET_TABLE_NAME}: {e}\nTraceback:\n{tb}", file=sys.stderr)


# --- 主執行邏輯 ---
if __name__ == "__main__":
    print("--- 正在執行靜態 IP 診斷測試 ---")
    try:
        # 連線到一個回傳 IP 的服務
        response = requests.get('https://ifconfig.me/ip', timeout=10)
        exit_ip = response.text.strip()
        print(f"**診斷結果：此 Job 的出站 IP 是： {exit_ip} **")

        # 檢查是否為靜態 IP (請將 YOUR_STATIC_IP 替換成您的實際靜態 IP)
        YOUR_STATIC_IP = os.environ.get("EXPECTED_STATIC_IP") 

        if exit_ip == YOUR_STATIC_IP:
            print("**🎉 VPC/NAT 設置成功！出站 IP 正確！**")
        else:
            print(f"**❌ VPC/NAT 設置失敗！出站 IP 不正確 (預期: {YOUR_STATIC_IP})**")
            # 如果 IP 錯了，讓程式在這裡退出，不要繼續連 MongoDB
            return 

    except Exception as e:
        print(f"**診斷失敗：無法連線 ifconfig.me，可能是網路或防火牆問題。錯誤: {e}**")
        return
    
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
        # 處理配置錯誤 (例如 PG_PASSWORD 未設定)
        tb = traceback.format_exc()
        log_to_mongo('CRITICAL',f"CRITICAL CONFIGURATION ERROR: {ve}", 
                     details={"error_message": str(ve), "recommendation": "請確認您在 Docker 執行命令中設定了 PG_PASSWORD。", "traceback": tb})
        print(f"CRITICAL CONFIGURATION ERROR: {ve}\nTraceback:\n{tb}", file=sys.stderr)

    except Exception as e:
        # 處理系統級別錯誤 (例如 PostgreSQL/Mongo 連線失敗)
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
        # 確保此系統級別錯誤能被 Docker/Airflow 捕捉到
        print(f"CRITICAL SYSTEM ERROR: {e}\nTraceback:\n{tb}", file=sys.stderr)
