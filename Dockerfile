FROM python:3.10-slim

WORKDIR /app

# 1. 優先安裝 SSL 憑證 (修復 [TLSV1_ALERT_INTERNAL_ERROR])
# python:3.10-slim 是基於 Debian 的，使用 apt-get
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# 啟動應用程式 
CMD ["python", "main.py"]
