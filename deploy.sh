#!/bin/bash
set -e # 任何指令失敗即終止

# --- 部署/基礎設施參數 (Deployment/Infra Config) ---
export REGION="us-central1"
export IMAGE_REPO="cloud-run-real-estate"
export IMAGE_NAME="cloud-run-postgre-mongo"
export IMAGE_TAG="v6-publicIP" 

export GCP_PROJECT_ID=$(gcloud config get-value project)
export IMAGE_PATH="${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${IMAGE_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# --- 應用程式配置 (App Config - 非機敏) ---
export JOB_NAME="cloud-run-real-estate-job"
export PG_USER='postgres'
export PG_DATABASE='postgres'
export MONGO_DB_NAME='real-estate-etl_monitoring'

export CLOUDSQL_PUBLIC_IP='136.114.207.45'

# --- 部署/執行指令 ---
gcloud builds submit . --tag $IMAGE_PATH

## 部屬時啟用 Cloud SQL Auth Proxy 加強安全性
gcloud run jobs deploy $JOB_NAME \
  --image "$IMAGE_PATH" \
  --region "$REGION" \
  --vpc-connector cloud-run-psc-connector \
  --vpc-egress all \
  --task-timeout 60s \
  --set-secrets MONGO_URI="MONGO_ATLAS_URI:latest",PG_PASSWORD="PG_PASSWORD:latest" \
  --set-env-vars MONGO_DB_NAME="$MONGO_DB_NAME",PG_USER="$PG_USER",PG_DATABASE="$PG_DATABASE",PG_HOST_PUBLIC="$CLOUDSQL_PUBLIC_IP" 


# 執行任務
gcloud run jobs execute $JOB_NAME --region $REGION --wait