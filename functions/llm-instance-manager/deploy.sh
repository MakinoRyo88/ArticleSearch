#!/bin/bash
# Cloud Functions デプロイスクリプト
# Phase 1: 手動制御機能

set -e

# 設定
PROJECT_ID="seo-optimize-464208"  # 実際のプロジェクトIDに変更
REGION="asia-northeast1"

echo "=== Cloud Functions デプロイ開始 ==="

# プロジェクト設定
gcloud config set project $PROJECT_ID

# 1. LLMインスタンス起動関数
echo "Deploying start-llm-instance function..."
gcloud functions deploy start-llm-instance \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --source . \
  --entry-point start_llm_instance \
  --memory 512MB \
  --timeout 540s \
  --max-instances 3

# 2. LLMインスタンス停止関数
echo "Deploying stop-llm-instance function..."
gcloud functions deploy stop-llm-instance \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --source . \
  --entry-point stop_llm_instance \
  --memory 256MB \
  --timeout 300s \
  --max-instances 3

# 3. LLMインスタンス状態確認関数
echo "Deploying get-llm-status function..."
gcloud functions deploy get-llm-status \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --source . \
  --entry-point get_llm_status \
  --memory 256MB \
  --timeout 60s \
  --max-instances 5

echo "=== デプロイ完了 ==="

# エンドポイント表示
echo ""
echo "📍 Cloud Functions エンドポイント:"
echo "起動: https://$REGION-$PROJECT_ID.cloudfunctions.net/start-llm-instance"
echo "停止: https://$REGION-$PROJECT_ID.cloudfunctions.net/stop-llm-instance"
echo "状態: https://$REGION-$PROJECT_ID.cloudfunctions.net/get-llm-status"

echo ""
echo "🧪 テストコマンド例:"
echo "# インスタンス起動"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/start-llm-instance"
echo ""
echo "# 状態確認"
echo "curl https://$REGION-$PROJECT_ID.cloudfunctions.net/get-llm-status"
echo ""
echo "# インスタンス停止"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/stop-llm-instance"