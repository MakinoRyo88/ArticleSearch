#!/bin/bash

# 環境変数設定
PROJECT_ID="seo-optimize-464208"
REGION="asia-northeast1"
FUNCTION_NAME="strapi-sync"
DATASET_ID="content_analysis"
STRAPI_BASE_URL="https://stg-mcs-backend-run-852986774845.asia-northeast1.run.app"

echo "🚀 strapi-sync関数をデプロイしています..."
echo "   Project: $PROJECT_ID"
echo "   Region: $REGION"
echo "   Strapi: $STRAPI_BASE_URL"
echo ""

# メイン関数をデプロイ
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=sync_strapi_data \
  --trigger-http \
  --allow-unauthenticated \
  --memory=2048MB \
  --timeout=540s \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=10 \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET_ID=$DATASET_ID,STRAPI_BASE_URL=$STRAPI_BASE_URL"

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ デプロイ完了!"
  echo ""
  echo "📋 関数情報:"
  echo "   名前: $FUNCTION_NAME"
  echo "   URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
  echo ""
  echo "🧪 テスト実行:"
  echo "   curl -X POST \"https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME\" \\"
  echo "     -H \"Content-Type: application/json\""
  echo ""
else
  echo ""
  echo "❌ デプロイ失敗"
  exit 1
fi