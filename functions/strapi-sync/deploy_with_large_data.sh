#!/bin/bash

# Strapi同期関数のデプロイ（膨大なデータ対応版）

set -e

echo "🚀 Deploying strapi-sync function with large data support..."

# 環境変数
PROJECT_ID="seo-optimize-464208"
FUNCTION_NAME="sync-strapi-data"
REGION="asia-northeast1"

# Strapi URLを入力
echo ""
read -p "Strapi Base URL (例: https://cms.shikaku-pass.com): " STRAPI_URL
echo ""

# APIトークンが必要か確認
read -p "APIトークンは必要ですか？ (y/n): " NEED_TOKEN
echo ""

if [[ "$NEED_TOKEN" == "y" || "$NEED_TOKEN" == "Y" ]]; then
    # Secretを使用してデプロイ
    echo "⚠️  Secretを使用してデプロイします..."
    echo "Secret Manager に STRAPI_BASE_URL と STRAPI_API_TOKEN が登録されている必要があります。"
    echo ""
    
    gcloud functions deploy $FUNCTION_NAME \
      --gen2 \
      --runtime=python311 \
      --region=$REGION \
      --source=. \
      --entry-point=sync_strapi_data \
      --trigger-http \
      --allow-unauthenticated \
      --timeout=540s \
      --memory=4096MB \
      --max-instances=10 \
      --min-instances=0 \
      --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET_ID=content_analysis,ARTICLES_TABLE_ID=articles,COURSES_TABLE_ID=courses" \
      --set-secrets="STRAPI_BASE_URL=STRAPI_BASE_URL:latest,STRAPI_API_TOKEN=STRAPI_API_TOKEN:latest"
else
    # 環境変数のみでデプロイ（Secretなし）
    echo "✅ 環境変数のみでデプロイします（Secretなし）..."
    echo ""
    
    gcloud functions deploy $FUNCTION_NAME \
      --gen2 \
      --runtime=python311 \
      --region=$REGION \
      --source=. \
      --entry-point=sync_strapi_data \
      --trigger-http \
      --allow-unauthenticated \
      --timeout=540s \
      --memory=4096MB \
      --max-instances=10 \
      --min-instances=0 \
      --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET_ID=content_analysis,ARTICLES_TABLE_ID=articles,COURSES_TABLE_ID=courses,STRAPI_BASE_URL=$STRAPI_URL"
fi

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Improvements:"
echo "   - Increased page size: 25 → 50"
echo "   - Enhanced retry: 3 → 5 attempts"
echo "   - Longer timeout: 180s → 300s"
echo "   - Better error handling: continues on failures"
echo "   - Memory increased: 2048MB → 4096MB"
echo "   - Connection pooling enabled"
echo "   - Progress logging added"
echo ""
echo "🧪 Test the function:"
echo "   curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo ""
