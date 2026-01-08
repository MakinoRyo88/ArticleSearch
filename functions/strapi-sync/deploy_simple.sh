#!/bin/bash

# Strapi同期関数のシンプルなデプロイ（環境変数のみ、Secretなし）

set -e

echo "🚀 Deploying strapi-sync function (simple mode - no secrets)..."

# 環境変数
PROJECT_ID="seo-optimize-464208"
FUNCTION_NAME="sync-strapi-data"
REGION="asia-northeast1"
STRAPI_URL="https://stg-mcs-backend-run-852986774845.asia-northeast1.run.app"

echo ""
echo "📝 Configuration:"
echo "   Project ID: $PROJECT_ID"
echo "   Function Name: $FUNCTION_NAME"
echo "   Region: $REGION"
echo "   Strapi URL: $STRAPI_URL"
echo ""

# デプロイ
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
echo "   - 🆕 No secrets required (API token optional)"
echo ""
echo "🧪 Test the function:"
echo "   curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo ""
