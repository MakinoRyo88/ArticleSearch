#!/bin/bash

# 設定値
PROJECT_ID="seo-optimize-464208"
SERVICE_NAME="seo-frontend-app"
REGION="asia-northeast1"
ARTIFACT_REGISTRY="asia-northeast1-docker.pkg.dev"
REPOSITORY="nextjs-apps"
API_BASE_URL="https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app"

echo "🚀 SEOフロントエンド Cloud Runデプロイ開始..."

# プロジェクト設定
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
echo "🔧 必要なAPIを有効化中..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Cloud Buildでビルド＆デプロイ（cloudbuild.yamlに全て定義済み）
echo "🔨 Cloud Buildでビルド＆デプロイ中..."
echo "   API_BASE_URL: $API_BASE_URL"

gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions=_API_BASE_URL="$API_BASE_URL" \
  .

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ デプロイが完了しました！"
  
  # サービスURLを取得
  SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format="value(status.url)" 2>/dev/null)
  
  if [ -n "$SERVICE_URL" ]; then
    echo ""
    echo "🎉 SEOフロントエンドのデプロイが完了しました！"
    echo ""
    echo "📊 サービス情報:"
    echo "  - サービス名: $SERVICE_NAME"
    echo "  - サービスURL: $SERVICE_URL"
    echo "  - リージョン: $REGION"
    echo "  - API接続先: $API_BASE_URL"
    echo ""
    echo "📋 サービス確認:"
    echo "  gcloud run services describe $SERVICE_NAME --region=$REGION"
    echo ""
    echo "📋 ログ確認:"
    echo "  gcloud run logs tail $SERVICE_NAME --region=$REGION"
    echo ""
  else
    echo "⚠️  サービスURLの取得に失敗しましたが、デプロイは成功しています"
  fi
else
  echo ""
  echo "❌ デプロイに失敗しました"
  echo ""
  echo "📋 ビルドログを確認してください:"
  echo "  gcloud builds list --limit=1"
  echo "  gcloud builds log \$(gcloud builds list --limit=1 --format='value(id)')"
  exit 1
fi
