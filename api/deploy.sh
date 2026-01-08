#!/bin/bash

# 設定値
PROJECT_ID="seo-optimize-464208"
SERVICE_NAME="seo-realtime-analysis-api"
REGION="asia-northeast1"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"
PORT="8080"

# Cloud Run設定
MEMORY="2Gi"
CPU="2"
MAX_INSTANCES="10"
MIN_INSTANCES="1"
CONCURRENCY="10"    # 長時間処理対応のため大幅削減
TIMEOUT="900s"

echo "🚀 SEOリアルタイム分析API Cloud Runデプロイ開始..."

# プロジェクト設���
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
echo "🔧 必要なAPIを有効化中..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable aiplatform.googleapis.com

# 環境変数設定
ENV_VARS="GCP_PROJECT=$PROJECT_ID"
ENV_VARS+=",NODE_ENV=production"
ENV_VARS+=",LOG_LEVEL=info"
ENV_VARS+=",RATE_LIMIT_MAX=1000"

# 1. Docker イメージビルド
echo "🔨 Docker イメージをビルド中..."
gcloud builds submit --tag $IMAGE_NAME --timeout=600s

if [ $? -eq 0 ]; then
  echo "✅ Docker イメージビルド完了"
else
  echo "❌ Docker イメージビルドに失敗"
  exit 1
fi

# 2. Cloud Run サービスデプロイ
echo "📦 Cloud Run サービスをデプロイ中..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --memory $MEMORY \
  --cpu $CPU \
  --max-instances $MAX_INSTANCES \
  --min-instances $MIN_INSTANCES \
  --concurrency $CONCURRENCY \
  --timeout $TIMEOUT \
  --set-env-vars="$ENV_VARS" \
  --port $PORT \
  --execution-environment gen2 \
  --cpu-boost \
  --no-cpu-throttling \
  --no-use-http2 \
  --quiet

if [ $? -eq 0 ]; then
  echo "✅ Cloud Run サービスデプロイ完了"
else
  echo "❌ Cloud Run サービスデプロイに失敗"
  echo "📋 ログを確認してください:"
  echo "gcloud run logs tail $SERVICE_NAME --region=$REGION"
  exit 1
fi

# 3. サービスURL取得
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)")

# 4. ヘルスチェック
echo "🏥 ヘルスチェック実行中..."
sleep 30

HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$SERVICE_URL/health" --max-time 30)

if [ "$HEALTH_RESPONSE" = "200" ]; then
  echo "✅ ヘルスチェック成功"
else
  echo "⚠️ ヘルスチェック失敗 (HTTP: $HEALTH_RESPONSE)"
  echo "📋 ログを確認してください:"
  echo "gcloud run logs tail $SERVICE_NAME --region=$REGION"
fi

# 5. 基本API テスト
echo "🧪 基本API テスト実行中..."

# ルートエンドポイントテスト
echo "ルートエンドポイントテスト..."
ROOT_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$SERVICE_URL/" --max-time 30)

if [ "$ROOT_RESPONSE" = "200" ]; then
  echo "✅ ルートエンドポイント正常"
else
  echo "⚠️ ルートエンドポイント異常 (HTTP: $ROOT_RESPONSE)"
fi

# API情報テスト
echo "API情報テスト..."
API_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$SERVICE_URL/api" --max-time 30)

if [ "$API_RESPONSE" = "200" ]; then
  echo "✅ API情報エンドポイント正常"
else
  echo "⚠️ API情報エンドポイント異常 (HTTP: $API_RESPONSE)"
fi

echo ""
echo "🎉 SEOリアルタイム分析APIのデプロイが完了しました！"
echo ""
echo "📊 サービス情報:"
echo "  - サービス名: $SERVICE_NAME"
echo "  - サービスURL: $SERVICE_URL"
echo "  - リージョン: $REGION"
echo "  - メモリ: $MEMORY"
echo "  - CPU: $CPU"
echo ""
echo "🔗 主要エンドポイント:"
echo "  - ヘルスチェック: $SERVICE_URL/health"
echo "  - API情報: $SERVICE_URL/api"
echo "  - 記事検索: $SERVICE_URL/api/search/articles"
echo "  - 類似度計算: $SERVICE_URL/api/similarity/{articleId}"
echo "  - 統合提案: $SERVICE_URL/api/recommendations/generate"
echo "  - Gemini説明: $SERVICE_URL/api/explanations/generate"
echo ""
echo "📈 監視・管理:"
echo "  - ログ確認: gcloud run logs tail $SERVICE_NAME --region=$REGION"
echo "  - メトリクス: Cloud Console > Cloud Run > $SERVICE_NAME"
echo "  - 設定変更: gcloud run services update $SERVICE_NAME --region=$REGION"
echo ""
echo "🔧 トラブルシューティング:"
echo "  - リアルタイムログ: gcloud run logs tail $SERVICE_NAME --region=$REGION --follow"
echo "  - サービス詳細: gcloud run services describe $SERVICE_NAME --region=$REGION"
echo ""
