#!/bin/bash

# 設定値
PROJECT_ID="seo-optimize-464208"
FUNCTION_NAME="generate-similarity-cache"
REGION="asia-northeast1"
RUNTIME="python39"
ENTRY_POINT="similarity_cache_handler"
MEMORY="4096MB"  # メモリを4GBに設定（類似度計算のため）
TIMEOUT="1800s"  # タイムアウトを30分に延長
SERVICE_ACCOUNT="cloudrun-processor@seo-optimize-464208.iam.gserviceaccount.com"
CPU="4"          # CPUを4に増加

# 類似度計算の設定値
MIN_PAGEVIEWS_THRESHOLD="100"     # 最小PV数閾値
SIMILARITY_THRESHOLD="0.3"        # 類似度閾値
MAX_SIMILAR_ARTICLES="20"         # 記事あたりの最大類似記事数
CACHE_EXPIRY_DAYS="7"            # キャッシュ有効期限（日）
BATCH_SIZE="50"                  # バッチサイズ

echo "🚀 類似度キャッシュ生成Cloud Functionsをデプロイ開始..."

# プロジェクト設定
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
echo "🔧 必要なAPIを有効化中..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable aiplatform.googleapis.com

# 環境変数を設定
ENV_VARS="GCP_PROJECT=$PROJECT_ID"
ENV_VARS+=",MIN_PAGEVIEWS_THRESHOLD=$MIN_PAGEVIEWS_THRESHOLD"
ENV_VARS+=",SIMILARITY_THRESHOLD=$SIMILARITY_THRESHOLD"
ENV_VARS+=",MAX_SIMILAR_ARTICLES=$MAX_SIMILAR_ARTICLES"
ENV_VARS+=",CACHE_EXPIRY_DAYS=$CACHE_EXPIRY_DAYS"
ENV_VARS+=",BATCH_SIZE=$BATCH_SIZE"

# 1. 高性能HTTP関数のデプロイ
echo "📦 高性能HTTP関数をデプロイ中..."
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --region=$REGION \
  --runtime=$RUNTIME \
  --source=. \
  --entry-point=$ENTRY_POINT \
  --trigger-http \
  --allow-unauthenticated \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --cpu=$CPU \
  --service-account=$SERVICE_ACCOUNT \
  --set-env-vars="$ENV_VARS" \
  --max-instances=3 \
  --min-instances=1 \
  --concurrency=1 \
  --quiet

if [ $? -eq 0 ]; then
  echo "✅ 高性能HTTP関数のデプロイ完了"
else
  echo "❌ HTTP関数のデプロイに失敗"
  exit 1
fi

# 2. 軽量テスト実行
echo "🧪 軽量テストを実行中..."
FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo "関数URL: $FUNCTION_URL"
echo "テスト実行中（最大1分間）..." # <--- 表示メッセージも変更

# バックグラウンドで実行し、進捗を表示
curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{"min_pageviews": 50, "similarity_threshold": 0.4, "max_similar_articles": 10}' \
  --max-time 60 \ # <--- 1200から60に変更
  --show-error \
  --silent > /tmp/similarity_test_result.json 2>&1 &

CURL_PID=$!
COUNTER=0

while kill -0 $CURL_PID 2>/dev/null; do
  COUNTER=$((COUNTER + 1))
  echo "テスト実行中... ${COUNTER}0秒経過"
  sleep 10
  
  if [ $COUNTER -ge 6 ]; then # <--- 120から6に変更
    echo "⚠️ テストが1分でタイムアウトしました" # <--- 表示メッセージも変更
    kill $CURL_PID 2>/dev/null
    break
  fi
done

wait $CURL_PID 2>/dev/null
TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
  echo "✅ テスト実行完了"
  if [ -f /tmp/similarity_test_result.json ]; then
    echo "📋 テスト結果:"
    cat /tmp/similarity_test_result.json | jq . 2>/dev/null || cat /tmp/similarity_test_result.json
  fi
else
  echo "⚠️ テスト実行でエラーが発生しました（エラーコード: $TEST_EXIT_CODE）"
  if [ -f /tmp/similarity_test_result.json ]; then
    echo "📋 エラー内容:"
    cat /tmp/similarity_test_result.json
  fi
fi

# 一時ファイルの削除
rm -f /tmp/similarity_test_result.json

echo ""
echo "🎉 類似度キャッシュ生成システムのデプロイが完了しました！"
echo ""
echo "📊 情報:"
echo "  - HTTP関数URL: $FUNCTION_URL"
echo "  - ログ確認: gcloud functions logs read $FUNCTION_NAME --region=$REGION"
echo "  - 手動実行: curl -X POST $FUNCTION_URL"
echo ""
echo "🔧 パフォーマンス設定:"
echo "  - メモリ: $MEMORY"
echo "  - CPU: $CPU"
echo "  - タイムアウト: $TIMEOUT"
echo "  - 最小PV閾値: $MIN_PAGEVIEWS_THRESHOLD"
echo "  - 類似度閾値: $SIMILARITY_THRESHOLD"
echo ""
echo "⚡ 高速化のポイント:"
echo "  - BigQuery ML Vector Searchによる高速類似度計算"
echo "  - バッチ処理による効率的なデータ処理"
echo "  - Vertex AI Geminiによる説明文生成"
echo "  - キャッシュテーブルによる結果保存"
echo ""
echo "📅 推奨実行スケジュール:"
echo "  - 週次実行（埋め込み生成後）"
echo "  - Cloud Schedulerでの自動実行設定を推奨"
echo ""
echo "🔍 監視ポイント:"
echo "  1. 実行時間とメモリ使用量"
echo "  2. BigQueryのクエリ実行統計"
echo "  3. Vertex AI APIの使用量"
echo "  4. キャッシュテーブルのサイズ"
