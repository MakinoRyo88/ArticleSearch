#!/bin/bash

# 設定値
PROJECT_ID="seo-optimize-464208"
FUNCTION_NAME="ga4-sync"
REGION="asia-northeast1"
RUNTIME="python39"
ENTRY_POINT="ga4_sync_handler"
MEMORY="2048MB"  # メモリを2GBに増加
TIMEOUT="900s"   # タイムアウトを15分に延長
SERVICE_ACCOUNT="cloudrun-processor@seo-optimize-464208.iam.gserviceaccount.com"
CPU="2"          # CPUを2に増加

# GA4から取得する日数設定
GA4_DAYS_BACK="7" # デフォルトを7日に設定。必要に応じて変更してください。

# GA4データ取得のLIMIT設定 (オプション: 制限なしにする場合は 'None' または空文字列を設定)
# 例: GA4_LIMIT="5000" で5000件に制限
# 例: GA4_LIMIT="None" または GA4_LIMIT="" で制限なし
GA4_LIMIT="None" # デフォルトは制限なし。必要に応じて数字を設定してください (例: GA4_LIMIT="5000")

echo "🚀 最適化されたGA4同期Cloud Functionsをデプロイ開始..."

# プロジェクト設定
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
echo "🔧 必要なAPIを有効化中..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable bigquery.googleapis.com

# 環境変数を設定
ENV_VARS="GCP_PROJECT=$PROJECT_ID,GA4_DAYS_BACK=$GA4_DAYS_BACK"
if [ "$GA4_LIMIT" != "None" ] && [ -n "$GA4_LIMIT" ]; then
  ENV_VARS+=",GA4_LIMIT=$GA4_LIMIT"
fi

# 1. 最適化されたHTTP関数のデプロイ
echo "📦 最適化されたHTTP関数をデプロイ中..."
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
  --max-instances=1 \
  --min-instances=0 \
  --concurrency=1 \
  --quiet

if [ $? -eq 0 ]; then
    echo "✅ 最適化されたHTTP関数のデプロイ完了"
else
    echo "❌ HTTP関数のデプロイに失敗"
    exit 1
fi

# 2. 軽量テスト実行
echo "🧪 軽量テストを実行中..."
FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"

echo "関数URL: $FUNCTION_URL"
echo "テスト実行中（最大10分間）..."

# バックグラウンドで実行し、進捗を表示
curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{}' \
  --max-time 600 \
  --show-error \
  --silent > /tmp/test_result.json 2>&1 &

CURL_PID=$!
COUNTER=0

while kill -0 $CURL_PID 2>/dev/null; do
    COUNTER=$((COUNTER + 1))
    echo "テスト実行中... ${COUNTER}0秒経過"
    sleep 10
    
    if [ $COUNTER -ge 60 ]; then
        echo "⚠️ テストが10分でタイムアウトしました"
        kill $CURL_PID 2>/dev/null
        break
    fi
done

wait $CURL_PID 2>/dev/null
TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ テスト実行完了"
    if [ -f /tmp/test_result.json ]; then
        echo "📋 テスト結果:"
        cat /tmp/test_result.json | jq . 2>/dev/null || cat /tmp/test_result.json
    fi
else
    echo "⚠️ テスト実行でエラーが発生しました（エラーコード: $TEST_EXIT_CODE）"
    if [ -f /tmp/test_result.json ]; then
        echo "📋 エラー内容:"
        cat /tmp/test_result.json
    fi
fi

# 一時ファイルの削除
rm -f /tmp/test_result.json

echo ""
echo "🎉 最適化されたGA4同期システムのデプロイが完了しました！"
echo ""
echo "📊 情報:"
echo "  - HTTP関数URL: $FUNCTION_URL"
echo "  - ログ確認: gcloud functions logs read $FUNCTION_NAME --region=$REGION"
echo "  - 手動実行: curl -X POST $FUNCTION_URL"
echo ""
echo "🔧 パフォーマンス監視:"
echo "  1. Cloud Consoleでメトリクスを確認"
echo "  2. 実行時間とメモリ使用量を監視"
echo "  3. BigQueryのクエリ実行統計を確認"
echo ""
echo "⚡ 高速化のポイント:"
echo "  - 並列データ取得・処理"
echo "  - 最適化されたBigQueryクエリ"
echo "  - MERGE文による効率的な更新"
#  - 不要なデータの事前フィルタリング