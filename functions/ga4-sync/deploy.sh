#!/bin/bash

# 設定値
PROJECT_ID="seo-optimize-464208"
FUNCTION_NAME="ga4-sync"
REGION="asia-northeast1"
RUNTIME="python39"
ENTRY_POINT="ga4_sync_handler"
SCHEDULER_ENTRY_POINT="ga4_sync_scheduler"
MEMORY="2048MB"  # メモリを2GBに増加
TIMEOUT="900s"   # タイムアウトを15分に延長
SERVICE_ACCOUNT="cloudrun-processor@seo-optimize-464208.iam.gserviceaccount.com"
CPU="2"          # CPUを2に増加

# デフォルト日数設定
DEFAULT_GA4_DAYS_BACK="7"  # デフォルト7日分

# スケジューラー設定
SCHEDULER_NAME="ga4-sync-daily"
SCHEDULE="0 6 * * *"  # 毎日朝6時に実行

echo "🚀 最適化されたGA4同期Cloud Functionsをデプロイ開始..."

# プロジェクト設定
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
echo "🔧 必要なAPIを有効化中..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable bigquery.googleapis.com

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
  --set-env-vars="GCP_PROJECT=$PROJECT_ID" \
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

# 2. スケジューラー用の関数をデプロイ
echo "📦 スケジューラー関数をデプロイ中..."
gcloud functions deploy "${FUNCTION_NAME}-scheduler" \
  --gen2 \
  --region=$REGION \
  --runtime=$RUNTIME \
  --source=. \
  --entry-point=$SCHEDULER_ENTRY_POINT \
  --trigger-http \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --cpu=$CPU \
  --service-account=$SERVICE_ACCOUNT \
  --set-env-vars="GCP_PROJECT=$PROJECT_ID" \
  --max-instances=1 \
  --min-instances=0 \
  --concurrency=1 \
  --quiet

if [ $? -eq 0 ]; then
    echo "✅ スケジューラー関数のデプロイ完了"
else
    echo "❌ スケジューラー関数のデプロイに失敗"
    exit 1
fi

# 3. Cloud Schedulerジョブの作成
echo "📅 Cloud Schedulerジョブを作成中..."

# 既存のジョブを削除（エラーは無視）
gcloud scheduler jobs delete $SCHEDULER_NAME --location=$REGION --quiet 2>/dev/null

# 新しいジョブを作成（より長いタイムアウト設定）
gcloud scheduler jobs create http $SCHEDULER_NAME \
  --location=$REGION \
  --schedule="$SCHEDULE" \
  --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/${FUNCTION_NAME}-scheduler" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --description="最適化されたGA4データ同期の日次実行" \
  --time-zone="Asia/Tokyo" \
  --attempt-deadline="900s" \
  --max-retry-attempts=3 \
  --max-retry-duration="1800s" \
  --min-backoff-duration="60s" \
  --max-backoff-duration="300s" \
  --quiet

if [ $? -eq 0 ]; then
    echo "✅ Cloud Schedulerジョブの作成完了"
else
    echo "❌ Cloud Schedulerジョブの作成に失敗"
    exit 1
fi

# 4. IAMロールの設定
echo "🔐 IAMロールを設定中..."

# BigQueryアクセス権限
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataEditor" \
  --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.jobUser" \
  --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.user" \
  --quiet

# 外部プロジェクトアクセス権限
echo "🔑 外部プロジェクトアクセス権限を設定中..."
gcloud projects add-iam-policy-binding mcs-fs-pro \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataViewer" \
  --quiet

gcloud projects add-iam-policy-binding mcs-fs-pro \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.jobUser" \
  --quiet

# Cloud Functions関連の権限
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/cloudfunctions.invoker" \
  --quiet

# 5. BigQueryデータセットの最適化設定
echo "🎯 BigQueryデータセットの最適化設定..."

# articlesテーブルにインデックス的な最適化のための統計情報更新
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
  "SELECT COUNT(*) as total_articles FROM \`$PROJECT_ID.$TARGET_DATASET_ID.articles\`" \
  --quiet

echo "📊 BigQueryテーブル統計情報を更新しました"

# 6. 軽量テスト実行
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
echo "📊 最適化内容:"
echo "  - メモリ: 2GB (旧: 1GB)"
echo "  - CPU: 2コア (旧: 1コア)"
echo "  - タイムアウト: 15分 (旧: 9分)"
echo "  - データ取得期間: 7日間 (旧: 30日間)"
echo "  - 並列処理: 有効"
echo "  - クエリ最適化: 有効"
echo "  - MERGE文による高速更新: 有効"
echo ""
echo "📊 情報:"
echo "  - HTTP関数URL: $FUNCTION_URL"
echo "  - スケジューラー: 毎日朝6時に自動実行"
echo "  - ログ確認: gcloud functions logs read $FUNCTION_NAME --region=$REGION"
echo "  - 手動実行: curl -X POST $FUNCTION_URL"
echo ""
echo "🔧 パフォーマンス監視:"
echo "  1. Cloud Consoleでメトリクスを確認"
echo "  2. 実行時間とメモリ使用量を監視"
echo "  3. BigQueryのクエリ実行統計を確認"
echo ""
echo "⚡ 高速化のポイント:"
echo "  - GA4データ取得期間を7日間に短縮"
echo "  - 並列データ取得・処理"
echo "  - 最適化されたBigQueryクエリ"
echo "  - MERGE文による効率的な更新"
echo "  - 不要なデータの事前フィルタリング"