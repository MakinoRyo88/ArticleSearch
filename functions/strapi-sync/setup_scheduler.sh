#!/bin/bash

# 環境変数設定
PROJECT_ID="seo-optimize-464208"
REGION="asia-northeast1"
FUNCTION_NAME="strapi-sync"
JOB_NAME="strapi-sync-weekly"

# 週次スケジュール設定 (毎週月曜日 朝9時)
gcloud scheduler jobs create http $JOB_NAME \
  --schedule="0 9 * * 1" \
  --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME" \
  --http-method=POST \
  --location=$REGION \
  --time-zone="Asia/Tokyo" \
  --description="Weekly Strapi data sync"

echo "Cloud Scheduler設定完了"
echo "Job名: $JOB_NAME"
echo "スケジュール: 毎週月曜日 朝9時"

# 手動実行用コマンド
echo ""
echo "手動実行コマンド:"
echo "gcloud scheduler jobs run $JOB_NAME --location=$REGION"