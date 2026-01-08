#!/bin/bash

# LLMプロキシCloud Functionのデプロイスクリプト

PROJECT_ID="seo-optimize-464208"
REGION="asia-northeast1"

echo "🚀 LLMプロキシ Cloud Functions デプロイ開始..."

# プロジェクト設定
gcloud config set project $PROJECT_ID

# テキスト生成プロキシをデプロイ
echo "📦 テキスト生成プロキシをデプロイ中..."
gcloud functions deploy llm-generate-text \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=llm_generate_text \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=300s \
  --memory=512Mi \
  --max-instances=10 \
  --quiet

if [ $? -eq 0 ]; then
  echo "✅ テキスト生成プロキシデプロイ完了"
else
  echo "❌ テキスト生成プロキシデプロイ失敗"
  exit 1
fi

# ヘルスチェックプロキシをデプロイ
echo "📦 ヘルスチェックプロキシをデプロイ中..."
gcloud functions deploy llm-health-check \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=llm_health_check \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=60s \
  --memory=256Mi \
  --max-instances=10 \
  --quiet

if [ $? -eq 0 ]; then
  echo "✅ ヘルスチェックプロキシデプロイ完了"
else
  echo "❌ ヘルスチェックプロキシデプロイ失敗"
  exit 1
fi

echo ""
echo "🎉 LLMプロキシ Cloud Functions デプロイ完了！"
echo ""
echo "🔗 エンドポイント:"
echo "  - テキスト生成: https://$REGION-$PROJECT_ID.cloudfunctions.net/llm-generate-text"
echo "  - ヘルスチェック: https://$REGION-$PROJECT_ID.cloudfunctions.net/llm-health-check"
echo ""