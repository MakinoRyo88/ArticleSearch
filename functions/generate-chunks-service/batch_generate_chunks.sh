#!/bin/bash

# 全記事のチャンク生成バッチスクリプト
# 779記事を10記事ずつバッチ処理

set -e

FUNCTION_URL="https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/generate-chunk-embeddings"
BATCH_SIZE=10
TOTAL_ARTICLES=779

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        🚀 全記事チャンク生成バッチ処理を開始します            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 設定:"
echo "   総記事数: ${TOTAL_ARTICLES}件"
echo "   バッチサイズ: ${BATCH_SIZE}件"
echo "   総バッチ数: $((($TOTAL_ARTICLES + $BATCH_SIZE - 1) / $BATCH_SIZE))回"
echo ""

# 結果を保存
RESULTS_FILE="chunk_generation_results_$(date +%Y%m%d_%H%M%S).log"
echo "📝 結果ログファイル: ${RESULTS_FILE}"
echo ""

# 確認
read -p "実行しますか？ (y/n): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "❌ キャンセルしました"
    exit 0
fi

echo "" | tee -a "$RESULTS_FILE"
echo "開始時刻: $(date)" | tee -a "$RESULTS_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# カウンター初期化
total_processed=0
total_chunks=0
failed_batches=0

# バッチループ
for offset in $(seq 0 $BATCH_SIZE $((TOTAL_ARTICLES - 1))); do
    batch_num=$(($offset / $BATCH_SIZE + 1))
    total_batches=$((($TOTAL_ARTICLES + $BATCH_SIZE - 1) / $BATCH_SIZE))
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$RESULTS_FILE"
    echo "📦 バッチ ${batch_num}/${total_batches}" | tee -a "$RESULTS_FILE"
    echo "   オフセット: ${offset}" | tee -a "$RESULTS_FILE"
    echo "   開始: $(date +%H:%M:%S)" | tee -a "$RESULTS_FILE"
    
    # API呼び出し
    response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "$FUNCTION_URL" \
        -H "Content-Type: application/json" \
        -d "{
            \"batch_size\": ${BATCH_SIZE},
            \"force_regenerate\": false,
            \"offset\": ${offset}
        }")
    
    # HTTPステータスとボディを分離
    http_status=$(echo "$response" | grep "HTTP_STATUS:" | cut -d: -f2)
    body=$(echo "$response" | sed '/HTTP_STATUS:/d')
    
    # 結果を解析
    if [[ "$http_status" == "200" ]]; then
        processed=$(echo "$body" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('processed_articles', 0))" 2>/dev/null || echo "0")
        chunks=$(echo "$body" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('generated_chunks', 0))" 2>/dev/null || echo "0")
        
        total_processed=$((total_processed + processed))
        total_chunks=$((total_chunks + chunks))
        
        echo "   ✅ 成功: ${processed}記事、${chunks}チャンク生成" | tee -a "$RESULTS_FILE"
        echo "   累計: ${total_processed}記事、${total_chunks}チャンク" | tee -a "$RESULTS_FILE"
    else
        echo "   ❌ 失敗: HTTP ${http_status}" | tee -a "$RESULTS_FILE"
        echo "   Response: ${body}" | tee -a "$RESULTS_FILE"
        failed_batches=$((failed_batches + 1))
        
        # 3回連続失敗で停止
        if [[ $failed_batches -ge 3 ]]; then
            echo "" | tee -a "$RESULTS_FILE"
            echo "💥 連続失敗が3回に達したため、処理を中断します" | tee -a "$RESULTS_FILE"
            break
        fi
    fi
    
    echo "   終了: $(date +%H:%M:%S)" | tee -a "$RESULTS_FILE"
    
    # APIレート制限対策（最後のバッチ以外）
    if [[ $offset -lt $((TOTAL_ARTICLES - BATCH_SIZE)) ]]; then
        echo "   ⏳ 待機: 5秒..." | tee -a "$RESULTS_FILE"
        sleep 5
    fi
done

echo "" | tee -a "$RESULTS_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$RESULTS_FILE"
echo "📊 最終結果" | tee -a "$RESULTS_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$RESULTS_FILE"
echo "   処理した記事数: ${total_processed}/${TOTAL_ARTICLES}" | tee -a "$RESULTS_FILE"
echo "   生成したチャンク数: ${total_chunks}" | tee -a "$RESULTS_FILE"
echo "   失敗したバッチ数: ${failed_batches}" | tee -a "$RESULTS_FILE"
echo "   完了率: $(awk "BEGIN {printf \"%.2f\", ($total_processed/$TOTAL_ARTICLES)*100}")%" | tee -a "$RESULTS_FILE"
echo "   終了時刻: $(date)" | tee -a "$RESULTS_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# BigQueryで確認
echo "🔍 BigQueryで結果を確認中..." | tee -a "$RESULTS_FILE"
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_chunks,
  COUNT(DISTINCT article_id) as articles_with_chunks,
  AVG(LENGTH(chunk_text)) as avg_chunk_length,
  MIN(LENGTH(chunk_text)) as min_chunk_length,
  MAX(LENGTH(chunk_text)) as max_chunk_length
FROM \`seo-optimize-464208.content_analysis.article_chunks\`
" | tee -a "$RESULTS_FILE"

echo "" | tee -a "$RESULTS_FILE"
echo "✅ バッチ処理完了！" | tee -a "$RESULTS_FILE"
echo "📝 詳細ログ: ${RESULTS_FILE}" | tee -a "$RESULTS_FILE"
