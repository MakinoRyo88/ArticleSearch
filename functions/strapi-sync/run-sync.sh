#!/bin/bash

# カラー設定
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# strapi-sync実行スクリプト
FUNCTION_URL="https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/strapi-sync"

echo ""
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║           📚 Strapi → BigQuery データ同期ツール               ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 確認メッセージ
echo -e "${YELLOW}⚠️  このスクリプトは以下を実行します:${NC}"
echo "   • Strapiから全記事・講座データを取得"
echo "   • full_content_html（HTMLタグ付き）を生成"
echo "   • full_content（プレーンテキスト）を生成"
echo "   • BigQueryのarticlesテーブルを更新"
echo ""
echo -e "${YELLOW}⏱️  予想実行時間: 5-10分${NC}"
echo ""

# 実行確認
read -p "実行しますか？ (y/n): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}✗ キャンセルしました${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}🚀 同期を開始します...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# 開始時刻を記録
START_TIME=$(date +%s)

# 一時ファイルを作成
TEMP_OUTPUT=$(mktemp)
TEMP_ERROR=$(mktemp)

# バックグラウンドでリクエストを実行
echo -e "${CYAN}📡 Cloud Functionを呼び出し中...${NC}"
echo ""

curl -X POST \
  "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -w "\n" \
  --max-time 600 \
  --connect-timeout 30 \
  -s \
  -o "$TEMP_OUTPUT" \
  2>"$TEMP_ERROR" &

CURL_PID=$!

# プログレスバー表示
echo -ne "${PURPLE}処理中: ${NC}"
COUNTER=0
while kill -0 $CURL_PID 2>/dev/null; do
    COUNTER=$((COUNTER + 1))
    ELAPSED=$((COUNTER * 2))
    
    # プログレスバー
    PROGRESS=$((ELAPSED % 60))
    BAR=""
    for i in $(seq 1 30); do
        if [ $i -le $((PROGRESS / 2)) ]; then
            BAR="${BAR}█"
        else
            BAR="${BAR}░"
        fi
    done
    
    echo -ne "\r${PURPLE}処理中: ${NC}[${BAR}] ${ELAPSED}秒経過"
    sleep 2
done

echo ""
echo ""

# 終了時刻を記録
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# 結果を確認
if [ -s "$TEMP_OUTPUT" ]; then
    # レスポンスをJSONとして解析
    STATUS=$(cat "$TEMP_OUTPUT" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    MESSAGE=$(cat "$TEMP_OUTPUT" | grep -o '"message":"[^"]*"' | cut -d'"' -f4 | sed 's/\\n/\n/g')
    
    if [ "$STATUS" = "success" ]; then
        echo -e "${GREEN}✅ 同期が完了しました！${NC}"
        echo ""
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}📊 結果:${NC}"
        echo ""
        
        # レスポンスを見やすく表示
        cat "$TEMP_OUTPUT" | python3 -m json.tool 2>/dev/null || cat "$TEMP_OUTPUT"
        
        echo ""
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${CYAN}⏱️  実行時間: ${DURATION}秒${NC}"
        echo ""
        
        # 次のステップを表示
        echo -e "${YELLOW}📋 次のステップ:${NC}"
        echo ""
        echo "1. APIレスポンスを確認:"
        echo -e "   ${CYAN}curl -s \"https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app/api/search/articles/221\" | jq .data.full_content${NC}"
        echo ""
        echo "2. フロントエンドを確認:"
        echo -e "   ${CYAN}https://seo-frontend-app-550580509369.asia-northeast1.run.app/compare/221/216${NC}"
        echo ""
        echo -e "${GREEN}✨ HTMLタグが除去されたクリーンなテキストが表示されるはずです！${NC}"
        echo ""
        
    else
        echo -e "${RED}❌ エラーが発生しました${NC}"
        echo ""
        echo -e "${RED}エラー内容:${NC}"
        echo "$MESSAGE"
        echo ""
        echo -e "${YELLOW}詳細なログを確認:${NC}"
        echo -e "   ${CYAN}gcloud functions logs read strapi-sync --region=asia-northeast1 --limit=50${NC}"
        echo ""
    fi
else
    # エラー出力を確認
    if [ -s "$TEMP_ERROR" ]; then
        echo -e "${RED}❌ リクエストエラーが発生しました${NC}"
        echo ""
        cat "$TEMP_ERROR"
        echo ""
    else
        echo -e "${RED}❌ 予期しないエラーが発生しました${NC}"
        echo ""
    fi
fi

# 一時ファイルを削除
rm -f "$TEMP_OUTPUT" "$TEMP_ERROR"

echo ""
