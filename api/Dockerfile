# Node.js 18 Alpine ベースイメージ
FROM node:18-alpine

# 必要なパッケージをインストール
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    && rm -rf /var/cache/apk/*

# 作業ディレクトリ設定
WORKDIR /app

# パッケージファイルをコピー
COPY package*.json ./

# 依存関係インストール
RUN npm ci --only=production --no-audit --no-fund && \
    npm cache clean --force

# アプリケーションファイルをコピー
COPY . .

# 非rootユーザー作成
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nextjs -u 1001

# ファイル所有権変更
RUN chown -R nextjs:nodejs /app
USER nextjs

# 環境変数設定
ENV NODE_ENV=production
ENV PORT=8080

# ポート公開
EXPOSE 8080

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:8080/health', (res) => { process.exit(res.statusCode === 200 ? 0 : 1) })"

# アプリケーション起動
CMD ["node", "app.js"]
