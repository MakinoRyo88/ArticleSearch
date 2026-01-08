# =========================================
# ステージ1: ビルダー (Builder)
# アプリケーションのビルドに必要なものを全てインストール
# =========================================
FROM node:20-alpine AS builder

WORKDIR /app

# ビルド時に使用する環境変数をARGで受け取る
ARG API_BASE_URL
ARG NEXT_PUBLIC_API_BASE_URL

# ENVで設定し、ビルドプロセス（next build）で使えるようにする
ENV API_BASE_URL=${API_BASE_URL}
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL:-${API_BASE_URL}}

# 依存関係をコピーしてインストール
COPY package.json package-lock.json* ./
# npm ci は package-lock.json に基づいて厳密にインストールする
RUN npm ci

# 全てのソースコードをコピー（.dockerignoreで除外されるものを除く）
COPY . .

# アプリケーションをビルド
RUN npm run build


# =========================================
# ステージ2: ランナー (Runner)
# 実際にアプリケーションを実行する最小限の環境
# =========================================
FROM node:20-alpine

WORKDIR /app

# 本番環境で必要な環境変数を設定
# （Cloud Runの環境変数設定で上書きされる）
ENV NODE_ENV=production
ENV PORT=8080

# ビルダーから本番実行に必要なファイルのみをコピー
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json

# Next.jsサーバーを起動するポートを指定
EXPOSE 8080

# コンテナ起動時にNext.jsサーバーを起動する
CMD ["npm", "start"]