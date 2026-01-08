# SEO記事統合提案システム フロントエンド

SEO最適化のための記事統合提案システムのフロントエンドアプリケーション（Next.js 14 + TypeScript）

## 🚀 デプロイ方法

### Cloud Runへのデプロイ

```bash
./deploy.sh
```

**デプロイの流れ**:
1. Cloud Buildでビルド（約5-10分）
2. Artifact Registryにイメージをプッシュ
3. Cloud Runサービスを自動デプロイ

**必要な権限**:
- Cloud Build Editor
- Cloud Run Admin
- Artifact Registry Writer

### 環境変数

本番環境では以下の環境変数が自動設定されます：

- `API_BASE_URL`: バックエンドAPIのURL
- `NEXT_PUBLIC_API_BASE_URL`: クライアント側で使用するAPI URL
- `NODE_ENV`: production
- `PORT`: 8080

## 🛠️ ローカル開発

### セットアップ

```bash
# 依存関係のインストール
npm install

# 環境変数の設定
cp .env .env.local
# .env.localを編集してAPI URLを設定

# 開発サーバー起動
npm run dev
```

開発サーバー: [http://localhost:3000](http://localhost:3000)

### ビルド確認

```bash
npm run build
npm run start
```

## 📊 主要機能

- **記事検索・一覧**: タイトル、講座名での検索
- **類似記事分析**: チャンクベース類似度検索
- **記事比較**: サイドバイサイド比較画面
- **統合提案**: AI による統合推奨とアクション提案

## 🔧 技術スタック

- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **UI Components**: Radix UI
- **Charts**: Recharts
- **Deployment**: Google Cloud Run

## 📁 プロジェクト構造

```
seo-frontend-app/
├── src/
│   ├── app/              # Next.js App Router
│   │   ├── page.tsx      # トップページ（記事一覧）
│   │   ├── articles/     # 記事関連ページ
│   │   │   └── [id]/
│   │   │       ├── analyze/  # 類似記事分析
│   │   │       └── compare/  # 記事比較
│   ├── components/       # UIコンポーネント
│   └── lib/
│       ├── api.ts        # APIクライアント
│       └── utils.ts      # ユーティリティ関数
├── public/               # 静的ファイル
├── Dockerfile            # Docker設定
├── cloudbuild.yaml       # Cloud Build設定
└── deploy.sh             # デプロイスクリプト
```

## 🔗 関連リソース

- **Backend API**: https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app
- **API Documentation**: `/API_USAGE_GUIDE.md`
- **Project Root**: `/Users/makinoaya/work/rag/ArticleSearch/`
