/**
 * SEO最適化リアルタイム分析API
 * Cloud Run上で動作するExpress.jsアプリケーション
 */

const express = require("express")
const cors = require("cors")
const helmet = require("helmet")
const compression = require("compression")
const rateLimit = require("express-rate-limit")
const winston = require("winston")

// 環境変数の読み込み（オプショナル）
try {
  require("dotenv").config()
} catch (error) {
  console.log("dotenv not available, using environment variables")
}

// ルーターのインポート
const searchRoutes = require("./routes/search")
const similarityRoutes = require("./routes/similarity")
const chunkSimilarityRoutes = require("./routes/chunk-similarity")
const recommendationsRoutes = require("./routes/recommendations")
const explanationsRoutes = require("./routes/explanations")
const monitoringRoutes = require("./routes/monitoring")
const instancesRoutes = require("./routes/instances")

// ログ設定
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json(),
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(winston.format.colorize(), winston.format.simple()),
    }),
  ],
})

// Express アプリケーション初期化
const app = express()
app.set('trust proxy', 1)
const PORT = process.env.PORT || 8080

logger.info("アプリケーション初期化開始", { port: PORT })

// セキュリティミドルウェア
app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        scriptSrc: ["'self'"],
        imgSrc: ["'self'", "data:", "https:"],
      },
    },
  }),
)

// CORS設定 - より寛容な設定で一時的に解決
app.use(
  cors({
    origin: true, // すべてのオリジンを許可（本番では適切に設定する）
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
    credentials: true,
    optionsSuccessStatus: 200,
    preflightContinue: false
  }),
)

// プリフライトリクエストを明示的に処理
app.options('*', cors())

// 圧縮
app.use(compression())

// レート制限
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15分
  max: Number.parseInt(process.env.RATE_LIMIT_MAX) || 1000, // リクエスト数制限
  message: {
    error: "Too many requests from this IP, please try again later.",
    retryAfter: "15 minutes",
  },
  standardHeaders: true,
  legacyHeaders: false,
})
app.use("/api/", limiter)

// ボディパーサー
app.use(express.json({ limit: "10mb" }))
app.use(express.urlencoded({ extended: true, limit: "10mb" }))

// リクエストログ
app.use((req, res, next) => {
  const start = Date.now()

  res.on("finish", () => {
    const duration = Date.now() - start
    logger.info("HTTP Request", {
      method: req.method,
      url: req.url,
      status: res.statusCode,
      duration: `${duration}ms`,
      userAgent: req.get("User-Agent"),
      ip: req.ip,
    })
  })

  next()
})

// ヘルスチェック
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    version: process.env.npm_package_version || "1.0.0",
    environment: process.env.NODE_ENV || "development",
  })
})

// ルートエンドポイント
app.get("/", (req, res) => {
  res.json({
    name: "SEO最適化リアルタイム分析API",
    version: "1.0.0",
    status: "running",
    timestamp: new Date().toISOString(),
  })
})

// API情報
app.get("/api", (req, res) => {
  res.json({
    name: "SEO最適化リアルタイム分析API",
    version: "1.0.0",
    description: "Strapi CMS記事データの類似度分析とSEO統合提案",
    endpoints: {
      search: "/api/search - 記事検索・一覧取得",
      similarity: "/api/similarity - 記事全体の類似度計算",
      chunkSimilarity: "/api/chunk-similarity - チャンクベース類似度計算（推奨）",
      recommendations: "/api/recommendations - 統合提案生成",
      explanations: "/api/explanations - Gemini説明文生成",
    },
    documentation: "https://github.com/your-org/seo-optimization-system",
  })
})

// APIルート
try {
  app.use("/api/search", searchRoutes)
  app.use("/api/similarity", similarityRoutes)
  app.use("/api/chunk-similarity", chunkSimilarityRoutes)
  app.use("/api/recommendations", recommendationsRoutes)
  app.use("/api/explanations", explanationsRoutes)
  app.use("/api/monitoring", monitoringRoutes)
  app.use("/api/instances", instancesRoutes)
  logger.info("APIルート初期化完了")
} catch (error) {
  logger.error("APIルート初期化でエラー", { error: error.message })
}

// 404エラーハンドリング
app.use("*", (req, res) => {
  res.status(404).json({
    error: "Endpoint not found",
    message: `The requested endpoint ${req.method} ${req.originalUrl} was not found.`,
    availableEndpoints: [
      "GET /health",
      "GET /api",
      "GET /api/search/articles",
      "GET /api/search/courses",
      "GET /api/similarity/:articleId",
      "POST /api/similarity/:articleId/analyze",
      "POST /api/recommendations/generate",
      "POST /api/explanations/generate",
    ],
  })
})

// グローバルエラーハンドリング
app.use((error, req, res, next) => {
  logger.error("Unhandled Error", {
    error: error.message,
    stack: error.stack,
    url: req.url,
    method: req.method,
    body: req.body,
  })

  // 本番環境では詳細なエラー情報を隠す
  const isDevelopment = process.env.NODE_ENV === "development"

  res.status(error.status || 500).json({
    error: "Internal Server Error",
    message: isDevelopment ? error.message : "Something went wrong",
    ...(isDevelopment && { stack: error.stack }),
    timestamp: new Date().toISOString(),
    requestId: req.headers["x-request-id"] || "unknown",
  })
})

// プロセス終了時の処理
process.on("SIGTERM", () => {
  logger.info("SIGTERM received, shutting down gracefully")
  process.exit(0)
})

process.on("SIGINT", () => {
  logger.info("SIGINT received, shutting down gracefully")
  process.exit(0)
})

// 未処理の例外をキャッチ
process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception", { error: error.message, stack: error.stack })
  process.exit(1)
})

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection", { reason, promise })
  process.exit(1)
})

// サーバー起動
const server = app.listen(PORT, "0.0.0.0", () => {
  logger.info(`🚀 SEO最適化リアルタイム分析API started on port ${PORT}`, {
    port: PORT,
    environment: process.env.NODE_ENV || "development",
    timestamp: new Date().toISOString(),
  })
})

// サーバーのタイムアウト設定
server.timeout = 600000 // 10分（ローカルLLM対応）

module.exports = app
