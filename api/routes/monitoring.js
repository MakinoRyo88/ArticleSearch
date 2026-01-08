/**
 * 監視・メトリクス API
 * Phase 3: 基本メトリクス収集システム
 */

const express = require("express")
const MetricsCollector = require("../services/metrics-collector")
const { formatResponse, formatError } = require("../utils/response-formatter")
const winston = require("winston")

const router = express.Router()
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

const metricsCollector = new MetricsCollector()

/**
 * GET /api/monitoring/usage-statistics
 * 使用統計取得
 */
router.get("/usage-statistics", async (req, res) => {
  try {
    const { timeRange = '24h' } = req.query

    logger.info("使用統計取得開始", { timeRange })

    const statistics = await metricsCollector.getUsageStatistics(timeRange)

    logger.info("使用統計取得完了", {
      timeRange,
      recordCount: statistics.length
    })

    res.json(formatResponse({
      time_range: timeRange,
      statistics,
      total_providers: statistics.length,
      generated_at: new Date().toISOString()
    }, "Usage statistics retrieved successfully"))

  } catch (error) {
    logger.error("使用統計取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get usage statistics", error.message))
  }
})

/**
 * GET /api/monitoring/provider-comparison
 * プロバイダー比較データ取得
 */
router.get("/provider-comparison", async (req, res) => {
  try {
    const { timeRange = '24h' } = req.query

    logger.info("プロバイダー比較データ取得開始", { timeRange })

    const comparison = await metricsCollector.getProviderComparison(timeRange)

    // コスト削減効果計算
    const vertexAI = comparison.find(p => p.provider === 'vertex-ai')
    const localLLM = comparison.find(p => p.provider === 'local-llm')
    let costSavings = null

    if (vertexAI && localLLM) {
      const vertexAICost = vertexAI.total_cost || 0
      const localLLMCost = localLLM.total_cost || 0
      const totalRequests = (vertexAI.total_requests || 0) + (localLLM.total_requests || 0)

      if (totalRequests > 0) {
        const estimatedVertexAICost = totalRequests * (vertexAI.avg_cost_per_request || 0)
        const actualTotalCost = vertexAICost + localLLMCost
        const savingsAmount = estimatedVertexAICost - actualTotalCost
        const savingsPercentage = estimatedVertexAICost > 0 ? (savingsAmount / estimatedVertexAICost) * 100 : 0

        costSavings = {
          estimated_vertex_ai_only_cost: estimatedVertexAICost,
          actual_total_cost: actualTotalCost,
          savings_amount: savingsAmount,
          savings_percentage: Math.round(savingsPercentage * 100) / 100
        }
      }
    }

    logger.info("プロバイダー比較データ取得完了", {
      timeRange,
      providersCount: comparison.length
    })

    res.json(formatResponse({
      time_range: timeRange,
      providers: comparison,
      cost_savings: costSavings,
      generated_at: new Date().toISOString()
    }, "Provider comparison data retrieved successfully"))

  } catch (error) {
    logger.error("プロバイダー比較データ取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get provider comparison", error.message))
  }
})

/**
 * GET /api/monitoring/health
 * 監視システムヘルスチェック
 */
router.get("/health", async (req, res) => {
  try {
    // 基本的なシステム状態確認
    const healthStatus = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      services: {
        metrics_collector: 'operational',
        bigquery: 'operational'
      },
      uptime: process.uptime(),
      memory: {
        used: process.memoryUsage().heapUsed / 1024 / 1024,
        total: process.memoryUsage().heapTotal / 1024 / 1024
      }
    }

    // 簡単なデータベース接続テスト
    try {
      await metricsCollector.getUsageStatistics('1h')
      healthStatus.services.bigquery = 'operational'
    } catch (error) {
      healthStatus.services.bigquery = 'degraded'
      healthStatus.status = 'degraded'
    }

    logger.info("監視システムヘルスチェック完了", { status: healthStatus.status })

    const statusCode = healthStatus.status === 'healthy' ? 200 : 503
    res.status(statusCode).json(formatResponse(healthStatus, "Health check completed"))

  } catch (error) {
    logger.error("ヘルスチェックでエラー", { error: error.message })
    res.status(503).json(formatError("Health check failed", error.message))
  }
})

/**
 * POST /api/monitoring/initialize
 * 監視テーブル初期化（開発時のみ）
 */
router.post("/initialize", async (req, res) => {
  try {
    if (process.env.NODE_ENV === 'production') {
      return res.status(403).json(formatError("Table initialization not allowed in production"))
    }

    logger.info("監視テーブル初期化開始")

    await metricsCollector.initializeTables()

    logger.info("監視テーブル初期化完了")

    res.json(formatResponse({
      initialized: true,
      timestamp: new Date().toISOString()
    }, "Monitoring tables initialized successfully"))

  } catch (error) {
    logger.error("テーブル初期化でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to initialize tables", error.message))
  }
})

/**
 * GET /api/monitoring/summary
 * 監視ダッシュボード用サマリー
 */
router.get("/summary", async (req, res) => {
  try {
    const { timeRange = '24h' } = req.query

    logger.info("監視サマリー取得開始", { timeRange })

    // 並行してデータ取得
    const [statistics, comparison] = await Promise.all([
      metricsCollector.getUsageStatistics(timeRange),
      metricsCollector.getProviderComparison(timeRange)
    ])

    // サマリー計算
    const totalRequests = statistics.reduce((sum, stat) => sum + (stat.total_requests || 0), 0)
    const totalCost = statistics.reduce((sum, stat) => sum + (stat.total_cost || 0), 0)
    const avgResponseTime = statistics.reduce((sum, stat, _, arr) =>
      sum + (stat.avg_response_time || 0) / arr.length, 0)

    const summary = {
      time_range: timeRange,
      overview: {
        total_requests: totalRequests,
        total_cost: Math.round(totalCost * 100) / 100,
        average_response_time: Math.round(avgResponseTime),
        active_providers: statistics.length
      },
      providers: comparison,
      generated_at: new Date().toISOString()
    }

    logger.info("監視サマリー取得完了", {
      timeRange,
      totalRequests,
      totalCost
    })

    res.json(formatResponse(summary, "Monitoring summary retrieved successfully"))

  } catch (error) {
    logger.error("監視サマリー取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get monitoring summary", error.message))
  }
})

module.exports = router