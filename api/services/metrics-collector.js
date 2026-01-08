/**
 * メトリクス収集サービス
 * LLMプロバイダーの使用状況とパフォーマンスメトリクスを収集・記録
 */

const BigQueryService = require("./bigquery-service")
const winston = require("winston")
const { v4: uuidv4 } = require('uuid')

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class MetricsCollector {
  constructor() {
    this.bigQueryService = new BigQueryService()
    this.sessionId = null
    this.startTime = null
  }

  /**
   * セッション開始
   */
  startSession() {
    this.sessionId = uuidv4()
    this.startTime = Date.now()
    logger.info("メトリクス収集セッション開始", { sessionId: this.sessionId })
    return this.sessionId
  }

  /**
   * 使用ログ記録
   */
  async recordUsage({
    provider,
    requestType = 'text_generation',
    responseTime,
    tokensUsed = 0,
    costEstimate = 0,
    success = true,
    errorMessage = null,
    additionalMetrics = {}
  }) {
    try {
      const usageLog = {
        timestamp: new Date(),
        session_id: this.sessionId || 'unknown',
        provider,
        request_type: requestType,
        response_time_ms: responseTime,
        tokens_used: tokensUsed,
        cost_estimate: costEstimate,
        success,
        error_message: errorMessage,
        ...additionalMetrics
      }

      // 一時的にBigQuery記録を無効化（Vertex AIテストのため）
      logger.info("使用ログ記録（BigQuery無効化中）", {
        provider,
        responseTime,
        success,
        costEstimate
      })

      return usageLog

    } catch (error) {
      logger.error("使用ログ記録エラー", { error: error.message })
      // エラーでも処理を継続
      return null
    }
  }

  /**
   * パフォーマンスメトリクス記録
   */
  async recordPerformanceMetric({
    metricName,
    metricValue,
    provider,
    region = 'asia-northeast1'
  }) {
    try {
      const metric = {
        timestamp: new Date(),
        metric_name: metricName,
        metric_value: metricValue,
        provider,
        region
      }

      // BigQueryにパフォーマンスメトリクスを挿入
      await this.insertPerformanceMetric(metric)

      logger.info("パフォーマンスメトリクス記録完了", {
        metricName,
        metricValue,
        provider
      })

      return metric

    } catch (error) {
      logger.error("パフォーマンスメトリクス記録エラー", { error: error.message })
      throw error
    }
  }

  /**
   * 使用ログをBigQueryに挿入
   */
  async insertUsageLog(usageLog) {
    const query = `
      INSERT INTO \`${process.env.GCP_PROJECT}.monitoring.usage_logs\`
      (timestamp, session_id, provider, request_type, response_time_ms, tokens_used, cost_estimate, success, error_message)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `

    const params = [
      usageLog.timestamp,
      usageLog.session_id,
      usageLog.provider,
      usageLog.request_type,
      usageLog.response_time_ms,
      usageLog.tokens_used,
      usageLog.cost_estimate,
      Boolean(usageLog.success), // 明示的にBoolean型に変換
      usageLog.error_message || null
    ]

    // BigQuery用のパラメータオプション
    const queryOptions = {
      params: params,
      parameterMode: 'POSITIONAL',
      types: [
        { type: 'TIMESTAMP' },
        { type: 'STRING' },
        { type: 'STRING' },
        { type: 'STRING' },
        { type: 'INT64' },
        { type: 'INT64' },
        { type: 'FLOAT64' },
        { type: 'BOOL' }, // BOOLEAN -> BOOL
        { type: 'STRING' }
      ]
    }

    await this.bigQueryService.executeQuery(query, params, queryOptions)
  }

  /**
   * パフォーマンスメトリクスをBigQueryに挿入
   */
  async insertPerformanceMetric(metric) {
    const query = `
      INSERT INTO \`${process.env.GCP_PROJECT}.monitoring.performance_metrics\`
      (timestamp, metric_name, metric_value, provider, region)
      VALUES (?, ?, ?, ?, ?)
    `

    const params = [
      metric.timestamp,
      metric.metric_name,
      metric.metric_value,
      metric.provider,
      metric.region
    ]

    await this.bigQueryService.executeQuery(query, params)
  }

  /**
   * メトリクス統計取得
   */
  async getUsageStatistics(timeRange = '24h') {
    try {
      const timeCondition = this.getTimeCondition(timeRange)

      const query = `
        SELECT
          provider,
          COUNT(*) as total_requests,
          AVG(response_time_ms) as avg_response_time,
          SUM(cost_estimate) as total_cost,
          COUNTIF(success = true) as successful_requests,
          COUNTIF(success = false) as failed_requests
        FROM \`${process.env.GCP_PROJECT}.monitoring.usage_logs\`
        WHERE timestamp >= ${timeCondition}
        GROUP BY provider
        ORDER BY total_requests DESC
      `

      const results = await this.bigQueryService.executeQuery(query)

      logger.info("使用統計取得完了", { timeRange, recordCount: results.length })

      return results

    } catch (error) {
      logger.error("使用統計取得エラー", { error: error.message })
      throw error
    }
  }

  /**
   * プロバイダー比較データ取得
   */
  async getProviderComparison(timeRange = '24h') {
    try {
      const timeCondition = this.getTimeCondition(timeRange)

      const query = `
        SELECT
          provider,
          AVG(response_time_ms) as avg_response_time,
          MIN(response_time_ms) as min_response_time,
          MAX(response_time_ms) as max_response_time,
          APPROX_QUANTILES(response_time_ms, 100)[OFFSET(95)] as p95_response_time,
          AVG(cost_estimate) as avg_cost_per_request,
          SUM(cost_estimate) as total_cost,
          COUNT(*) as total_requests,
          COUNTIF(success = true) / COUNT(*) * 100 as success_rate
        FROM \`${process.env.GCP_PROJECT}.monitoring.usage_logs\`
        WHERE timestamp >= ${timeCondition}
        GROUP BY provider
        ORDER BY total_requests DESC
      `

      const results = await this.bigQueryService.executeQuery(query)

      logger.info("プロバイダー比較データ取得完了", { timeRange, recordCount: results.length })

      return results

    } catch (error) {
      logger.error("プロバイダー比較データ取得エラー", { error: error.message })
      throw error
    }
  }

  /**
   * 時間条件生成ヘルパー
   */
  getTimeCondition(timeRange) {
    const intervals = {
      '1h': 'TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)',
      '24h': 'TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)',
      '7d': 'TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)',
      '30d': 'TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)'
    }

    return intervals[timeRange] || intervals['24h']
  }

  /**
   * テーブル初期化（開発時のみ）
   */
  async initializeTables() {
    if (process.env.NODE_ENV === 'production') {
      logger.warn("本番環境でのテーブル初期化はスキップされます")
      return
    }

    try {
      // まずデータセットを作成
      await this.createDatasetIfNotExists()

      // 使用ログテーブル作成
      const createUsageLogsQuery = `
        CREATE TABLE IF NOT EXISTS \`${process.env.GCP_PROJECT}.monitoring.usage_logs\` (
          timestamp TIMESTAMP,
          session_id STRING,
          provider STRING,
          request_type STRING,
          response_time_ms INT64,
          tokens_used INT64,
          cost_estimate FLOAT64,
          success BOOLEAN,
          error_message STRING
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY provider, request_type
      `

      // パフォーマンスメトリクステーブル作成
      const createMetricsQuery = `
        CREATE TABLE IF NOT EXISTS \`${process.env.GCP_PROJECT}.monitoring.performance_metrics\` (
          timestamp TIMESTAMP,
          metric_name STRING,
          metric_value FLOAT64,
          provider STRING,
          region STRING
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY metric_name, provider
      `

      await this.bigQueryService.executeQuery(createUsageLogsQuery)
      await this.bigQueryService.executeQuery(createMetricsQuery)

      logger.info("監視テーブル初期化完了")

    } catch (error) {
      logger.error("テーブル初期化エラー", { error: error.message })
      throw error
    }
  }

  /**
   * 監視データセット作成
   */
  async createDatasetIfNotExists() {
    try {
      const { BigQuery } = require('@google-cloud/bigquery')
      const bigquery = new BigQuery({ projectId: process.env.GCP_PROJECT })

      const datasetId = 'monitoring'
      const dataset = bigquery.dataset(datasetId)

      const [exists] = await dataset.exists()
      if (!exists) {
        await dataset.create({
          location: 'asia-northeast1',
          description: 'LLM monitoring and metrics data'
        })
        logger.info(`監視データセット作成完了: ${datasetId}`)
      } else {
        logger.info(`監視データセット既存: ${datasetId}`)
      }
    } catch (error) {
      logger.error("データセット作成エラー", { error: error.message })
      throw error
    }
  }
}

module.exports = MetricsCollector