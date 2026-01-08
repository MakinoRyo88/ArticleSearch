/**
 * BigQuery サービス
 * データベース操作の共通処理
 */

const { BigQuery } = require("@google-cloud/bigquery")
const winston = require("winston")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class BigQueryService {
  constructor() {
    this.projectId = process.env.GCP_PROJECT || "seo-optimize-464208"
    this.datasetId = "content_analysis"
    this.client = new BigQuery({
      projectId: this.projectId,
      location: "asia-northeast1",
    })

    logger.info("BigQuery サービス初期化", { projectId: this.projectId })
  }

  /**
   * クエリを実行して結果を返す
   * parametersは配列（POSITIONAL）またはオブジェクト（NAMED）に対応
   */
  async executeQuery(query, parameters = [], extraOptions = {}) {
    try {
      const isNamed = typeof parameters === 'object' && !Array.isArray(parameters) && Object.keys(parameters).length > 0;
      const isPositional = Array.isArray(parameters) && parameters.length > 0;

      const options = {
        query: query,
        location: "asia-northeast1",
        ...extraOptions // 追加オプションを統合
      }

      if (isNamed) {
        options.params = parameters;
        options.parameterMode = "NAMED";
      } else if (isPositional) {
        options.params = parameters;
        options.parameterMode = "POSITIONAL";
      }

      logger.info("BigQuery クエリ実行開始", {
        queryLength: query.length,
        parameterMode: options.parameterMode || 'NONE',
        paramCount: isNamed ? Object.keys(parameters).length : (isPositional ? parameters.length : 0),
      })

      const [job] = await this.client.createQueryJob(options)
      const [rows] = await job.getQueryResults()

      logger.info("BigQuery クエリ実行完了", {
        rowCount: rows.length,
        jobId: job.id,
      })

      return rows
    } catch (error) {
      logger.error("BigQuery クエリ実行でエラー", {
        error: error.message,
        query: query.substring(0, 200) + "...",
        parameters: parameters,
      })
      throw error
    }
  }

  /**
   * ストリーミング挿入
   */
  async insertRows(tableName, rows) {
    try {
      const table = this.client.dataset(this.datasetId).table(tableName)
      await table.insert(rows)

      logger.info("BigQuery データ挿入完了", {
        tableName,
        rowCount: rows.length,
      })
    } catch (error) {
      logger.error("BigQuery データ挿入でエラー", {
        error: error.message,
        tableName,
        rowCount: rows.length,
      })
      throw error
    }
  }

  /**
   * テーブル存在確認
   */
  async tableExists(tableName) {
    try {
      const table = this.client.dataset(this.datasetId).table(tableName)
      const [exists] = await table.exists()
      return exists
    } catch (error) {
      logger.info("テーブル存在確認でエラー", { error: error.message, tableName })
      return false
    }
  }

  /**
   * クエリコスト見積もり
   */
  async estimateQueryCost(query) {
    try {
      const options = {
        query: query,
        dryRun: true,
        location: "asia-northeast1",
      }

      const [job] = await this.client.createQueryJob(options)
      const bytesProcessed = job.metadata.statistics.totalBytesProcessed

      logger.info("クエリコスト見積もり", {
        bytesProcessed: Number.parseInt(bytesProcessed).toLocaleString(),
      })

      return Number.parseInt(bytesProcessed)
    } catch (error) {
      logger.info("クエリコスト見積もりでエラー", { error: error.message })
      return null
    }
  }
}

module.exports = BigQueryService
