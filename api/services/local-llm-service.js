/**
 * ローカルLLMサービス
 * Compute Engine上のLlamaモデルとの通信を管理
 */

const winston = require("winston")
const config = require("../config/llm-providers")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class LocalLLMService {
  constructor() {
    this.config = config.providers['local-llm']
    this.computeConfig = config.compute_engine
    this.endpoint = null // 動的に設定
    this.isAvailable = false
    this.lastHealthCheck = null
  }

  /**
   * エンドポイントURLを設定
   */
  setEndpoint(externalIP) {
    this.endpoint = `http://${externalIP}:${this.config.endpoint_port}`
    logger.info(`Local LLM endpoint set to: ${this.endpoint}`)
  }

  /**
   * テキスト生成
   */
  async generateText(prompt, options = {}) {
    try {
      if (!this.endpoint) {
        throw new Error('Local LLM endpoint not set. Instance may not be running.')
      }

      const { maxOutputTokens = 4096, temperature = 0.7 } = options

      logger.info("Local LLM テキスト生成開始", {
        endpoint: this.endpoint,
        promptLength: prompt.length,
        maxOutputTokens,
        temperature,
      })

      const startTime = Date.now()

      const response = await fetch(`${this.endpoint}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'qwen2.5:7b',
          messages: [
            {
              role: 'system',
              content: 'あなたは詳細で構造化された長文記事を作成する専門ライターです。最低3000文字以上の詳細な記事を必ず書いてください。見出しを使い、具体例を交えて、読者に価値のある実用的な内容を提供してください。各セクションを十分に詳しく書き、情報量を豊富にしてください。'
            },
            {
              role: 'user',
              content: prompt + '\n\n【絶対要件】最低3000文字以上で、詳細な長文記事を必ず作成してください。マークダウン形式で見出しを使い、構造化された内容にしてください。各セクションを詳しく書き、具体例、実践方法、事例を豊富に含めてください。'
            }
          ],
          max_tokens: Math.max(maxOutputTokens, 2048),
          temperature: temperature,
          top_p: 0.9,
          stream: false,
          options: {
            num_predict: 2048,
            stop: []
          }
        }),
        timeout: this.config.timeout
      })

      if (!response.ok) {
        throw new Error(`Local LLM API error: ${response.status} ${response.statusText}`)
      }

      const result = await response.json()
      const generatedText = result.choices[0]?.message?.content || ""

      const responseTime = Date.now() - startTime

      logger.info("Local LLM テキスト生成完了", {
        responseLength: generatedText.length,
        responseTime: `${responseTime}ms`,
        tokensUsed: result.usage?.total_tokens || 'unknown'
      })

      return generatedText

    } catch (error) {
      logger.error("Local LLM テキスト生成でエラー", {
        error: error.message,
        endpoint: this.endpoint,
        promptLength: prompt.length,
      })
      throw error
    }
  }

  /**
   * ヘルスチェック
   */
  async checkHealth() {
    try {
      if (!this.endpoint) {
        return { status: 'unavailable', message: 'No endpoint configured' }
      }

      const response = await fetch(`${this.endpoint}/health`, {
        method: 'GET',
        timeout: 10000 // 10秒
      })

      if (response.ok) {
        const healthData = await response.json()
        this.isAvailable = true
        this.lastHealthCheck = new Date()

        return {
          status: 'available',
          message: 'Local LLM is healthy',
          endpoint: this.endpoint,
          last_check: this.lastHealthCheck,
          model_info: healthData
        }
      } else {
        this.isAvailable = false
        return {
          status: 'unhealthy',
          message: `Health check failed: ${response.status}`,
          endpoint: this.endpoint
        }
      }

    } catch (error) {
      this.isAvailable = false
      logger.error("Local LLM ヘルスチェックエラー", { error: error.message })

      return {
        status: 'unavailable',
        message: error.message,
        endpoint: this.endpoint
      }
    }
  }

  /**
   * モデル情報取得
   */
  async getModelInfo() {
    try {
      if (!this.endpoint) {
        throw new Error('Endpoint not configured')
      }

      const response = await fetch(`${this.endpoint}/v1/models`, {
        method: 'GET',
        timeout: 10000
      })

      if (response.ok) {
        return await response.json()
      } else {
        throw new Error(`Models API error: ${response.status}`)
      }

    } catch (error) {
      logger.error("モデル情報取得エラー", { error: error.message })
      return { error: error.message }
    }
  }

  /**
   * 統計情報取得
   */
  getStats() {
    return {
      provider: 'local-llm',
      endpoint: this.endpoint,
      is_available: this.isAvailable,
      last_health_check: this.lastHealthCheck,
      config: {
        cost_per_request: this.config.cost_per_request,
        timeout: this.config.timeout,
        max_tokens: this.config.max_tokens
      }
    }
  }

  /**
   * フォールバック対応テキスト生成
   */
  generateFallbackText(prompt) {
    logger.warn("ローカルLLMフォールバック応答を生成")

    if (prompt.includes("統合")) {
      return `# 記事統合提案

この2つの記事の統合について詳細な分析を行いました。

## 統合の推奨理由
- 内容の重複が確認され、SEO効果の向上が期待できます
- ユーザーエクスペリエンスの改善に貢献します

## 次のステップ
1. 詳細な内容分析を実施
2. 統合戦略の策定
3. 実装計画の作成

*注: このはローカルLLMが利用できない場合の簡易応答です。*`
    }

    return "記事の統合について詳細な分析を行い、適切な提案を提供いたします。現在ローカルLLMが利用できないため、簡易応答となっております。"
  }
}

module.exports = LocalLLMService