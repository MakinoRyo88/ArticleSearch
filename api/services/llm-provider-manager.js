/**
 * LLMプロバイダー管理サービス
 * Vertex AI と ローカルLLM の統合管理
 */

const VertexAIService = require("./vertex-ai-service")
const LocalLLMService = require("./local-llm-service")
const MetricsCollector = require("./metrics-collector")
const config = require("../config/llm-providers")
const winston = require("winston")
const axios = require("axios")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class LLMProviderManager {
  constructor() {
    this.vertexAIService = new VertexAIService()
    this.localLLMService = new LocalLLMService()
    this.metricsCollector = new MetricsCollector()
    this.autoScaler = null // 遅延初期化
    this.providers = config.providers
    this.cloudFunctions = config.cloud_functions

    // メトリクス収集セッション開始
    this.metricsCollector.startSession()
  }

  /**
   * AutoScaler の遅延初期化
   */
  getAutoScaler() {
    if (!this.autoScaler) {
      const AutoScaler = require("./auto-scaler")
      this.autoScaler = new AutoScaler(this.localLLMService)
    }
    return this.autoScaler
  }

  /**
   * メインのテキスト生成エントリーポイント
   */
  async generateText(prompt, provider = null, options = {}) {
    // プロバイダー決定ロジック
    const selectedProvider = provider || config.default_provider

    logger.info("LLMプロバイダー管理: テキスト生成開始", {
      selectedProvider,
      promptLength: prompt.length,
      fallbackEnabled: config.auto_selection_rules.fallback_on_failure
    })

    try {
      // プロバイダーの有効性チェック
      if (!this.providers[selectedProvider] || !this.providers[selectedProvider].enabled) {
        logger.warn(`プロバイダー ${selectedProvider} は無効です。Vertex AIにフォールバック`)
        return await this.generateWithVertexAI(prompt, options)
      }

      switch (selectedProvider) {
        case 'vertex-ai':
          return await this.generateWithVertexAI(prompt, options)

        case 'local-llm':
          return await this.generateWithLocalLLM(prompt, options)

        default:
          throw new Error(`Unknown provider: ${selectedProvider}`)
      }

    } catch (error) {
      logger.error(`${selectedProvider} での生成に失敗`, { error: error.message, stack: error.stack })

      // 自動フォールバックを有効化（特にタイムアウト・接続エラー時）
      if (config.auto_selection_rules.fallback_on_failure && selectedProvider !== config.fallback_provider) {
        const shouldFallback = error.message.includes('timeout') ||
                              error.message.includes('LOCAL_LLM_UNAVAILABLE') ||
                              error.message.includes('AbortError') ||
                              error.name === 'AbortError'

        if (shouldFallback) {
          logger.info(`フォールバック実行: ${selectedProvider} → ${config.fallback_provider}`, {
            originalError: error.message,
            reason: 'timeout_or_connection_error'
          })
          return await this.generateWithFallback(prompt, selectedProvider, options)
        }
      }

      throw error
    }
  }

  /**
   * Vertex AI でテキスト生成
   */
  async generateWithVertexAI(prompt, options = {}) {
    const startTime = Date.now()

    try {
      const result = await this.vertexAIService.generateText(prompt, options)
      const responseTime = Date.now() - startTime
      const cost = this.providers['vertex-ai'].cost_per_request

      // メトリクス記録
      await this.metricsCollector.recordUsage({
        provider: 'vertex-ai',
        requestType: 'text_generation',
        responseTime,
        tokensUsed: result.length / 4, // 概算トークン数
        costEstimate: cost,
        success: true,
        additionalMetrics: {
          prompt_length: prompt.length,
          response_length: result.length
        }
      })

      logger.info("Vertex AI生成完了", {
        provider: 'vertex-ai',
        responseTime,
        cost
      })

      return {
        content: result,
        provider: 'vertex-ai',
        cost,
        response_time: responseTime
      }

    } catch (error) {
      const responseTime = Date.now() - startTime

      // エラーメトリクス記録
      await this.metricsCollector.recordUsage({
        provider: 'vertex-ai',
        requestType: 'text_generation',
        responseTime,
        tokensUsed: 0,
        costEstimate: 0,
        success: false,
        errorMessage: error.message,
        additionalMetrics: {
          prompt_length: prompt.length
        }
      })

      logger.error("Vertex AI生成エラー", { error: error.message })
      throw error
    }
  }

  /**
   * インスタンスの外部IPアドレスを動的取得
   */
  async getInstanceIP() {
    try {
      const { InstancesClient } = require('@google-cloud/compute')
      const computeClient = new InstancesClient()

      const INSTANCE_CONFIG = {
        name: 'llm-gpu-instance',
        zone: 'asia-northeast1-c',
        project: 'seo-optimize-464208'
      }

      const [instance] = await computeClient.get({
        project: INSTANCE_CONFIG.project,
        zone: INSTANCE_CONFIG.zone,
        instance: INSTANCE_CONFIG.name,
      })

      // インスタンスが実行中で外部IPが割り当てられている場合
      if (instance.status === 'RUNNING' &&
          instance.networkInterfaces &&
          instance.networkInterfaces[0] &&
          instance.networkInterfaces[0].accessConfigs &&
          instance.networkInterfaces[0].accessConfigs[0]) {

        const externalIP = instance.networkInterfaces[0].accessConfigs[0].natIP
        logger.info('インスタンスIP取得成功', {
          instanceName: INSTANCE_CONFIG.name,
          status: instance.status,
          externalIP
        })
        return externalIP
      }

      logger.warn('インスタンスIPが利用できません', {
        instanceName: INSTANCE_CONFIG.name,
        status: instance.status,
        hasNetworkInterface: !!instance.networkInterfaces?.[0],
        hasAccessConfig: !!instance.networkInterfaces?.[0]?.accessConfigs?.[0]
      })
      return null

    } catch (error) {
      logger.error('インスタンスIP取得エラー', {
        error: error.message,
        code: error.code,
        stack: error.stack
      })
      return null
    }
  }

  /**
   * ローカルLLM でテキスト生成（直接Ollama接続）
   */
  async generateWithLocalLLM(prompt, options = {}) {
    const startTime = Date.now()

    try {
      const { maxOutputTokens = 15000, temperature = 0.7, timeout = 180000 } = options // L4 GPU超長文処理（15Kトークン、3分タイムアウト）

      // 動的にインスタンスのIPアドレスを取得
      const instanceIP = await this.getInstanceIP()
      if (!instanceIP) {
        throw new Error('LOCAL_LLM_UNAVAILABLE: Unable to get instance IP address')
      }
      const ollamaEndpoint = `http://${instanceIP}:11434` // 動的IP取得

      logger.info("ローカルLLM（Ollama直接接続）テキスト生成開始", {
        endpoint: ollamaEndpoint,
        promptLength: prompt.length,
        maxOutputTokens,
        temperature,
        timeout
      })

      try {
        // axiosを使用してより安定したHTTP接続
        const response = await axios.post(`${ollamaEndpoint}/api/generate`, {
          model: 'qwen2.5:7b-instruct',
          prompt: prompt,
          stream: false,
          options: {
            num_predict: maxOutputTokens,
            temperature: temperature,
            repeat_penalty: 1.1,
            top_k: 40,
            top_p: 0.9,
            stop: []
          }
        }, {
          timeout: timeout,
          headers: {
            'Content-Type': 'application/json'
          },
          validateStatus: function (status) {
            return status >= 200 && status < 300
          }
        })

        const result = response.data
        const generatedContent = result.response || ''

        const responseTime = Date.now() - startTime

        logger.info("ローカルLLM（Ollama直接接続）生成完了", {
          provider: 'local-llm',
          responseTime,
          cost: (this.providers?.['local-llm'] || config.providers['local-llm'])?.cost_per_request || 5,
          responseLength: generatedContent.length,
          tokens_evaluated: result.eval_count || 'unknown',
          http_status: response.status
        })

        return {
          content: generatedContent,
          provider: 'local-llm',
          cost: (this.providers?.['local-llm'] || config.providers['local-llm'])?.cost_per_request || 5,
          response_time: responseTime,
          ollama_info: {
            direct_connection: true,
            endpoint: ollamaEndpoint,
            model: 'qwen2.5:7b-instruct',
            eval_count: result.eval_count,
            eval_duration: result.eval_duration
          }
        }

      } catch (axiosError) {
        if (axiosError.code === 'ECONNABORTED') {
          throw new Error(`LOCAL_LLM_UNAVAILABLE: Ollama request timeout after ${timeout}ms`)
        } else if (axiosError.response) {
          throw new Error(`LOCAL_LLM_UNAVAILABLE: Ollama API error: ${axiosError.response.status} ${axiosError.response.statusText}`)
        } else if (axiosError.request) {
          throw new Error(`LOCAL_LLM_UNAVAILABLE: Unable to reach Ollama server at ${ollamaEndpoint}`)
        } else {
          throw new Error(`LOCAL_LLM_UNAVAILABLE: Request setup error: ${axiosError.message}`)
        }
      }

    } catch (error) {
      const responseTime = Date.now() - startTime
      logger.error("ローカルLLM（Ollama直接接続）生成エラー", {
        error: error.message,
        errorType: error.constructor.name,
        responseTime,
        endpoint: typeof ollamaEndpoint !== 'undefined' ? ollamaEndpoint : 'undefined',
        promptLength: prompt.length,
        timeout,
        stack: error.stack
      })
      throw error
    }
  }

  /**
   * フォールバック実行
   */
  async generateWithFallback(prompt, failedProvider, options = {}) {
    const fallbackProvider = config.fallback_provider

    logger.warn(`フォールバック実行開始: ${failedProvider} → ${fallbackProvider}`, {
      promptLength: prompt.length,
      originalProvider: failedProvider,
      fallbackProvider
    })

    try {
      if (fallbackProvider === 'vertex-ai') {
        logger.info("Vertex AIフォールバック実行中")
        const result = await this.generateWithVertexAI(prompt, options)
        result.is_fallback = true
        result.original_provider = failedProvider
        result.fallback_reason = 'local_llm_timeout_or_error'

        logger.info("フォールバック成功", {
          fallbackProvider: 'vertex-ai',
          originalProvider: failedProvider,
          responseLength: result.content.length
        })

        return result
      } else {
        // フォールバック先がローカルLLMの場合（通常は想定外）
        const result = await this.generateWithLocalLLM(prompt, options)
        result.is_fallback = true
        result.original_provider = failedProvider
        return result
      }

    } catch (fallbackError) {
      logger.error("フォールバックも失敗", {
        originalProvider: failedProvider,
        fallbackProvider,
        error: fallbackError.message,
        stack: fallbackError.stack
      })

      // 最終フォールバック: シンプルなテキスト応答
      logger.warn("緊急フォールバック実行")
      return {
        content: this.generateEmergencyFallback(prompt),
        provider: 'emergency-fallback',
        cost: 0,
        response_time: 100,
        is_fallback: true,
        original_provider: failedProvider,
        fallback_provider: fallbackProvider,
        error_message: fallbackError.message
      }
    }
  }

  /**
   * 緊急時フォールバック応答
   */
  generateEmergencyFallback(prompt) {
    logger.warn("緊急時フォールバック応答を生成")

    if (prompt.includes("統合")) {
      return `# 記事統合提案（緊急応答）

申し訳ございません。現在AIサービスに一時的な問題が発生しております。

## 基本的な統合提案
1. **内容の重複確認**: 2つの記事の類似性を手動で確認してください
2. **SEO影響評価**: 検索順位への影響を慎重に検討してください
3. **統合戦略**: より包括的な記事への統合を検討してください

詳細な分析は、サービス復旧後に再実行をお願いいたします。

*このは緊急時の簡易応答です。*`
    }

    return "申し訳ございません。現在AIサービスに問題が発生しており、詳細な分析を実行できません。しばらくしてから再度お試しください。"
  }

  /**
   * 全プロバイダーの状態取得（直接Ollama接続）
   */
  async getProviderStatus() {
    const status = {
      timestamp: new Date().toISOString(),
      providers: {}
    }

    // Vertex AI状態（安全チェック）
    const vertexProvider = this.providers?.['vertex-ai'] || config.providers['vertex-ai']
    status.providers['vertex-ai'] = {
      name: vertexProvider?.name || 'Vertex AI',
      status: 'available', // Vertex AIは基本的に常時利用可能
      cost_per_request: vertexProvider?.cost_per_request || 50,
      speed: vertexProvider?.speed || '即座',
      description: vertexProvider?.description || '高精度・リアルタイム処理'
    }

    // ローカルLLM状態（直接Ollama接続）
    let instanceRunning = false // スコープを外側に移動
    let currentInstanceIP = null

    try {
      // 動的にインスタンスIPを取得
      currentInstanceIP = await this.getInstanceIP()
      if (!currentInstanceIP) {
        // インスタンスが停止状態の場合の正しい応答
        const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']
        status.providers['local-llm'] = {
          name: localProvider?.name || 'ローカルLLM',
          status: 'unavailable',
          cost_per_request: localProvider?.cost_per_request || 5,
          speed: localProvider?.speed || '2-3分（起動時）',
          description: localProvider?.description || 'コスト効率・カスタマイズ可能',
          error: 'Instance stopped or no external IP assigned',
          endpoint: 'instance-not-running'
        }
        return status
      }

      const ollamaEndpoint = `http://${currentInstanceIP}:11434`

      logger.info("ローカルLLM状態チェック開始", { endpoint: ollamaEndpoint, currentIP: currentInstanceIP })

      // 動的インスタンス検索を使用
      try {

        // 手動でアクティブインスタンスを検索
        const { InstancesClient } = require('@google-cloud/compute')
        const computeClient = new InstancesClient()

        // 複数ゾーンの設定
        const INSTANCE_CONFIGS = [
          {
            name: 'llm-gpu-instance',
            zone: 'asia-northeast1-c',
            project: 'seo-optimize-464208'
          },
          {
            name: 'llm-gpu-instance-alt1',
            zone: 'asia-northeast1-a',
            project: 'seo-optimize-464208'
          },
          {
            name: 'llm-gpu-instance-alt2',
            zone: 'asia-northeast1-b',
            project: 'seo-optimize-464208'
          }
        ]

        let activeInstance = null
        let activeConfig = null

        // 全ゾーンを検索してアクティブインスタンスを探す
        for (const config of INSTANCE_CONFIGS) {
          try {
            const [instance] = await computeClient.get({
              project: config.project,
              zone: config.zone,
              instance: config.name,
            })

            if (instance) {
              activeInstance = instance
              activeConfig = config
              logger.info("インスタンス発見", {
                zone: config.zone,
                instance: config.name,
                status: instance.status
              })
              break
            }
          } catch (error) {
            logger.info("インスタンス確認", {
              zone: config.zone,
              instance: config.name,
              error: error.message
            })
            continue
          }
        }

        if (activeInstance) {
          // GCPインスタンス状態に基づく処理
          const gcpStatus = activeInstance.status
          logger.info("インスタンス状態確認", {
            zone: activeConfig.zone,
            instance: activeConfig.name,
            gcpStatus,
            ip: activeInstance.networkInterfaces?.[0]?.accessConfigs?.[0]?.natIP
          })

          // 中間状態（STOPPING, STARTING等）の場合は早期リターン
          if (gcpStatus === 'STOPPING') {
            const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']
            status.providers['local-llm'] = {
              name: localProvider?.name || 'ローカルLLM',
              status: 'stopping',
              cost_per_request: localProvider?.cost_per_request || 5,
              speed: localProvider?.speed || '2-3分（起動時）',
              description: localProvider?.description || 'コスト効率・カスタマイズ可能',
              instance_info: {
                status: 'stopping',
                zone: activeConfig.zone,
                message: 'Instance is stopping',
                gcpStatus: gcpStatus
              },
              endpoint: 'stopping-instance'
            }
            return status
          }

          if (gcpStatus === 'STARTING' || gcpStatus === 'PROVISIONING' || gcpStatus === 'STAGING') {
            const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']
            status.providers['local-llm'] = {
              name: localProvider?.name || 'ローカルLLM',
              status: 'starting',
              cost_per_request: localProvider?.cost_per_request || 5,
              speed: localProvider?.speed || '2-3分（起動時）',
              description: localProvider?.description || 'コスト効率・カスタマイズ可能',
              instance_info: {
                status: 'starting',
                zone: activeConfig.zone,
                message: 'Instance is starting',
                gcpStatus: gcpStatus
              },
              endpoint: 'starting-instance'
            }
            return status
          }

          // RUNNING状態の場合はOllamaチェックを継続
          if (gcpStatus === 'RUNNING') {
            instanceRunning = true
            if (activeInstance.networkInterfaces?.[0]?.accessConfigs?.[0]?.natIP) {
              currentInstanceIP = activeInstance.networkInterfaces[0].accessConfigs[0].natIP
            }
          }
        } else {
          instanceRunning = false
          logger.info("インスタンスが見つかりません")
        }

        // インスタンスが起動中でない場合は早期リターン
        if (!instanceRunning) {
          const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']

          // 停止状態を詳細に報告
          const instanceStatus = activeInstance ? activeInstance.status : 'NOT_FOUND'
          const isTerminated = instanceStatus === 'TERMINATED'

          status.providers['local-llm'] = {
            name: localProvider?.name || 'ローカルLLM',
            status: 'unavailable', // 停止状態は常にunavailable
            cost_per_request: localProvider?.cost_per_request || 5,
            speed: localProvider?.speed || '2-3分（起動時）',
            description: localProvider?.description || 'コスト効率・カスタマイズ可能',
            instance_info: {
              status: instanceStatus,
              zone: activeConfig ? activeConfig.zone : 'unknown',
              message: isTerminated ? 'All instances are stopped' :
                      instanceStatus === 'NOT_FOUND' ? 'No instances found in any zone' :
                      'Instance is starting up',
              direct_connection: true,
              checked_zones: INSTANCE_CONFIGS.length
            },
            endpoint: 'no-running-instance'
          }

          logger.info("ローカルLLM停止状態レスポンス", {
            instanceStatus,
            activeConfig: activeConfig ? activeConfig.zone : null,
            checkedZones: INSTANCE_CONFIGS.length
          })

          return status
        }
      } catch (instanceError) {
        logger.warn("インスタンス状態確認でエラー", { error: instanceError.message })
        // インスタンス状態が確認できない場合は Ollama チェックを続行
      }

      // axiosを使用してヘルスチェック（タイムアウト大幅延長）
      const healthResponse = await axios.get(`${ollamaEndpoint}/api/tags`, {
        timeout: 45000, // 45秒に大幅延長（インスタンス起動時対応）
        validateStatus: function (status) {
          return status >= 200 && status < 300
        }
      })

      logger.info("ローカルLLM接続成功", {
        status: healthResponse.status,
        modelsCount: healthResponse.data?.models?.length || 0
      })

      let finalStatus = 'unavailable'
      let instanceInfo = {
        status: 'unavailable',
        message: 'Unknown status',
        direct_connection: true
      }

      const healthData = healthResponse.data
      if (healthData.models && healthData.models.length > 0) {
        finalStatus = 'available'
        instanceInfo = {
          status: 'available',
          instance_ip: '35.213.56.40',
          message: 'Healthy via direct Ollama connection',
          direct_connection: true,
          available_models: healthData.models.map(m => m.name)
        }
      } else {
        instanceInfo.message = 'No models available on Ollama server'
      }

      const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']
      status.providers['local-llm'] = {
        name: localProvider?.name || 'ローカルLLM',
        status: finalStatus,
        cost_per_request: localProvider?.cost_per_request || 5,
        speed: localProvider?.speed || '2-3分（起動時）',
        description: localProvider?.description || 'コスト効率・カスタマイズ可能',
        instance_info: instanceInfo,
        endpoint: 'direct-ollama-connection'
      }

    } catch (error) {
      logger.error("ローカルLLM状態取得エラー", {
        error: error.message,
        code: error.code,
        responseStatus: error.response?.status,
        responseData: error.response?.data,
        stack: error.stack,
        endpoint: currentInstanceIP ? `http://${currentInstanceIP}:11434` : 'unknown'
      })

      let errorMessage = error.message
      let finalStatus = 'error'

      if (error.code === 'ECONNABORTED') {
        errorMessage = 'Health check timeout after 45 seconds (instance may be starting up)'
        finalStatus = 'error'  // タイムアウトは実際のエラー状態として扱う
      } else if (error.response) {
        errorMessage = `Ollama server error: ${error.response.status} ${error.response.statusText}`
      } else if (error.request) {
        errorMessage = 'Unable to connect to Ollama server'
        finalStatus = 'unavailable'  // 接続不可は利用不可として扱う
      }

      const localProvider = this.providers?.['local-llm'] || config.providers['local-llm']
      status.providers['local-llm'] = {
        name: localProvider?.name || 'ローカルLLM',
        status: finalStatus,
        cost_per_request: localProvider?.cost_per_request || 5,
        speed: localProvider?.speed || '2-3分（起動時）',
        description: localProvider?.description || 'コスト効率・カスタマイズ可能',
        error: errorMessage,
        endpoint: 'direct-ollama-connection'
      }
    }

    return status
  }

  /**
   * 推奨プロバイダー取得
   */
  getRecommendedProvider(criteria = 'balanced') {
    switch (criteria) {
      case 'cost':
        return 'local-llm'
      case 'speed':
        return 'vertex-ai'
      case 'balanced':
      default:
        return config.default_provider
    }
  }

  /**
   * バッチテキスト生成（複数プロンプト）
   */
  async generateTextBatch(prompts, provider = null, options = {}) {
    const selectedProvider = provider || config.default_provider

    logger.info("バッチテキスト生成開始", {
      provider: selectedProvider,
      promptCount: prompts.length
    })

    const results = []

    for (const [index, prompt] of prompts.entries()) {
      try {
        const result = await this.generateText(prompt, selectedProvider, options)
        results.push({
          index,
          success: true,
          result
        })

      } catch (error) {
        logger.error(`バッチ処理 ${index} でエラー`, { error: error.message })
        results.push({
          index,
          success: false,
          error: error.message
        })
      }
    }

    const successCount = results.filter(r => r.success).length

    logger.info("バッチテキスト生成完了", {
      total: prompts.length,
      success: successCount,
      failed: prompts.length - successCount
    })

    return results
  }

  /**
   * コスト計算
   */
  calculateCost(provider, requestCount = 1) {
    const costPerRequest = this.providers[provider]?.cost_per_request || 0
    return costPerRequest * requestCount
  }

  /**
   * サービス統計取得
   */
  getServiceStats() {
    return {
      vertex_ai: this.vertexAIService.getStats?.() || { provider: 'vertex-ai' },
      local_llm: this.localLLMService.getStats(),
      config: {
        default_provider: config.default_provider,
        fallback_provider: config.fallback_provider,
        auto_fallback_enabled: config.auto_selection_rules.fallback_on_failure
      }
    }
  }
}

module.exports = LLMProviderManager