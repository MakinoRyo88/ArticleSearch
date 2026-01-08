/**
 * Auto Scaler サービス
 * ローカルLLMインスタンスの自動起動・停止・監視
 */

const winston = require("winston")
const config = require("../config/llm-providers")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class AutoScaler {
  constructor(localLLMService) {
    this.localLLMService = localLLMService
    this.config = config.providers['local-llm']
    this.computeConfig = config.compute_engine
    this.cloudFunctions = config.cloud_functions
    this.lastActivity = new Date()
    this.healthCheckInterval = null
    this.autoShutdownTimer = null
  }

  /**
   * インスタンスの可用性確認と自動起動
   */
  async ensureAvailable() {
    logger.info("インスタンス可用性確認開始")

    try {
      // 現在の状態確認
      const currentStatus = await this.getStatus()

      logger.info("現在のインスタンス状態", currentStatus)

      switch (currentStatus.status) {
        case 'available':
          // 使用中マーク更新
          this.markActivity()
          return currentStatus

        case 'starting':
          // 起動中の場合は待機
          logger.info("インスタンス起動中、待機します...")
          return await this.waitForReady()

        case 'unavailable':
        case 'stopped':
          // 起動が必要
          logger.info("インスタンス起動を開始します...")
          return await this.start()

        default:
          throw new Error(`Unknown instance status: ${currentStatus.status}`)
      }

    } catch (error) {
      logger.error("インスタンス可用性確認エラー", { error: error.message })
      throw error
    }
  }

  /**
   * インスタンス状態取得
   */
  async getStatus() {
    try {
      // Cloud Functions経由で状態確認
      const response = await fetch(this.cloudFunctions.get_status, {
        method: 'GET',
        timeout: 15000
      })

      if (response.ok) {
        const statusData = await response.json()
        return this.parseStatusResponse(statusData)
      } else {
        return {
          status: 'unavailable',
          message: `Status check failed: ${response.status}`,
          timestamp: new Date().toISOString()
        }
      }

    } catch (error) {
      logger.error("状態取得エラー", { error: error.message })

      // フォールバック: ヘルスチェック試行
      const healthResult = await this.localLLMService.checkHealth()

      return {
        status: healthResult.status === 'available' ? 'available' : 'unavailable',
        message: healthResult.message,
        timestamp: new Date().toISOString(),
        fallback: true
      }
    }
  }

  /**
   * Cloud Functions応答の解析
   */
  parseStatusResponse(response) {
    if (response.status === 'success' && response.instance_status === 'RUNNING') {
      // インスタンスは稼働中だが、external_ipが取得できない場合
      if (!response.external_ip) {
        logger.warn("インスタンスは稼働中ですが、external_ipが取得できていません", response)
        return {
          status: 'unavailable',
          external_ip: null,
          instance_name: response.instance_name,
          message: 'Instance is running but external IP is not available. Please restart the instance.',
          timestamp: response.timestamp || new Date().toISOString(),
          requires_restart: true
        }
      }

      return {
        status: 'available',
        external_ip: response.external_ip,
        instance_name: response.instance_name,
        message: 'Instance is running and ready',
        timestamp: response.timestamp || new Date().toISOString()
      }
    } else if (response.instance_status === 'PROVISIONING' || response.instance_status === 'STAGING') {
      return {
        status: 'starting',
        instance_name: response.instance_name,
        message: 'Instance is starting up',
        timestamp: response.timestamp || new Date().toISOString()
      }
    } else {
      return {
        status: 'unavailable',
        message: response.message || 'Instance not available',
        timestamp: response.timestamp || new Date().toISOString()
      }
    }
  }

  /**
   * インスタンス起動
   */
  async start() {
    logger.info("ローカルLLMインスタンス起動開始")

    try {
      const response = await fetch(this.cloudFunctions.start_instance, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          zone: this.computeConfig.zone,
          instance_name: this.computeConfig.instance_name
        }),
        timeout: 60000 // 1分
      })

      if (!response.ok) {
        throw new Error(`Start instance failed: ${response.status} ${response.statusText}`)
      }

      const startResult = await response.json()
      logger.info("インスタンス起動リクエスト完了", startResult)

      // 起動完了まで待機
      const readyResult = await this.waitForReady()

      // ヘルスモニタリング開始
      this.startHealthMonitoring()

      // 自動シャットダウンタイマー設定
      this.scheduleAutoShutdown()

      return readyResult

    } catch (error) {
      logger.error("インスタンス起動エラー", { error: error.message })
      throw new Error(`Failed to start instance: ${error.message}`)
    }
  }

  /**
   * インスタンス準備完了まで待機（改善版）
   */
  async waitForReady(maxWaitTime = this.config.startup_timeout) {
    const startTime = Date.now()
    const checkInterval = 15000 // 15秒間隔

    logger.info("インスタンス準備完了を待機", {
      maxWaitTime: `${maxWaitTime / 1000}秒`,
      checkInterval: `${checkInterval / 1000}秒`
    })

    let consecutiveFailures = 0
    const maxConsecutiveFailures = 5

    while (Date.now() - startTime < maxWaitTime) {
      try {
        const status = await this.getStatus()

        if (status.status === 'available' && status.external_ip) {
          // エンドポイント設定
          this.localLLMService.setEndpoint(status.external_ip)

          // 複数回のヘルスチェック試行
          let healthCheckSuccess = false
          for (let i = 0; i < 3; i++) {
            const health = await this.localLLMService.checkHealth()
            if (health.status === 'available') {
              healthCheckSuccess = true
              break
            }
            logger.info(`ヘルスチェック試行 ${i + 1}/3 失敗、再試行中...`)
            await this.sleep(5000) // 5秒待機して再試行
          }

          if (healthCheckSuccess) {
            logger.info("インスタンス準備完了", {
              waitTime: `${(Date.now() - startTime) / 1000}秒`,
              external_ip: status.external_ip
            })

            this.markActivity()
            return status
          } else {
            logger.warn("IPアドレスは取得できましたが、ヘルスチェックが失敗しました")
          }
        } else {
          logger.info("インスタンス状態確認中", {
            status: status.status,
            external_ip: status.external_ip || 'null',
            elapsed: `${Math.round((Date.now() - startTime) / 1000)}秒`
          })
        }

        // 待機
        await this.sleep(checkInterval)
        consecutiveFailures = 0

      } catch (error) {
        consecutiveFailures++
        logger.warn("準備完了チェックでエラー", {
          error: error.message,
          consecutiveFailures,
          maxConsecutiveFailures
        })

        // 連続失敗が多い場合は早期終了
        if (consecutiveFailures >= maxConsecutiveFailures) {
          throw new Error(`Too many consecutive failures during instance ready check: ${error.message}`)
        }

        await this.sleep(checkInterval)
      }
    }

    throw new Error(`Instance ready timeout after ${maxWaitTime / 1000} seconds`)
  }

  /**
   * 使用中マーク
   */
  markActivity() {
    this.lastActivity = new Date()
    logger.debug("Activity marked", { timestamp: this.lastActivity })

    // 自動シャットダウンタイマーリセット
    this.scheduleAutoShutdown()
  }

  /**
   * ヘルス監視開始
   */
  startHealthMonitoring() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval)
    }

    this.healthCheckInterval = setInterval(async () => {
      try {
        const health = await this.localLLMService.checkHealth()

        if (health.status !== 'available') {
          logger.warn("ヘルスチェック失敗", health)
        }

      } catch (error) {
        logger.error("定期ヘルスチェックエラー", { error: error.message })
      }
    }, this.config.health_check_interval)

    logger.info("ヘルス監視開始", {
      interval: `${this.config.health_check_interval / 1000}秒`
    })
  }

  /**
   * 自動シャットダウンスケジュール
   */
  scheduleAutoShutdown() {
    // 既存タイマーをクリア
    if (this.autoShutdownTimer) {
      clearTimeout(this.autoShutdownTimer)
    }

    // 新しいタイマーを設定
    this.autoShutdownTimer = setTimeout(async () => {
      const idleTime = Date.now() - this.lastActivity.getTime()

      if (idleTime >= this.config.auto_shutdown_timeout) {
        logger.info("アイドルタイムアウトにより自動シャットダウン実行", {
          idleTime: `${Math.round(idleTime / 1000)}秒`
        })

        await this.stop()
      }
    }, this.config.auto_shutdown_timeout)

    logger.debug("自動シャットダウンスケジュール設定", {
      timeout: `${this.config.auto_shutdown_timeout / 1000}秒`
    })
  }

  /**
   * インスタンス停止
   */
  async stop() {
    logger.info("ローカルLLMインスタンス停止開始")

    try {
      // ヘルス監視停止
      if (this.healthCheckInterval) {
        clearInterval(this.healthCheckInterval)
        this.healthCheckInterval = null
      }

      // 自動シャットダウンタイマー停止
      if (this.autoShutdownTimer) {
        clearTimeout(this.autoShutdownTimer)
        this.autoShutdownTimer = null
      }

      // インスタンス停止
      const response = await fetch(this.cloudFunctions.stop_instance, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          zone: this.computeConfig.zone,
          instance_name: this.computeConfig.instance_name
        }),
        timeout: 60000
      })

      if (response.ok) {
        const result = await response.json()
        logger.info("インスタンス停止完了", result)
        return result
      } else {
        throw new Error(`Stop instance failed: ${response.status}`)
      }

    } catch (error) {
      logger.error("インスタンス停止エラー", { error: error.message })
      throw error
    }
  }

  /**
   * リソース使用状況取得
   */
  async getResourceUsage() {
    return {
      last_activity: this.lastActivity,
      idle_time: Date.now() - this.lastActivity.getTime(),
      auto_shutdown_in: this.config.auto_shutdown_timeout - (Date.now() - this.lastActivity.getTime()),
      health_monitoring_active: !!this.healthCheckInterval,
      config: {
        startup_timeout: this.config.startup_timeout,
        health_check_interval: this.config.health_check_interval,
        auto_shutdown_timeout: this.config.auto_shutdown_timeout
      }
    }
  }

  /**
   * 強制的なクリーンアップ
   */
  cleanup() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval)
      this.healthCheckInterval = null
    }

    if (this.autoShutdownTimer) {
      clearTimeout(this.autoShutdownTimer)
      this.autoShutdownTimer = null
    }

    logger.info("AutoScaler cleanup completed")
  }

  /**
   * ユーティリティ: スリープ
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}

module.exports = AutoScaler