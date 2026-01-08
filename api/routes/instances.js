/**
 * インスタンス管理API
 * Google Compute Engineインスタンスの停止・開始・状態確認
 * Google Cloud Compute Engine Client Library使用版
 */

const express = require('express')
const router = express.Router()
const winston = require('winston')
const { InstancesClient } = require('@google-cloud/compute')

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

// Compute Engine設定 - プライマリインスタンスのみに単純化
const INSTANCE_CONFIG = {
  name: 'llm-gpu-instance',
  zone: 'asia-northeast1-c',
  project: 'seo-optimize-464208',
  description: 'プライマリLLM GPUインスタンス'
}

// Google Cloud Compute Engine クライアント初期化
const computeClient = new InstancesClient()

/**
 * インスタンス管理エンドポイント（フロントエンド互換）
 * GET /api/instances/manage?instance_name=llm-gpu-instance (状態取得)
 * POST /api/instances/manage (アクション実行)
 */
router.get('/manage', async (req, res) => {
  try {
    const { instance_name } = req.query

    logger.info('インスタンス状態取得リクエスト (GET)', {
      instance_name,
      expectedName: INSTANCE_CONFIG.name
    })

    // インスタンス名の確認
    if (instance_name && instance_name !== INSTANCE_CONFIG.name) {
      return res.status(400).json({
        success: false,
        error: `Unknown instance: ${instance_name}. Expected: ${INSTANCE_CONFIG.name}`
      })
    }

    // 状態取得
    const status = await getInstanceStatus()

    res.json({
      success: true,
      ...status
    })

  } catch (error) {
    logger.error('インスタンス状態取得エラー (GET)', {
      error: error.message,
      stack: error.stack
    })

    res.status(500).json({
      success: false,
      error: {
        message: error.message
      }
    })
  }
})

/**
 * インスタンス管理エンドポイント
 * POST /api/instances/manage
 */
router.post('/manage', async (req, res) => {
  const { action } = req.body

  logger.info('インスタンス管理リクエスト', {
    action,
    instance: INSTANCE_CONFIG.name,
    zone: INSTANCE_CONFIG.zone
  })

  try {
    let result
    switch (action) {
      case 'stop':
        result = await stopInstance()
        break
      case 'start':
        result = await startInstance()
        break
      case 'restart':
        result = await restartInstance()
        break
      case 'status':
        result = await getInstanceStatus()
        break
      default:
        return res.status(400).json({
          success: false,
          error: `Unknown action: ${action}`
        })
    }

    res.json({
      success: true,
      ...result
    })

  } catch (error) {
    // リソース不足エラーの特別な処理
    if (error.message.includes('RESOURCE_EXHAUSTED')) {
      logger.error('インスタンス管理: リソース不足エラー', {
        action,
        error: error.message,
        timestamp: new Date().toISOString()
      })

      return res.status(503).json({
        success: false,
        error: {
          type: 'RESOURCE_EXHAUSTED',
          message: 'GPUリソースが不足しているため、インスタンスを起動できません。しばらくしてから再試行してください。',
          action,
          recommendation: '時間をおいて再試行するか、GCPコンソールで直接確認してください。',
          timestamp: new Date().toISOString()
        }
      })
    }

    // その他の予期せぬエラー
    logger.error('インスタンス管理エラー', {
      action,
      error: error.message,
      stack: error.stack
    })

    res.status(500).json({
      success: false,
      error: {
        message: error.message,
        action
      }
    })
  }
})

/**
 * インスタンス状態取得
 * GET /api/instances/status
 */
router.get('/status', async (req, res) => {
  try {
    const status = await getInstanceStatus()
    res.json({
      success: true,
      ...status
    })

  } catch (error) {
    logger.error('インスタンス状態取得エラー', {
      error: error.message,
      stack: error.stack
    })

    res.status(500).json({
      success: false,
      error: {
        message: error.message
      }
    })
  }
})

/**
 * インスタンス停止
 */
async function stopInstance() {
  logger.info('インスタンス停止処理開始', INSTANCE_CONFIG)

  try {
    const [operation] = await computeClient.stop({
      project: INSTANCE_CONFIG.project,
      zone: INSTANCE_CONFIG.zone,
      instance: INSTANCE_CONFIG.name,
    })

    logger.info('インスタンス停止操作開始', {
      operationId: operation.id
    })

    // オペレーション完了を待たずに応答を返す（フロントのポーリングに任せる）
    return {
      action: 'stop',
      status: 'stopping',
      message: 'インスタンスの停止処理を開始しました。'
    }
  } catch (error) {
    // 既に停止している場合などのエラーは許容する
    if (error.message.includes('must be running')) {
      logger.warn('インスタンスは既に停止されています。', { error: error.message })
      return {
        action: 'stop',
        status: 'unavailable',
        message: 'インスタンスは既に停止されています。'
      }
    }
    logger.error('インスタンス停止失敗', { error: error.message })
    throw new Error(`インスタンスの停止に失敗しました: ${error.message}`)
  }
}

/**
 * インスタンス開始（非同期リクエスト対応）
 */
async function startInstance() {
  logger.info('インスタンス開始リクエスト受信', INSTANCE_CONFIG)

  // 既に稼働中のインスタンスがないか確認
  const currentStatus = await getInstanceStatus()
  if (currentStatus.status === 'available') {
    logger.info('インスタンスは既に稼働中です。')
    return {
      action: 'start',
      status: 'available',
      message: 'インスタンスは既に稼働しています。'
    }
  }

  try {
    // start APIを呼び出すが、オペレーションの完了は待たない
    const [operation] = await computeClient.start({
      project: INSTANCE_CONFIG.project,
      zone: INSTANCE_CONFIG.zone,
      instance: INSTANCE_CONFIG.name,
    })

    logger.info('インスタンス開始操作をGCPに要求しました。', {
      operationId: operation.name
    })

    // 起動要求が受け付けられたら、すぐにレスポンスを返す
    return {
      action: 'start',
      status: 'starting',
      message: 'インスタンスの起動処理を開始しました。'
    }
  } catch (error) {
    if (error.message.includes('RESOURCE_EXHAUSTED')) {
      logger.warn(`リソース不足のため起動に失敗。`, { error: error.message })
      // エラーを再スローして上位のハンドラで処理
      throw error
    } else {
      logger.error(`起動中に予期せぬエラーが発生しました。`, { error: error.message })
      throw new Error(`インスタンスの開始に失敗しました: ${error.message}`)
    }
  }
}

/**
 * インスタンス再起動
 */
async function restartInstance() {
  logger.info('インスタンス再起動処理開始')
  try {
    await stopInstance()
    // 停止を待たずにすぐに開始リクエストを送る
    const startResult = await startInstance()
    logger.info('インスタンス再起動リクエスト完了', { result: startResult })
    return {
      ...startResult,
      action: 'restart',
      message: 'インスタンスの再起動処理を開始しました。'
    }
  } catch (error) {
    logger.error('インスタンス再起動失敗', { error: error.message })
    throw new Error(`インスタンスの再起動に失敗しました: ${error.message}`)
  }
}

/**
 * インスタンス状態取得（単純化）
 */
async function getInstanceStatus() {
  try {
    const [instance] = await computeClient.get({
      project: INSTANCE_CONFIG.project,
      zone: INSTANCE_CONFIG.zone,
      instance: INSTANCE_CONFIG.name,
    })
    return buildInstanceStatus(instance, INSTANCE_CONFIG)
  } catch (error) {
    // インスタンスが見つからない場合
    if (error.code === 5) { // 5 = NOT_FOUND
      logger.error('インスタンスが見つかりません。', { name: INSTANCE_CONFIG.name })
      return {
        status: 'error',
        message: `インスタンス '${INSTANCE_CONFIG.name}' が見つかりません。`,
        gcpStatus: 'NOT_FOUND',
      }
    }
    logger.error('インスタンス状態取得失敗', { error: error.message, code: error.code })
    throw new Error(`インスタンス状態取得に失敗しました: ${error.message}`)
  }
}

/**
 * インスタンス状態レスポンス構築
 */
function buildInstanceStatus(instance, config) {
  const gcpStatus = instance.status
  const machineType = instance.machineType ? instance.machineType.split('/').pop() : 'unknown'
  const acceleratorType = instance.guestAccelerators && instance.guestAccelerators.length > 0
    ? instance.guestAccelerators[0].acceleratorType.split('/').pop()
    : 'none'
  const externalIP = instance.networkInterfaces && instance.networkInterfaces[0] && instance.networkInterfaces[0].accessConfigs && instance.networkInterfaces[0].accessConfigs[0]
    ? instance.networkInterfaces[0].accessConfigs[0].natIP
    : 'none'

  let status = 'error';
  let message = `インスタンス状態: ${gcpStatus} (${config.zone})`;

  switch (gcpStatus) {
    case 'RUNNING':
      status = 'available';
      message = `インスタンスが実行中です (${config.zone})`;
      break;
    case 'TERMINATED':
      status = 'unavailable';
      message = `インスタンスが停止されています (${config.zone})`;
      break;
    case 'PROVISIONING':
    case 'STAGING':
      status = 'starting';
      message = `インスタンスを起動しています... (${gcpStatus})`;
      break;
    case 'STOPPING':
      status = 'stopping';
      message = `インスタンスを停止しています... (${gcpStatus})`;
      break;
    default: // REPAIRING, SUSPENDED, SUSPENDING などのその他の状態
      status = 'error';
      message = `インスタンスが不安定な状態です: ${gcpStatus} (${config.zone})`;
      break;
  }

  // フロントエンドが期待するフィールドを構築
  const result = {
    instance: config.name,
    zone: config.zone,
    status: status, // フロントエンド用の統一された状態
    gcpStatus: gcpStatus, // GCPの生のステータス
    machineType,
    acceleratorType,
    externalIP,
    timestamp: new Date().toISOString(),
    description: config.description,
    primary: config.primary,
    message: message,
    // 後方互換性のためのフィールド
    running: status === 'available',
    stopped: status === 'unavailable',
    available: status === 'available',
    unavailable: status !== 'available',
    state: status
  }

  return result
}

/**
 * オペレーション完了待機（リソース不足エラーの早期検出対応）
 */
async function waitForOperation(operation, config) {
  const { ZoneOperationsClient } = require('@google-cloud/compute')
  const operationsClient = new ZoneOperationsClient()

  const operationName = operation.name
  const maxWaitTime = 300000 // 5分
  const pollInterval = 5000 // 5秒
  const startTime = Date.now()

  while (Date.now() - startTime < maxWaitTime) {
    try {
      const [currentOperation] = await operationsClient.get({
        project: config.project,
        zone: config.zone,
        operation: operationName,
      })

      if (currentOperation.status === 'DONE') {
        if (currentOperation.error) {
          // リソース不足エラーの特別処理
          const errorMessage = JSON.stringify(currentOperation.error)

          if (errorMessage.includes('ZONE_RESOURCE_POOL_EXHAUSTED') ||
              errorMessage.includes('RESOURCE_POOL_EXHAUSTED') ||
              errorMessage.includes('nvidia-l4') ||
              errorMessage.includes('accelerator') && errorMessage.includes('unavailable')) {

            logger.error('Google Cloud リソース不足エラー検出', {
              operationName,
              error: currentOperation.error,
              zone: INSTANCE_CONFIG.zone,
              timestamp: new Date().toISOString()
            })

            throw new Error('RESOURCE_EXHAUSTED: L4 GPUリソースが現在利用できません。asia-northeast1-c ゾーンでのリソースが不足しています。別のゾーンでの実行を検討してください。')
          }

          throw new Error(`Operation failed: ${errorMessage}`)
        }
        logger.info('オペレーション完了', {
          operationName,
          duration: Date.now() - startTime + 'ms'
        })
        return
      }

      // オペレーション進行中のログを詳細化
      logger.info('オペレーション進行中', {
        operationName,
        status: currentOperation.status,
        progress: currentOperation.progress || 'unknown',
        elapsed: Math.round((Date.now() - startTime) / 1000) + 's'
      })

      await new Promise(resolve => setTimeout(resolve, pollInterval))

    } catch (error) {
      // リソース不足エラーは再度スローして上位でキャッチ
      if (error.message.includes('RESOURCE_EXHAUSTED')) {
        throw error
      }

      logger.warn('オペレーション状態確認中にエラー', {
        error: error.message,
        operationName,
        elapsed: Math.round((Date.now() - startTime) / 1000) + 's'
      })
      await new Promise(resolve => setTimeout(resolve, pollInterval))
    }
  }

  throw new Error(`オペレーションタイムアウト: ${operationName}（${Math.round((Date.now() - startTime) / 1000)}秒経過）`)
}

/**
 * インスタンス状態の待機
 */
async function waitForInstanceState(targetState, timeoutSeconds = 300) {
  const startTime = Date.now()
  const timeout = timeoutSeconds * 1000

  while (Date.now() - startTime < timeout) {
    try {
      const status = await getInstanceStatus()
      if (status.status === targetState) {
        logger.info('インスタンス状態変更確認', {
          targetState,
          currentState: status.status,
          waitTime: Math.round((Date.now() - startTime) / 1000) + 's'
        })
        return true
      }

      await new Promise(resolve => setTimeout(resolve, 5000)) // 5秒待機

    } catch (error) {
      logger.warn('状態確認中にエラー', { error: error.message })
      await new Promise(resolve => setTimeout(resolve, 5000))
    }
  }

  throw new Error(`タイムアウト: インスタンス状態が${targetState}になりませんでした`)
}

/**
 * GET /api/instances/zones/status
 * 全ゾーンのインスタンス状態を確認
 */
router.get('/zones/status', async (req, res) => {
  try {
    logger.info('全ゾーンのインスタンス状態確認開始')

    const zoneStatuses = []

    for (const config of INSTANCE_CONFIGS) {
      try {
        const [instance] = await computeClient.get({
          project: config.project,
          zone: config.zone,
          instance: config.name,
        })

        const instanceStatus = buildInstanceStatus(instance, config)
        zoneStatuses.push({
          zone: config.zone,
          instance: config.name,
          status: instanceStatus.status,
          running: instanceStatus.running,
          available: instanceStatus.available,
          primary: config.primary,
          description: config.description,
          externalIP: instanceStatus.externalIP,
          message: instanceStatus.message
        })

        logger.info('ゾーン状態確認完了', {
          zone: config.zone,
          instance: config.name,
          status: instanceStatus.status
        })

      } catch (error) {
        // インスタンスが存在しない場合
        zoneStatuses.push({
          zone: config.zone,
          instance: config.name,
          status: 'NOT_FOUND',
          running: false,
          available: false,
          primary: config.primary,
          description: config.description,
          externalIP: 'none',
          message: `インスタンスが見つかりません (${config.zone})`
        })

        logger.info('ゾーン状態確認: インスタンス未発見', {
          zone: config.zone,
          instance: config.name,
          error: error.message
        })
      }
    }

    // 稼働中のインスタンスがあるかチェック
    const activeZones = zoneStatuses.filter(zone => zone.running)
    const recommendation = activeZones.length === 0
      ? 'リソースが利用可能なゾーンでインスタンスを作成してください'
      : `稼働中: ${activeZones.map(z => z.zone).join(', ')}`

    res.json({
      success: true,
      data: {
        zones: zoneStatuses,
        summary: {
          total_zones: INSTANCE_CONFIGS.length,
          running_instances: activeZones.length,
          available_zones: zoneStatuses.filter(z => z.available).length,
          recommendation: recommendation
        }
      }
    })

    logger.info('全ゾーン状態確認完了', {
      total_zones: INSTANCE_CONFIGS.length,
      running_instances: activeZones.length
    })

  } catch (error) {
    logger.error('全ゾーン状態確認でエラー', {
      error: error.message,
      stack: error.stack
    })

    res.status(500).json({
      success: false,
      error: {
        message: error.message
      }
    })
  }
})

module.exports = router