import { NextRequest, NextResponse } from 'next/server'

const API_BASE_URL = 'https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app'

function generateMockProviderStatus() {
  return {
    success: true,
    data: {
      timestamp: new Date().toISOString(),
      providers: {
        'vertex-ai': {
          name: 'Vertex AI',
          status: 'available',
          cost_per_request: 0.05,
          speed: 'fast',
          description: '高精度・リアルタイム処理',
          endpoint: 'vertex-ai-endpoint'
        },
        'local-llm': {
          name: 'ローカルLLM',
          status: 'available',
          cost_per_request: 0.005,
          speed: 'medium',
          description: 'コスト効率・カスタマイズ可能',
          endpoint: 'via-cloud-function-proxy',
          instance_info: {
            status: 'available',
            message: 'ローカルLLMインスタンスが稼働中です',
            via_cloud_function: true,
            management_enabled: true
          }
        }
      }
    }
  }
}

export async function GET(request: NextRequest) {
  try {
    console.log('⚙️ プロバイダー状態API呼び出し開始')

    // ローカル開発時はCloud Run APIをスキップしてCloud Functionを直接呼び出し
    const isDevelopment = process.env.NODE_ENV === 'development'

    if (!isDevelopment) {
      // 本番環境でのみCloud Run APIを試行（短いタイムアウト）
      try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), 8000) // 8秒タイムアウト

        const response = await fetch(`${API_BASE_URL}/providers/status`, {
          method: 'GET',
          signal: controller.signal
        })

        clearTimeout(timeoutId)

        if (response.ok) {
          const data = await response.json()
          return NextResponse.json(data)
        }
      } catch (error) {
        console.warn('Cloud Run providers API failed, checking instance status directly:', error)
      }
    } else {
      console.log('🔧 開発環境のため、Cloud Run APIをスキップしてCloud Functionを直接呼び出し')
    }

    // ローカルLLMインスタンスの実際の状態を取得
    let localLlmStatus = 'available'
    let localLlmMessage = 'ローカルLLMインスタンスが稼働中です'

    try {
      const instanceResponse = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-info', {
        method: 'GET',
        signal: AbortSignal.timeout(15000) // 15秒タイムアウト
      })

      if (instanceResponse.ok) {
        const instanceData = await instanceResponse.json()

        console.log('📊 インスタンス状態チェック:', {
          success: instanceData.success,
          status: instanceData.status,
          timestamp: new Date().toISOString()
        })

        if (instanceData.success && instanceData.status === 'RUNNING') {
          localLlmStatus = 'available'
          localLlmMessage = 'ローカルLLMインスタンスが稼働中です'
        } else if (instanceData.status === 'TERMINATED' || instanceData.status === 'STOPPED') {
          localLlmStatus = 'stopped'
          localLlmMessage = 'ローカルLLMインスタンスが停止中です'
        } else if (instanceData.status === 'NOT_FOUND') {
          localLlmStatus = 'not_found'
          localLlmMessage = 'ローカルLLMインスタンスが見つかりません'
        } else {
          localLlmStatus = 'unknown'
          localLlmMessage = `インスタンス状態: ${instanceData.status}`
        }
      } else {
        localLlmStatus = 'unavailable'
        localLlmMessage = 'インスタンス状態を取得できません'
      }
    } catch (error) {
      console.warn('Failed to check instance status:', error)
      localLlmStatus = 'error'
      localLlmMessage = 'インスタンス接続エラー'
    }

    // 動的なモック応答を生成
    const dynamicResponse = {
      success: true,
      data: {
        timestamp: new Date().toISOString(),
        providers: {
          'vertex-ai': {
            name: 'Vertex AI',
            status: 'available',
            cost_per_request: 0.05,
            speed: 'fast',
            description: '高精度・リアルタイム処理',
            endpoint: 'vertex-ai-endpoint'
          },
          'local-llm': {
            name: 'ローカルLLM',
            status: localLlmStatus,
            cost_per_request: 0.005,
            speed: 'medium',
            description: 'コスト効率・カスタマイズ可能',
            endpoint: 'via-cloud-function-proxy',
            instance_info: {
              status: localLlmStatus,
              message: localLlmMessage,
              via_cloud_function: true,
              management_enabled: true
            }
          }
        }
      }
    }

    return NextResponse.json(dynamicResponse)

  } catch (error) {
    console.error('Providers API error:', error)

    // エラー時でもモック応答を返す
    const mockResponse = generateMockProviderStatus()
    return NextResponse.json(mockResponse)
  }
}