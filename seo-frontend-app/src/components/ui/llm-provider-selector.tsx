"use client"

import React, { useState, useEffect, useCallback, useRef } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { getProviderStatusUrl, getInstanceManageUrl } from "@/lib/api-config"
import { Button } from "@/components/ui/button"
import {
  Cpu,
  Zap,
  DollarSign,
  Clock,
  CheckCircle2,
  AlertCircle,
  Loader2,
  RefreshCw,
  Power,
  Square,
  Settings
} from "lucide-react"

interface LLMProvider {
  id: 'vertex-ai' | 'local-llm'
  name: string
  description: string
  cost: string
  speed: string
  status: 'available' | 'starting' | 'unavailable' | 'error'
  details?: {
    cost_per_request?: number
    endpoint?: string
    response_time?: string
  }
}

interface LLMProviderSelectorProps {
  selectedProvider: 'vertex-ai' | 'local-llm'
  onProviderChange: (provider: 'vertex-ai' | 'local-llm') => void
  disabled?: boolean
  className?: string
}

const defaultProviders: LLMProvider[] = [
  {
    id: 'vertex-ai',
    name: 'Vertex AI',
    description: '高精度・リアルタイム処理',
    cost: '約¥50/回',
    speed: '即座',
    status: 'available'
  },
  {
    id: 'local-llm',
    name: 'ローカルLLM',
    description: 'コスト効率・カスタマイズ可能',
    cost: '約¥5/回',
    speed: '2-3分（起動時）',
    status: 'unavailable'
  }
]

export function LLMProviderSelector({
  selectedProvider,
  onProviderChange,
  disabled = false,
  className = ""
}: LLMProviderSelectorProps) {
  const [providers, setProviders] = useState<LLMProvider[]>(defaultProviders)
  const [loading, setLoading] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [isStartingInstance, setIsStartingInstance] = useState(false)
  const [isStoppingInstance, setIsStoppingInstance] = useState(false)

  // stateとRefを同期
  useEffect(() => {
    isStartingRef.current = isStartingInstance
  }, [isStartingInstance])

  useEffect(() => {
    isStoppingRef.current = isStoppingInstance
  }, [isStoppingInstance])

  // タイムアウト管理用のRef
  const startTimeoutRef = useRef<NodeJS.Timeout | null>(null)
  const stopTimeoutRef = useRef<NodeJS.Timeout | null>(null)

  // フラグの最新値を保持するRef
  const isStartingRef = useRef(false)
  const isStoppingRef = useRef(false)

  // プロバイダー状態を取得
  const fetchProviderStatus = useCallback(async () => {
    console.log(`🔍 状態確認中 - フラグ状態: 起動中=${isStartingRef.current}, 停止中=${isStoppingRef.current}`)
    setLoading(true)
    try {
      const response = await fetch(getProviderStatusUrl())
      const result = await response.json()

      if (result.success) {
        // インスタンス状態APIのレスポンスを処理
        setProviders(prev => prev.map(provider => {
          if (provider.id === 'local-llm') {
            // インスタンスAPIから取得した状態を使用
            const newStatus = result.status
            const oldStatus = provider.status

            console.log(`📊 ローカルLLM詳細: ${oldStatus} → ${newStatus}, フラグ: 起動中=${isStartingRef.current}, 停止中=${isStoppingRef.current}`)

            // 状態変化に基づく確実なフラグ管理
            if (newStatus === 'available') {
              // 起動完了の確認
              if (isStartingRef.current) {
                console.log('✅ 起動完了確認！フラグをリセットします')
                setIsStartingInstance(false)
                if (startTimeoutRef.current) {
                  clearTimeout(startTimeoutRef.current)
                  startTimeoutRef.current = null
                }
              }
              // 停止フラグが誤って設定されている場合もリセット
              if (isStoppingRef.current) {
                console.log('✅ available状態なのに停止フラグが設定されている - リセットします')
                setIsStoppingInstance(false)
                if (stopTimeoutRef.current) {
                  clearTimeout(stopTimeoutRef.current)
                  stopTimeoutRef.current = null
                }
              }
            } else if (newStatus === 'unavailable') {
              // 停止完了の確認
              if (isStoppingRef.current) {
                console.log('✅ 停止完了確認！停止フラグをリセットします')
                setIsStoppingInstance(false)
                if (stopTimeoutRef.current) {
                  clearTimeout(stopTimeoutRef.current)
                  stopTimeoutRef.current = null
                }
              }
              // 起動フラグが誤って設定されている場合もリセット
              if (isStartingRef.current) {
                console.log('✅ unavailable状態なのに起動フラグが設定されている - リセットします')
                setIsStartingInstance(false)
                if (startTimeoutRef.current) {
                  clearTimeout(startTimeoutRef.current)
                  startTimeoutRef.current = null
                }
              }
            } else if (newStatus === 'starting') {
              // startingステータスの場合
              console.log(`⚠️ startingステータス検出 - 起動フラグ=${isStartingRef.current}, 停止フラグ=${isStoppingRef.current}`)

              // 停止フラグをリセット
              if (isStoppingRef.current) {
                console.log('⚠️ starting時に停止フラグをリセット')
                setIsStoppingInstance(false)
                if (stopTimeoutRef.current) {
                  clearTimeout(stopTimeoutRef.current)
                  stopTimeoutRef.current = null
                }
              }

              // ユーザーがボタンを押していない場合（起動フラグがfalse）は、自動的に起動フラグもfalseのままにする
              if (!isStartingRef.current) {
                console.log('⚠️ ユーザー操作なしでstartingステータス検出 - 起動フラグを維持（false）')
                // 何もしない（フラグはfalseのまま）
              }
            }

            // 状態変更をログに出力
            if (newStatus !== oldStatus) {
              console.log(`🔄 ${provider.name} 状態変更: ${oldStatus} → ${newStatus}`)
            }

            return {
              ...provider,
              status: newStatus,
              cost: '約¥5/回',
              speed: newStatus === 'available' ? '1-2分' : '2-3分（起動時）',
              description: 'コスト効率・カスタマイズ可能',
              details: {
                cost_per_request: 5,
                endpoint: result.externalIP ? `http://${result.externalIP}:11434` : 'not-available',
                response_time: newStatus === 'available' ? '1-2分' : undefined
              }
            }
          }
          return provider
        }))
        setLastUpdated(new Date())
      }
    } catch (error) {
      console.error('❌ プロバイダー状態取得エラー:', error)
      // API接続失敗時のフォールバック
      setProviders(prev => prev.map(p => ({ ...p, status: 'error' })))
    } finally {
      setLoading(false)
    }
  }, []) // 依存配列を空にして無限ループを防ぐ

  // ローカルLLMインスタンス起動
  const startLocalLLMInstance = async () => {
    setIsStartingInstance(true)
    console.log('🚀 ローカルLLMインスタンス起動開始...')

    // タイムアウト設定 (2分)
    startTimeoutRef.current = setTimeout(() => {
      console.log('⏰ 起動処理タイムアウト - フラグをリセットします')
      setIsStartingInstance(false)
      setProviders(prev => prev.map(p =>
        p.id === 'local-llm' ? { ...p, status: 'unavailable' } : p
      ))
      startTimeoutRef.current = null
    }, 120000)

    try {
      const response = await fetch(getInstanceManageUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'start', instance_name: 'llm-gpu-instance' }),
      })

      const responseData = await response.json()
      console.log('📡 API Response:', responseData)

      if (!response.ok) {
        throw new Error(`起動要求失敗: ${response.status} - ${responseData.error?.message || 'Unknown error'}`)
      }

      console.log('✅ 起動要求が正常に送信されました。状態監視を継続します...')
      // 起動処理中の状態を設定
      setProviders(prev => prev.map(p =>
        p.id === 'local-llm' ? { ...p, status: 'starting' } : p
      ))
      // 即座にポーリングを実行し、その後10秒間隔で継続
      setTimeout(fetchProviderStatus, 2000) // 2秒後
      setTimeout(fetchProviderStatus, 5000) // 5秒後
      setTimeout(fetchProviderStatus, 10000) // 10秒後

    } catch (error) {
      console.error('🔴 ローカルLLMインスタンス起動エラー:', error)

      // リソース不足エラーの特別な処理
      let errorStatus = 'error'
      if (error.message.includes('ZONE_RESOURCE_POOL_EXHAUSTED') || error.message.includes('RESOURCE_EXHAUSTED')) {
        console.log('💡 リソース不足エラーが検出されました。後ほど再試行をお勧めします。')
        errorStatus = 'unavailable' // リソース不足の場合は停止状態に戻す
      }

      setProviders(prev => prev.map(p =>
        p.id === 'local-llm' ? { ...p, status: errorStatus } : p
      ))
      setIsStartingInstance(false)
      if (startTimeoutRef.current) {
        clearTimeout(startTimeoutRef.current)
        startTimeoutRef.current = null
      }
    }
    // ここではsetIsStartingInstance(false)を呼ばない - ポーリングで状態を管理
  }

  // ローカルLLMインスタンス停止
  const stopLocalLLMInstance = async () => {
    setIsStoppingInstance(true)
    console.log('🛑 ローカルLLMインスタンス停止開始...')

    // タイムアウト設定 (3分)
    stopTimeoutRef.current = setTimeout(() => {
      console.log('⏰ 停止処理タイムアウト - フラグをリセットします')
      setIsStoppingInstance(false)
      stopTimeoutRef.current = null
    }, 180000)

    try {
      const response = await fetch(getInstanceManageUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'stop', instance_name: 'llm-gpu-instance' }),
      })

      const responseData = await response.json()
      console.log('📡 API Response:', responseData)

      if (!response.ok) {
        throw new Error(`停止要求失敗: ${response.status} - ${responseData.error?.message || 'Unknown error'}`)
      }

      console.log('✅ 停止要求が正常に送信されました。状態監視を継続します...')
      // 停止処理中の状態を設定
      setProviders(prev => prev.map(p =>
        p.id === 'local-llm' ? { ...p, status: 'stopping' } : p
      ))
      // 即座にポーリングを実行し、その後10秒間隔で継続
      setTimeout(fetchProviderStatus, 2000) // 2秒後
      setTimeout(fetchProviderStatus, 5000) // 5秒後
      setTimeout(fetchProviderStatus, 10000) // 10秒後

    } catch (error) {
      console.error('🔴 ローカルLLMインスタンス停止エラー:', error)
      setProviders(prev => prev.map(p =>
        p.id === 'local-llm' ? { ...p, status: 'error' } : p
      ))
      setIsStoppingInstance(false)
      if (stopTimeoutRef.current) {
        clearTimeout(stopTimeoutRef.current)
        stopTimeoutRef.current = null
      }
    }
    // ここではsetIsStoppingInstance(false)を呼ばない - ポーリングで状態を管理
  }

  // 初回読み込み時と定期的なポーリング
  // 初回マウント時のみ実行（フラグをリセットしてから状態取得）
  useEffect(() => {
    console.log('🔄 コンポーネント初期化 - フラグをリセット')

    // 強制的にフラグをリセット（重要）
    setIsStartingInstance(false)
    setIsStoppingInstance(false)
    isStartingRef.current = false
    isStoppingRef.current = false

    // 初回ステータス取得
    fetchProviderStatus()

    const interval = setInterval(fetchProviderStatus, 10000) // 10秒ごとに状態を更新
    return () => clearInterval(interval) // コンポーネントのアンマウント時にクリーンアップ
  }, []) // 依存配列を空にして初回のみ実行

  // 起動・停止処理中は更に頻繁にポーリング
  useEffect(() => {
    if (isStartingInstance || isStoppingInstance) {
      console.log(`⏱️  処理中のため高頻度ポーリング開始 (起動中: ${isStartingInstance}, 停止中: ${isStoppingInstance})`)
      const fastInterval = setInterval(() => {
        console.log(`🔄 処理中ポーリング実行中... (起動中: ${isStartingInstance}, 停止中: ${isStoppingInstance})`)
        fetchProviderStatus()
      }, 10000) // 10秒ごと（処理中でも同じ間隔）
      return () => {
        console.log('⏹️  高頻度ポーリング終了')
        clearInterval(fastInterval)
      }
    }
  }, [isStartingInstance, isStoppingInstance, fetchProviderStatus])

  // ステータスアイコンとカラー
  const getStatusDisplay = (status: LLMProvider['status']) => {
    switch (status) {
      case 'available':
        return {
          icon: <CheckCircle2 className="h-4 w-4 text-green-500" />,
          text: '利用可能',
          color: 'text-green-600',
          bgColor: 'bg-green-50 border-green-200'
        }
      case 'starting':
        return {
          icon: <Loader2 className="h-4 w-4 text-yellow-500 animate-spin" />,
          text: '起動中...',
          color: 'text-yellow-600',
          bgColor: 'bg-yellow-50 border-yellow-200'
        }
      case 'stopping':
        return {
          icon: <Loader2 className="h-4 w-4 text-orange-500 animate-spin" />,
          text: '停止中...',
          color: 'text-orange-600',
          bgColor: 'bg-orange-50 border-orange-200'
        }
      case 'unavailable':
        return {
          icon: <AlertCircle className="h-4 w-4 text-gray-500" />,
          text: '利用不可',
          color: 'text-gray-600',
          bgColor: 'bg-gray-50 border-gray-200'
        }
      case 'error':
        return {
          icon: <AlertCircle className="h-4 w-4 text-red-500" />,
          text: 'エラー',
          color: 'text-red-600',
          bgColor: 'bg-red-50 border-red-200'
        }
    }
  }

  return (
    <Card className={`${className}`}>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Cpu className="h-5 w-5 text-blue-500" />
          LLMプロバイダー選択
          <Button
            variant="ghost"
            size="sm"
            onClick={fetchProviderStatus}
            disabled={loading}
            className="ml-auto"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </Button>
        </CardTitle>
        <CardDescription className="flex items-center gap-2">
          用途に応じてAIプロバイダーを選択してください
          {lastUpdated && (
            <span className="text-xs text-muted-foreground">
              最終更新: {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </CardDescription>
      </CardHeader>

      <CardContent>
        <RadioGroup
          value={selectedProvider}
          onValueChange={onProviderChange}
          disabled={disabled}
          className="space-y-4"
        >
          {providers.map((provider) => {
            const statusDisplay = getStatusDisplay(provider.status)
            const isProviderUnavailable = provider.status === 'unavailable'
            const isProviderAvailable = provider.status === 'available'
            const isStarting = provider.id === 'local-llm' && isStartingInstance
            const isStopping = provider.status === 'stopping' || (provider.id === 'local-llm' && isStoppingInstance)

            // ローカルLLMの場合、処理中は選択を無効化
            const isLocalLLMProcessing = provider.id === 'local-llm' && (isStarting || isStopping)
            const isDisabled = disabled || (provider.id === 'local-llm' && ['starting', 'stopping'].includes(provider.status))
            const canSelectProvider = !isDisabled

            return (
              <div key={provider.id} className="space-y-2">
                <Label
                  htmlFor={provider.id}
                  className={`flex items-center space-x-3 p-4 rounded-lg border-2 transition-all ${
                    selectedProvider === provider.id
                      ? 'border-blue-500 bg-blue-50'
                      : statusDisplay.bgColor
                  } ${
                    !canSelectProvider
                      ? 'opacity-50 cursor-not-allowed'
                      : 'cursor-pointer hover:border-blue-300'
                  }`}
                >
                  <RadioGroupItem
                    value={provider.id}
                    id={provider.id}
                    disabled={!canSelectProvider}
                    className="mt-0.5"
                  />

                  <div className="flex-1 space-y-2">
                    {/* プロバイダー名と状態 */}
                    <div className="flex items-center justify-between">
                      <span className="font-semibold text-lg">{provider.name}</span>
                      <div className="flex items-center gap-1">
                        {statusDisplay.icon}
                        <span className={`text-sm font-medium ${statusDisplay.color}`}>
                          {statusDisplay.text}
                        </span>
                      </div>
                    </div>

                    {/* 説明 */}
                    <p className={`text-sm ${!canSelectProvider ? 'text-gray-400' : 'text-muted-foreground'}`}>
                      {isLocalLLMProcessing
                        ? (isStarting ? 'インスタンスを起動しています...' : 'インスタンスを停止しています...')
                        : provider.description
                      }
                    </p>

                    {/* コストと速度情報 */}
                    <div className="flex gap-4">
                      <div className="flex items-center gap-1">
                        <DollarSign className="h-3 w-3 text-green-600" />
                        <span className="text-sm font-medium">{provider.cost}</span>
                      </div>

                      <div className="flex items-center gap-1">
                        <Clock className="h-3 w-3 text-blue-600" />
                        <span className="text-sm font-medium">{provider.speed}</span>
                      </div>
                    </div>
                  </div>
                </Label>

                {/* ローカルLLMが利用可能または停止中の場合の停止ボタン */}
                {provider.id === 'local-llm' && (isProviderAvailable || provider.status === 'stopping' || isStopping) && !isStarting && (
                  <div className="ml-7 p-3 bg-orange-50 border border-orange-200 rounded-md">
                    <div className="flex items-center justify-between">
                      <p className="text-sm text-orange-700">
                        {(provider.status === 'stopping' || isStopping) ? 'インスタンスを停止しています...' : 'インスタンスが稼働中です。使用しない場合は停止してください。'}
                      </p>
                      <Button
                        onClick={stopLocalLLMInstance}
                        disabled={provider.status === 'stopping' || isStopping}
                        size="sm"
                        className="ml-3 bg-orange-600 hover:bg-orange-700 text-white disabled:opacity-50"
                      >
                        {(provider.status === 'stopping' || isStopping) ? (
                          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                        ) : (
                          <Power className="h-4 w-4 mr-1" />
                        )}
                        {(provider.status === 'stopping' || isStopping) ? '停止中...' : '停止'}
                      </Button>
                    </div>
                  </div>
                )}

                {/* ローカルLLMが利用不可または起動中の場合の起動ボタン */}
                {provider.id === 'local-llm' && (isProviderUnavailable || provider.status === 'starting' || isStarting) && !isStopping && provider.status !== 'stopping' && (
                  <div className="ml-7 p-3 bg-blue-50 border border-blue-200 rounded-md">
                    <div className="flex items-center justify-between">
                      <p className="text-sm text-blue-700">
                        {(provider.status === 'starting' || isStarting) ? 'インスタンスを起動しています...' : 'インスタンスは現在停止中です。'}
                      </p>
                      <Button
                        onClick={startLocalLLMInstance}
                        disabled={provider.status === 'starting' || isStarting}
                        size="sm"
                        className="ml-3 bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50"
                      >
                        {(provider.status === 'starting' || isStarting) ? (
                          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                        ) : (
                          <Zap className="h-4 w-4 mr-1" />
                        )}
                        {(provider.status === 'starting' || isStarting) ? '起動中...' : '起動'}
                      </Button>
                    </div>
                  </div>
                )}

                {/* エラー状態の場合の追加情報 */}
                {provider.status === 'error' && (
                  <div className="ml-7 p-3 bg-red-50 border border-red-200 rounded-md">
                    <p className="text-sm text-red-700">
                      エラーが発生しました。しばらくしてから更新ボタンを押してください。
                    </p>
                  </div>
                )}
              </div>
            )
          })}
        </RadioGroup>

        {/* 選択中プロバイダーの詳細情報 */}
        <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <h4 className="font-medium text-blue-900 mb-2">選択中のプロバイダー</h4>
          {(() => {
            const selected = providers.find(p => p.id === selectedProvider)
            if (!selected) return null

            return (
              <div className="space-y-1">
                <p className="text-sm text-blue-800">
                  <strong>{selected.name}</strong> - {selected.description}
                </p>
                <div className="flex gap-4 text-sm text-blue-700">
                  <span>コスト: {selected.cost}</span>
                  <span>速度: {selected.speed}</span>
                </div>
                {selectedProvider === 'local-llm' && selected.status === 'unavailable' && (
                  <p className="text-sm text-blue-600 mt-2">
                    ℹ️ ローカルLLMを選択した場合、生成時に自動的に起動されます
                  </p>
                )}
              </div>
            )
          })()}
        </div>
      </CardContent>
    </Card>
  )
}