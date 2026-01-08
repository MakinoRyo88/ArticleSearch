'use client'

import React, { useState, useEffect } from 'react'
import Link from 'next/link'
import { ArrowLeft, RefreshCw, Activity, BarChart3, Clock, DollarSign } from 'lucide-react'
import {
  getMonitoringSummary,
  getProviderComparison,
  MonitoringSummary,
  ProviderComparison,
  CostSavings,
  ApiResponse
} from '@/lib/api'

interface TimeRangeOption {
  value: string
  label: string
}

const timeRangeOptions: TimeRangeOption[] = [
  { value: '1h', label: '1時間' },
  { value: '24h', label: '24時間' },
  { value: '7d', label: '7日間' },
  { value: '30d', label: '30日間' }
]

export default function MonitoringDashboard() {
  const [selectedTimeRange, setSelectedTimeRange] = useState('24h')
  const [summary, setSummary] = useState<MonitoringSummary | null>(null)
  const [providerData, setProviderData] = useState<ProviderComparison[]>([])
  const [costSavings, setCostSavings] = useState<CostSavings | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date())

  const fetchData = async () => {
    setLoading(true)
    setError(null)

    try {
      const [summaryResponse, comparisonResponse] = await Promise.all([
        getMonitoringSummary(selectedTimeRange),
        getProviderComparison(selectedTimeRange)
      ])

      if (summaryResponse.success && summaryResponse.data) {
        setSummary(summaryResponse.data)
      } else {
        setError(summaryResponse.error?.message || 'サマリー取得に失敗しました')
      }

      if (comparisonResponse.success && comparisonResponse.data) {
        setProviderData(comparisonResponse.data.providers)
        setCostSavings(comparisonResponse.data.cost_savings)
      } else {
        setError(comparisonResponse.error?.message || 'プロバイダー比較データの取得に失敗しました')
      }

      setLastUpdated(new Date())
    } catch (err) {
      setError('データの取得中にエラーが発生しました')
      console.error('Monitoring data fetch error:', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
  }, [selectedTimeRange])

  const formatCost = (cost: number) => {
    return `¥${cost.toFixed(2)}`
  }

  const formatResponseTime = (time: number) => {
    return `${Math.round(time)}ms`
  }

  const getProviderDisplayName = (provider: string) => {
    switch (provider) {
      case 'vertex-ai':
        return 'Vertex AI'
      case 'local-llm':
        return 'ローカルLLM'
      default:
        return provider
    }
  }

  const getProviderStatus = (provider: ProviderComparison) => {
    const now = new Date()
    const lastUsed = new Date(provider.last_used)
    const hoursSinceLastUse = (now.getTime() - lastUsed.getTime()) / (1000 * 60 * 60)

    if (hoursSinceLastUse < 1) {
      return { status: 'active', color: 'text-green-600 bg-green-100' }
    } else if (hoursSinceLastUse < 24) {
      return { status: 'idle', color: 'text-yellow-600 bg-yellow-100' }
    } else {
      return { status: 'inactive', color: 'text-gray-600 bg-gray-100' }
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* ヘッダー */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="inline-flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
              >
                <ArrowLeft className="h-5 w-5" />
                ホームに戻る
              </Link>
              <h1 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
                <Activity className="h-6 w-6 text-blue-600" />
                LLMプロバイダー監視ダッシュボード
              </h1>
            </div>

            <div className="flex items-center gap-4">
              <select
                value={selectedTimeRange}
                onChange={(e) => setSelectedTimeRange(e.target.value)}
                className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                {timeRangeOptions.map(option => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>

              <button
                onClick={fetchData}
                disabled={loading}
                className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                更新
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* エラー表示 */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-red-800">{error}</p>
          </div>
        )}

        {/* 概要統計 */}
        {summary && (
          <div className="mb-8 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">総リクエスト数</p>
                  <p className="text-2xl font-bold text-gray-900">{summary.overview.total_requests}</p>
                </div>
                <BarChart3 className="h-8 w-8 text-blue-600" />
              </div>
            </div>

            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">総コスト</p>
                  <p className="text-2xl font-bold text-gray-900">{formatCost(summary.overview.total_cost)}</p>
                </div>
                <DollarSign className="h-8 w-8 text-green-600" />
              </div>
            </div>

            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">平均レスポンス時間</p>
                  <p className="text-2xl font-bold text-gray-900">{formatResponseTime(summary.overview.average_response_time)}</p>
                </div>
                <Clock className="h-8 w-8 text-yellow-600" />
              </div>
            </div>

            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">アクティブプロバイダー</p>
                  <p className="text-2xl font-bold text-gray-900">{summary.overview.active_providers}</p>
                </div>
                <Activity className="h-8 w-8 text-purple-600" />
              </div>
            </div>
          </div>
        )}

        {/* コスト削減効果 */}
        {costSavings && (
          <div className="mb-8 bg-gradient-to-r from-green-50 to-blue-50 p-6 rounded-lg border border-green-200">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">💰 コスト削減効果</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <p className="text-sm text-gray-600">Vertex AIのみ使用した場合のコスト</p>
                <p className="text-xl font-bold text-gray-700">{formatCost(costSavings.estimated_vertex_ai_only_cost)}</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">実際の総コスト</p>
                <p className="text-xl font-bold text-green-600">{formatCost(costSavings.actual_total_cost)}</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">削減効果</p>
                <p className="text-xl font-bold text-green-600">
                  {formatCost(costSavings.savings_amount)} ({costSavings.savings_percentage}%削減)
                </p>
              </div>
            </div>
          </div>
        )}

        {/* プロバイダー比較 */}
        <div className="bg-white rounded-lg border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">📊 プロバイダー比較</h2>
            <p className="text-sm text-gray-600">期間: {selectedTimeRange}</p>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    プロバイダー
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    リクエスト数
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    総コスト
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    平均レスポンス時間
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    成功率
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    状態
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {providerData.map((provider) => {
                  const status = getProviderStatus(provider)
                  return (
                    <tr key={provider.provider}>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="font-medium text-gray-900">
                          {getProviderDisplayName(provider.provider)}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-gray-900">
                        {provider.total_requests.toLocaleString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-gray-900">
                        {formatCost(provider.total_cost)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-gray-900">
                        {formatResponseTime(provider.avg_response_time)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-gray-900">
                        {(provider.success_rate * 100).toFixed(1)}%
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${status.color}`}>
                          {status.status}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>

        {/* 最終更新時刻 */}
        <div className="mt-6 text-center">
          <p className="text-sm text-gray-500">
            最終更新: {lastUpdated.toLocaleString('ja-JP')}
          </p>
        </div>
      </main>
    </div>
  )
}