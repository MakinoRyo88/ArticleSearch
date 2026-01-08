import { NextRequest, NextResponse } from 'next/server'

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { action, instance_name = 'llm-gpu-instance' } = body

    // リクエストボディのバリデーション
    if (!action || !['start', 'stop', 'restart', 'restore-from-snapshot'].includes(action)) {
      return NextResponse.json(
        { error: 'Invalid action. Must be one of: start, stop, restart, restore-from-snapshot' },
        { status: 400 }
      )
    }

    console.log(`Instance management action: ${action} for ${instance_name}`)

    try {
      let response

      switch (action) {
        case 'start':
          response = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-manager', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'start', instance_name }),
            signal: AbortSignal.timeout(300000) // 5分タイムアウト
          })
          break

        case 'stop':
          response = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-manager', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'stop', instance_name }),
            signal: AbortSignal.timeout(300000) // 5分タイムアウト
          })
          break

        case 'restart':
          response = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-manager', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'stop', instance_name }),
            signal: AbortSignal.timeout(300000) // 5分タイムアウト
          })

          if (!response.ok) {
            throw new Error('Failed to stop instance')
          }

          // 少し待ってから起動
          await new Promise(resolve => setTimeout(resolve, 10000))

          response = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-manager', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'start', instance_name }),
            signal: AbortSignal.timeout(300000) // 5分タイムアウト
          })
          break

        case 'restore-from-snapshot':
          response = await fetch('https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-manager', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              action: 'restore-from-snapshot',
              instance_name,
              snapshot_name: 'llm-disk-20251203-153422'  // 最新のスナップショット
            }),
            signal: AbortSignal.timeout(300000) // 5分タイムアウト
          })
          break

        default:
          return NextResponse.json(
            { error: 'Unknown action' },
            { status: 400 }
          )
      }

      if (response.ok) {
        const data = await response.json()
        return NextResponse.json({
          success: true,
          action,
          instance_name,
          data
        })
      } else {
        const error = await response.text()
        return NextResponse.json(
          { error: `Action ${action} failed: ${error}` },
          { status: response.status }
        )
      }

    } catch (error) {
      console.error(`Instance management error for ${action}:`, error)
      return NextResponse.json(
        { error: `Failed to execute ${action}: ${error instanceof Error ? error.message : 'Unknown error'}` },
        { status: 502 }
      )
    }

  } catch (error) {
    console.error('Instance management API error:', error)
    return NextResponse.json(
      { error: 'Invalid request body' },
      { status: 400 }
    )
  }
}

// インスタンス状態の取得
export async function GET(request: NextRequest) {
  try {
    const url = new URL(request.url)
    const instance_name = url.searchParams.get('instance_name') || 'llm-gpu-instance'

    // インスタンス情報を取得
    const instanceResponse = await fetch(`https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/instance-info?instance_name=${instance_name}`, {
      method: 'GET',
      signal: AbortSignal.timeout(30000) // 30秒タイムアウト
    })

    if (instanceResponse.ok) {
      const instanceData = await instanceResponse.json()

      return NextResponse.json({
        success: true,
        instance_name,
        status: instanceData.success ? instanceData.status || 'unknown' : 'unavailable',
        instance_data: instanceData
      })
    } else {
      return NextResponse.json({
        success: false,
        instance_name,
        status: 'unavailable',
        error: 'Instance info check failed'
      })
    }

  } catch (error) {
    console.error('Instance status check error:', error)
    return NextResponse.json({
      success: false,
      status: 'error',
      error: error instanceof Error ? error.message : 'Unknown error'
    })
  }
}