/**
 * LLMプロバイダー設定ファイル
 * Phase 2: Vertex AI vs ローカルLLM選択機能
 */

module.exports = {
  providers: {
    'vertex-ai': {
      enabled: true,
      name: 'Vertex AI',
      description: '高精度・リアルタイム処理',
      cost_per_request: 50,
      speed: '即座',
      max_tokens: 8192,
      timeout: 120000, // 2分
      priority: 1
    },
    'local-llm': {
      enabled: true,
      name: 'ローカルLLM',
      description: 'コスト効率・カスタマイズ可能',
      cost_per_request: 5,
      speed: '2-3分（起動時）',
      max_tokens: 4096,
      timeout: 300000, // 5分
      priority: 2,
      endpoint_port: 8080,
      startup_timeout: 300000, // 5分
      health_check_interval: 30000, // 30秒
      auto_shutdown_timeout: 1800000 // 30分
    }
  },

  default_provider: 'vertex-ai',
  fallback_provider: 'vertex-ai',

  // 自動選択ルール
  auto_selection_rules: {
    cost_priority: 'local-llm',
    speed_priority: 'vertex-ai',
    fallback_on_failure: true
  },

  // Cloud Functions URL（実環境で設定）
  cloud_functions: {
    start_instance: process.env.LLM_START_FUNCTION_URL || 'https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/start-llm-instance',
    stop_instance: process.env.LLM_STOP_FUNCTION_URL || 'https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/stop-llm-instance',
    get_status: process.env.LLM_STATUS_FUNCTION_URL || 'https://asia-northeast1-seo-optimize-464208.cloudfunctions.net/get-llm-status'
  },

  // Compute Engine設定
  compute_engine: {
    project_id: process.env.GCP_PROJECT || 'seo-optimize-464208',
    zone: 'asia-northeast1-c', // スクリプトと一致するようにゾーンを修正
    instance_name: 'llm-gpu-instance'
  }
}