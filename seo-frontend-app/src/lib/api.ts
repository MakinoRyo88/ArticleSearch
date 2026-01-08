/**
 * SEO最適化API クライアント
 * Cloud RunにデプロイされたAPIとの通信を管理
 */

// 直接Cloud Run APIに接続（プロキシ使用しない）
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app';

export interface ApiResponse<T> {
  success: boolean;
  message?: string;
  data?: T;
  error?: {
    message: string;
    details?: string;
  };
  metadata?: {
    timestamp: string;
  };
}

export interface Article {
  id: string;
  title: string;
  link: string;
  koza_id: string;
  koza_name: string | null;
  koza_slug: string | null;
  pageviews: number;
  engaged_sessions: number;
  avg_engagement_time?: number;
  organic_sessions?: number;
  search_keywords?: string[];
  created_at: string | { value: string };
  updated_at: string | { value: string };
  last_synced?: string | { value: string };
  has_embedding?: boolean;
  full_content?: string;
  chunk_count?: number;
}

export interface SimilarArticle extends Article {
  similarity_score: number;
  peak_similarity_score?: number;
  avg_similarity_score?: number;
  matching_base_chunks: number;
  matching_similar_chunks: number;
  actual_matching_count?: number;
  matching_ratio?: number;
  base_total_chunks?: number;
  similar_total_chunks?: number;
  recommendation_type: 'MERGE_CONTENT' | 'REDIRECT_301' | 'CROSS_LINK' | 'REVIEW' | 'MONITOR';
  recommendation_priority?: number;
  explanation_text: string;
  confidence_score: number;
  top_matching_chunks?: Array<{
    base_chunk_title: string;
    similar_chunk_title: string;
    similarity_score: number;
  }>;
}

export interface ChunkSimilarityResponse {
  base_article: Article;
  similar_articles: SimilarArticle[];
  metadata: {
    total_found: number;
    threshold_used: number;
    top_chunks_per_base: number;
    search_method: string;
    filters_applied: {
      min_pageviews: number;
    };
  };
}

export interface PaginationInfo {
  current_page: number;
  per_page: number;
  total_count: number;
  total_pages: number;
  has_next: boolean;
  has_prev: boolean;
}

export interface ArticleSearchParams {
  search?: string;
  koza_id?: string;
  min_pageviews?: number;
  max_pageviews?: number;
  min_engaged_sessions?: number;
  max_engaged_sessions?: number;
  sort?: string;
  page?: number;
  limit?: number;
}

export interface ArticleSearchResponse {
  articles: Article[];
  pagination: PaginationInfo;
  filters: Record<string, any>;
}

export interface Course {
  id: string;
  name: string;
  slug: string;
}

export interface ChunkStats {
  total_chunks: number;
  articles_with_chunks: number;
  avg_chunk_length: number;
  min_chunk_length: number;
  max_chunk_length: number;
  chunks_with_embeddings: number;
  avg_chunks_per_article: number;
  coverage_rate: number;
}

export interface SearchStats {
  total_articles: number;
  total_courses: number;
  total_pageviews: number;
  avg_pageviews: number;
  max_pageviews: number;
  min_pageviews: number;
  max_engaged_sessions: number;
}

/**
 * チャンクベース類似検索（推奨）
 */
export async function searchSimilarArticles(
  articleId: string,
  options: {
    limit?: number;
    threshold?: number;
    min_pageviews?: number;
    top_chunks?: number;
  } = {}
): Promise<ApiResponse<ChunkSimilarityResponse>> {
  const params = new URLSearchParams();
  if (options.limit) params.set('limit', String(options.limit));
  if (options.threshold) params.set('threshold', String(options.threshold));
  if (options.min_pageviews) params.set('min_pageviews', String(options.min_pageviews));
  if (options.top_chunks) params.set('top_chunks', String(options.top_chunks));

  const url = `${API_BASE_URL}/api/chunk-similarity/${articleId}?${params.toString()}`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch similar articles:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * チャンク統計情報取得
 */
export async function getChunkStats(): Promise<ApiResponse<ChunkStats>> {
  const url = `${API_BASE_URL}/api/chunk-similarity/stats`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch chunk stats:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 記事検索・一覧取得
 */
export async function searchArticles(
  params: ArticleSearchParams = {}
): Promise<ApiResponse<ArticleSearchResponse>> {
  const searchParams = new URLSearchParams();
  
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      searchParams.set(key, String(value));
    }
  });

  const url = `${API_BASE_URL}/api/search/articles?${searchParams.toString()}`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to search articles:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 特定記事の詳細取得
 */
export async function getArticle(articleId: string): Promise<ApiResponse<Article>> {
  const url = `${API_BASE_URL}/api/search/articles/${articleId}`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch article:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 講座一覧取得
 */
export async function getCourses(): Promise<ApiResponse<Course[]>> {
  const url = `${API_BASE_URL}/api/search/courses`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch courses:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 統計情報取得
 */
export async function getSearchStats(): Promise<ApiResponse<SearchStats>> {
  const url = `${API_BASE_URL}/api/search/stats`;
  
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch search stats:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * ヘルスチェック
 */
export async function healthCheck(): Promise<{
  status: string;
  timestamp: string;
  version: string;
  environment: string;
}> {
  const url = `${API_BASE_URL}/health`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      cache: 'no-store',
    });

    return await response.json();
  } catch (error) {
    console.error('Health check failed:', error);
    return {
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      version: 'unknown',
      environment: 'unknown',
    };
  }
}

// ===========================================
// 監視API関連の型定義とクライアント関数
// ===========================================

export interface MonitoringHealth {
  status: 'healthy' | 'degraded';
  timestamp: string;
  services: {
    metrics_collector: string;
    bigquery: string;
  };
  uptime: number;
  memory: {
    used: number;
    total: number;
  };
}

export interface UsageStatistic {
  provider: string;
  total_requests: number;
  total_cost: number;
  avg_response_time: number;
  avg_cost_per_request: number;
  success_rate: number;
  period_start: string;
  period_end: string;
}

export interface ProviderComparison {
  provider: string;
  total_requests: number;
  total_cost: number;
  avg_response_time: number;
  avg_cost_per_request: number;
  success_rate: number;
  last_used: string;
}

export interface CostSavings {
  estimated_vertex_ai_only_cost: number;
  actual_total_cost: number;
  savings_amount: number;
  savings_percentage: number;
}

export interface MonitoringSummary {
  time_range: string;
  overview: {
    total_requests: number;
    total_cost: number;
    average_response_time: number;
    active_providers: number;
  };
  providers: ProviderComparison[];
  generated_at: string;
}

/**
 * 監視システムヘルスチェック
 */
export async function getMonitoringHealth(): Promise<ApiResponse<MonitoringHealth>> {
  const url = `${API_BASE_URL}/api/monitoring/health`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch monitoring health:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 使用統計取得
 */
export async function getUsageStatistics(timeRange: string = '24h'): Promise<ApiResponse<UsageStatistic[]>> {
  const url = `${API_BASE_URL}/api/monitoring/usage-statistics?timeRange=${timeRange}`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch usage statistics:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * プロバイダー比較データ取得
 */
export async function getProviderComparison(timeRange: string = '24h'): Promise<ApiResponse<{
  time_range: string;
  providers: ProviderComparison[];
  cost_savings: CostSavings | null;
  generated_at: string;
}>> {
  const url = `${API_BASE_URL}/api/monitoring/provider-comparison?timeRange=${timeRange}`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch provider comparison:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * 監視サマリー取得
 */
export async function getMonitoringSummary(timeRange: string = '24h'): Promise<ApiResponse<MonitoringSummary>> {
  const url = `${API_BASE_URL}/api/monitoring/summary?timeRange=${timeRange}`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch monitoring summary:', error);
    return {
      success: false,
      error: {
        message: 'ネットワークエラーが発生しました',
        details: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

// ===========================================
// インスタンス管理API
// ===========================================

export interface InstanceStatus {
  success: boolean;
  instance_name: string;
  status: string;
  instance_data?: any;
  error?: string;
}

export interface InstanceManagementResult {
  success: boolean;
  action: string;
  instance_name: string;
  data?: any;
  error?: string;
}

/**
 * インスタンス状態取得
 */
export async function getInstanceStatus(instanceName: string = 'llm-gpu-instance'): Promise<InstanceStatus> {
  const url = `${API_BASE_URL}/api/instances/manage?instance_name=${instanceName}`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      cache: 'no-store',
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to get instance status:', error);
    return {
      success: false,
      instance_name: instanceName,
      status: 'error',
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * インスタンス管理アクション実行
 */
export async function manageInstance(
  action: 'start' | 'stop' | 'restart' | 'restore-from-snapshot',
  instanceName: string = 'llm-gpu-instance'
): Promise<InstanceManagementResult> {
  const url = `${API_BASE_URL}/api/instances/manage`;

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        action,
        instance_name: instanceName,
      }),
    });

    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`Failed to execute ${action} on instance:`, error);
    return {
      success: false,
      action,
      instance_name: instanceName,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
