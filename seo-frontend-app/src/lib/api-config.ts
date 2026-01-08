/**
 * API設定の共通定数
 */

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'https://seo-realtime-analysis-api-550580509369.asia-northeast1.run.app';

/**
 * APIエンドポイントのヘルパー関数
 */
export const getApiUrl = (path: string): string => {
  return `${API_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`;
};

/**
 * 記事検索API
 */
export const getArticleUrl = (id: string): string => {
  return getApiUrl(`/api/search/articles/${id}`);
};

/**
 * 記事一覧API
 */
export const getArticlesListUrl = (params?: URLSearchParams): string => {
  const baseUrl = getApiUrl('/api/search/articles');
  return params ? `${baseUrl}?${params.toString()}` : baseUrl;
};

/**
 * LLMプロバイダー状態API
 */
export const getProviderStatusUrl = (): string => {
  return getApiUrl('/api/instances/status');
};

/**
 * 統合提案生成API
 */
export const getExplanationsGenerateUrl = (): string => {
  return getApiUrl('/api/explanations/generate');
};

/**
 * インスタンス管理API
 */
export const getInstanceManageUrl = (params?: URLSearchParams): string => {
  const baseUrl = getApiUrl('/api/instances/manage');
  return params ? `${baseUrl}?${params.toString()}` : baseUrl;
};

/**
 * 講座一覧API
 */
export const getCoursesUrl = (): string => {
  return getApiUrl('/api/search/courses');
};

/**
 * 統計情報API
 */
export const getStatsUrl = (): string => {
  return getApiUrl('/api/search/stats');
};