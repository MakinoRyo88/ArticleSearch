import os
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
import functions_framework
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from threading import Lock
from urllib.parse import urlparse
import time
import json

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数
SOURCE_PROJECT_ID = 'mcs-fs-pro'
SOURCE_DATASET_ID = 'analytics_250893262'
TARGET_PROJECT_ID = os.environ.get('GCP_PROJECT', 'seo-optimize-464208')
TARGET_DATASET_ID = 'content_analysis'

# GA4から取得する日数 (環境変数から取得、デフォルトは7日)
GA4_DAYS_BACK = int(os.environ.get('GA4_DAYS_BACK', '7'))

# GA4データ取得のLIMIT (環境変数から取得、デフォルトはNone=制限なし)
# 'None' や空文字列が設定された場合は制限なし、数字が設定された場合はその値で制限
ga4_limit_str = os.environ.get('GA4_LIMIT')
GA4_LIMIT: Optional[int] = int(ga4_limit_str) if ga4_limit_str and ga4_limit_str.isdigit() else None

# 最適化されたバッチサイズ
PARALLEL_BATCH_SIZE = 50
BULK_INSERT_BATCH_SIZE = 500
MAX_WORKERS = 2
TIMEOUT_SECONDS = 480  # 8分のタイムアウト

class GA4DataSync:
    def __init__(self):
        self.source_client = bigquery.Client(project=SOURCE_PROJECT_ID)
        self.target_client = bigquery.Client(project=TARGET_PROJECT_ID)
        self.lock = Lock()
        self.start_time = datetime.now()
        
    def check_timeout(self, operation_name: str) -> bool:
        """タイムアウトチェック"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed > TIMEOUT_SECONDS:
            logger.warning(f"{operation_name}: タイムアウト間近 ({elapsed:.2f}秒経過)")
            return True
        return False
    
    def get_available_ga4_tables(self, days_back: int = 7) -> List[str]:
        """利用可能なGA4テーブルを取得"""
        try:
            end_date = datetime.now()
            available_tables = []
            
            # 過去数日分のテーブルをチェック
            for i in range(days_back):
                check_date = end_date - timedelta(days=i)
                table_suffix = check_date.strftime('%Y%m%d')
                table_name = f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}"
                
                try:
                    # テーブルの存在確認
                    table_ref = self.source_client.get_table(table_name)
                    available_tables.append(table_suffix)
                    logger.info(f"利用可能なテーブル: events_{table_suffix}")
                except Exception:
                    logger.warning(f"テーブルが見つかりません: events_{table_suffix}")
            
            return available_tables
            
        except Exception as e:
            logger.error(f"テーブル確認エラー: {str(e)}")
            return []
    
    def get_ga4_pageviews_optimized(self, days_back: int = None) -> List[Dict]:
        """最適化されたGA4データ取得（利用可能なテーブルのみ使用）"""
        # GA4_DAYS_BACK をデフォルト値として使用
        if days_back is None:
            days_back = GA4_DAYS_BACK
            
        if self.check_timeout("GA4データ取得開始"):
            raise TimeoutError("GA4データ取得でタイムアウト")
        
        # 利用可能なテーブルを取得
        available_tables = self.get_available_ga4_tables(days_back + 2) # 念のため2日余分にチェック
        
        if not available_tables:
            logger.error("利用可能なGA4テーブルがありません")
            return []
        
        # 指定された日数分のテーブルを使用
        tables_to_use = available_tables[:days_back]
        logger.info(f"使用するテーブル（{len(tables_to_use)}日分）: {tables_to_use}")
        
        if not tables_to_use:
            logger.warning("指定された日数に対応する利用可能なテーブルがありません。")
            return []

        # 複数テーブルからデータを取得するクエリに変更
        table_queries = []
        for table_suffix in tables_to_use:
            table_queries.append(f"""
            SELECT
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
                CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
                traffic_source.medium as traffic_medium,
                COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'), 0) as engagement_time_msec,
                event_name
            FROM `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}`
            WHERE (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%foresight.jp%column%'
            """)
        
        # GA4_LIMIT の値に基づいて LIMIT 句を生成
        limit_clause = f"LIMIT {GA4_LIMIT}" if GA4_LIMIT is not None else ""

        query = f"""
        WITH combined_events AS (
            {' UNION ALL '.join(table_queries)}
        )
        SELECT
            page_location,
            COUNTIF(event_name = 'page_view') as pageviews,
            COUNT(DISTINCT IF(traffic_medium = 'organic', session_id, NULL)) as organic_sessions,
            COUNT(DISTINCT IF(event_name = 'user_engagement', session_id, NULL)) as engaged_sessions,
            SUM(engagement_time_msec) as total_engagement_time_msec,
            COUNT(DISTINCT session_id) as total_sessions
        FROM combined_events
        WHERE
            page_location IS NOT NULL
            AND REGEXP_CONTAINS(page_location, r'https://www\.foresight\.jp/[^/]+/column/[^?#]+')
        GROUP BY page_location
        HAVING pageviews >= 1
        ORDER BY pageviews DESC
        {limit_clause}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                use_legacy_sql=False,
                maximum_bytes_billed=2 * 10**9,  # 2GB制限に削減
                dry_run=False
            )
            
            logger.info("GA4クエリ実行開始")
            query_job = self.source_client.query(query, job_config=job_config)
            
            # より短い間隔でジョブの状態を監視
            timeout_count = 0
            while query_job.state != 'DONE':
                if timeout_count > 10:  # 60秒待機に短縮
                    logger.warning("GA4クエリをキャンセル中...")
                    query_job.cancel()
                    raise TimeoutError("GA4クエリがタイムアウトしました")
                time.sleep(6)
                timeout_count += 1
                logger.info(f"GA4クエリ実行中... ({timeout_count * 6}秒経過)")
                
            results = query_job.result(timeout=30)
            
            pageviews_data = []
            for row in results:
                pageviews_data.append({
                    'page_location': row.page_location,
                    'pageviews': row.pageviews,
                    'organic_sessions': row.organic_sessions,
                    'engaged_sessions': row.engaged_sessions,
                    'total_engagement_time_msec': row.total_engagement_time_msec,
                    'total_sessions': row.total_sessions
                })
            
            logger.info(f"GA4から {len(pageviews_data)} 件のページビューデータを取得")
            return pageviews_data
            
        except Exception as e:
            logger.error(f"GA4データ取得エラー: {str(e)}")
            # 複数テーブルでフォールバックを試行
            # フォールバック時も GA4_DAYS_BACK を考慮するように修正
            return self.get_ga4_pageviews_multi_table_fallback(available_tables[:GA4_DAYS_BACK])

    def get_ga4_pageviews_multi_table_fallback(self, available_tables: List[str]) -> List[Dict]:
        """複数テーブルを使用したフォールバック処理"""
        logger.warning("複数テーブルフォールバックモードでGA4データを取得")
        
        if not available_tables:
            return []
        
        # 最大3つのテーブルを使用 (または GA4_DAYS_BACK の日数分)
        tables_to_use = available_tables[:max(3, GA4_DAYS_BACK)] # 少なくとも3つ、または指定された日数分
        logger.info(f"使用するテーブル: {tables_to_use}")
        
        # UNION ALLを使用して複数テーブルから取得
        table_queries = []
        for table_suffix in tables_to_use:
            table_queries.append(f"""
            SELECT
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
                CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
                traffic_source.medium as traffic_medium,
                COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'), 0) as engagement_time_msec,
                event_name
            FROM `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}`
            WHERE (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%foresight.jp%column%'
            """)
        
        # フォールバック用のLIMIT (デフォルトは2000、GA4_LIMITが設定されていればそれを使用)
        fallback_limit = 2000 if GA4_LIMIT is None else GA4_LIMIT
        limit_clause = f"LIMIT {fallback_limit}" if fallback_limit is not None else ""

        query = f"""
        WITH combined_events AS (
            {' UNION ALL '.join(table_queries)}
        )
        SELECT
            page_location,
            COUNTIF(event_name = 'page_view') as pageviews,
            COUNT(DISTINCT IF(traffic_medium = 'organic', session_id, NULL)) as organic_sessions,
            COUNT(DISTINCT IF(event_name = 'user_engagement', session_id, NULL)) as engaged_sessions,
            SUM(engagement_time_msec) as total_engagement_time_msec,
            COUNT(DISTINCT session_id) as total_sessions
        FROM combined_events
        WHERE
            page_location IS NOT NULL
            AND REGEXP_CONTAINS(page_location, r'https://www\.foresight\.jp/[^/]+/column/[^?#]+')
        GROUP BY page_location
        HAVING pageviews >= 1
        ORDER BY pageviews DESC
        {limit_clause}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                maximum_bytes_billed=1 * 10**9,  # 1GB制限
            )
            
            query_job = self.source_client.query(query, job_config=job_config)
            
            # より短いタイムアウトでジョブを監視
            timeout_count = 0
            while query_job.state != 'DONE':
                if timeout_count > 8:  # 48秒待機
                    query_job.cancel()
                    raise TimeoutError("フォールバッククエリがタイムアウトしました")
                time.sleep(6)
                timeout_count += 1
                logger.info(f"フォールバッククエリ実行中... ({timeout_count * 6}秒経過)")
                
            results = query_job.result(timeout=30)
            
            pageviews_data = []
            for row in results:
                pageviews_data.append({
                    'page_location': row.page_location,
                    'pageviews': row.pageviews,
                    'organic_sessions': row.organic_sessions,
                    'engaged_sessions': row.engaged_sessions,
                    'total_engagement_time_msec': row.total_engagement_time_msec,
                    'total_sessions': row.total_sessions
                })
            
            logger.info(f"フォールバックモードで {len(pageviews_data)} 件取得")
            return pageviews_data
            
        except Exception as e:
            logger.error(f"フォールバックモードでもエラー: {str(e)}")
            # 最後の手段として最小限のデータを取得
            return self.get_ga4_pageviews_minimal(available_tables[0] if available_tables else None)

    def get_ga4_pageviews_minimal(self, table_suffix: str) -> List[Dict]:
        """最小限のGA4データ取得"""
        if not table_suffix:
            return []
            
        logger.warning("最小限モードでGA4データを取得")
        
        # 最小限モードのLIMIT (デフォルトは500、GA4_LIMITが設定されていればそれを使用)
        minimal_limit = 500 if GA4_LIMIT is None else GA4_LIMIT
        limit_clause = f"LIMIT {minimal_limit}" if minimal_limit is not None else ""

        # 非常にシンプルなクエリ
        query = f"""
        WITH combined_events AS (
           SELECT
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
                CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
                traffic_source.medium as traffic_medium,
                COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'), 0) as engagement_time_msec,
                event_name
            FROM `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}`
            WHERE (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%foresight.jp%column%'
        )
        SELECT
            page_location,
            COUNTIF(event_name = 'page_view') as pageviews,
            COUNT(DISTINCT IF(traffic_medium = 'organic', session_id, NULL)) as organic_sessions,
            COUNT(DISTINCT IF(event_name = 'user_engagement', session_id, NULL)) as engaged_sessions,
            SUM(engagement_time_msec) as total_engagement_time_msec,
            COUNT(DISTINCT session_id) as total_sessions
        FROM combined_events
        WHERE
            page_location IS NOT NULL
        GROUP BY page_location
        HAVING pageviews >= 1
        ORDER BY pageviews DESC
        {limit_clause}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                maximum_bytes_billed=500 * 10**6,  # 500MB制限
            )
            
            query_job = self.source_client.query(query, job_config=job_config)
            results = query_job.result(timeout=45)
            
            pageviews_data = []
            for row in results:
                 pageviews_data.append({
                    'page_location': row.page_location,
                    'pageviews': row.pageviews,
                    'organic_sessions': row.organic_sessions,
                    'engaged_sessions': row.engaged_sessions,
                    'total_engagement_time_msec': row.total_engagement_time_msec,
                    'total_sessions': row.total_sessions
                })
            
            logger.info(f"最小限モードで {len(pageviews_data)} 件取得")
            return pageviews_data
            
        except Exception as e:
            logger.error(f"最小限モードでもエラー: {str(e)}")
            return []

    def get_courses_articles_mapping_optimized(self) -> Dict:
        """最適化されたマッピングデータ取得"""
        if self.check_timeout("マッピングデータ取得開始"):
            raise TimeoutError("マッピングデータ取得でタイムアウト")
        
        # まずテーブルの存在確認
        try:
            # coursesテーブルの確認
            courses_query = f"""
            SELECT COUNT(*) as count
            FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.courses`
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(use_query_cache=True)
            query_job = self.target_client.query(courses_query, job_config=job_config)
            courses_result = query_job.result(timeout=20)
            courses_count = list(courses_result)[0].count
            logger.info(f"coursesテーブル: {courses_count} 件")
            
            # articlesテーブルの確認
            articles_query = f"""
            SELECT COUNT(*) as count
            FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles`
            LIMIT 1
            """
            
            query_job = self.target_client.query(articles_query, job_config=job_config)
            articles_result = query_job.result(timeout=20)
            articles_count = list(articles_result)[0].count
            logger.info(f"articlesテーブル: {articles_count} 件")
            
            if courses_count == 0 or articles_count == 0:
                logger.error(f"テーブルにデータがありません: courses={courses_count}, articles={articles_count}")
                return {}
                
        except Exception as e:
            logger.error(f"テーブル確認エラー: {str(e)}")
            return {}
            
        # メインクエリを段階的に実行
        try:
            # まず結合前の基本データを確認
            basic_query = f"""
            SELECT
                c.slug as course_slug,
                c.id as course_id,
                COUNT(a.id) as article_count
            FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.courses` c
            LEFT JOIN `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles` a
            ON c.id = a.koza_id
            WHERE c.slug IS NOT NULL AND c.slug != ''
            GROUP BY c.slug, c.id
            HAVING article_count > 0
            LIMIT 10
            """
            
            job_config = bigquery.QueryJobConfig(use_query_cache=True)
            query_job = self.target_client.query(basic_query, job_config=job_config)
            basic_results = query_job.result(timeout=30)
            
            basic_data = list(basic_results)
            logger.info(f"基本結合データ: {len(basic_data)} 件")
            
            if len(basic_data) == 0:
                logger.error("coursesとarticlesの結合でデータが取得できません")
                return {}
            
            # マッピングデータのLIMIT (デフォルトは10000、GA4_LIMITが設定されていればそれを使用)
            # ここはGA4_LIMITではなく、別途マッピングデータのLIMITを考慮すべきですが、
            # GA4_LIMITと連動させる場合は以下のように調整
            mapping_limit_clause = f"LIMIT 10000" # デフォルトのマッピング制限は維持

            query = f"""
            SELECT
                c.slug as course_slug,
                a.id as article_id,
                a.link as article_link,
                a.title as article_title,
                COALESCE(a.pageviews, 0) as current_pageviews,
                COALESCE(a.organic_sessions, 0) as current_organic_sessions,
                COALESCE(a.engaged_sessions, 0) as current_engaged_sessions,
                COALESCE(a.avg_engagement_time, 0) as current_avg_engagement_time
            FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.courses` c
            JOIN `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles` a
            ON c.id = a.koza_id
            WHERE 
                c.slug IS NOT NULL 
                AND c.slug != ''
                AND a.link IS NOT NULL
                AND a.link != ''
                AND a.id IS NOT NULL
            {mapping_limit_clause}
            """
            
            job_config = bigquery.QueryJobConfig(use_query_cache=True)
            
            query_job = self.target_client.query(query, job_config=job_config)
            results = query_job.result(timeout=45)
            
            mapping = {}
            row_count = 0
            
            for row in results:
                row_count += 1
                
                # linkカラムの値を正規化
                article_link = row.article_link.strip()
                if not article_link.endswith('/'):
                    article_link += '/'
                
                # パターンを生成
                pattern = f"{row.course_slug}/column/{article_link}"
                
                mapping[pattern] = {
                    'article_id': row.article_id,
                    'course_slug': row.course_slug,
                    'article_link': article_link,
                    'article_title': row.article_title,
                    'current_pageviews': row.current_pageviews,
                    'current_organic_sessions': row.current_organic_sessions,
                    'current_engaged_sessions': row.current_engaged_sessions,
                    'current_avg_engagement_time': row.current_avg_engagement_time
                }
                
                # 最初の5件のデータをログ出力
                if row_count <= 5:
                    logger.info(f"マッピング例 {row_count}: {pattern} -> {row.article_id}")
            
            logger.info(f"記事マッピングデータを {len(mapping)} 件取得（処理行数: {row_count}）")
            
            if len(mapping) == 0:
                logger.error("マッピングデータが0件です。クエリ結果をデバッグします")
                # デバッグ用のサンプルクエリ
                debug_query = f"""
                SELECT
                    c.slug as course_slug,
                    a.id as article_id,
                    a.link as article_link,
                    a.title as article_title
                FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.courses` c
                JOIN `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles` a
                ON c.id = a.koza_id
                LIMIT 3
                """
                
                debug_job = self.target_client.query(debug_query, job_config=job_config)
                debug_results = debug_job.result(timeout=30)
                
                debug_rows = list(debug_results)
                logger.info(f"デバッグ: 結合結果サンプル {len(debug_rows)} 件")
                for i, row in enumerate(debug_rows):
                    logger.info(f"デバッグ {i+1}: course_slug='{row.course_slug}', article_id='{row.article_id}', link='{row.article_link}'")
            
            return mapping
            
        except Exception as e:
            logger.error(f"マッピングデータ取得エラー: {str(e)}")
            logger.error(f"エラー詳細: {type(e).__name__}")
            return {}

    def normalize_ga4_url(self, url: str) -> Optional[str]:
        """GA4のURLを正規化してパスを抽出"""
        if not url:
            return None
        
        try:
            parsed_url = urlparse(url)
            
            if parsed_url.netloc != 'www.foresight.jp':
                return None
            
            path = parsed_url.path.strip('/')
            
            if '/column/' not in path:
                return None
            
            if path and not path.endswith('/'):
                path += '/'
            
            return path
                
        except Exception as e:
            logger.debug(f"URL正規化エラー for '{url}': {str(e)}")
            return None

    def match_urls_and_aggregate_optimized(self, ga4_data: List[Dict], url_patterns: Dict) -> List[Dict]:
        """最適化されたURLマッチングとページビュー集計"""
        if self.check_timeout("URLマッチング開始"):
            raise TimeoutError("URLマッチングでタイムアウト")
            
        # 正規化されたパスごとのメトリクスを集計
        path_metrics = {}
        
        logger.info(f"GA4データ正規化開始: {len(ga4_data)} 件")
        
        # バッチ処理で正規化
        batch_size = 1000
        for i in range(0, len(ga4_data), batch_size):
            batch = ga4_data[i:i+batch_size]
            
            for ga4_item in batch:
                ga4_url = ga4_item['page_location']
                normalized_path = self.normalize_ga4_url(ga4_url)
                
                if normalized_path:
                    if normalized_path not in path_metrics:
                        path_metrics[normalized_path] = {
                            'pageviews': 0,
                            'organic_sessions': 0,
                            'engaged_sessions': 0,
                            'total_engagement_time_msec': 0,
                            'total_sessions': 0
                        }
                    path_metrics[normalized_path]['pageviews'] += ga4_item['pageviews']
                    path_metrics[normalized_path]['organic_sessions'] += ga4_item['organic_sessions']
                    path_metrics[normalized_path]['engaged_sessions'] += ga4_item['engaged_sessions']
                    path_metrics[normalized_path]['total_engagement_time_msec'] += ga4_item['total_engagement_time_msec']
                    path_metrics[normalized_path]['total_sessions'] += ga4_item['total_sessions']

            # 進捗ログ
            if i % 2000 == 0:
                logger.info(f"正規化進捗: {i}/{len(ga4_data)}")
        
        logger.info(f"正規化後のユニークパス数: {len(path_metrics)}")
        
        # マッチング処理
        pageviews_updates = []
        matched_count = 0
        
        for normalized_path, metrics in path_metrics.items():
            if normalized_path in url_patterns:
                article_info = url_patterns[normalized_path]
                
                # 平均エンゲージメント時間を計算（秒単位）
                avg_engagement_time = (
                    metrics['total_engagement_time_msec'] / (metrics['total_sessions'] * 1000)
                    if metrics['total_sessions'] > 0 else 0
                )
                
                # 差分がある場合のみ更新対象とする
                pageviews_diff = abs(article_info['current_pageviews'] - metrics['pageviews'])
                organic_sessions_diff = abs(article_info['current_organic_sessions'] - metrics['organic_sessions'])
                engaged_sessions_diff = abs(article_info['current_engaged_sessions'] - metrics['engaged_sessions'])
                avg_engagement_time_diff = abs(article_info['current_avg_engagement_time'] - avg_engagement_time)

                if pageviews_diff > 0 or organic_sessions_diff > 0 or engaged_sessions_diff > 0 or avg_engagement_time_diff > 0.1: # 0.1秒以上の差
                    pageviews_updates.append({
                        'article_id': article_info['article_id'],
                        'pageviews': metrics['pageviews'],
                        'organic_sessions': metrics['organic_sessions'],
                        'engaged_sessions': metrics['engaged_sessions'],
                        'avg_engagement_time': avg_engagement_time,
                        'course_slug': article_info['course_slug'],
                        'article_title': article_info['article_title'][:50] + '...' if len(article_info['article_title']) > 50 else article_info['article_title']
                    })
                
                matched_count += 1
        
        logger.info(f"マッチング結果: {matched_count} 件マッチ、{len(pageviews_updates)} 件更新対象")
        
        return pageviews_updates

    def update_pageviews_batch_optimized(self, pageviews_updates: List[Dict]):
        """最適化された一括更新処理"""
        if not pageviews_updates:
            logger.info("更新対象のデータがありません")
            return
        
        if self.check_timeout("一括更新開始"):
            raise TimeoutError("一括更新でタイムアウト")
        
        # バッチサイズで分割して処理
        batch_size = BULK_INSERT_BATCH_SIZE
        total_updated = 0
        
        for i in range(0, len(pageviews_updates), batch_size):
            batch = pageviews_updates[i:i+batch_size]
            
            try:
                # 一括更新用のクエリを実行
                pageviews_cases = []
                organic_sessions_cases = []
                engaged_sessions_cases = []
                avg_engagement_time_cases = []
                article_ids = []
                
                for update in batch:
                    article_id_str = f"'{update['article_id']}'"
                    article_ids.append(article_id_str)
                    pageviews_cases.append(f"WHEN {article_id_str} THEN {update['pageviews']}")
                    organic_sessions_cases.append(f"WHEN {article_id_str} THEN {update['organic_sessions']}")
                    engaged_sessions_cases.append(f"WHEN {article_id_str} THEN {update['engaged_sessions']}")
                    avg_engagement_time_cases.append(f"WHEN {article_id_str} THEN {update['avg_engagement_time']:.4f}")

                update_query = f"""
                UPDATE `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles`
                SET 
                    pageviews = CASE id
                        {' '.join(pageviews_cases)}
                        ELSE pageviews
                    END,
                    organic_sessions = CASE id
                        {' '.join(organic_sessions_cases)}
                        ELSE organic_sessions
                    END,
                    engaged_sessions = CASE id
                        {' '.join(engaged_sessions_cases)}
                        ELSE engaged_sessions
                    END,
                    avg_engagement_time = CASE id
                        {' '.join(avg_engagement_time_cases)}
                        ELSE avg_engagement_time
                    END,
                    last_synced = CURRENT_TIMESTAMP()
                WHERE id IN ({', '.join(article_ids)})
                """
                
                job_config = bigquery.QueryJobConfig()
                
                query_job = self.target_client.query(update_query, job_config=job_config)
                query_job.result(timeout=75)
                
                total_updated += len(batch)
                logger.info(f"バッチ更新完了: {total_updated}/{len(pageviews_updates)} 件")
                
            except Exception as e:
                logger.error(f"バッチ更新エラー (バッチ {i//batch_size + 1}): {str(e)}")
                # 個別更新にフォールバック
                self.update_pageviews_individual_optimized(batch)
                total_updated += len(batch)
        
        logger.info(f"全体更新完了: {total_updated} 件")

    def update_pageviews_individual_optimized(self, pageviews_updates: List[Dict]):
        """最適化された個別更新処理"""
        updated_count = 0
        
        for update in pageviews_updates:
            if self.check_timeout("個別更新"):
                logger.warning(f"個別更新でタイムアウト: {updated_count} 件完了")
                break
                
            try:
                update_query = f"""
                UPDATE `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles`
                SET 
                    pageviews = {update['pageviews']},
                    organic_sessions = {update['organic_sessions']},
                    engaged_sessions = {update['engaged_sessions']},
                    avg_engagement_time = {update['avg_engagement_time']:.4f},
                    last_synced = CURRENT_TIMESTAMP()
                WHERE id = '{update['article_id']}'
                """
                
                job_config = bigquery.QueryJobConfig()
                
                query_job = self.target_client.query(update_query, job_config=job_config)
                query_job.result(timeout=20)
                updated_count += 1
                
            except Exception as e:
                logger.error(f"記事ID {update['article_id']} 更新エラー: {str(e)}")
        
        logger.info(f"個別更新完了: {updated_count} 件")

    def sync_pageviews_optimized(self):
        """最適化されたメイン同期処理"""
        logger.info("最適化されたGA4ページビュー同期処理開始")
        
        try:
            # 段階的にデータを取得（並行処理は避ける）
            logger.info("Step 1: マッピングデータ取得")
            mapping = self.get_courses_articles_mapping_optimized()
            
            if not mapping:
                logger.warning("マッピングデータが取得できませんでした")
                return {'status': 'failed', 'error': 'No mapping data'}
            
            logger.info(f"Step 2: GA4データ取得（{GA4_DAYS_BACK}日分）")
            # GA4_DAYS_BACK を明示的に渡す
            ga4_data = self.get_ga4_pageviews_optimized(days_back=GA4_DAYS_BACK)
            
            if not ga4_data:
                logger.warning("GA4データが取得できませんでした")
                return {'status': 'failed', 'error': 'No GA4 data'}
            
            logger.info("Step 3: URLマッチング")
            pageviews_updates = self.match_urls_and_aggregate_optimized(ga4_data, mapping)
            
            logger.info("Step 4: データ更新")
            if pageviews_updates:
                self.update_pageviews_batch_optimized(pageviews_updates)
            else:
                logger.info("更新対象データなし")
            
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            logger.info(f"最適化された同期処理完了 (実行時間: {elapsed_time:.2f}秒)")
            
            return {
                'status': 'success',
                'total_ga4_records': len(ga4_data),
                'mapped_articles': len(mapping),
                'updated_records': len(pageviews_updates),
                'execution_time_seconds': elapsed_time
            }
            
        except TimeoutError as e:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            logger.error(f"タイムアウトエラー (実行時間: {elapsed_time:.2f}秒): {str(e)}")
            return {
                'status': 'timeout',
                'error': str(e),
                'execution_time_seconds': elapsed_time
            }
            
        except Exception as e:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            logger.error(f"同期処理エラー (実行時間: {elapsed_time:.2f}秒): {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'execution_time_seconds': elapsed_time
            }

@functions_framework.http
def ga4_sync_handler(request):
    """最適化されたCloud Functions HTTPハンドラー"""
    try:
        sync_service = GA4DataSync()
        result = sync_service.sync_pageviews_optimized()
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except TimeoutError as e:
        logger.error(f"リクエストタイムアウト: {str(e)}")
        return {
            'statusCode': 408,
            'body': {'error': 'Request timeout', 'message': str(e)}
        }
        
    except Exception as e:
        logger.error(f"GA4同期処理エラー: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

@functions_framework.cloud_event
def ga4_sync_scheduler(cloud_event):
    """最適化されたCloud Scheduler用ハンドラー"""
    try:
        sync_service = GA4DataSync()
        result = sync_service.sync_pageviews_optimized()
        
        logger.info(f"スケジュール実行完了: {result}")
        
    except Exception as e:
        logger.error(f"スケジュール実行エラー: {str(e)}")
        # スケジューラーでは例外を再発生させない（リトライを避けるため）
        pass