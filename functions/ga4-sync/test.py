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

# 設定可能な日数（デフォルト値）
DEFAULT_DAYS_BACK = 7  # デフォルト7日分
GA4_DAYS_BACK = int(os.environ.get('GA4_DAYS_BACK', DEFAULT_DAYS_BACK))

# 最適化されたバッチサイズ
PARALLEL_BATCH_SIZE = 50
BULK_INSERT_BATCH_SIZE = 500
MAX_WORKERS = 2
GA4_LIMIT = 5000
TIMEOUT_SECONDS = 480  # 8分のタイムアウト

class GA4DataSync:
    def __init__(self):
        self.source_client = bigquery.Client(project=SOURCE_PROJECT_ID)
        self.target_client = bigquery.Client(project=TARGET_PROJECT_ID)
        self.lock = Lock()
        self.start_time = datetime.now()
        self.ga4_days_back = GA4_DAYS_BACK
        
    def check_timeout(self, operation_name: str) -> bool:
        """タイムアウトチェック"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed > TIMEOUT_SECONDS:
            logger.warning(f"{operation_name}: タイムアウト間近 ({elapsed:.2f}秒経過)")
            return True
        return False
    
    def get_available_ga4_tables(self, days_back: int = None) -> List[str]:
        """利用可能なGA4テーブルを取得"""
        if days_back is None:
            days_back = self.ga4_days_back
            
        try:
            end_date = datetime.now()
            available_tables = []
            
            logger.info(f"GA4テーブル確認: 過去{days_back}日分")
            
            # 過去指定日数分のテーブルをチェック
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
            
            logger.info(f"利用可能なテーブル数: {len(available_tables)}/{days_back}")
            return available_tables
            
        except Exception as e:
            logger.error(f"テーブル確認エラー: {str(e)}")
            return []
    
    def get_ga4_pageviews_multiple_days(self, days_back: int = None) -> List[Dict]:
        """指定日数分のGA4データを取得して合計"""
        if days_back is None:
            days_back = self.ga4_days_back
            
        if self.check_timeout("GA4データ取得開始"):
            raise TimeoutError("GA4データ取得でタイムアウト")
        
        # 利用可能なテーブルを取得
        available_tables = self.get_available_ga4_tables(days_back)
        
        if not available_tables:
            logger.error("利用可能なGA4テーブルがありません")
            return []
        
        logger.info(f"GA4データ取得: {len(available_tables)}テーブル（{days_back}日分）")
        
        # 複数テーブルからデータを取得するクエリ
        table_queries = []
        for table_suffix in available_tables:
            table_queries.append(f"""
            SELECT
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
                1 as page_count,
                '{table_suffix}' as table_date
            FROM `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}`
            WHERE
                event_name = 'page_view'
                AND (
                    SELECT value.string_value 
                    FROM UNNEST(event_params) 
                    WHERE key = 'page_location'
                ) LIKE '%foresight.jp%column%'
            """)
        
        # すべてのテーブルからデータを統合して集計
        query = f"""
        WITH combined_events AS (
            {' UNION ALL '.join(table_queries)}
        ),
        filtered_events AS (
            SELECT
                page_location,
                page_count
            FROM combined_events
            WHERE
                page_location IS NOT NULL
                AND REGEXP_CONTAINS(page_location, r'https://www\.foresight\.jp/[^/]+/column/[^?#]+')
        )
        SELECT
            page_location,
            SUM(page_count) as pageviews,
            COUNT(DISTINCT SUBSTR(page_location, 1, 100)) as url_variations
        FROM filtered_events
        GROUP BY page_location
        HAVING pageviews >= 1
        ORDER BY pageviews DESC
        LIMIT {GA4_LIMIT}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                use_legacy_sql=False,
                maximum_bytes_billed=3 * 10**9,  # 3GB制限（複数日分のため増加）
                dry_run=False
            )
            
            logger.info(f"GA4クエリ実行開始（{len(available_tables)}テーブル統合）")
            query_job = self.source_client.query(query, job_config=job_config)
            
            # ジョブの状態を監視
            timeout_count = 0
            max_wait_cycles = 15  # 最大90秒待機
            while query_job.state != 'DONE':
                if timeout_count > max_wait_cycles:
                    logger.warning("GA4クエリをキャンセル中...")
                    query_job.cancel()
                    raise TimeoutError("GA4クエリがタイムアウトしました")
                time.sleep(6)
                timeout_count += 1
                logger.info(f"GA4クエリ実行中... ({timeout_count * 6}秒経過)")
                
            results = query_job.result(timeout=45)
            
            pageviews_data = []
            total_pageviews = 0
            
            for row in results:
                pageviews_data.append({
                    'page_location': row.page_location,
                    'pageviews': row.pageviews
                })
                total_pageviews += row.pageviews
            
            logger.info(f"GA4から {len(pageviews_data)} 件のページビューデータを取得")
            logger.info(f"総ページビュー数: {total_pageviews:,} （{days_back}日分）")
            logger.info(f"平均ページビュー/URL: {total_pageviews/len(pageviews_data):.1f}" if pageviews_data else "平均ページビュー/URL: 0")
            
            return pageviews_data
            
        except Exception as e:
            logger.error(f"GA4データ取得エラー: {str(e)}")
            # フォールバック処理
            return self.get_ga4_pageviews_fallback(available_tables[:3])  # 最大3テーブルでフォールバック

    def get_ga4_pageviews_fallback(self, available_tables: List[str]) -> List[Dict]:
        """フォールバック処理（テーブル数を制限）"""
        if not available_tables:
            return []
            
        logger.warning(f"フォールバックモードでGA4データを取得（{len(available_tables)}テーブル）")
        
        # より制限されたクエリ
        table_queries = []
        for table_suffix in available_tables:
            table_queries.append(f"""
            SELECT
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
                1 as page_count
            FROM `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.events_{table_suffix}`
            WHERE
                event_name = 'page_view'
                AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%foresight.jp%column%'
            """)
        
        query = f"""
        WITH combined_events AS (
            {' UNION ALL '.join(table_queries)}
        )
        SELECT
            page_location,
            SUM(page_count) as pageviews
        FROM combined_events
        WHERE
            page_location IS NOT NULL
            AND REGEXP_CONTAINS(page_location, r'https://www\.foresight\.jp/[^/]+/column/[^?#]+')
        GROUP BY page_location
        HAVING pageviews >= 1
        ORDER BY pageviews DESC
        LIMIT 2000
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                maximum_bytes_billed=1 * 10**9,  # 1GB制限
            )
            
            query_job = self.source_client.query(query, job_config=job_config)
            
            # より短いタイムアウト
            timeout_count = 0
            while query_job.state != 'DONE':
                if timeout_count > 10:  # 60秒待機
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
                    'pageviews': row.pageviews
                })
            
            logger.info(f"フォールバックモードで {len(pageviews_data)} 件取得")
            return pageviews_data
            
        except Exception as e:
            logger.error(f"フォールバックモードでもエラー: {str(e)}")
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
            # メインクエリを実行
            query = f"""
            SELECT
                c.slug as course_slug,
                a.id as article_id,
                a.link as article_link,
                a.title as article_title,
                COALESCE(a.pageviews, 0) as current_pageviews
            FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.courses` c
            JOIN `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles` a
            ON c.id = a.koza_id
            WHERE 
                c.slug IS NOT NULL 
                AND c.slug != ''
                AND a.link IS NOT NULL
                AND a.link != ''
                AND a.id IS NOT NULL
            LIMIT 10000
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
                    'current_pageviews': row.current_pageviews
                }
                
                # 最初の5件のデータをログ出力
                if row_count <= 5:
                    logger.info(f"マッピング例 {row_count}: {pattern} -> {row.article_id}")
            
            logger.info(f"記事マッピングデータを {len(mapping)} 件取得（処理行数: {row_count}）")
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
            
        # 正規化されたパスごとのページビューを集計
        path_pageviews = {}
        
        logger.info(f"GA4データ正規化開始: {len(ga4_data)} 件（{self.ga4_days_back}日分の合計）")
        
        # バッチ処理で正規化
        batch_size = 1000
        for i in range(0, len(ga4_data), batch_size):
            batch = ga4_data[i:i+batch_size]
            
            for ga4_item in batch:
                ga4_url = ga4_item['page_location']
                normalized_path = self.normalize_ga4_url(ga4_url)
                
                if normalized_path:
                    path_pageviews[normalized_path] = path_pageviews.get(normalized_path, 0) + ga4_item['pageviews']
            
            # 進捗ログ
            if i % 2000 == 0:
                logger.info(f"正規化進捗: {i}/{len(ga4_data)}")
        
        logger.info(f"正規化後のユニークパス数: {len(path_pageviews)}")
        
        # マッチング処理
        pageviews_updates = []
        matched_count = 0
        
        for normalized_path, total_pageviews in path_pageviews.items():
            if normalized_path in url_patterns:
                article_info = url_patterns[normalized_path]
                
                # 差分がある場合のみ更新対象とする
                if abs(article_info['current_pageviews'] - total_pageviews) > 0:
                    pageviews_updates.append({
                        'article_id': article_info['article_id'],
                        'pageviews': total_pageviews,
                        'old_pageviews': article_info['current_pageviews'],
                        'course_slug': article_info['course_slug'],
                        'article_title': article_info['article_title'][:50] + '...' if len(article_info['article_title']) > 50 else article_info['article_title']
                    })
                
                matched_count += 1
        
        logger.info(f"マッチング結果: {matched_count} 件マッチ、{len(pageviews_updates)} 件更新対象")
        
        # 更新内容の統計情報
        if pageviews_updates:
            total_old_pageviews = sum(update['old_pageviews'] for update in pageviews_updates)
            total_new_pageviews = sum(update['pageviews'] for update in pageviews_updates)
            logger.info(f"ページビュー更新統計: {total_old_pageviews:,} → {total_new_pageviews:,} （差分: {total_new_pageviews - total_old_pageviews:+,}）")
        
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
                update_cases = []
                article_ids = []
                
                for update in batch:
                    article_ids.append(f"'{update['article_id']}'")
                    update_cases.append(f"WHEN '{update['article_id']}' THEN {update['pageviews']}")
                
                update_query = f"""
                UPDATE `{TARGET_PROJECT_ID}.{TARGET_DATASET_ID}.articles`
                SET 
                    pageviews = CASE id
                        {' '.join(update_cases)}
                        ELSE pageviews
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

    def sync_pageviews_optimized(self, custom_days_back: int = None):
        """最適化されたメイン同期処理"""
        # カスタム日数が指定された場合は上書き
        if custom_days_back is not None:
            self.ga4_days_back = custom_days_back
            
        logger.info(f"GA4ページビュー同期処理開始（{self.ga4_days_back}日分）")
        
        try:
            # 段階的にデータを取得
            logger.info("Step 1: マッピングデータ取得")
            mapping = self.get_courses_articles_mapping_optimized()
            
            if not mapping:
                logger.warning("マッピングデータが取得できませんでした")
                return {'status': 'failed', 'error': 'No mapping data'}
            
            logger.info(f"Step 2: GA4データ取得（{self.ga4_days_back}日分）")
            ga4_data = self.get_ga4_pageviews_multiple_days(self.ga4_days_back)
            
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
            logger.info(f"同期処理完了 (実行時間: {elapsed_time:.2f}秒)")
            
            return {
                'status': 'success',
                'days_back': self.ga4_days_back,
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
    """Cloud Functions HTTPハンドラー"""
    try:
        # リクエストから日数を取得
        custom_days_back = None
        if request.method == 'POST':
            try:
                request_json = request.get_json(silent=True)
                if request_json and 'days_back' in request_json:
                    custom_days_back = int(request_json['days_back'])
                    logger.info(f"カスタム日数指定: {custom_days_back}日")
            except (ValueError, TypeError) as e:
                logger.warning(f"日数パラメータの解析エラー: {str(e)}")
        
        sync_service = GA4DataSync()
        result = sync_service.sync_pageviews_optimized(custom_days_back)
        
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
    """Cloud Scheduler用ハンドラー"""
    try:
        sync_service = GA4DataSync()
        result = sync_service.sync_pageviews_optimized()
        
        logger.info(f"スケジュール実行完了: {result}")
        
    except Exception as e:
        logger.error(f"スケジュール実行エラー: {str(e)}")
        # スケジューラーでは例外を再発生させない（リトライを避けるため）
        pass