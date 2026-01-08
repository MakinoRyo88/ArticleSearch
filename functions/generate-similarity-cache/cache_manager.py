"""
キャッシュ管理モジュール
類似度キャッシュの保存・更新・削除を管理
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, bq_client):
        """キャッシュマネージャーの初期化"""
        self.bq_client = bq_client
        self.project_id = bq_client.project_id
        self.dataset_id = bq_client.dataset_id
        self.cache_table = f"{self.project_id}.{self.dataset_id}.similarity_cache"

    def save_cache_entries(self, cache_entries: List[Dict]) -> int:
        """
        キャッシュエントリをBigQueryに保存
        
        Args:
            cache_entries: 保存するキャッシュエントリのリスト
            
        Returns:
            保存されたエントリ数
        """
        if not cache_entries:
            logger.info("保存するキャッシュエントリがありません")
            return 0
        
        try:
            # テーブルスキーマの確認・作成
            self._ensure_cache_table_exists()
            
            # 既存キャッシュの削除（同じbase_article_idのエントリ）
            base_article_ids = list(set([entry['base_article_id'] for entry in cache_entries]))
            self._delete_existing_cache(base_article_ids)
            
            # 新しいキャッシュエントリを挿入
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            
            # データの前処理
            processed_entries = self._preprocess_cache_entries(cache_entries)
            
            # BigQueryに挿入
            job = self.bq_client.client.load_table_from_json(
                processed_entries, 
                self.cache_table, 
                job_config=job_config
            )
            job.result()  # 完了まで待機
            
            logger.info(f"キャッシュエントリを{len(processed_entries)}件保存完了")
            return len(processed_entries)
            
        except Exception as e:
            logger.error(f"キャッシュエントリ保存でエラー: {str(e)}")
            raise

    def _ensure_cache_table_exists(self):
        """キャッシュテーブルの存在確認・作成"""
        
        schema = [
            bigquery.SchemaField("base_article_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("similar_article_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("similarity_score", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("confidence_score", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("traffic_impact_prediction", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("recommendation_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("explanation_text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cached_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("expires_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        try:
            table_ref = self.bq_client.client.dataset(self.dataset_id).table("similarity_cache")
            table = self.bq_client.client.get_table(table_ref)
            logger.info("キャッシュテーブルが既に存在します")
            
        except Exception:
            # テーブルが存在しない場合は作成
            logger.info("キャッシュテーブルを作成中...")
            
            table_ref = self.bq_client.client.dataset(self.dataset_id).table("similarity_cache")
            table = bigquery.Table(table_ref, schema=schema)
            
            # パーティション設定（cached_atでパーティション）
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="cached_at"
            )
            
            # クラスタリング設定
            table.clustering_fields = ["base_article_id", "similarity_score"]
            
            table = self.bq_client.client.create_table(table)
            logger.info("キャッシュテーブルを作成完了")

    def _delete_existing_cache(self, base_article_ids: List[str]):
        """既存キャッシュの削除"""
        if not base_article_ids:
            return
        
        # IDリストをクエリ用に変換
        ids_str = "', '".join(base_article_ids)
        
        delete_query = f"""
        DELETE FROM `{self.cache_table}`
        WHERE base_article_id IN ('{ids_str}')
        """
        
        try:
            job = self.bq_client.client.query(delete_query)
            job.result()
            
            logger.info(f"既存キャッシュを削除: {len(base_article_ids)}記事分")
            
        except Exception as e:
            logger.warning(f"既存キャッシュ削除でエラー: {str(e)}")

    def _preprocess_cache_entries(self, cache_entries: List[Dict]) -> List[Dict]:
        """キャッシュエントリの前処理"""
        processed_entries = []
        
        for entry in cache_entries:
            try:
                processed_entry = {
                    'base_article_id': str(entry['base_article_id']),
                    'similar_article_id': str(entry['similar_article_id']),
                    'similarity_score': float(entry['similarity_score']),
                    'confidence_score': float(entry.get('confidence_score', 0.8)),
                    'traffic_impact_prediction': int(entry.get('traffic_impact_prediction', 0)),
                    'recommendation_type': str(entry.get('recommendation_type', 'MONITOR')),
                    'explanation_text': str(entry.get('explanation_text', ''))[:500],  # 文字数制限
                    'cached_at': entry['cached_at'].isoformat() if hasattr(entry['cached_at'], 'isoformat') else str(entry['cached_at']),
                    'expires_at': entry['expires_at'].isoformat() if hasattr(entry['expires_at'], 'isoformat') else str(entry['expires_at'])
                }
                
                processed_entries.append(processed_entry)
                
            except Exception as e:
                logger.warning(f"キャッシュエントリ前処理でエラー: {str(e)}, エントリ: {entry}")
                continue
        
        return processed_entries

    def cleanup_expired_cache(self) -> int:
        """期限切れキャッシュの削除"""
        
        delete_query = f"""
        DELETE FROM `{self.cache_table}`
        WHERE expires_at < CURRENT_TIMESTAMP()
        """
        
        try:
            job = self.bq_client.client.query(delete_query)
            job.result()
            
            # 削除件数を取得
            count_query = f"""
            SELECT COUNT(*) as deleted_count
            FROM `{self.cache_table}`
            WHERE expires_at < CURRENT_TIMESTAMP()
            """
            
            # 実際には削除後なので0になるが、ログ用に実行前の件数を取得
            logger.info("期限切れキャッシュを削除完了")
            return 0  # 正確な削除件数は取得困難なため0を返す
            
        except Exception as e:
            logger.error(f"期限切れキャッシュ削除でエラー: {str(e)}")
            return 0

    def get_cached_similarities(
        self, 
        base_article_id: str, 
        limit: int = 20,
        min_similarity: float = 0.3
    ) -> List[Dict]:
        """キャッシュされた類似度データを取得"""
        
        query = f"""
        SELECT 
            base_article_id,
            similar_article_id,
            similarity_score,
            confidence_score,
            traffic_impact_prediction,
            recommendation_type,
            explanation_text,
            cached_at
        FROM `{self.cache_table}`
        WHERE 
            base_article_id = '{base_article_id}'
            AND similarity_score >= {min_similarity}
            AND expires_at > CURRENT_TIMESTAMP()
        ORDER BY similarity_score DESC
        LIMIT {limit}
        """
        
        try:
            results = self.bq_client.execute_query(query)
            
            cached_similarities = []
            for row in results:
                cached_similarities.append({
                    'base_article_id': row.base_article_id,
                    'similar_article_id': row.similar_article_id,
                    'similarity_score': float(row.similarity_score),
                    'confidence_score': float(row.confidence_score) if row.confidence_score else 0.8,
                    'traffic_impact_prediction': int(row.traffic_impact_prediction) if row.traffic_impact_prediction else 0,
                    'recommendation_type': row.recommendation_type,
                    'explanation_text': row.explanation_text,
                    'cached_at': row.cached_at
                })
            
            logger.info(f"キャッシュから{len(cached_similarities)}件の類似度データを取得")
            return cached_similarities
            
        except Exception as e:
            logger.error(f"キャッシュ取得でエラー: {str(e)}")
            return []

    def get_cache_statistics(self) -> Dict:
        """キャッシュ統計情報を取得"""
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_entries,
            COUNT(DISTINCT base_article_id) as unique_base_articles,
            AVG(similarity_score) as avg_similarity_score,
            MIN(cached_at) as oldest_cache,
            MAX(cached_at) as newest_cache,
            COUNTIF(expires_at > CURRENT_TIMESTAMP()) as valid_entries,
            COUNTIF(expires_at <= CURRENT_TIMESTAMP()) as expired_entries
        FROM `{self.cache_table}`
        """
        
        try:
            results = list(self.bq_client.execute_query(stats_query))
            
            if results:
                row = results[0]
                return {
                    'total_entries': int(row.total_entries),
                    'unique_base_articles': int(row.unique_base_articles),
                    'avg_similarity_score': float(row.avg_similarity_score) if row.avg_similarity_score else 0.0,
                    'oldest_cache': row.oldest_cache,
                    'newest_cache': row.newest_cache,
                    'valid_entries': int(row.valid_entries),
                    'expired_entries': int(row.expired_entries)
                }
            else:
                return {
                    'total_entries': 0,
                    'unique_base_articles': 0,
                    'avg_similarity_score': 0.0,
                    'oldest_cache': None,
                    'newest_cache': None,
                    'valid_entries': 0,
                    'expired_entries': 0
                }
                
        except Exception as e:
            logger.error(f"キャッシュ統計取得でエラー: {str(e)}")
            return {}
