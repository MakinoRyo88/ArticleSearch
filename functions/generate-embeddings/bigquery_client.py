from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class BigQueryClient:
    """BigQuery操作クライアント（大量データ対応版）"""
    
    def __init__(self, project_id: str = "seo-optimize-464208"):
        self.project_id = project_id
        self.dataset_id = "content_analysis"
        self.articles_table = "articles"
        
        self.client = bigquery.Client(project=project_id)
        logger.info(f"BigQuery初期化完了 - Project: {project_id}")
    
    def get_articles_statistics(self) -> Dict[str, Any]:
        """
        記事の統計情報を取得
        
        Returns:
            統計情報辞書
        """
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_articles,
                COUNT(CASE WHEN content_embedding IS NULL OR ARRAY_LENGTH(content_embedding) = 0 THEN 1 END) as no_embedding,
                COUNT(CASE WHEN content_embedding IS NOT NULL AND ARRAY_LENGTH(content_embedding) > 0 THEN 1 END) as has_embedding,
                COUNT(CASE WHEN full_content IS NULL OR full_content = '' THEN 1 END) as no_content,
                COUNT(CASE WHEN full_content IS NOT NULL AND full_content != '' THEN 1 END) as has_content,
                AVG(CASE WHEN pageviews IS NOT NULL THEN pageviews ELSE 0 END) as avg_pageviews,
                MAX(updated_at) as last_updated
            FROM `{self.project_id}.{self.dataset_id}.{self.articles_table}`
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                stats = {
                    'total_articles': row.total_articles,
                    'no_embedding': row.no_embedding,
                    'has_embedding': row.has_embedding,
                    'no_content': row.no_content,
                    'has_content': row.has_content,
                    'avg_pageviews': float(row.avg_pageviews) if row.avg_pageviews else 0,
                    'last_updated': row.last_updated.isoformat() if row.last_updated else None,
                    'embedding_coverage': (row.has_embedding / row.total_articles * 100) if row.total_articles > 0 else 0
                }
                
                logger.info(f"記事統計: {stats}")
                return stats
            
            return {}
            
        except Exception as e:
            logger.error(f"統計情報取得エラー: {str(e)}")
            return {}
    
    def get_articles_for_embedding(self, limit: int = 50, force_regenerate: bool = False, 
                                 offset: int = 0) -> List[Dict[str, Any]]:
        """
        埋め込み生成対象の記事を取得（ページネーション対応）
        
        Args:
            limit: 取得件数制限
            force_regenerate: 既存の埋め込みを再生成するか
            offset: オフセット（ページネーション用）
            
        Returns:
            記事データのリスト
        """
        try:
            # 条件に応じてクエリを構築
            if force_regenerate:
                # 全記事を対象（コンテンツがある記事のみ）
                where_clause = """
                WHERE full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            else:
                # 埋め込みが未生成の記事のみ
                where_clause = """
                WHERE (content_embedding IS NULL OR ARRAY_LENGTH(content_embedding) = 0)
                AND full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            
            query = f"""
            SELECT 
                id,
                title,
                full_content,
                qanda_content,
                koza_id,
                pageviews,
                content_type,
                created_at,
                updated_at,
                CHAR_LENGTH(full_content) as content_length
            FROM `{self.project_id}.{self.dataset_id}.{self.articles_table}`
            {where_clause}
            ORDER BY 
                CASE WHEN pageviews IS NULL THEN 0 ELSE pageviews END DESC,
                content_length DESC,
                updated_at DESC
            LIMIT {limit}
            OFFSET {offset}
            """
            
            logger.info(f"記事取得クエリ実行: force_regenerate={force_regenerate}, limit={limit}, offset={offset}")
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            articles = []
            for row in results:
                articles.append({
                    'id': row.id,
                    'title': row.title,
                    'full_content': row.full_content,
                    'qanda_content': row.qanda_content,
                    'koza_id': row.koza_id,
                    'pageviews': row.pageviews,
                    'content_type': row.content_type,
                    'created_at': row.created_at,
                    'updated_at': row.updated_at,
                    'content_length': row.content_length
                })
            
            logger.info(f"記事取得完了: {len(articles)}件 (offset: {offset})")
            return articles
            
        except Exception as e:
            logger.error(f"記事取得エラー: {str(e)}")
            return []
    
    def get_total_articles_count(self, force_regenerate: bool = False) -> int:
        """
        処理対象記事の総数を取得
        
        Args:
            force_regenerate: 既存の埋め込みを再生成するか
            
        Returns:
            総記事数
        """
        try:
            if force_regenerate:
                where_clause = """
                WHERE full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            else:
                where_clause = """
                WHERE (content_embedding IS NULL OR ARRAY_LENGTH(content_embedding) = 0)
                AND full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            
            query = f"""
            SELECT COUNT(*) as total_count
            FROM `{self.project_id}.{self.dataset_id}.{self.articles_table}`
            {where_clause}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                total_count = row.total_count
                logger.info(f"処理対象記事総数: {total_count}")
                return total_count
            
            return 0
            
        except Exception as e:
            logger.error(f"総記事数取得エラー: {str(e)}")
            return 0
    
    def get_articles_by_course(self, course_id: str, limit: int = 100, 
                             force_regenerate: bool = False) -> List[Dict[str, Any]]:
        """
        講座別に記事を取得
        
        Args:
            course_id: 講座ID
            limit: 取得件数制限
            force_regenerate: 既存の埋め込みを再生成するか
            
        Returns:
            記事データのリスト
        """
        try:
            if force_regenerate:
                where_clause = f"""
                WHERE koza_id = '{course_id}'
                AND full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            else:
                where_clause = f"""
                WHERE koza_id = '{course_id}'
                AND (content_embedding IS NULL OR ARRAY_LENGTH(content_embedding) = 0)
                AND full_content IS NOT NULL 
                AND full_content != ''
                AND CHAR_LENGTH(full_content) > 50
                """
            
            query = f"""
            SELECT 
                id,
                title,
                full_content,
                qanda_content,
                koza_id,
                pageviews,
                content_type,
                created_at,
                updated_at
            FROM `{self.project_id}.{self.dataset_id}.{self.articles_table}`
            {where_clause}
            ORDER BY 
                CASE WHEN pageviews IS NULL THEN 0 ELSE pageviews END DESC,
                updated_at DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            articles = []
            for row in results:
                articles.append({
                    'id': row.id,
                    'title': row.title,
                    'full_content': row.full_content,
                    'qanda_content': row.qanda_content,
                    'koza_id': row.koza_id,
                    'pageviews': row.pageviews,
                    'content_type': row.content_type,
                    'created_at': row.created_at,
                    'updated_at': row.updated_at
                })
            
            logger.info(f"講座{course_id}の記事取得完了: {len(articles)}件")
            return articles
            
        except Exception as e:
            logger.error(f"講座別記事取得エラー: {str(e)}")
            return []
    
    def update_article_embedding(self, article_id: str, embedding: List[float], model_name: str) -> bool:
        """
        記事の埋め込みベクトルを更新
        
        Args:
            article_id: 記事ID
            embedding: 埋め込みベクトル
            model_name: 使用したモデル名
            
        Returns:
            更新成功フラグ
        """
        try:
            query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.articles_table}`
            SET 
                content_embedding = {embedding},
                embedding_model = '{model_name}',
                updated_at = CURRENT_TIMESTAMP()
            WHERE id = '{article_id}'
            """
            
            query_job = self.client.query(query)
            query_job.result()  # 完了を待機
            
            # 更新件数を確認
            if query_job.num_dml_affected_rows > 0:
                logger.info(f"記事ID {article_id}: 埋め込み更新成功")
                return True
            else:
                logger.warning(f"記事ID {article_id}: 更新対象が見つかりません")
                return False
                
        except Exception as e:
            logger.error(f"記事ID {article_id}: 埋め込み更新エラー - {str(e)}")
            return False
    
    def get_course_statistics(self) -> List[Dict[str, Any]]:
        """
        講座別の統計情報を取得
        
        Returns:
            講座別統計情報のリスト
        """
        try:
            query = f"""
            SELECT 
                a.koza_id,
                c.name as course_name,
                COUNT(*) as total_articles,
                COUNT(CASE WHEN a.content_embedding IS NULL OR ARRAY_LENGTH(a.content_embedding) = 0 THEN 1 END) as no_embedding,
                COUNT(CASE WHEN a.content_embedding IS NOT NULL AND ARRAY_LENGTH(a.content_embedding) > 0 THEN 1 END) as has_embedding,
                AVG(CASE WHEN a.pageviews IS NOT NULL THEN a.pageviews ELSE 0 END) as avg_pageviews,
                SUM(CASE WHEN a.pageviews IS NOT NULL THEN a.pageviews ELSE 0 END) as total_pageviews
            FROM `{self.project_id}.{self.dataset_id}.{self.articles_table}` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.courses` c ON a.koza_id = c.id
            WHERE a.full_content IS NOT NULL AND a.full_content != ''
            GROUP BY a.koza_id, c.name
            ORDER BY total_articles DESC
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            course_stats = []
            for row in results:
                stats = {
                    'course_id': row.koza_id,
                    'course_name': row.course_name or f"講座{row.koza_id}",
                    'total_articles': row.total_articles,
                    'no_embedding': row.no_embedding,
                    'has_embedding': row.has_embedding,
                    'avg_pageviews': float(row.avg_pageviews) if row.avg_pageviews else 0,
                    'total_pageviews': row.total_pageviews or 0,
                    'embedding_coverage': (row.has_embedding / row.total_articles * 100) if row.total_articles > 0 else 0
                }
                course_stats.append(stats)
            
            logger.info(f"講座別統計取得完了: {len(course_stats)}講座")
            return course_stats
            
        except Exception as e:
            logger.error(f"講座別統計取得エラー: {str(e)}")
            return []
