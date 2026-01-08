from google.cloud import bigquery
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class ArticlesManager:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "consultation_forum"
    
    def get_articles_list(self, page: int = 1, limit: int = 20, course_id: Optional[str] = None) -> Dict[str, Any]:
        """記事一覧取得"""
        try:
            offset = (page - 1) * limit
            
            # WHERE句の構築
            where_clause = "WHERE status = 'published'"
            if course_id:
                where_clause += f" AND course_id = '{course_id}'"
            
            # 記事一覧クエリ
            query = f"""
            SELECT 
                article_id,
                title,
                SUBSTR(content_preview, 1, 200) as excerpt,
                course_id,
                course_name,
                created_at,
                updated_at,
                view_count,
                like_count
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit} OFFSET {offset}
            """
            
            # 総数取得クエリ
            count_query = f"""
            SELECT COUNT(*) as total
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            {where_clause}
            """
            
            # クエリ実行
            articles_job = self.client.query(query)
            count_job = self.client.query(count_query)
            
            articles = []
            for row in articles_job.result():
                articles.append({
                    'article_id': row.article_id,
                    'title': row.title,
                    'excerpt': row.excerpt,
                    'course_id': row.course_id,
                    'course_name': row.course_name,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                    'view_count': row.view_count or 0,
                    'like_count': row.like_count or 0
                })
            
            total = list(count_job.result())[0].total
            
            return {
                'articles': articles,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'total_pages': (total + limit - 1) // limit
                }
            }
            
        except Exception as e:
            logger.error(f"記事一覧取得エラー: {str(e)}")
            # サンプルデータを返す
            return self._get_sample_articles_list(page, limit)
    
    def get_article_detail(self, article_id: str) -> Optional[Dict[str, Any]]:
        """記事詳細取得"""
        try:
            query = f"""
            SELECT 
                article_id,
                title,
                content_preview,
                course_id,
                course_name,
                created_at,
                updated_at,
                view_count,
                like_count,
                tags
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            WHERE article_id = '{article_id}' AND status = 'published'
            """
            
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            if not results:
                return self._get_sample_article_detail(article_id)
            
            row = results[0]
            return {
                'article_id': row.article_id,
                'title': row.title,
                'content': row.content_preview,
                'course_id': row.course_id,
                'course_name': row.course_name,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                'view_count': row.view_count or 0,
                'like_count': row.like_count or 0,
                'tags': row.tags.split(',') if row.tags else []
            }
            
        except Exception as e:
            logger.error(f"記事詳細取得エラー: {str(e)}")
            return self._get_sample_article_detail(article_id)
    
    def get_courses_list(self) -> Dict[str, Any]:
        """講座一覧取得"""
        try:
            query = f"""
            SELECT 
                course_id,
                course_name,
                COUNT(*) as article_count
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            WHERE status = 'published'
            GROUP BY course_id, course_name
            ORDER BY article_count DESC
            """
            
            query_job = self.client.query(query)
            
            courses = []
            for row in query_job.result():
                courses.append({
                    'course_id': row.course_id,
                    'course_name': row.course_name,
                    'article_count': row.article_count
                })
            
            return {'courses': courses}
            
        except Exception as e:
            logger.error(f"講座一覧取得エラー: {str(e)}")
            return self._get_sample_courses_list()
    
    def get_course_stats(self, course_id: str) -> Dict[str, Any]:
        """講座統計情報取得"""
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_articles,
                AVG(view_count) as avg_views,
                SUM(view_count) as total_views,
                AVG(like_count) as avg_likes,
                SUM(like_count) as total_likes
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            WHERE course_id = '{course_id}' AND status = 'published'
            """
            
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            if not results:
                return self._get_sample_course_stats(course_id)
            
            row = results[0]
            return {
                'course_id': course_id,
                'total_articles': row.total_articles or 0,
                'avg_views': float(row.avg_views) if row.avg_views else 0.0,
                'total_views': row.total_views or 0,
                'avg_likes': float(row.avg_likes) if row.avg_likes else 0.0,
                'total_likes': row.total_likes or 0
            }
            
        except Exception as e:
            logger.error(f"講座統計取得エラー: {str(e)}")
            return self._get_sample_course_stats(course_id)
    
    def _get_sample_articles_list(self, page: int, limit: int) -> Dict[str, Any]:
        """サンプル記事一覧"""
        articles = []
        for i in range(limit):
            article_id = f"sample-{page}-{i+1}"
            articles.append({
                'article_id': article_id,
                'title': f'サンプル記事 {article_id}',
                'excerpt': f'これは記事 {article_id} の抜粋です。',
                'course_id': 'sample-course',
                'course_name': 'サンプル講座',
                'created_at': '2024-01-01T00:00:00',
                'updated_at': '2024-01-01T00:00:00',
                'view_count': 100,
                'like_count': 10
            })
        
        return {
            'articles': articles,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': 100,
                'total_pages': 5
            }
        }
    
    def _get_sample_article_detail(self, article_id: str) -> Dict[str, Any]:
        """サンプル記事詳細"""
        return {
            'article_id': article_id,
            'title': f'サンプル記事: {article_id}',
            'content': f'これは記事 {article_id} の詳細内容です。',
            'course_id': 'sample-course',
            'course_name': 'サンプル講座',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00',
            'view_count': 100,
            'like_count': 10,
            'tags': ['サンプル', 'テスト']
        }
    
    def _get_sample_courses_list(self) -> Dict[str, Any]:
        """サンプル講座一覧"""
        return {
            'courses': [
                {'course_id': 'sample-course-1', 'course_name': 'サンプル講座1', 'article_count': 50},
                {'course_id': 'sample-course-2', 'course_name': 'サンプル講座2', 'article_count': 30},
                {'course_id': 'sample-course-3', 'course_name': 'サンプル講座3', 'article_count': 20}
            ]
        }
    
    def _get_sample_course_stats(self, course_id: str) -> Dict[str, Any]:
        """サンプル講座統計"""
        return {
            'course_id': course_id,
            'total_articles': 50,
            'avg_views': 150.5,
            'total_views': 7525,
            'avg_likes': 12.3,
            'total_likes': 615
        }
