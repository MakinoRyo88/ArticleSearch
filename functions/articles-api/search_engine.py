from google.cloud import bigquery
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SearchEngine:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "consultation_forum"
    
    def search_articles(self, query: str, limit: int = 10, course_id: Optional[str] = None) -> Dict[str, Any]:
        """記事検索"""
        try:
            # WHERE句の構築
            where_clause = "WHERE status = 'published'"
            if course_id:
                where_clause += f" AND course_id = '{course_id}'"
            
            # 検索クエリ（タイトルと内容で検索）
            search_query = f"""
            SELECT 
                article_id,
                title,
                SUBSTR(content_preview, 1, 200) as excerpt,
                course_id,
                course_name,
                created_at,
                view_count,
                like_count
            FROM `{self.project_id}.{self.dataset_id}.articles_with_course_info`
            {where_clause}
            AND (
                LOWER(title) LIKE LOWER('%{query}%')
                OR LOWER(content_preview) LIKE LOWER('%{query}%')
            )
            ORDER BY 
                CASE 
                    WHEN LOWER(title) LIKE LOWER('%{query}%') THEN 1
                    ELSE 2
                END,
                view_count DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(search_query)
            
            articles = []
            for row in query_job.result():
                articles.append({
                    'article_id': row.article_id,
                    'title': row.title,
                    'excerpt': row.excerpt,
                    'course_id': row.course_id,
                    'course_name': row.course_name,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'view_count': row.view_count or 0,
                    'like_count': row.like_count or 0,
                    'relevance_score': 0.8  # 仮の関連度スコア
                })
            
            return {
                'query': query,
                'results': articles,
                'total_found': len(articles)
            }
            
        except Exception as e:
            logger.error(f"記事検索エラー: {str(e)}")
            return self._get_sample_search_results(query, limit)
    
    def find_similar_articles(self, article_id: str, limit: int = 5, threshold: float = 0.7) -> Dict[str, Any]:
        """類似記事検索"""
        try:
            # 類似記事検索クエリ（埋め込みベクトルを使用）
            similarity_query = f"""
            WITH target_article AS (
                SELECT embedding
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings`
                WHERE article_id = '{article_id}'
            ),
            similarities AS (
                SELECT 
                    e.article_id,
                    ML.DISTANCE(e.embedding, t.embedding, 'COSINE') as distance
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings` e
                CROSS JOIN target_article t
                WHERE e.article_id != '{article_id}'
            )
            SELECT 
                a.article_id,
                a.title,
                SUBSTR(a.content_preview, 1, 200) as excerpt,
                a.course_id,
                a.course_name,
                s.distance,
                (1 - s.distance) as similarity_score
            FROM similarities s
            JOIN `{self.project_id}.{self.dataset_id}.articles_with_course_info` a
                ON s.article_id = a.article_id
            WHERE (1 - s.distance) >= {threshold}
                AND a.status = 'published'
            ORDER BY similarity_score DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(similarity_query)
            
            similar_articles = []
            for row in query_job.result():
                similar_articles.append({
                    'article_id': row.article_id,
                    'title': row.title,
                    'excerpt': row.excerpt,
                    'course_id': row.course_id,
                    'course_name': row.course_name,
                    'similarity_score': float(row.similarity_score)
                })
            
            return {
                'target_article_id': article_id,
                'similar_articles': similar_articles,
                'threshold': threshold
            }
            
        except Exception as e:
            logger.error(f"類似記事検索エラー: {str(e)}")
            return self._get_sample_similar_articles(article_id, limit)
    
    def _get_sample_search_results(self, query: str, limit: int) -> Dict[str, Any]:
        """サンプル検索結果"""
        articles = []
        for i in range(min(limit, 3)):
            article_id = f"search-result-{i+1}"
            articles.append({
                'article_id': article_id,
                'title': f'検索結果: {query} - {article_id}',
                'excerpt': f'「{query}」に関連する記事内容です。',
                'course_id': 'sample-course',
                'course_name': 'サンプル講座',
                'created_at': '2024-01-01T00:00:00',
                'view_count': 100,
                'like_count': 10,
                'relevance_score': 0.9 - (i * 0.1)
            })
        
        return {
            'query': query,
            'results': articles,
            'total_found': len(articles)
        }
    
    def _get_sample_similar_articles(self, article_id: str, limit: int) -> Dict[str, Any]:
        """サンプル類似記事"""
        similar_articles = []
        for i in range(min(limit, 3)):
            similar_id = f"similar-{article_id}-{i+1}"
            similar_articles.append({
                'article_id': similar_id,
                'title': f'類似記事: {similar_id}',
                'excerpt': f'記事 {article_id} と類似した内容です。',
                'course_id': 'sample-course',
                'course_name': 'サンプル講座',
                'similarity_score': 0.9 - (i * 0.1)
            })
        
        return {
            'target_article_id': article_id,
            'similar_articles': similar_articles,
            'threshold': 0.7
        }
