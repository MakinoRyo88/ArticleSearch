from google.cloud import bigquery
import logging
from typing import Dict, List, Optional, Any
import numpy as np

logger = logging.getLogger(__name__)

class SimilaritySearchManager:
    """類似記事検索管理クラス - UI特化機能付き"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "seo_analysis"
    
    def find_similar_articles(self, article_id: Optional[str] = None, 
                            course_slug: Optional[str] = None, 
                            article_link: Optional[str] = None,
                            limit: int = 10) -> Dict[str, Any]:
        """類似記事検索 - 既存APIと互換"""
        try:
            # 対象記事の特定
            target_article = self._get_target_article(article_id, course_slug, article_link)
            if not target_article:
                return {'error': 'Target article not found'}
            
            # 類似記事検索クエリ
            query = f"""
            WITH target_embedding AS (
                SELECT embedding
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings`
                WHERE article_id = ?
            ),
            similarities AS (
                SELECT 
                    ae.article_id,
                    a.title,
                    a.link,
                    a.course_slug,
                    a.course_name,
                    a.word_count,
                    ML.DISTANCE(ae.embedding, te.embedding, 'COSINE') as distance,
                    (1 - ML.DISTANCE(ae.embedding, te.embedding, 'COSINE')) as similarity
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings` ae
                JOIN `{self.project_id}.{self.dataset_id}.articles` a ON ae.article_id = a.id
                CROSS JOIN target_embedding te
                WHERE ae.article_id != ?
                ORDER BY similarity DESC
                LIMIT {limit}
            )
            SELECT * FROM similarities
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(None, "STRING", target_article['id']),
                    bigquery.ScalarQueryParameter(None, "STRING", target_article['id'])
                ]
            )
            
            results = self.client.query(query, job_config=job_config).result()
            
            similar_articles = []
            for row in results:
                similar_articles.append({
                    'article_id': row.article_id,
                    'title': row.title,
                    'link': row.link,
                    'course_slug': row.course_slug,
                    'course_name': row.course_name,
                    'word_count': row.word_count,
                    'similarity_score': float(row.similarity),
                    'distance': float(row.distance)
                })
            
            return {
                'target_article': target_article,
                'similar_articles': similar_articles,
                'count': len(similar_articles)
            }
            
        except Exception as e:
            logger.error(f"類似記事検索エラー: {str(e)}")
            return {'error': str(e)}
    
    def analyze_article_similarity(self, article_id: Optional[str] = None,
                                 course_slug: Optional[str] = None,
                                 article_link: Optional[str] = None,
                                 analysis_type: str = 'detailed') -> Dict[str, Any]:
        """記事類似度分析 - UI特化機能"""
        try:
            # 対象記事の特定
            target_article = self._get_target_article(article_id, course_slug, article_link)
            if not target_article:
                return {'error': 'Target article not found'}
            
            # 詳細分析
            if analysis_type == 'detailed':
                return self._detailed_similarity_analysis(target_article)
            elif analysis_type == 'course_comparison':
                return self._course_comparison_analysis(target_article)
            elif analysis_type == 'content_overlap':
                return self._content_overlap_analysis(target_article)
            else:
                return self._basic_similarity_analysis(target_article)
                
        except Exception as e:
            logger.error(f"類似度分析エラー: {str(e)}")
            return {'error': str(e)}
    
    def _get_target_article(self, article_id: Optional[str], 
                          course_slug: Optional[str], 
                          article_link: Optional[str]) -> Optional[Dict[str, Any]]:
        """対象記事の特定"""
        try:
            if article_id:
                where_clause = "WHERE id = ?"
                param = article_id
            elif course_slug and article_link:
                where_clause = "WHERE course_slug = ? AND link = ?"
                param = [course_slug, article_link]
            else:
                return None
            
            query = f"""
            SELECT id, title, link, course_slug, course_name, word_count
            FROM `{self.project_id}.{self.dataset_id}.articles`
            {where_clause}
            LIMIT 1
            """
            
            if isinstance(param, list):
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(None, "STRING", p) for p in param
                    ]
                )
            else:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(None, "STRING", param)
                    ]
                )
            
            results = self.client.query(query, job_config=job_config).result()
            
            for row in results:
                return {
                    'id': row.id,
                    'title': row.title,
                    'link': row.link,
                    'course_slug': row.course_slug,
                    'course_name': row.course_name,
                    'word_count': row.word_count
                }
            
            return None
            
        except Exception as e:
            logger.error(f"対象記事特定エラー: {str(e)}")
            return None
    
    def _detailed_similarity_analysis(self, target_article: Dict[str, Any]) -> Dict[str, Any]:
        """詳細類似度分析"""
        try:
            query = f"""
            WITH target_embedding AS (
                SELECT embedding
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings`
                WHERE article_id = ?
            ),
            all_similarities AS (
                SELECT 
                    ae.article_id,
                    a.title,
                    a.link,
                    a.course_slug,
                    a.course_name,
                    a.word_count,
                    (1 - ML.DISTANCE(ae.embedding, te.embedding, 'COSINE')) as similarity
                FROM `{self.project_id}.{self.dataset_id}.article_embeddings` ae
                JOIN `{self.project_id}.{self.dataset_id}.articles` a ON ae.article_id = a.id
                CROSS JOIN target_embedding te
                WHERE ae.article_id != ?
            ),
            similarity_stats AS (
                SELECT 
                    COUNT(*) as total_comparisons,
                    AVG(similarity) as avg_similarity,
                    STDDEV(similarity) as std_similarity,
                    MIN(similarity) as min_similarity,
                    MAX(similarity) as max_similarity
                FROM all_similarities
            ),
            high_similarity AS (
                SELECT *
                FROM all_similarities
                WHERE similarity >= 0.8
                ORDER BY similarity DESC
                LIMIT 20
            ),
            course_similarities AS (
                SELECT 
                    course_slug,
                    course_name,
                    COUNT(*) as article_count,
                    AVG(similarity) as avg_similarity,
                    MAX(similarity) as max_similarity
                FROM all_similarities
                GROUP BY course_slug, course_name
                ORDER BY avg_similarity DESC
            )
            SELECT 
                'stats' as type,
                TO_JSON_STRING(similarity_stats) as data
            FROM similarity_stats
            UNION ALL
            SELECT 
                'high_similarity' as type,
                TO_JSON_STRING(ARRAY_AGG(high_similarity)) as data
            FROM high_similarity
            UNION ALL
            SELECT 
                'course_similarities' as type,
                TO_JSON_STRING(ARRAY_AGG(course_similarities)) as data
            FROM course_similarities
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(None, "STRING", target_article['id']),
                    bigquery.ScalarQueryParameter(None, "STRING", target_article['id'])
                ]
            )
            
            results = self.client.query(query, job_config=job_config).result()
            
            analysis_data = {}
            for row in results:
                analysis_data[row.type] = row.data
            
            return {
                'target_article': target_article,
                'analysis_type': 'detailed',
                'statistics': analysis_data.get('stats'),
                'high_similarity_articles': analysis_data.get('high_similarity'),
                'course_similarities': analysis_data.get('course_similarities')
            }
            
        except Exception as e:
            logger.error(f"詳細分析エラー: {str(e)}")
            return {'error': str(e)}
    
    def _basic_similarity_analysis(self, target_article: Dict[str, Any]) -> Dict[str, Any]:
        """基本類似度分析"""
        # 基本的な類似記事検索と同じ
        return self.find_similar_articles(article_id=target_article['id'], limit=10)
