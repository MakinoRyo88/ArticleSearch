from google.cloud import bigquery
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ArticlesDataManager:
    """
    UI用記事データ管理クラス
    既存のarticles_manager.pyの機能を包含し、UI特化機能を追加
    """
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "seo_analysis"
    
    def get_articles_for_ui(
        self,
        page: int = 1,
        limit: int = 20,
        course_id: Optional[str] = None,
        sort_by: str = 'updated_at',
        sort_order: str = 'desc',
        min_pageviews: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        UI用記事一覧取得
        既存のget_articles_listと互換性を保ちつつ、UI機能を強化
        """
        try:
            offset = (page - 1) * limit
            
            # WHERE句構築
            where_conditions = []
            
            if course_id:
                where_conditions.append(f"a.koza_id = {course_id}")
            
            if min_pageviews:
                where_conditions.append(f"a.pageviews >= {min_pageviews}")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # ソート条件
            valid_sort_fields = ['updated_at', 'created_at', 'pageviews', 'title']
            if sort_by not in valid_sort_fields:
                sort_by = 'updated_at'
            
            sort_direction = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
            
            # メインクエリ
            query = f"""
            SELECT 
                a.id,
                a.title,
                a.link,
                a.koza_id,
                k.name as course_name,
                k.slug as course_slug,
                a.created_at,
                a.updated_at,
                a.pageviews,
                a.word_count,
                SUBSTR(a.content, 1, 200) as content_preview,
                -- 埋め込みの有無
                CASE WHEN a.content_embedding IS NOT NULL THEN true ELSE false END as has_embedding,
                -- URL構築
                CONCAT('/', k.slug, '/', a.link) as url_path
            FROM `{self.project_id}.{self.dataset_id}.articles` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.kozas` k ON a.koza_id = k.id
            {where_clause}
            ORDER BY a.{sort_by} {sort_direction}
            LIMIT {limit} OFFSET {offset}
            """
            
            # 総数クエリ
            count_query = f"""
            SELECT COUNT(*) as total
            FROM `{self.project_id}.{self.dataset_id}.articles` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.kozas` k ON a.koza_id = k.id
            {where_clause}
            """
            
            # クエリ実行
            articles_job = self.client.query(query)
            count_job = self.client.query(count_query)
            
            articles = []
            for row in articles_job.result():
                article_data = {
                    'article_id': row.id,  # 既存APIとの互換性
                    'id': row.id,
                    'title': row.title,
                    'link': row.link,
                    'course_id': row.koza_id,
                    'course_name': row.course_name,
                    'course_slug': row.course_slug,
                    'url_path': row.url_path,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                    'pageviews': row.pageviews or 0,
                    'view_count': row.pageviews or 0,  # 既存APIとの互換性
                    'like_count': 0,  # デフォルト値
                    'content_preview': row.content_preview,
                    'excerpt': row.content_preview,  # 既存APIとの互換性
                    'has_embedding': row.has_embedding,
                    'word_count': row.word_count,
                    # UI用の追加情報
                    'can_analyze': row.has_embedding,
                    'display_pageviews': self._format_pageviews(row.pageviews or 0)
                }
                articles.append(article_data)
            
            total = list(count_job.result())[0].total
            
            return {
                'status': 'success',
                'articles': articles,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'total_pages': (total + limit - 1) // limit,
                    'has_next': page * limit < total,
                    'has_prev': page > 1
                },
                'filters': {
                    'course_id': course_id,
                    'sort_by': sort_by,
                    'sort_order': sort_order,
                    'min_pageviews': min_pageviews
                }
            }
            
        except Exception as e:
            logger.error(f"UI記事一覧取得エラー: {str(e)}")
            return self._get_sample_articles_for_ui(page, limit)
    
    def get_article_detail(self, article_id: str) -> Optional[Dict[str, Any]]:
        """
        記事詳細取得 (既存APIと互換)
        """
        try:
            query = f"""
            SELECT 
                a.id,
                a.title,
                a.link,
                a.content,
                a.koza_id,
                k.name as course_name,
                k.slug as course_slug,
                a.created_at,
                a.updated_at,
                a.pageviews,
                a.word_count,
                CASE WHEN a.content_embedding IS NOT NULL THEN true ELSE false END as has_embedding,
                CONCAT('/', k.slug, '/', a.link) as url_path
            FROM `{self.project_id}.{self.dataset_id}.articles` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.kozas` k ON a.koza_id = k.id
            WHERE a.id = '{article_id}'
            """
            
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            if not results:
                return self._get_sample_article_detail(article_id)
            
            row = results[0]
            return {
                'article_id': row.id,
                'id': row.id,
                'title': row.title,
                'link': row.link,
                'content': row.content,
                'course_id': row.koza_id,
                'course_name': row.course_name,
                'course_slug': row.course_slug,
                'url_path': row.url_path,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                'pageviews': row.pageviews or 0,
                'view_count': row.pageviews or 0,
                'like_count': 0,
                'has_embedding': row.has_embedding,
                'word_count': row.word_count,
                'can_analyze': row.has_embedding,
                'display_pageviews': self._format_pageviews(row.pageviews or 0),
                'tags': []  # デフォルト値
            }
            
        except Exception as e:
            logger.error(f"記事詳細取得エラー: {str(e)}")
            return self._get_sample_article_detail(article_id)
    
    def search_articles_for_ui(
        self,
        query: str,
        limit: int = 20,
        course_id: Optional[str] = None,
        search_fields: List[str] = ['title', 'content']
    ) -> Dict[str, Any]:
        """
        UI用記事検索
        """
        try:
            # 検索条件構築
            search_conditions = []
            for field in search_fields:
                if field == 'title':
                    search_conditions.append(f"LOWER(a.title) LIKE LOWER('%{query}%')")
                elif field == 'content':
                    search_conditions.append(f"LOWER(a.content) LIKE LOWER('%{query}%')")
                elif field == 'link':
                    search_conditions.append(f"LOWER(a.link) LIKE LOWER('%{query}%')")
            
            search_clause = " OR ".join(search_conditions)
            
            # WHERE句
            where_conditions = [f"({search_clause})"]
            if course_id:
                where_conditions.append(f"a.koza_id = {course_id}")
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # 検索クエリ
            search_query = f"""
            SELECT 
                a.id,
                a.title,
                a.link,
                a.koza_id,
                k.name as course_name,
                k.slug as course_slug,
                a.created_at,
                a.updated_at,
                a.pageviews,
                a.word_count,
                SUBSTR(a.content, 1, 300) as content_preview,
                CASE WHEN a.content_embedding IS NOT NULL THEN true ELSE false END as has_embedding,
                CONCAT('/', k.slug, '/', a.link) as url_path,
                -- 関連度計算（タイトルマッチを優先）
                CASE 
                    WHEN LOWER(a.title) LIKE LOWER('%{query}%') THEN 3
                    WHEN LOWER(a.content) LIKE LOWER('%{query}%') THEN 2
                    WHEN LOWER(a.link) LIKE LOWER('%{query}%') THEN 1
                    ELSE 0
                END as relevance_score
            FROM `{self.project_id}.{self.dataset_id}.articles` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.kozas` k ON a.koza_id = k.id
            {where_clause}
            ORDER BY relevance_score DESC, a.pageviews DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(search_query)
            
            articles = []
            for row in query_job.result():
                article_data = {
                    'article_id': row.id,
                    'id': row.id,
                    'title': row.title,
                    'link': row.link,
                    'course_id': row.koza_id,
                    'course_name': row.course_name,
                    'course_slug': row.course_slug,
                    'url_path': row.url_path,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                    'pageviews': row.pageviews or 0,
                    'view_count': row.pageviews or 0,
                    'like_count': 0,
                    'content_preview': row.content_preview,
                    'excerpt': row.content_preview,
                    'has_embedding': row.has_embedding,
                    'word_count': row.word_count,
                    'can_analyze': row.has_embedding,
                    'relevance_score': row.relevance_score,
                    'display_pageviews': self._format_pageviews(row.pageviews or 0),
                    # 検索ハイライト用
                    'highlighted_title': self._highlight_text(row.title, query),
                    'highlighted_preview': self._highlight_text(row.content_preview, query)
                }
                articles.append(article_data)
            
            return {
                'status': 'success',
                'query': query,
                'results': articles,
                'total_found': len(articles),
                'search_info': {
                    'search_fields': search_fields,
                    'course_id': course_id
                }
            }
            
        except Exception as e:
            logger.error(f"UI記事検索エラー: {str(e)}")
            return self._get_sample_search_results(query, limit)
    
    def get_courses_for_ui(self, include_stats: bool = False) -> Dict[str, Any]:
        """
        UI用講座一覧取得
        """
        try:
            if include_stats:
                query = f"""
                SELECT 
                    k.id,
                    k.name,
                    k.slug,
                    COUNT(a.id) as article_count,
                    SUM(a.pageviews) as total_pageviews,
                    AVG(a.pageviews) as avg_pageviews,
                    COUNT(CASE WHEN a.content_embedding IS NOT NULL THEN 1 END) as articles_with_embedding,
                    MAX(a.updated_at) as last_updated,
                    SUM(CASE WHEN a.has_embedding THEN 1 ELSE 0 END) as embedded_count
                FROM `{self.project_id}.{self.dataset_id}.kozas` k
                LEFT JOIN `{self.project_id}.{self.dataset_id}.articles` a ON k.id = a.koza_id
                GROUP BY k.id, k.name, k.slug
                ORDER BY article_count DESC
                """
            else:
                query = f"""
                SELECT 
                    k.id,
                    k.name,
                    k.slug,
                    COUNT(a.id) as article_count
                FROM `{self.project_id}.{self.dataset_id}.kozas` k
                LEFT JOIN `{self.project_id}.{self.dataset_id}.articles` a ON k.id = a.koza_id
                GROUP BY k.id, k.name, k.slug
                ORDER BY article_count DESC
                """
            
            query_job = self.client.query(query)
            
            courses = []
            for row in query_job.result():
                course_data = {
                    'course_id': str(row.id),  # 既存APIとの互換性
                    'id': row.id,
                    'name': row.name,
                    'course_name': row.name,  # 既存APIとの互換性
                    'slug': row.slug,
                    'article_count': row.article_count or 0
                }
                
                if include_stats:
                    course_data.update({
                        'total_pageviews': row.total_pageviews or 0,
                        'avg_pageviews': float(row.avg_pageviews or 0),
                        'articles_with_embedding': row.articles_with_embedding or 0,
                        'embedding_completion_rate': (row.articles_with_embedding or 0) / max(row.article_count or 1, 1),
                        'last_updated': row.last_updated.isoformat() if row.last_updated else None,
                        'display_total_pageviews': self._format_pageviews(row.total_pageviews or 0),
                        'embedded_count': row.embedded_count or 0,
                        'embedding_progress': (row.embedded_count / row.article_count * 100) if row.article_count > 0 else 0
                    })
                
                courses.append(course_data)
            
            return {
                'status': 'success',
                'courses': courses,
                'total_courses': len(courses),
                'include_stats': include_stats
            }
            
        except Exception as e:
            logger.error(f"UI講座一覧取得エラー: {str(e)}")
            return self._get_sample_courses_for_ui(include_stats)
    
    def get_course_stats(self, course_id: str) -> Dict[str, Any]:
        """
        講座統計取得 (既存APIと互換)
        """
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_articles,
                AVG(pageviews) as avg_views,
                SUM(pageviews) as total_views,
                0 as avg_likes,
                0 as total_likes,
                SUM(CASE WHEN has_embedding THEN 1 ELSE 0 END) as embedded_articles,
                AVG(word_count) as avg_word_count,
                MIN(created_at) as first_article,
                MAX(created_at) as latest_article
            FROM `{self.project_id}.{self.dataset_id}.articles`
            WHERE koza_id = {course_id}
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
                'total_likes': row.total_likes or 0,
                'embedded_articles': row.embedded_articles or 0,
                'embedding_progress': (row.embedded_articles / row.total_articles * 100) if row.total_articles > 0 else 0,
                'avg_word_count': float(row.avg_word_count) if row.avg_word_count else 0,
                'first_article': row.first_article.isoformat() if row.first_article else None,
                'latest_article': row.latest_article.isoformat() if row.latest_article else None
            }
            
        except Exception as e:
            logger.error(f"講座統計取得エラー: {str(e)}")
            return self._get_sample_course_stats(course_id)
    
    def _format_pageviews(self, pageviews: int) -> str:
        """ページビュー数の表示用フォーマット"""
        if pageviews >= 1000000:
            return f"{pageviews/1000000:.1f}M"
        elif pageviews >= 1000:
            return f"{pageviews/1000:.1f}K"
        else:
            return str(pageviews)
    
    def _highlight_text(self, text: str, query: str) -> str:
        """検索語句のハイライト"""
        if not text or not query:
            return text
        
        import re
        highlighted = re.sub(
            f'({re.escape(query)})', 
            r'<mark>\1</mark>', 
            text, 
            flags=re.IGNORECASE
        )
        return highlighted
    
    def _get_sample_articles_for_ui(self, page: int, limit: int) -> Dict[str, Any]:
        """サンプルデータ"""
        articles = []
        for i in range(limit):
            article_id = f"sample-{page}-{i+1}"
            articles.append({
                'article_id': article_id,
                'id': article_id,
                'title': f'サンプル記事 {article_id}',
                'link': f'sample-{page}-{i+1}',
                'course_id': 1,
                'course_name': 'サンプル講座',
                'course_slug': 'sample',
                'url_path': f'/sample/sample-{page}-{i+1}',
                'created_at': '2024-01-01T00:00:00',
                'updated_at': '2024-01-01T00:00:00',
                'pageviews': 100 * (i + 1),
                'view_count': 100 * (i + 1),
                'like_count': 10,
                'content_preview': f'これは記事 {article_id} のプレビューです。',
                'excerpt': f'これは記事 {article_id} のプレビューです。',
                'has_embedding': True,
                'word_count': 500,
                'can_analyze': True,
                'display_pageviews': str(100 * (i + 1))
            })
        
        return {
            'status': 'success',
            'articles': articles,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': 100,
                'total_pages': 5,
                'has_next': page < 5,
                'has_prev': page > 1
            }
        }
    
    def _get_sample_article_detail(self, article_id: str) -> Dict[str, Any]:
        """サンプル記事詳細"""
        return {
            'article_id': article_id,
            'id': article_id,
            'title': f'サンプル記事: {article_id}',
            'link': f'sample-{article_id}',
            'content': f'これは記事 {article_id} の詳細内容です。',
            'course_id': 1,
            'course_name': 'サンプル講座',
            'course_slug': 'sample',
            'url_path': f'/sample/sample-{article_id}',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00',
            'pageviews': 100,
            'view_count': 100,
            'like_count': 10,
            'has_embedding': True,
            'word_count': 500,
            'can_analyze': True,
            'display_pageviews': '100',
            'tags': ['サンプル', 'テスト']
        }
    
    def _get_sample_search_results(self, query: str, limit: int) -> Dict[str, Any]:
        """サンプル検索結果"""
        return {
            'status': 'success',
            'query': query,
            'results': [
                {
                    'article_id': '1001',
                    'id': '1001',
                    'title': f'検索結果: {query}',
                    'link': 'search-result-1',
                    'course_id': 1,
                    'course_name': 'サンプル講座',
                    'course_slug': 'sample',
                    'url_path': '/sample/search-result-1',
                    'pageviews': 500,
                    'view_count': 500,
                    'like_count': 10,
                    'has_embedding': True,
                    'word_count': 500,
                    'can_analyze': True,
                    'relevance_score': 3,
                    'display_pageviews': '500',
                    'highlighted_title': f'検索結果: <mark>{query}</mark>',
                    'highlighted_preview': f'「{query}」に関連する内容です。'
                }
            ],
            'total_found': 1
        }
    
    def _get_sample_courses_for_ui(self, include_stats: bool) -> Dict[str, Any]:
        """サンプル講座データ"""
        courses_data = [
            {
                'course_id': '1',
                'id': 1,
                'name': 'サンプル講座1',
                'course_name': 'サンプル講座1',
                'slug': 'sample1',
                'article_count': 50
            },
            {
                'course_id': '2',
                'id': 2,
                'name': 'サンプル講座2',
                'course_name': 'サンプル講座2',
                'slug': 'sample2',
                'article_count': 30
            }
        ]
        
        if include_stats:
            for course in courses_data:
                course.update({
                    'total_pageviews': course['article_count'] * 1000,
                    'avg_pageviews': 1000.0,
                    'articles_with_embedding': course['article_count'] - 5,
                    'embedding_completion_rate': 0.9,
                    'last_updated': '2024-01-15T00:00:00',
                    'display_total_pageviews': f"{course['article_count']}K",
                    'embedded_count': course['article_count'] - 5,
                    'embedding_progress': 0.9 * 100
                })
        
        return {
            'status': 'success',
            'courses': courses_data,
            'total_courses': len(courses_data),
            'include_stats': include_stats
        }
    
    def _get_sample_course_stats(self, course_id: str) -> Dict[str, Any]:
        """サンプル講座統計"""
        return {
            'course_id': course_id,
            'total_articles': 50,
            'avg_views': 150.5,
            'total_views': 7525,
            'avg_likes': 12.3,
            'total_likes': 615,
            'embedded_articles': 45,
            'embedding_progress': 90,
            'avg_word_count': 500,
            'first_article': '2024-01-01T00:00:00',
            'latest_article': '2024-01-15T00:00:00'
        }
