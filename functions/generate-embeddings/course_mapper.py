from google.cloud import bigquery
import logging
from typing import Dict, Optional
import json

logger = logging.getLogger(__name__)

class CourseMapper:
    """講座情報のマッピングクラス"""
    
    def __init__(self, project_id: str = "seo-optimize-464208"):
        self.project_id = project_id
        self.dataset_id = "content_analysis"
        self.courses_table = "courses"
        self.client = bigquery.Client(project=project_id)
        self._course_cache = {}
        self._cache_loaded = False
        
        logger.info(f"CourseMapper初期化完了 - Project: {project_id}")
    
    def load_course_mapping(self) -> Dict[str, str]:
        """
        BigQueryから講座マッピングを取得
        
        Returns:
            講座IDと名前のマッピング辞書
        """
        if self._cache_loaded and self._course_cache:
            return self._course_cache
        
        try:
            query = f"""
            SELECT 
                id,
                name,
                slug,
                total_articles
            FROM `{self.project_id}.{self.dataset_id}.{self.courses_table}`
            WHERE name IS NOT NULL 
            AND name != ''
            ORDER BY id
            """
            
            logger.info("講座マッピング取得開始")
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            course_mapping = {}
            for row in results:
                course_id = str(row.id)
                course_name = row.name
                course_slug = row.slug
                total_articles = row.total_articles or 0
                
                # IDをキーとして名前を格納
                course_mapping[course_id] = course_name
                
                logger.info(f"講座登録: ID={course_id}, Name={course_name}, Slug={course_slug}, Articles={total_articles}")
            
            self._course_cache = course_mapping
            self._cache_loaded = True
            
            logger.info(f"講座マッピング取得完了: {len(course_mapping)}件")
            return course_mapping
            
        except Exception as e:
            logger.error(f"講座マッピング取得エラー: {str(e)}")
            # フォールバック用の基本マッピング
            return self._get_fallback_mapping()
    
    def get_course_name(self, course_id: str) -> str:
        """
        講座IDから講座名を取得
        
        Args:
            course_id: 講座ID
            
        Returns:
            講座名（見つからない場合は"講座{ID}"）
        """
        if not self._cache_loaded:
            self.load_course_mapping()
        
        course_id_str = str(course_id)
        return self._course_cache.get(course_id_str, f"講座{course_id_str}")
    
    def get_course_info(self, course_id: str) -> Dict[str, str]:
        """
        講座IDから詳細情報を取得
        
        Args:
            course_id: 講座ID
            
        Returns:
            講座情報辞書
        """
        try:
            query = f"""
            SELECT 
                id,
                name,
                slug,
                description,
                total_articles,
                total_pageviews
            FROM `{self.project_id}.{self.dataset_id}.{self.courses_table}`
            WHERE id = '{course_id}'
            LIMIT 1
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return {
                    'id': str(row.id),
                    'name': row.name or f"講座{row.id}",
                    'slug': row.slug or '',
                    'description': row.description or '',
                    'total_articles': row.total_articles or 0,
                    'total_pageviews': row.total_pageviews or 0
                }
            
            # 見つからない場合
            return {
                'id': str(course_id),
                'name': f"講座{course_id}",
                'slug': '',
                'description': '',
                'total_articles': 0,
                'total_pageviews': 0
            }
            
        except Exception as e:
            logger.error(f"講座情報取得エラー: {str(e)}")
            return {
                'id': str(course_id),
                'name': f"講座{course_id}",
                'slug': '',
                'description': '',
                'total_articles': 0,
                'total_pageviews': 0
            }
    
    def _get_fallback_mapping(self) -> Dict[str, str]:
        """フォールバッ���用の基本マッピング"""
        return {
            "1": "行政書士",
            "2": "社会保険労務士", 
            "3": "FP（ファイナンシャルプランナー）",
            "4": "宅建士（宅地建物取引士）",
            "5": "マンション管理士・管理業務主任者",
            "6": "簿記2級・3級",
            "26": "IT（ITパスポート）",
            "27": "通関士",
            "28": "旅行業務取扱管理者",
            "39": "年金アドバイザー3級",
            "40": "危険物取扱者乙種4類",
            "45": "司法書士",
            "46": "基本情報技術者",
            "47": "証券外務員"
        }
    
    def refresh_cache(self) -> bool:
        """キャッシュを強制更新"""
        try:
            self._cache_loaded = False
            self._course_cache = {}
            self.load_course_mapping()
            return True
        except Exception as e:
            logger.error(f"キャッシュ更新エラー: {str(e)}")
            return False
    
    def get_all_courses(self) -> Dict[str, Dict]:
        """全講座の詳細情報を取得"""
        try:
            query = f"""
            SELECT 
                id,
                name,
                slug,
                description,
                total_articles,
                total_pageviews,
                created_at,
                updated_at
            FROM `{self.project_id}.{self.dataset_id}.{self.courses_table}`
            ORDER BY total_articles DESC, id
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            all_courses = {}
            for row in results:
                course_id = str(row.id)
                all_courses[course_id] = {
                    'id': course_id,
                    'name': row.name or f"講座{course_id}",
                    'slug': row.slug or '',
                    'description': row.description or '',
                    'total_articles': row.total_articles or 0,
                    'total_pageviews': row.total_pageviews or 0,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None
                }
            
            logger.info(f"全講座情報取得完了: {len(all_courses)}件")
            return all_courses
            
        except Exception as e:
            logger.error(f"全講座情報取得エラー: {str(e)}")
            return {}
