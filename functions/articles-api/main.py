import functions_framework
from google.cloud import bigquery
import json
import logging
from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs
from articles_manager import ArticlesManager
from search_engine import SearchEngine

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQueryクライアント初期化
client = bigquery.Client()

# グローバルインスタンス
articles_manager = ArticlesManager()
search_engine = SearchEngine()

@functions_framework.http
def articles_api(request):
    """記事一覧・検索API"""
    try:
        # CORS対応
        if request.method == 'OPTIONS':
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '3600'
            }
            return ('', 204, headers)
        
        # CORSヘッダーを設定
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Content-Type': 'application/json'
        }
        
        # パス解析
        path = request.path
        method = request.method
        
        logger.info(f"Request: {method} {path}")
        
        # ルーティング
        if path == '/health' or path == '/' and method == 'GET':
            return json.dumps({'status': 'healthy', 'service': 'articles-api'}), 200, headers
        
        elif path == '/articles' and method == 'GET':
            # 記事一覧取得
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 20))
            course_id = request.args.get('course_id')
            
            result = articles_manager.get_articles_list(
                page=page,
                limit=limit,
                course_id=course_id
            )
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path.startswith('/articles/') and method == 'GET':
            # 記事詳細取得
            article_id = path.split('/')[-1]
            result = articles_manager.get_article_detail(article_id)
            
            if result:
                return json.dumps(result, ensure_ascii=False), 200, headers
            else:
                return json.dumps({'error': 'Article not found'}), 404, headers
        
        elif path == '/articles/search' and method == 'POST':
            # 記事検索
            request_json = request.get_json()
            if not request_json:
                return json.dumps({'error': 'Invalid JSON'}), 400, headers
            
            query = request_json.get('query', '')
            limit = int(request_json.get('limit', 10))
            course_id = request_json.get('course_id')
            
            result = search_engine.search_articles(
                query=query,
                limit=limit,
                course_id=course_id
            )
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/articles/similar' and method == 'POST':
            # 類似記事検索
            request_json = request.get_json()
            if not request_json:
                return json.dumps({'error': 'Invalid JSON'}), 400, headers
            
            article_id = request_json.get('article_id')
            limit = int(request_json.get('limit', 5))
            threshold = float(request_json.get('threshold', 0.7))
            
            if not article_id:
                return json.dumps({'error': 'article_id is required'}), 400, headers
            
            result = search_engine.find_similar_articles(
                article_id=article_id,
                limit=limit,
                threshold=threshold
            )
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/courses' and method == 'GET':
            # 講座一覧取得
            result = articles_manager.get_courses_list()
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path.startswith('/courses/') and path.endswith('/stats') and method == 'GET':
            # 講座統計情報取得
            course_id = path.split('/')[-2]
            result = articles_manager.get_course_stats(course_id)
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        else:
            return json.dumps({'error': 'Not found'}), 404, headers
            
    except Exception as e:
        logger.error(f"Articles API error: {str(e)}")
        return json.dumps({
            'error': 'Internal server error',
            'message': str(e)
        }), 500, headers
