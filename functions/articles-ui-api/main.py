import functions_framework
from flask import request, jsonify
import json
import logging
from articles_data_manager import ArticlesDataManager
from similarity_search_manager import SimilaritySearchManager
from integration_suggestions_manager import IntegrationSuggestionsManager

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# マネージャーインスタンス
articles_manager = ArticlesDataManager()
similarity_manager = SimilaritySearchManager()
integration_manager = IntegrationSuggestionsManager()

@functions_framework.http
def articles_ui_api(request):
    """統合UI API - 既存APIとの互換性を保ちつつUI特化機能を提供"""
    
    # CORS設定
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json; charset=utf-8'
    }
    
    try:
        # パスとメソッドを取得
        path = request.path.strip('/')
        method = request.method
        
        logger.info(f"Request: {method} /{path}")
        
        # ヘルスチェック
        if path == 'health':
            return jsonify({
                'status': 'healthy',
                'service': 'articles-ui-api',
                'version': '1.0.0',
                'features': [
                    'articles_management',
                    'similarity_search',
                    'integration_suggestions',
                    'ui_optimization'
                ]
            }), 200, headers
        
        # 記事関連エンドポイント
        if path.startswith('articles'):
            return handle_articles_endpoints(path, method, request, headers)
        
        # 講座関連エンドポイント
        elif path.startswith('courses'):
            return handle_courses_endpoints(path, method, request, headers)
        
        else:
            return jsonify({
                'error': 'Not Found',
                'message': f'Endpoint /{path} not found'
            }), 404, headers
            
    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500, headers

def handle_articles_endpoints(path, method, request, headers):
    """記事関連エンドポイントの処理"""
    
    path_parts = path.split('/')
    
    try:
        # GET /articles - 記事一覧（既存APIと互換）
        if path == 'articles' and method == 'GET':
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 20))
            course_slug = request.args.get('course_slug')
            
            result = articles_manager.get_articles_list(
                page=page,
                limit=limit,
                course_slug=course_slug
            )
            return jsonify(result), 200, headers
        
        # GET /articles/{id} - 記事詳細（既存APIと互換）
        elif len(path_parts) == 2 and method == 'GET':
            article_id = path_parts[1]
            result = articles_manager.get_article_detail(article_id)
            return jsonify(result), 200, headers
        
        # POST /articles/search - 記事検索（既存APIと互換）
        elif path == 'articles/search' and method == 'POST':
            data = request.get_json()
            query = data.get('query', '')
            limit = data.get('limit', 20)
            course_slug = data.get('course_slug')
            
            result = articles_manager.search_articles(
                query=query,
                limit=limit,
                course_slug=course_slug
            )
            return jsonify(result), 200, headers
        
        # POST /articles/similar - 類似記事検索（既存APIと互換）
        elif path == 'articles/similar' and method == 'POST':
            data = request.get_json()
            article_id = data.get('article_id')
            course_slug = data.get('course_slug')
            article_link = data.get('article_link')
            limit = data.get('limit', 10)
            
            result = similarity_manager.find_similar_articles(
                article_id=article_id,
                course_slug=course_slug,
                article_link=article_link,
                limit=limit
            )
            return jsonify(result), 200, headers
        
        # POST /articles/analyze - 類似度分析（UI特化機能）
        elif path == 'articles/analyze' and method == 'POST':
            data = request.get_json()
            article_id = data.get('article_id')
            course_slug = data.get('course_slug')
            article_link = data.get('article_link')
            analysis_type = data.get('analysis_type', 'detailed')
            
            result = similarity_manager.analyze_article_similarity(
                article_id=article_id,
                course_slug=course_slug,
                article_link=article_link,
                analysis_type=analysis_type
            )
            return jsonify(result), 200, headers
        
        # POST /articles/integration-suggestions - 統合提案（UI特化機能）
        elif path == 'articles/integration-suggestions' and method == 'POST':
            data = request.get_json()
            article_ids = data.get('article_ids', [])
            similarity_threshold = data.get('similarity_threshold', 0.8)
            
            result = integration_manager.generate_integration_suggestions(
                article_ids=article_ids,
                similarity_threshold=similarity_threshold
            )
            return jsonify(result), 200, headers
        
        # GET /articles/integration-groups - 統合グループ一覧（UI特化機能）
        elif path == 'articles/integration-groups' and method == 'GET':
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 20))
            
            result = integration_manager.get_integration_groups(
                page=page,
                limit=limit
            )
            return jsonify(result), 200, headers
        
        # POST /articles/integration-groups - 統合グループ作成（UI特化機能）
        elif path == 'articles/integration-groups' and method == 'POST':
            data = request.get_json()
            group_name = data.get('group_name')
            article_ids = data.get('article_ids', [])
            integration_strategy = data.get('integration_strategy', 'merge')
            
            result = integration_manager.create_integration_group(
                group_name=group_name,
                article_ids=article_ids,
                integration_strategy=integration_strategy
            )
            return jsonify(result), 200, headers
        
        else:
            return jsonify({
                'error': 'Not Found',
                'message': f'Articles endpoint not found: {path}'
            }), 404, headers
            
    except Exception as e:
        logger.error(f"Articles endpoint error: {str(e)}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500, headers

def handle_courses_endpoints(path, method, request, headers):
    """講座関連エンドポイントの処理"""
    
    path_parts = path.split('/')
    
    try:
        # GET /courses - 講座一覧（既存APIと互換）
        if path == 'courses' and method == 'GET':
            result = articles_manager.get_courses_list()
            return jsonify(result), 200, headers
        
        # GET /courses/{slug}/stats - 講座統計（既存APIと互換）
        elif len(path_parts) == 3 and path_parts[2] == 'stats' and method == 'GET':
            course_slug = path_parts[1]
            result = articles_manager.get_course_stats(course_slug)
            return jsonify(result), 200, headers
        
        else:
            return jsonify({
                'error': 'Not Found',
                'message': f'Courses endpoint not found: {path}'
            }), 404, headers
            
    except Exception as e:
        logger.error(f"Courses endpoint error: {str(e)}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500, headers
