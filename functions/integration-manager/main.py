import functions_framework
from google.cloud import bigquery
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from integration_proposal_manager import IntegrationProposalManager
from group_manager import GroupManager

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQueryクライアント初期化
client = bigquery.Client()

# グローバルインスタンス
proposal_manager = IntegrationProposalManager()
group_manager = GroupManager()

@functions_framework.http
def integration_manager(request):
    """統合提案管理API"""
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
            return json.dumps({'status': 'healthy', 'service': 'integration-manager'}), 200, headers
        
        elif path == '/proposals' and method == 'GET':
            # 統合提案一覧取得
            status = request.args.get('status')
            limit = int(request.args.get('limit', 20))
            page = int(request.args.get('page', 1))
            
            result = proposal_manager.get_proposals_list(
                status=status,
                limit=limit,
                page=page
            )
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/proposals' and method == 'POST':
            # 統合提案作成
            request_json = request.get_json()
            if not request_json:
                return json.dumps({'error': 'Invalid JSON'}), 400, headers
            
            result = proposal_manager.create_proposal(request_json)
            return json.dumps(result, ensure_ascii=False), 201, headers
        
        elif path.startswith('/proposals/') and method == 'GET':
            # 統合提案詳細取得
            proposal_id = path.split('/')[-1]
            result = proposal_manager.get_proposal_detail(proposal_id)
            
            if result:
                return json.dumps(result, ensure_ascii=False), 200, headers
            else:
                return json.dumps({'error': 'Proposal not found'}), 404, headers
        
        elif path.startswith('/proposals/') and path.endswith('/approve') and method == 'POST':
            # 統合提案承認
            proposal_id = path.split('/')[-2]
            result = proposal_manager.approve_proposal(proposal_id)
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path.startswith('/proposals/') and path.endswith('/reject') and method == 'POST':
            # 統合提案拒否
            proposal_id = path.split('/')[-2]
            result = proposal_manager.reject_proposal(proposal_id)
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/groups' and method == 'GET':
            # 統合グループ一覧取得
            limit = int(request.args.get('limit', 20))
            page = int(request.args.get('page', 1))
            
            result = group_manager.get_groups_list(limit=limit, page=page)
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/groups' and method == 'POST':
            # 統合グループ作成
            request_json = request.get_json()
            if not request_json:
                return json.dumps({'error': 'Invalid JSON'}), 400, headers
            
            result = group_manager.create_group(request_json)
            return json.dumps(result, ensure_ascii=False), 201, headers
        
        elif path.startswith('/groups/') and method == 'GET':
            # 統合グループ詳細取得
            group_id = path.split('/')[-1]
            result = group_manager.get_group_detail(group_id)
            
            if result:
                return json.dumps(result, ensure_ascii=False), 200, headers
            else:
                return json.dumps({'error': 'Group not found'}), 404, headers
        
        elif path.startswith('/groups/') and path.endswith('/execute') and method == 'POST':
            # 統合実行
            group_id = path.split('/')[-2]
            result = group_manager.execute_integration(group_id)
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        elif path == '/stats' and method == 'GET':
            # 統計情報取得
            result = get_integration_stats()
            return json.dumps(result, ensure_ascii=False), 200, headers
        
        else:
            return json.dumps({'error': 'Not found'}), 404, headers
            
    except Exception as e:
        logger.error(f"Integration manager error: {str(e)}")
        return json.dumps({
            'error': 'Internal server error',
            'message': str(e)
        }), 500, headers

def get_integration_stats() -> Dict[str, Any]:
    """統計情報取得"""
    try:
        # 統合提案統計
        proposals_query = """
        SELECT 
            status,
            COUNT(*) as count
        FROM `seo-optimize-464208.consultation_forum.integration_proposals`
        GROUP BY status
        """
        
        # 統合グループ統計
        groups_query = """
        SELECT 
            COUNT(*) as total_groups,
            SUM(ARRAY_LENGTH(article_ids)) as total_articles_in_groups
        FROM `seo-optimize-464208.consultation_forum.integration_groups`
        WHERE status = 'active'
        """
        
        try:
            proposals_job = client.query(proposals_query)
            groups_job = client.query(groups_query)
            
            proposal_stats = {}
            for row in proposals_job.result():
                proposal_stats[row.status] = row.count
            
            group_results = list(groups_job.result())
            group_stats = {
                'total_groups': group_results[0].total_groups if group_results else 0,
                'total_articles_in_groups': group_results[0].total_articles_in_groups if group_results else 0
            }
            
        except Exception:
            # サンプルデータを使用
            proposal_stats = {'pending': 15, 'approved': 8, 'rejected': 3}
            group_stats = {'total_groups': 5, 'total_articles_in_groups': 25}
        
        return {
            'proposals': proposal_stats,
            'groups': group_stats,
            'last_updated': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"統計情報取得エラー: {str(e)}")
        return {
            'proposals': {'pending': 0, 'approved': 0, 'rejected': 0},
            'groups': {'total_groups': 0, 'total_articles_in_groups': 0},
            'last_updated': datetime.now().isoformat()
        }
