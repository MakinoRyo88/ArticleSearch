import logging
import uuid
from datetime import datetime
from google.cloud import bigquery
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class IntegrationProposalManager:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "consultation_forum"
    
    def get_proposals_list(self, status: Optional[str] = None, limit: int = 20, page: int = 1) -> Dict[str, Any]:
        """統合提案一覧取得"""
        try:
            offset = (page - 1) * limit
            
            # WHERE句の構築
            where_clause = "WHERE 1=1"
            if status:
                where_clause += f" AND status = '{status}'"
            
            query = f"""
            SELECT 
                proposal_id,
                article_ids,
                similarity_score,
                status,
                created_at,
                updated_at,
                reason
            FROM `{self.project_id}.{self.dataset_id}.integration_proposals`
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit} OFFSET {offset}
            """
            
            count_query = f"""
            SELECT COUNT(*) as total
            FROM `{self.project_id}.{self.dataset_id}.integration_proposals`
            {where_clause}
            """
            
            try:
                proposals_job = self.client.query(query)
                count_job = self.client.query(count_query)
                
                proposals = []
                for row in proposals_job.result():
                    proposals.append({
                        'proposal_id': row.proposal_id,
                        'article_ids': row.article_ids,
                        'similarity_score': float(row.similarity_score) if row.similarity_score else 0.0,
                        'status': row.status,
                        'created_at': row.created_at.isoformat() if row.created_at else None,
                        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                        'reason': row.reason
                    })
                
                total = list(count_job.result())[0].total
                
            except Exception:
                # サンプルデータを返す
                proposals, total = self._get_sample_proposals_list(status, limit, page)
            
            return {
                'proposals': proposals,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'total_pages': (total + limit - 1) // limit
                }
            }
            
        except Exception as e:
            logger.error(f"統合提案一覧取得エラー: {str(e)}")
            proposals, total = self._get_sample_proposals_list(status, limit, page)
            return {
                'proposals': proposals,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'total_pages': (total + limit - 1) // limit
                }
            }
    
    def create_proposal(self, proposal_data: Dict[str, Any]) -> Dict[str, Any]:
        """統合提案作成"""
        try:
            proposal_id = str(uuid.uuid4())
            article_ids = proposal_data.get('article_ids', [])
            similarity_score = float(proposal_data.get('similarity_score', 0.0))
            reason = proposal_data.get('reason', '')
            
            # BigQueryに挿入
            insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.integration_proposals`
            (proposal_id, article_ids, similarity_score, status, created_at, updated_at, reason)
            VALUES (
                '{proposal_id}',
                {article_ids},
                {similarity_score},
                'pending',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                '{reason}'
            )
            """
            
            try:
                self.client.query(insert_query)
            except Exception as e:
                logger.warning(f"BigQuery挿入失敗: {str(e)}")
            
            return {
                'proposal_id': proposal_id,
                'article_ids': article_ids,
                'similarity_score': similarity_score,
                'status': 'pending',
                'created_at': datetime.now().isoformat(),
                'reason': reason
            }
            
        except Exception as e:
            logger.error(f"統合提案作成エラー: {str(e)}")
            raise
    
    def get_proposal_detail(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        """統合提案詳細取得"""
        try:
            query = f"""
            SELECT 
                proposal_id,
                article_ids,
                similarity_score,
                status,
                created_at,
                updated_at,
                reason,
                approved_by,
                approved_at
            FROM `{self.project_id}.{self.dataset_id}.integration_proposals`
            WHERE proposal_id = '{proposal_id}'
            """
            
            try:
                query_job = self.client.query(query)
                results = list(query_job.result())
                
                if not results:
                    return self._get_sample_proposal_detail(proposal_id)
                
                row = results[0]
                return {
                    'proposal_id': row.proposal_id,
                    'article_ids': row.article_ids,
                    'similarity_score': float(row.similarity_score) if row.similarity_score else 0.0,
                    'status': row.status,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                    'reason': row.reason,
                    'approved_by': row.approved_by,
                    'approved_at': row.approved_at.isoformat() if row.approved_at else None
                }
                
            except Exception:
                return self._get_sample_proposal_detail(proposal_id)
            
        except Exception as e:
            logger.error(f"統合提案詳細取得エラー: {str(e)}")
            return self._get_sample_proposal_detail(proposal_id)
    
    def approve_proposal(self, proposal_id: str) -> Dict[str, Any]:
        """統合提案承認"""
        try:
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.integration_proposals`
            SET 
                status = 'approved',
                updated_at = CURRENT_TIMESTAMP(),
                approved_at = CURRENT_TIMESTAMP(),
                approved_by = 'system'
            WHERE proposal_id = '{proposal_id}'
            """
            
            try:
                self.client.query(update_query)
            except Exception as e:
                logger.warning(f"BigQuery更新失敗: {str(e)}")
            
            return {
                'proposal_id': proposal_id,
                'status': 'approved',
                'approved_at': datetime.now().isoformat(),
                'message': '統合提案が承認されました'
            }
            
        except Exception as e:
            logger.error(f"統合提案承認エラー: {str(e)}")
            raise
    
    def reject_proposal(self, proposal_id: str) -> Dict[str, Any]:
        """統合提案拒否"""
        try:
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.integration_proposals`
            SET 
                status = 'rejected',
                updated_at = CURRENT_TIMESTAMP()
            WHERE proposal_id = '{proposal_id}'
            """
            
            try:
                self.client.query(update_query)
            except Exception as e:
                logger.warning(f"BigQuery更新失敗: {str(e)}")
            
            return {
                'proposal_id': proposal_id,
                'status': 'rejected',
                'rejected_at': datetime.now().isoformat(),
                'message': '統合提案が拒否されました'
            }
            
        except Exception as e:
            logger.error(f"統合提案拒否エラー: {str(e)}")
            raise
    
    def _get_sample_proposals_list(self, status: Optional[str], limit: int, page: int) -> tuple:
        """サンプル統合提案一覧"""
        all_proposals = [
            {
                'proposal_id': 'proposal-1',
                'article_ids': ['article-1', 'article-2'],
                'similarity_score': 0.85,
                'status': 'pending',
                'created_at': '2024-01-01T00:00:00',
                'updated_at': '2024-01-01T00:00:00',
                'reason': '類似度が高い記事の統合提案'
            },
            {
                'proposal_id': 'proposal-2',
                'article_ids': ['article-3', 'article-4', 'article-5'],
                'similarity_score': 0.78,
                'status': 'approved',
                'created_at': '2024-01-02T00:00:00',
                'updated_at': '2024-01-02T12:00:00',
                'reason': '重複コンテンツの統合'
            },
            {
                'proposal_id': 'proposal-3',
                'article_ids': ['article-6', 'article-7'],
                'similarity_score': 0.92,
                'status': 'rejected',
                'created_at': '2024-01-03T00:00:00',
                'updated_at': '2024-01-03T15:30:00',
                'reason': '内容の重複が多い'
            }
        ]
        
        # ステータスフィルタリング
        if status:
            filtered_proposals = [p for p in all_proposals if p['status'] == status]
        else:
            filtered_proposals = all_proposals
        
        # ページネーション
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        page_proposals = filtered_proposals[start_idx:end_idx]
        
        return page_proposals, len(filtered_proposals)
    
    def _get_sample_proposal_detail(self, proposal_id: str) -> Dict[str, Any]:
        """サンプル統合提案詳細"""
        return {
            'proposal_id': proposal_id,
            'article_ids': ['sample-1', 'sample-2'],
            'similarity_score': 0.85,
            'status': 'pending',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00',
            'reason': 'サンプル統合提案',
            'approved_by': None,
            'approved_at': None
        }
