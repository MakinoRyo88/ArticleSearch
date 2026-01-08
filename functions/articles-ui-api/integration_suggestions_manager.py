from google.cloud import bigquery
import logging
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class IntegrationSuggestionsManager:
    """統合提案管理クラス - UI特化機能"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "seo-optimize-464208"
        self.dataset_id = "seo_analysis"
    
    def generate_integration_suggestions(self, article_ids: List[str], 
                                       similarity_threshold: float = 0.8) -> Dict[str, Any]:
        """統合提案生成"""
        try:
            if len(article_ids) < 2:
                return {'error': 'At least 2 articles required for integration suggestions'}
            
            # 記事間の類似度計算
            similarities = self._calculate_pairwise_similarities(article_ids)
            
            # 統合候補グループの特定
            integration_groups = self._identify_integration_groups(similarities, similarity_threshold)
            
            # 各グループの統合提案生成
            suggestions = []
            for group in integration_groups:
                suggestion = self._generate_group_suggestion(group)
                suggestions.append(suggestion)
            
            return {
                'article_ids': article_ids,
                'similarity_threshold': similarity_threshold,
                'integration_suggestions': suggestions,
                'total_groups': len(suggestions)
            }
            
        except Exception as e:
            logger.error(f"統合提案生成エラー: {str(e)}")
            return {'error': str(e)}
    
    def get_integration_groups(self, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        """統合グループ一覧取得"""
        try:
            # 統合グループテーブルが存在しない場合は空の結果を返す
            # 実際の実装では、統合グループを保存するテーブルを作成する必要がある
            
            return {
                'groups': [],
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': 0,
                    'pages': 0
                },
                'message': 'Integration groups table not yet implemented'
            }
            
        except Exception as e:
            logger.error(f"統合グループ一覧取得エラー: {str(e)}")
            return {'error': str(e)}
    
    def create_integration_group(self, group_name: str, article_ids: List[str], 
                               integration_strategy: str = 'merge') -> Dict[str, Any]:
        """統合グループ作成"""
        try:
            # 統合グループの作成（実際の実装では専用テーブルに保存）
            group_data = {
                'group_name': group_name,
                'article_ids': article_ids,
                'integration_strategy': integration_strategy,
                'created_at': datetime.now().isoformat(),
                'status': 'pending'
            }
            
            # 統合提案の生成
            suggestions = self.generate_integration_suggestions(article_ids)
            
            return {
                'group': group_data,
                'suggestions': suggestions,
                'message': 'Integration group created successfully (in-memory only)'
            }
            
        except Exception as e:
            logger.error(f"統合グループ作成エラー: {str(e)}")
            return {'error': str(e)}
    
    def _calculate_pairwise_similarities(self, article_ids: List[str]) -> List[Dict[str, Any]]:
        """記事間の類似度計算"""
        try:
            # 記事IDのペアを作成
            pairs = []
            for i in range(len(article_ids)):
                for j in range(i + 1, len(article_ids)):
                    pairs.append((article_ids[i], article_ids[j]))
            
            if not pairs:
                return []
            
            # 類似度計算クエリ
            pair_conditions = []
            params = []
            for i, (id1, id2) in enumerate(pairs):
                pair_conditions.append(f"(ae1.article_id = ? AND ae2.article_id = ?)")
                params.extend([id1, id2])
            
            query = f"""
            SELECT 
                ae1.article_id as article1_id,
                ae2.article_id as article2_id,
                a1.title as article1_title,
                a2.title as article2_title,
                a1.course_slug as article1_course,
                a2.course_slug as article2_course,
                (1 - ML.DISTANCE(ae1.embedding, ae2.embedding, 'COSINE')) as similarity
            FROM `{self.project_id}.{self.dataset_id}.article_embeddings` ae1
            JOIN `{self.project_id}.{self.dataset_id}.article_embeddings` ae2 ON ae1.article_id != ae2.article_id
            JOIN `{self.project_id}.{self.dataset_id}.articles` a1 ON ae1.article_id = a1.id
            JOIN `{self.project_id}.{self.dataset_id}.articles` a2 ON ae2.article_id = a2.id
            WHERE ({' OR '.join(pair_conditions)})
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(None, "STRING", param) 
                    for param in params
                ]
            )
            
            results = self.client.query(query, job_config=job_config).result()
            
            similarities = []
            for row in results:
                similarities.append({
                    'article1_id': row.article1_id,
                    'article2_id': row.article2_id,
                    'article1_title': row.article1_title,
                    'article2_title': row.article2_title,
                    'article1_course': row.article1_course,
                    'article2_course': row.article2_course,
                    'similarity': float(row.similarity)
                })
            
            return similarities
            
        except Exception as e:
            logger.error(f"類似度計算エラー: {str(e)}")
            return []
    
    def _identify_integration_groups(self, similarities: List[Dict[str, Any]], 
                                   threshold: float) -> List[List[Dict[str, Any]]]:
        """統合候補グループの特定"""
        try:
            # 閾値以上の類似度を持つペアを抽出
            high_similarity_pairs = [
                sim for sim in similarities 
                if sim['similarity'] >= threshold
            ]
            
            if not high_similarity_pairs:
                return []
            
            # グラフベースのクラスタリング（簡易版）
            groups = []
            processed_articles = set()
            
            for pair in high_similarity_pairs:
                article1_id = pair['article1_id']
                article2_id = pair['article2_id']
                
                if article1_id not in processed_articles and article2_id not in processed_articles:
                    # 新しいグループを作成
                    group = [pair]
                    processed_articles.add(article1_id)
                    processed_articles.add(article2_id)
                    
                    # 関連する他のペアを探す
                    for other_pair in high_similarity_pairs:
                        if other_pair == pair:
                            continue
                        
                        other_article1 = other_pair['article1_id']
                        other_article2 = other_pair['article2_id']
                        
                        if (other_article1 in [article1_id, article2_id] or 
                            other_article2 in [article1_id, article2_id]):
                            group.append(other_pair)
                            processed_articles.add(other_article1)
                            processed_articles.add(other_article2)
                    
                    groups.append(group)
            
            return groups
            
        except Exception as e:
            logger.error(f"グループ特定エラー: {str(e)}")
            return []
    
    def _generate_group_suggestion(self, group: List[Dict[str, Any]]) -> Dict[str, Any]:
        """グループの統合提案生成"""
        try:
            # グループ内の記事を収集
            article_ids = set()
            courses = set()
            avg_similarity = 0
            
            for pair in group:
                article_ids.add(pair['article1_id'])
                article_ids.add(pair['article2_id'])
                courses.add(pair['article1_course'])
                courses.add(pair['article2_course'])
                avg_similarity += pair['similarity']
            
            avg_similarity = avg_similarity / len(group) if group else 0
            
            # 統合戦略の決定
            if len(courses) == 1:
                strategy = 'merge_within_course'
                strategy_description = '同一講座内での記事統合を推奨'
            else:
                strategy = 'cross_course_merge'
                strategy_description = '講座横断での記事統合を推奨'
            
            # 統合の利点分析
            benefits = []
            if avg_similarity > 0.9:
                benefits.append('非常に高い類似度による重複コンテンツの削減')
            if len(courses) > 1:
                benefits.append('講座横断での一貫性向上')
            if len(article_ids) >= 3:
                benefits.append('複数記事の統合による包括的なコンテンツ作成')
            
            return {
                'group_id': f"group_{hash(frozenset(article_ids)) % 10000}",
                'article_ids': list(article_ids),
                'article_count': len(article_ids),
                'courses_involved': list(courses),
                'avg_similarity': avg_similarity,
                'integration_strategy': strategy,
                'strategy_description': strategy_description,
                'estimated_benefits': benefits,
                'similarity_pairs': group,
                'priority': 'high' if avg_similarity > 0.9 else 'medium' if avg_similarity > 0.8 else 'low'
            }
            
        except Exception as e:
            logger.error(f"グループ提案生成エラー: {str(e)}")
            return {'error': str(e)}
