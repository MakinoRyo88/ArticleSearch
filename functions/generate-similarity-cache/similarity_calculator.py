"""
類似度計算モジュール
BigQuery ML Vector Searchを使用した高速類似度計算
"""

import logging
from typing import Dict, List, Optional
import numpy as np
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class SimilarityCalculator:
    def __init__(self, bq_client):
        """類似度計算器の初期化"""
        self.bq_client = bq_client
        self.project_id = bq_client.project_id
        self.dataset_id = bq_client.dataset_id

    def find_similar_articles(
        self, 
        base_article_id: str, 
        base_embedding: List[float], 
        all_articles: List[Dict],
        threshold: float = 0.3,
        max_results: int = 20
    ) -> List[Dict]:
        """
        指定記事に類似する記事を検索
        
        Args:
            base_article_id: 基点記事ID
            base_embedding: 基点記事の埋め込みベクトル
            all_articles: 全記事データ
            threshold: 類似度閾値
            max_results: 最大結果数
            
        Returns:
            類似記事のリスト
        """
        try:
            # BigQuery ML Vector Searchを使用
            similar_articles = self._vector_search_bigquery(
                base_article_id, base_embedding, threshold, max_results
            )
            
            # 結果が少ない場合は、メモリ内計算でフォールバック
            if len(similar_articles) < max_results // 2:
                logger.info(f"BigQuery結果が少ないため、メモリ内計算でフォールバック")
                memory_results = self._calculate_similarity_in_memory(
                    base_article_id, base_embedding, all_articles, threshold, max_results
                )
                
                # 結果をマージ（重複除去）
                existing_ids = {article['id'] for article in similar_articles}
                for article in memory_results:
                    if article['id'] not in existing_ids:
                        similar_articles.append(article)
                        existing_ids.add(article['id'])
            
            # 類似度でソート
            similar_articles.sort(key=lambda x: x['similarity_score'], reverse=True)
            
            return similar_articles[:max_results]
            
        except Exception as e:
            logger.error(f"類似記事検索でエラー: {str(e)}")
            # フォールバック: メモリ内計算
            return self._calculate_similarity_in_memory(
                base_article_id, base_embedding, all_articles, threshold, max_results
            )

    def _vector_search_bigquery(
        self, 
        base_article_id: str, 
        base_embedding: List[float], 
        threshold: float, 
        max_results: int
    ) -> List[Dict]:
        """BigQuery ML Vector Searchを使用した類似度計算"""
        
        # 埋め込みベクトルを文字列に変換
        embedding_str = '[' + ','.join(map(str, base_embedding)) + ']'
        
        query = f"""
        WITH base_embedding AS (
            SELECT {embedding_str} as query_embedding
        ),
        similarity_scores AS (
            SELECT 
                a.id,
                a.title,
                a.link,
                a.koza_id,
                a.koza_name,
                a.pageviews,
                a.engaged_sessions,
                a.search_keywords,
                -- コサイン類似度計算
                (
                    SELECT SUM(query_val * content_val) / (
                        SQRT(SUM(query_val * query_val)) * 
                        SQRT(SUM(content_val * content_val))
                    )
                    FROM UNNEST(b.query_embedding) AS query_val WITH OFFSET query_pos
                    JOIN UNNEST(a.content_embedding) AS content_val WITH OFFSET content_pos
                    ON query_pos = content_pos
                ) AS similarity_score
            FROM `{self.project_id}.{self.dataset_id}.articles` a
            CROSS JOIN base_embedding b
            WHERE 
                a.id != '{base_article_id}'
                AND a.content_embedding IS NOT NULL
                AND ARRAY_LENGTH(a.content_embedding) > 0
        )
        SELECT *
        FROM similarity_scores
        WHERE similarity_score >= {threshold}
        ORDER BY similarity_score DESC
        LIMIT {max_results}
        """
        
        try:
            results = self.bq_client.execute_query(query)
            
            similar_articles = []
            for row in results:
                similar_articles.append({
                    'id': row.id,
                    'title': row.title,
                    'link': row.link,
                    'koza_id': row.koza_id,
                    'koza_name': row.koza_name,
                    'pageviews': row.pageviews,
                    'engaged_sessions': row.engaged_sessions,
                    'search_keywords': row.search_keywords or [],
                    'similarity_score': float(row.similarity_score),
                    'confidence_score': self._calculate_confidence_score(row.similarity_score)
                })
            
            logger.info(f"BigQuery Vector Search: {len(similar_articles)}件の類似記事を検出")
            return similar_articles
            
        except Exception as e:
            logger.error(f"BigQuery Vector Searchでエラー: {str(e)}")
            return []

    def _calculate_similarity_in_memory(
        self, 
        base_article_id: str, 
        base_embedding: List[float], 
        all_articles: List[Dict],
        threshold: float, 
        max_results: int
    ) -> List[Dict]:
        """メモリ内でのコサイン類似度計算（フォールバック）"""
        
        base_vector = np.array(base_embedding)
        similar_articles = []
        
        for article in all_articles:
            if article['id'] == base_article_id:
                continue
                
            if not article.get('content_embedding'):
                continue
            
            try:
                # コサイン類似度計算
                article_vector = np.array(article['content_embedding'])
                
                # ベクトルの正規化
                base_norm = np.linalg.norm(base_vector)
                article_norm = np.linalg.norm(article_vector)
                
                if base_norm == 0 or article_norm == 0:
                    continue
                
                # コサイン類似度
                similarity = np.dot(base_vector, article_vector) / (base_norm * article_norm)
                
                if similarity >= threshold:
                    similar_articles.append({
                        'id': article['id'],
                        'title': article['title'],
                        'link': article['link'],
                        'koza_id': article['koza_id'],
                        'koza_name': article['koza_name'],
                        'pageviews': article['pageviews'],
                        'engaged_sessions': article['engaged_sessions'],
                        'search_keywords': article['search_keywords'],
                        'similarity_score': float(similarity),
                        'confidence_score': self._calculate_confidence_score(similarity)
                    })
                    
            except Exception as e:
                logger.warning(f"記事 {article['id']} の類似度計算でエラー: {str(e)}")
                continue
        
        # 類似度でソート
        similar_articles.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        logger.info(f"メモリ内計算: {len(similar_articles)}件の類似記事を検出")
        return similar_articles[:max_results]

    def _calculate_confidence_score(self, similarity_score: float) -> float:
        """信頼度スコアの計算"""
        # 類似度に基づく信頼度計算
        if similarity_score >= 0.8:
            return 0.95
        elif similarity_score >= 0.6:
            return 0.85
        elif similarity_score >= 0.4:
            return 0.75
        else:
            return 0.65

    def calculate_batch_similarities(
        self, 
        article_pairs: List[tuple], 
        embedding_dict: Dict[str, List[float]]
    ) -> List[Dict]:
        """複数記事ペアの類似度を一括計算"""
        
        similarities = []
        
        for base_id, target_id in article_pairs:
            if base_id not in embedding_dict or target_id not in embedding_dict:
                continue
            
            try:
                base_vector = np.array(embedding_dict[base_id])
                target_vector = np.array(embedding_dict[target_id])
                
                # コサイン類似度計算
                base_norm = np.linalg.norm(base_vector)
                target_norm = np.linalg.norm(target_vector)
                
                if base_norm == 0 or target_norm == 0:
                    continue
                
                similarity = np.dot(base_vector, target_vector) / (base_norm * target_norm)
                
                similarities.append({
                    'base_article_id': base_id,
                    'target_article_id': target_id,
                    'similarity_score': float(similarity),
                    'confidence_score': self._calculate_confidence_score(similarity)
                })
                
            except Exception as e:
                logger.warning(f"記事ペア ({base_id}, {target_id}) の類似度計算でエラー: {str(e)}")
                continue
        
        return similarities
