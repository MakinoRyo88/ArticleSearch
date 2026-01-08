"""
類似度キャッシュ生成 Cloud Functions
高PV記事の類似度を事前計算してキャッシュテーブルに保存
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import functions_framework
from google.cloud import bigquery
from google.cloud import aiplatform
import vertexai
from vertexai.generative_models import GenerativeModel

from similarity_calculator import SimilarityCalculator
from cache_manager import CacheManager
from bigquery_client import BigQueryClient

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数
PROJECT_ID = os.getenv('GCP_PROJECT', 'seo-optimize-464208')
DATASET_ID = 'content_analysis'
REGION = 'asia-northeast1'

# 設定値
MIN_PAGEVIEWS_THRESHOLD = int(os.getenv('MIN_PAGEVIEWS_THRESHOLD', '100'))  # 最小PV数
SIMILARITY_THRESHOLD = float(os.getenv('SIMILARITY_THRESHOLD', '0.3'))      # 類似度閾値
MAX_SIMILAR_ARTICLES = int(os.getenv('MAX_SIMILAR_ARTICLES', '20'))         # 記事あたりの最大類似記事数
CACHE_EXPIRY_DAYS = int(os.getenv('CACHE_EXPIRY_DAYS', '7'))               # キャッシュ有効期限（日）
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))                            # バッチサイズ

class SimilarityCacheGenerator:
    def __init__(self):
        """類似度キャッシュ生成器の初期化"""
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.region = REGION
        
        # クライアント初期化
        self.bq_client = BigQueryClient(PROJECT_ID, DATASET_ID)
        self.similarity_calc = SimilarityCalculator(self.bq_client)
        self.cache_manager = CacheManager(self.bq_client)
        
        # Vertex AI初期化
        vertexai.init(project=PROJECT_ID, location=REGION)
        self.gemini_model = GenerativeModel("gemini-2.0-flash-001")
        
        logger.info(f"類似度キャッシュ生成器を初期化: プロジェクト={PROJECT_ID}")

    def get_high_traffic_articles(self) -> List[Dict]:
        """高トラフィック記事を取得"""
        query = f"""
        SELECT 
            id,
            title,
            link,
            koza_id,
            koza_name,
            pageviews,
            engaged_sessions,
            content_embedding,
            search_keywords
        FROM `{self.project_id}.{self.dataset_id}.articles`
        WHERE 
            pageviews >= {MIN_PAGEVIEWS_THRESHOLD}
            AND content_embedding IS NOT NULL
            AND ARRAY_LENGTH(content_embedding) > 0
        ORDER BY pageviews DESC
        LIMIT 20
        """
        
        logger.info(f"高トラフィック記事を取得中（最小PV: {MIN_PAGEVIEWS_THRESHOLD}）")
        results = self.bq_client.execute_query(query)
        
        articles = []
        for row in results:
            articles.append({
                'id': row.id,
                'title': row.title,
                'link': row.link,
                'koza_id': row.koza_id,
                'koza_name': row.koza_name,
                'pageviews': row.pageviews,
                'engaged_sessions': row.engaged_sessions,
                'content_embedding': row.content_embedding,
                'search_keywords': row.search_keywords or []
            })
        
        logger.info(f"高トラフィック記事を{len(articles)}件取得")
        return articles

    def generate_similarity_cache_batch(self, articles: List[Dict]) -> Dict:
        """類似度キャッシュをバッチ生成"""
        total_articles = len(articles)
        processed_count = 0
        cache_entries = []
        errors = []
        
        logger.info(f"類似度キャッシュ生成開始: {total_articles}記事")
        
        # バッチ処理
        for i in range(0, total_articles, BATCH_SIZE):
            batch_articles = articles[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_articles + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"バッチ {batch_num}/{total_batches} 処理中 ({len(batch_articles)}記事)")
            
            try:
                batch_cache_entries = self._process_article_batch(batch_articles, articles)
                cache_entries.extend(batch_cache_entries)
                processed_count += len(batch_articles)
                
                logger.info(f"バッチ {batch_num} 完了: {len(batch_cache_entries)}件のキャッシュエントリ生成")
                
            except Exception as e:
                error_msg = f"バッチ {batch_num} でエラー: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue
        
        # キャッシュエントリをBigQueryに保存
        if cache_entries:
            try:
                saved_count = self.cache_manager.save_cache_entries(cache_entries)
                logger.info(f"キャッシュエントリを{saved_count}件保存")
            except Exception as e:
                error_msg = f"キャッシュ保存でエラー: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        return {
            'total_articles': total_articles,
            'processed_articles': processed_count,
            'cache_entries_generated': len(cache_entries),
            'errors': errors,
            'success': len(errors) == 0
        }

    def _process_article_batch(self, batch_articles: List[Dict], all_articles: List[Dict]) -> List[Dict]:
        """記事バッチの類似度計算処理"""
        cache_entries = []
        
        for base_article in batch_articles:
            try:
                # 類似記事を計算
                similar_articles = self.similarity_calc.find_similar_articles(
                    base_article['id'],
                    base_article['content_embedding'],
                    all_articles,
                    threshold=SIMILARITY_THRESHOLD,
                    max_results=MAX_SIMILAR_ARTICLES
                )
                
                # 各類似記事に対してキャッシュエントリを生成
                for similar_article in similar_articles:
                    try:
                        # トラフィック影響予測
                        traffic_prediction = self._predict_traffic_impact(
                            base_article, similar_article
                        )
                        
                        # 推奨アクションを決定
                        recommendation_type = self._determine_recommendation_type(
                            similar_article['similarity_score'],
                            traffic_prediction
                        )
                        
                        # Gemini APIのレート制限を避けるために待機
                        time.sleep(2) # (2秒待機)
                        # Gemini による説明文生成
                        explanation = self._generate_explanation(
                            base_article, similar_article, recommendation_type
                        )
                        
                        # キャッシュエントリ作成
                        cache_entry = {
                            'base_article_id': base_article['id'],
                            'similar_article_id': similar_article['id'],
                            'similarity_score': similar_article['similarity_score'],
                            'confidence_score': similar_article.get('confidence_score', 0.8),
                            'traffic_impact_prediction': traffic_prediction,
                            'recommendation_type': recommendation_type,
                            'explanation_text': explanation,
                            'cached_at': datetime.utcnow(),
                            'expires_at': datetime.utcnow() + timedelta(days=CACHE_EXPIRY_DAYS)
                        }
                        
                        cache_entries.append(cache_entry)
                        
                    except Exception as e:
                        logger.warning(f"類似記事処理でエラー (base: {base_article['id']}, similar: {similar_article['id']}): {str(e)}")
                        continue
                
                logger.info(f"記事 {base_article['id']} の類似度計算完了: {len(similar_articles)}件")
                
            except Exception as e:
                logger.error(f"記事 {base_article['id']} の処理でエラー: {str(e)}")
                continue
        
        return cache_entries

    def _predict_traffic_impact(self, base_article: Dict, similar_article: Dict) -> int:
        """統合時のトラフィック影響を予測"""
        base_pv = base_article.get('pageviews') or 0
        similar_pv = similar_article.get('pageviews') or 0
        similarity_score = similar_article.get('similarity_score', 0)
        
        # 簡単な予測モデル（実際にはより複雑なモデルを使用）
        # 類似度が高いほど、統合効果が高いと仮定
        combined_pv = base_pv + similar_pv
        synergy_factor = 1 + (similarity_score * 0.2)  # 最大20%のシナジー効果
        
        predicted_pv = int(combined_pv * synergy_factor)
        return predicted_pv

    def _determine_recommendation_type(self, similarity_score: float, traffic_prediction: int) -> str:
        """推奨アクションタイプを決定"""
        if similarity_score >= 0.8:
            return "MERGE_CONTENT"  # コンテンツ統合
        elif similarity_score >= 0.6:
            return "REDIRECT_301"   # 301リダイレクト
        elif similarity_score >= 0.4:
            return "CROSS_LINK"     # 相互リンク
        else:
            return "MONITOR"        # 監視のみ

    def _generate_explanation(self, base_article: Dict, similar_article: Dict, recommendation_type: str) -> str:
        """Gemini による統合説明文生成"""
        try:
            prompt = f"""
SEO専門家として、以下の2つの記事の統合について分析し、具体的な提案を日本語で提供してください。

【基点記事】
- タイトル: {base_article['title']}
- 講座: {base_article.get('koza_name', 'N/A')}
- 月間PV: {(base_article.get('pageviews') or 0):,}

【類似記事】
- タイトル: {similar_article['title']}
- 講座: {similar_article.get('koza_name', 'N/A')}
- 月間PV: {(similar_article.get('pageviews') or 0):,}
- 類似度スコア: {similar_article['similarity_score']:.3f}

【推奨アクション】: {recommendation_type}

以下の観点から150文字以内で簡潔に説明してください：
1. なぜこの統合が効果的なのか
2. 期待できるSEO効果
3. 実装時の注意点

回答は簡潔で実用的な内容にしてください。
"""
            
            response = self.gemini_model.generate_content(prompt)
            explanation = response.text.strip()
            
            # 文字数制限
            if len(explanation) > 200:
                explanation = explanation[:197] + "..."
            
            return explanation
            
        except Exception as e:
            logger.warning(f"Gemini説明文生成でエラー: {str(e)}")
            # フォールバック説明文
            return f"類似度{similar_article['similarity_score']:.1%}の記事です。{recommendation_type}を検討してください。"

    def cleanup_expired_cache(self) -> int:
        """期限切れキャッシュの削除"""
        return self.cache_manager.cleanup_expired_cache()

@functions_framework.http
def similarity_cache_handler(request):
    """Cloud Functions HTTPエントリーポイント"""
    try:
        logger.info("類似度キャッシュ生成処理を開始")
        
        # リクエストパラメータ解析
        request_json = request.get_json(silent=True) or {}
        
        # 設定値をリクエストから上書き可能
        global MIN_PAGEVIEWS_THRESHOLD, SIMILARITY_THRESHOLD, MAX_SIMILAR_ARTICLES
        MIN_PAGEVIEWS_THRESHOLD = request_json.get('min_pageviews', MIN_PAGEVIEWS_THRESHOLD)
        SIMILARITY_THRESHOLD = request_json.get('similarity_threshold', SIMILARITY_THRESHOLD)
        MAX_SIMILAR_ARTICLES = request_json.get('max_similar_articles', MAX_SIMILAR_ARTICLES)
        
        # 類似度キャッシュ生成器初期化
        generator = SimilarityCacheGenerator()
        
        # 期限切れキャッシュの削除
        logger.info("期限切れキャッシュを削除中...")
        deleted_count = generator.cleanup_expired_cache()
        logger.info(f"期限切れキャッシュを{deleted_count}件削除")
        
        # 高トラフィック記事取得
        articles = generator.get_high_traffic_articles()
        
        if not articles:
            return {
                'success': True,
                'message': '処理対象の記事がありません',
                'total_articles': 0,
                'processed_articles': 0,
                'cache_entries_generated': 0,
                'deleted_expired_cache': deleted_count
            }
        
        # 類似度キャッシュ生成
        result = generator.generate_similarity_cache_batch(articles)
        result['deleted_expired_cache'] = deleted_count
        
        if result['success']:
            logger.info("類似度キャッシュ生成処理が正常に完了")
        else:
            logger.warning("類似度キャッシュ生成処理で一部エラーが発生")
        
        return result
        
    except Exception as e:
        error_msg = f"類似度キャッシュ生成処理でエラー: {str(e)}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg,
            'total_articles': 0,
            'processed_articles': 0,
            'cache_entries_generated': 0
        }
