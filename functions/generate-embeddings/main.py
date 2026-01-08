import functions_framework
from google.cloud import bigquery
from google.cloud import aiplatform
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
import time
import re
from universal_text_processor import UniversalTextProcessor  # 更新
from vertex_ai_client import VertexAIClient
from bigquery_client import BigQueryClient

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def generate_embeddings(request):
    """
    記事の埋め込みベクトルを生成するメイン関数（全資格対応版）
    """
    try:
        # リクエストパラメータの取得
        request_json = request.get_json(silent=True)
        batch_size = request_json.get('batch_size', 50) if request_json else 50
        force_regenerate = request_json.get('force_regenerate', False) if request_json else False
        test_mode = request_json.get('test_mode', False) if request_json else False
        stats_only = request_json.get('stats_only', False) if request_json else False
        course_id = request_json.get('course_id', None) if request_json else None
        offset = request_json.get('offset', 0) if request_json else 0
        max_articles = request_json.get('max_articles', None) if request_json else None
        
        logger.info(f"全資格対応埋め込み生成開始 - batch_size: {batch_size}, force_regenerate: {force_regenerate}, test_mode: {test_mode}, stats_only: {stats_only}, course_id: {course_id}, offset: {offset}")
        
        # クライアント初期化
        bq_client = BigQueryClient()
        
        # 統計情報のみの場合
        if stats_only:
            logger.info("統計情報取得モード")
            general_stats = bq_client.get_articles_statistics()
            course_stats = bq_client.get_course_statistics()
            
            return {
                "status": "stats_complete",
                "general_stats": general_stats,
                "course_stats": course_stats,
                "message": f"総記事数: {general_stats.get('total_articles', 0)}, 埋め込み未生成: {general_stats.get('no_embedding', 0)}",
                "processor_type": "universal"  # 汎用プロセッサー使用を明示
            }
        
        vertex_client = VertexAIClient()
        text_processor = UniversalTextProcessor()  # 汎用プロセッサーを使用
        
        # テストモードの場合
        if test_mode:
            logger.info("テストモード: 利用可能なモデルをチェック中...")
            available_models = vertex_client.test_model_availability()
            stats = bq_client.get_articles_statistics()
            return {
                "status": "test_complete",
                "available_models": available_models,
                "stats": stats,
                "processor_type": "universal",
                "message": f"利用可能なモデル: {', '.join(available_models) if available_models else 'なし'}"
            }
        
        # 処理対象記事の取得
        if course_id:
            # 講座別処理
            articles = bq_client.get_articles_by_course(course_id, batch_size, force_regenerate)
            total_count = len(articles)  # 簡易的な総数
        else:
            # 全記事処理
            total_count = bq_client.get_total_articles_count(force_regenerate)
            articles = bq_client.get_articles_for_embedding(batch_size, force_regenerate, offset)
        
        if not articles:
            logger.info("処理対象の記事がありません")
            return {
                "status": "success", 
                "message": "処理対象の記事がありません", 
                "processed_count": 0,
                "total_count": total_count,
                "offset": offset,
                "processor_type": "universal"
            }
        
        logger.info(f"処理対象記事数: {len(articles)} / 総数: {total_count}")
        
        processed_count = 0
        failed_count = 0
        field_stats = {}  # 分野別統計
        
        # 最大処理数の制限
        if max_articles and len(articles) > max_articles:
            articles = articles[:max_articles]
            logger.info(f"最大処理数制限により {max_articles} 件に制限")
        
        # 記事ごとに埋め込み生成
        for i, article in enumerate(articles):
            try:
                logger.info(f"記事 {i+1}/{len(articles)} 処理中: ID={article['id']} (全体進捗: {offset + i + 1}/{total_count})")
                
                # 汎用テキスト前処理
                processed_text = text_processor.process_article_content(
                    article['full_content'], 
                    article['qanda_content'],
                    article.get('title', ''),
                    str(article.get('koza_id', ''))
                )
                
                if not processed_text.strip():
                    logger.warning(f"記事ID {article['id']}: 処理後のテキストが空です")
                    failed_count += 1
                    continue
                
                # 処理統計の取得
                stats = text_processor.get_processing_stats(
                    article['full_content'],
                    article['qanda_content'],
                    processed_text,
                    str(article.get('koza_id', ''))
                )
                
                # 分野別統計の更新
                field_type = stats.get('field_type', 'general')
                if field_type not in field_stats:
                    field_stats[field_type] = {'count': 0, 'success': 0}
                field_stats[field_type]['count'] += 1
                
                logger.info(f"記事ID {article['id']}: テキスト処理完了 ({len(processed_text)}文字, 分野: {field_type})")
                
                # 埋め込み生成
                embedding = vertex_client.generate_embedding(processed_text)
                
                # REST APIで失敗した場合はSDKで試行
                if embedding is None:
                    logger.info(f"記事ID {article['id']}: SDK版で再試行")
                    embedding = vertex_client.generate_embedding_with_sdk(processed_text)
                
                if embedding:
                    # BigQueryに保存
                    success = bq_client.update_article_embedding(
                        article['id'], 
                        embedding, 
                        vertex_client.model_name
                    )
                    
                    if success:
                        processed_count += 1
                        field_stats[field_type]['success'] += 1
                        logger.info(f"記事ID {article['id']}: 埋め込み生成・保存完了")
                    else:
                        failed_count += 1
                        logger.error(f"記事ID {article['id']}: BigQuery保存に失敗")
                else:
                    failed_count += 1
                    logger.error(f"記事ID {article['id']}: 埋め込み生成に失敗")
                
                # API制限対策のための待機
                time.sleep(0.2)
                
            except Exception as e:
                failed_count += 1
                logger.error(f"記事ID {article['id']}: 処理中にエラー - {str(e)}")
                continue
        
        # 次のバッチがあるかチェック
        next_offset = offset + len(articles)
        has_more = next_offset < total_count
        
        result = {
            "status": "success",
            "message": f"全資格対応埋め込み生成完了 (バッチ {offset//batch_size + 1})",
            "processed_count": processed_count,
            "failed_count": failed_count,
            "batch_articles": len(articles),
            "total_count": total_count,
            "current_offset": offset,
            "next_offset": next_offset if has_more else None,
            "has_more": has_more,
            "progress_percentage": round((next_offset / total_count * 100), 2) if total_count > 0 else 100,
            "model_used": vertex_client.model_name,
            "course_id": course_id,
            "processor_type": "universal",
            "field_statistics": field_stats
        }
        
        logger.info(f"バッチ処理完了: {result}")
        return result
        
    except Exception as e:
        logger.error(f"メイン処理でエラー: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "processor_type": "universal"
        }, 500
