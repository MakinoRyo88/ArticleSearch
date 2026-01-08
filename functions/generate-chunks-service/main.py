import logging
import os
import time
from datetime import datetime
import functions_framework

# 各クラスをそれぞれのファイルからインポート
from bigquery_client import BigQueryClient
from vertex_ai_client import VertexAIClient
from chunk_processor import ChunkProcessor

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数からプロジェクトIDを取得
PROJECT_ID = os.getenv('GCP_PROJECT')

@functions_framework.http
def generate_chunk_embeddings(request):
    """記事をチャンクに分割し、埋め込みベクトルを生成して保存するメイン関数"""
    try:
        request_json = request.get_json(silent=True) or {}
        batch_size = request_json.get('batch_size', 5)
        force_regenerate = request_json.get('force_regenerate', False)
        offset = request_json.get('offset', 0)

        logger.info(f"チャンク埋め込み生成開始 - project_id: {PROJECT_ID}, batch_size: {batch_size}, force_regenerate: {force_regenerate}, offset: {offset}")

        # 各クライアントを初期化
        bq_client = BigQueryClient(project_id=PROJECT_ID)
        vertex_client = VertexAIClient(project_id=PROJECT_ID)
        chunk_processor = ChunkProcessor()

        # 処理対象の記事を取得
        articles = bq_client.get_articles_for_chunking(limit=batch_size, offset=offset)

        if not articles:
            logger.info("処理対象の記事がありません。")
            return {"status": "success", "message": "処理対象の記事がありません。"}, 200

        logger.info(f"{len(articles)}件の記事を処理します。")

        all_chunks_to_insert = []
        processed_article_ids = []

        # 記事ごとにループ処理
        for article in articles:
            article_id = str(article['id'])
            try:
                # HTML版を優先、なければテキスト版を使用
                full_content_html = article.get('full_content_html') or article.get('full_content', "")
                if not full_content_html.strip():
                    logger.warning(f"記事ID {article_id}: HTMLコンテンツが空のためスキップ。")
                    continue

                logger.info(f"記事ID {article_id}: HTML形式でチャンク分割を開始 (文字数: {len(full_content_html)})")
                chunks = chunk_processor.split_into_chunks(full_content_html)
                if not chunks:
                    continue

                for i, chunk in enumerate(chunks):
                    text_for_embedding = f"記事タイトル: {article['title'] or ''}\nセクション: {chunk['title']}\n\n{chunk['text']}"
                    embedding = vertex_client.generate_embedding(text_for_embedding)
                    time.sleep(0.1)

                    if embedding:
                        all_chunks_to_insert.append({
                            "chunk_id": f"{article_id}_{i}",
                            "article_id": article_id,
                            "koza_id": str(article['koza_id']) if article['koza_id'] is not None else None,
                            "chunk_index": i,
                            "chunk_title": chunk['title'],
                            "chunk_text": chunk['text'],
                            "content_embedding": embedding,
                            "embedding_model": vertex_client.model_name,
                            "created_at": datetime.utcnow().isoformat()
                        })
                processed_article_ids.append(article_id)

            except Exception as e:
                logger.error(f"記事ID {article_id} の処理中にエラー: {e}", exc_info=True)
                continue

        if all_chunks_to_insert:
            bq_client.insert_chunks(all_chunks_to_insert, processed_article_ids, force_regenerate)

        return {
            "status": "success",
            "processed_articles": len(processed_article_ids),
            "generated_chunks": len(all_chunks_to_insert)
        }, 200

    except Exception as e:
        logger.error(f"メイン処理で予期せぬエラー: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}, 500
