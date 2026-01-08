import logging
from typing import List, Dict, Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

class BigQueryClient:
    """BigQueryとのやり取りを管理するクライアント"""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.articles_table_id = f"{project_id}.content_analysis.articles"
        self.chunks_table_id = f"{project_id}.content_analysis.article_chunks"

    def get_articles_for_chunking(self, limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
        """チャンキング処理対象の記事をarticlesテーブルから取得する。"""
        query = f"""
            SELECT id, title, full_content, full_content_html, koza_id
            FROM `{self.articles_table_id}`
            WHERE (full_content IS NOT NULL AND LENGTH(full_content) > 100)
               OR (full_content_html IS NOT NULL AND LENGTH(full_content_html) > 100)
            ORDER BY CAST(id AS INT64)
            LIMIT {limit} OFFSET {offset}
        """
        try:
            logger.info(f"Executing query to fetch articles: \n{query}")
            query_job = self.client.query(query)
            results = [dict(row) for row in query_job.result()]
            logger.info(f"{len(results)}件の記事を取得しました。")
            return results
        except Exception as e:
            logger.error(f"記事の取得中にエラーが発生しました: {e}", exc_info=True)
            return []

    def insert_chunks(self, chunks: List[Dict[str, Any]], article_ids: List[str], force_regenerate: bool):
        """生成されたチャンクをarticle_chunksテーブルに保存する。"""
        if not chunks or not article_ids:
            logger.warning("挿入するチャンクデータまたは記事IDがありません。")
            return

        try:
            if force_regenerate and article_ids:
                logger.info(f"{len(article_ids)}件の記事IDに対応する既存チャンクを削除します。")
                formatted_ids = ", ".join([f"'{_id}'" for _id in article_ids])
                
                delete_query = f"DELETE FROM `{self.chunks_table_id}` WHERE article_id IN ({formatted_ids})"
                delete_job = self.client.query(delete_query)
                delete_job.result()
                logger.info("既存チャンクの削除が完了しました。")

            errors = self.client.insert_rows_json(self.chunks_table_id, chunks)
            if not errors:
                logger.info(f"{len(chunks)}件の新しいチャンクが正常に挿入されました。")
            else:
                logger.error(f"チャンクの挿入中にエラーが発生しました: {errors}")

        except Exception as e:
            logger.error(f"チャンクの保存処理中にエラーが発生しました: {e}", exc_info=True)
            raise
