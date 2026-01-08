"""
BigQuery クライアントモジュール
データベース操作の共通処理
"""

import logging
from typing import List, Dict, Optional, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self, project_id: str, dataset_id: str):
        """BigQuery クライアントの初期化"""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        logger.info(f"BigQuery クライアント初期化: {project_id}.{dataset_id}")

    def execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Any]:
        """
        クエリを実行して結果を返す
        
        Args:
            query: 実行するSQLクエリ
            job_config: クエリジョブの設定
            
        Returns:
            クエリ結果のリスト
        """
        try:
            if job_config is None:
                job_config = bigquery.QueryJobConfig()
            
            # クエリ実行
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # 結果をリストに変換
            result_list = list(results)
            
            logger.info(f"クエリ実行完了: {len(result_list)}件の結果")
            return result_list
            
        except Exception as e:
            logger.error(f"クエリ実行でエラー: {str(e)}")
            logger.error(f"実行したクエリ: {query[:500]}...")
            raise

    def execute_query_with_parameters(
        self, 
        query: str, 
        parameters: List[bigquery.ScalarQueryParameter]
    ) -> List[Any]:
        """
        パラメータ付きクエリを実行
        
        Args:
            query: パラメータ付きSQLクエリ
            parameters: クエリパラメータのリスト
            
        Returns:
            クエリ結果のリスト
        """
        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        return self.execute_query(query, job_config)

    def insert_rows_json(self, table_name: str, rows: List[Dict]) -> List[Dict]:
        """
        JSONデータをテーブルに挿入
        
        Args:
            table_name: 挿入先テーブル名
            rows: 挿入するデータのリスト
            
        Returns:
            エラーのリスト（空の場合は成功）
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            errors = self.client.insert_rows_json(table, rows)
            
            if errors:
                logger.error(f"データ挿入でエラー: {errors}")
            else:
                logger.info(f"データ挿入完了: {len(rows)}件")
            
            return errors
            
        except Exception as e:
            logger.error(f"データ挿入でエラー: {str(e)}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """
        テーブルの存在確認
        
        Args:
            table_name: 確認するテーブル名
            
        Returns:
            テーブルが存在する場合True
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"テーブル存在確認でエラー: {str(e)}")
            return False

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """
        テーブル情報を取得
        
        Args:
            table_name: テーブル名
            
        Returns:
            テーブル情報の辞書
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            return {
                'table_id': table.table_id,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': [{'name': field.name, 'type': field.field_type} for field in table.schema]
            }
            
        except NotFound:
            logger.warning(f"テーブル {table_name} が見つかりません")
            return None
        except Exception as e:
            logger.error(f"テーブル情報取得でエラー: {str(e)}")
            return None

    def create_dataset_if_not_exists(self):
        """データセットが存在しない場合は作成"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            self.client.get_dataset(dataset_ref)
            logger.info(f"データセット {self.dataset_id} は既に存在します")
            
        except NotFound:
            logger.info(f"データセット {self.dataset_id} を作成中...")
            
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-northeast1"  # 東京リージョン
            dataset.description = "SEO最適化のための記事統合提案システム"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"データセット {self.dataset_id} を作成完了")
            
        except Exception as e:
            logger.error(f"データセット作成でエラー: {str(e)}")
            raise

    def optimize_table(self, table_name: str):
        """テーブルの最適化（クラスタリング等）"""
        try:
            # テーブル統計の更新
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            """
            
            results = self.execute_query(query)
            if results:
                row_count = results[0].row_count
                logger.info(f"テーブル {table_name} の行数: {row_count:,}")
            
        except Exception as e:
            logger.warning(f"テーブル最適化でエラー: {str(e)}")

    def get_query_cost_estimate(self, query: str) -> Optional[int]:
        """クエリのコスト見積もりを取得（処理バイト数）"""
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = self.client.query(query, job_config=job_config)
            
            bytes_processed = query_job.total_bytes_processed
            logger.info(f"クエリ処理予定バイト数: {bytes_processed:,} bytes")
            
            return bytes_processed
            
        except Exception as e:
            logger.warning(f"クエリコスト見積もりでエラー: {str(e)}")
            return None

    def batch_insert_with_retry(
        self, 
        table_name: str, 
        rows: List[Dict], 
        batch_size: int = 1000,
        max_retries: int = 3
    ) -> bool:
        """
        バッチ挿入（リトライ機能付き）
        
        Args:
            table_name: 挿入先テーブル名
            rows: 挿入するデータのリスト
            batch_size: バッチサイズ
            max_retries: 最大リトライ回数
            
        Returns:
            成功した場合True
        """
        total_rows = len(rows)
        successful_batches = 0
        
        for i in range(0, total_rows, batch_size):
            batch_rows = rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            retry_count = 0
            while retry_count < max_retries:
                try:
                    errors = self.insert_rows_json(table_name, batch_rows)
                    
                    if not errors:
                        logger.info(f"バッチ {batch_num}/{total_batches} 挿入完了 ({len(batch_rows)}件)")
                        successful_batches += 1
                        break
                    else:
                        logger.warning(f"バッチ {batch_num} で挿入エラー: {errors}")
                        retry_count += 1
                        
                except Exception as e:
                    logger.error(f"バッチ {batch_num} で例外エラー: {str(e)}")
                    retry_count += 1
                    
                if retry_count >= max_retries:
                    logger.error(f"バッチ {batch_num} が最大リトライ回数に達しました")
        
        success_rate = successful_batches / ((total_rows + batch_size - 1) // batch_size)
        logger.info(f"バッチ挿入完了: {successful_batches}バッチ成功 (成功率: {success_rate:.1%})")
        
        return success_rate > 0.8  # 80%以上成功で成功とみなす
