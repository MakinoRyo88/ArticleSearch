from google.cloud import aiplatform
import logging
from typing import List, Optional
import time
import json
import requests
from google.auth import default
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)

class VertexAIClient:
    """Vertex AI Text Embeddings APIクライアント（修正版）"""
    
    def __init__(self, project_id: str = "seo-optimize-464208", location: str = "asia-northeast1"):
        self.project_id = project_id
        self.location = location
        # 正しいモデル名に修正
        self.model_name = "text-embedding-004"  # または "textembedding-gecko@latest"
        
        # Vertex AI初期化
        aiplatform.init(project=project_id, location=location)
        
        # 認証情報の取得
        self.credentials, _ = default()
        
        # APIエンドポイント
        self.endpoint = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{location}/publishers/google/models/{self.model_name}:predict"
        
        logger.info(f"Vertex AI初期化完了 - Project: {project_id}, Location: {location}, Model: {self.model_name}")
    
    def generate_embedding(self, text: str, retry_count: int = 3) -> Optional[List[float]]:
        """
        テキストの埋め込みベクトルを生成（REST API使用）
        
        Args:
            text: 埋め込みを生成するテキスト
            retry_count: リトライ回数
            
        Returns:
            埋め込みベクトル（失敗時はNone）
        """
        if not text or not text.strip():
            logger.warning("空のテキストが渡されました")
            return None
        
        for attempt in range(retry_count):
            try:
                # アクセストークンの取得
                self.credentials.refresh(Request())
                access_token = self.credentials.token
                
                # リクエストヘッダー
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                # リクエストボディ
                payload = {
                    "instances": [
                        {
                            "content": text,
                            "task_type": "RETRIEVAL_DOCUMENT"  # 文書検索用の埋め込み
                        }
                    ]
                }
                
                # API呼び出し
                response = requests.post(
                    self.endpoint,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'predictions' in result and len(result['predictions']) > 0:
                        prediction = result['predictions'][0]
                        if 'embeddings' in prediction:
                            embedding_vector = prediction['embeddings']['values']
                            logger.info(f"埋め込み生成成功 - 次元数: {len(embedding_vector)}")
                            return embedding_vector
                        else:
                            logger.warning("レスポンスに埋め込みデータがありません")
                            return None
                    else:
                        logger.warning("予測結果が空です")
                        return None
                else:
                    logger.warning(f"API呼び出し失敗: {response.status_code} - {response.text}")
                    
                    # 404エラーの場合は別のモデルを試す
                    if response.status_code == 404 and attempt == 0:
                        logger.info("別のモデル名で再試行します")
                        self.model_name = "textembedding-gecko@latest"
                        self.endpoint = f"https://{self.location}-aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/publishers/google/models/{self.model_name}:predict"
                        continue
                    
                    if attempt < retry_count - 1:
                        wait_time = (2 ** attempt) + 1
                        logger.info(f"{wait_time}秒待機してリトライします")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"埋め込み生成に失敗しました: {response.status_code} - {response.text}")
                        return None
                        
            except Exception as e:
                logger.warning(f"埋め込み生成試行 {attempt + 1}/{retry_count} 失敗: {str(e)}")
                
                if attempt < retry_count - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.info(f"{wait_time}秒待機してリトライします")
                    time.sleep(wait_time)
                else:
                    logger.error(f"埋め込み生成に失敗しました: {str(e)}")
                    return None
        
        return None
    
    def generate_embedding_with_sdk(self, text: str, retry_count: int = 3) -> Optional[List[float]]:
        """
        SDK使用版の埋め込み生成（フォールバック用）
        """
        try:
            from vertexai.language_models import TextEmbeddingModel
            
            # 利用可能なモデル名を順番に試す
            model_names = [
                "text-embedding-004",
                "textembedding-gecko@latest", 
                "textembedding-gecko@003",
                "textembedding-gecko@002",
                "textembedding-gecko@001"
            ]
            
            for model_name in model_names:
                try:
                    logger.info(f"モデル {model_name} で埋め込み生成を試行")
                    model = TextEmbeddingModel.from_pretrained(model_name)
                    embeddings = model.get_embeddings([text])
                    
                    if embeddings and len(embeddings) > 0:
                        embedding_vector = embeddings[0].values
                        logger.info(f"埋め込み生成成功 (SDK) - モデル: {model_name}, 次元数: {len(embedding_vector)}")
                        self.model_name = model_name  # 成功したモデル名を保存
                        return embedding_vector
                        
                except Exception as e:
                    logger.warning(f"モデル {model_name} での生成失敗: {str(e)}")
                    continue
            
            logger.error("すべてのモデルで埋め込み生成に失敗しました")
            return None
            
        except ImportError:
            logger.error("vertexai.language_models のインポートに失敗しました")
            return None
        except Exception as e:
            logger.error(f"SDK使用版埋め込み生成エラー: {str(e)}")
            return None
    
    def test_model_availability(self) -> List[str]:
        """
        利用可能なモデルをテスト
        
        Returns:
            利用可能なモデル名のリスト
        """
        available_models = []
        test_text = "これはテストです。"
        
        model_names = [
            "text-embedding-004",
            "textembedding-gecko@latest",
            "textembedding-gecko@003", 
            "textembedding-gecko@002",
            "textembedding-gecko@001"
        ]
        
        for model_name in model_names:
            try:
                # 一時的にモデル名を変更してテスト
                original_model = self.model_name
                original_endpoint = self.endpoint
                
                self.model_name = model_name
                self.endpoint = f"https://{self.location}-aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/publishers/google/models/{self.model_name}:predict"
                
                result = self.generate_embedding(test_text, retry_count=1)
                
                if result is not None:
                    available_models.append(model_name)
                    logger.info(f"モデル {model_name} は利用可能です")
                else:
                    logger.warning(f"モデル {model_name} は利用できません")
                
                # 元の設定に戻す
                self.model_name = original_model
                self.endpoint = original_endpoint
                
            except Exception as e:
                logger.warning(f"モデル {model_name} のテスト中にエラー: {str(e)}")
        
        return available_models
    
    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 5) -> List[Optional[List[float]]]:
        """
        複数テキストの埋め込みをバッチ生成
        
        Args:
            texts: テキストのリスト
            batch_size: バッチサイズ
            
        Returns:
            埋め込みベクトルのリスト
        """
        results = []
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            logger.info(f"バッチ処理 {i//batch_size + 1}: {len(batch_texts)}件")
            
            # 個別に処理（REST APIの制限のため）
            for text in batch_texts:
                embedding = self.generate_embedding(text)
                results.append(embedding)
                
                # API制限対策
                time.sleep(0.1)
        
        return results
