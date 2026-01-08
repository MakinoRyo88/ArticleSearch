import logging
import time
from typing import List, Optional

import requests
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import aiplatform

logger = logging.getLogger(__name__)

class VertexAIClient:
    """Vertex AI Text Embeddings APIクライアント"""
    
    def __init__(self, project_id: str, location: str = "asia-northeast1"):
        self.project_id = project_id
        self.location = location
        self.model_name = "text-embedding-004"
        
        aiplatform.init(project=project_id, location=location)
        
        self.credentials, _ = default()
        
        self.endpoint = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{location}/publishers/google/models/{self.model_name}:predict"
        
        logger.info(f"Vertex AI Client initialized - Project: {project_id}, Model: {self.model_name}")
    
    def generate_embedding(self, text: str, retry_count: int = 3) -> Optional[List[float]]:
        """テキストの埋め込みベクトルを生成（REST API使用）"""
        if not text or not text.strip():
            logger.warning("Embedding generation skipped for empty text.")
            return None
        
        for attempt in range(retry_count):
            try:
                self.credentials.refresh(Request())
                access_token = self.credentials.token
                
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    "instances": [
                        {
                            "content": text,
                            "task_type": "RETRIEVAL_DOCUMENT"
                        }
                    ]
                }
                
                response = requests.post(self.endpoint, headers=headers, json=payload, timeout=60)
                
                response.raise_for_status()

                result = response.json()
                
                if 'predictions' in result and len(result['predictions']) > 0:
                    prediction = result['predictions'][0]
                    if 'embeddings' in prediction and 'values' in prediction['embeddings']:
                        embedding_vector = prediction['embeddings']['values']
                        logger.info(f"Embedding generated successfully. Dimensions: {len(embedding_vector)}")
                        return embedding_vector

                logger.warning(f"API response did not contain expected embedding data. Response: {result}")
                return None

            except requests.exceptions.RequestException as e:
                logger.warning(f"API call failed on attempt {attempt + 1}/{retry_count}: {e}")
                if attempt < retry_count - 1:
                    wait_time = (2 ** attempt)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Failed to generate embedding after multiple retries.")
                    return None
        return None
