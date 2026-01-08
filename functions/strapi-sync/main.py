import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import functions_framework

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID', 'content_analysis')
ARTICLES_TABLE_ID = os.environ.get('ARTICLES_TABLE_ID', 'articles')
COURSES_TABLE_ID = os.environ.get('COURSES_TABLE_ID', 'courses')
STRAPI_BASE_URL = os.environ.get('STRAPI_BASE_URL')
STRAPI_API_TOKEN = os.environ.get('STRAPI_API_TOKEN')

# BigQueryクライアント初期化
try:
    client = bigquery.Client(project=PROJECT_ID)
    logger.info(f"BigQuery client initialized for project: {PROJECT_ID}")
except Exception as e:
    logger.error(f"BigQuery client initialization failed: {e}")
    client = None

def make_request_with_retry(url: str, headers: Dict, params: Dict = None, max_retries: int = 5, 
                          timeout: int = 300, backoff_factor: float = 2.0) -> requests.Response:
    """リトライ機能付きHTTPリクエスト（膨大なデータに対応）"""
    session = requests.Session()
    # コネクションプールの設定
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=0  # 手動でリトライを管理
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Request attempt {attempt + 1}/{max_retries + 1} to {url}")
            
            # タイムアウトを段階的に増加（より長めに）
            current_timeout = timeout * (1 + attempt * 0.5)
            
            response = session.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=(30, current_timeout),  # (接続タイムアウト, 読み取りタイムアウト)
                stream=False
            )
            
            response.raise_for_status()
            logger.info(f"Request successful on attempt {attempt + 1}, size: {len(response.content)} bytes")
            return response
            
        except requests.exceptions.Timeout as e:
            if attempt < max_retries:
                wait_time = backoff_factor ** attempt
                logger.warning(f"Request timeout on attempt {attempt + 1}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {max_retries + 1} attempts due to timeout")
                raise
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = backoff_factor ** attempt
                logger.warning(f"Request failed on attempt {attempt + 1}: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {max_retries + 1} attempts: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries:
                time.sleep(backoff_factor ** attempt)
            else:
                raise
    
    raise Exception("Unexpected error in make_request_with_retry")

def extract_text_from_html(html_content: str) -> str:
    """HTMLからテキストを抽出"""
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # 画像のalt属性も含める
        for img in soup.find_all('img'):
            if img.get('alt'):
                img.replace_with(f" {img.get('alt')} ")
        
        return soup.get_text(separator=' ', strip=True)
    except Exception as e:
        logger.error(f"HTML parsing error: {e}")
        return str(html_content)

def fetch_strapi_courses_paginated() -> List[Dict]:
    """Strapiから講座データをページネーション対応で取得"""
    if not STRAPI_BASE_URL:
        raise ValueError("STRAPI_BASE_URL is not set")
    
    all_courses = []
    page = 1
    page_size = 25  # ページサイズを小さくして負荷軽減
    
    headers = {}
    if STRAPI_API_TOKEN:
        headers['Authorization'] = f'Bearer {STRAPI_API_TOKEN}'
    
    try:
        while True:
            url = f"{STRAPI_BASE_URL}/api/courses"
            params = {
                'fields[0]': 'slug',
                'fields[1]': 'name',
                'populate': '*',
                'pagination[page]': page,
                'pagination[pageSize]': page_size
            }
            
            logger.info(f"Fetching courses page {page} from: {url}")
            
            response = make_request_with_retry(url, headers, params)
            data = response.json()
            
            courses_data = data.get('data', [])
            if not courses_data:
                logger.info(f"No more courses found on page {page}")
                break
            
            all_courses.extend(courses_data)
            logger.info(f"Page {page}: {len(courses_data)} courses fetched. Total: {len(all_courses)}")
            
            # ページネーション情報をチェック
            pagination = data.get('meta', {}).get('pagination', {})
            current_page = pagination.get('page', page)
            page_count = pagination.get('pageCount', 1)
            
            if current_page >= page_count:
                logger.info(f"All pages fetched. Total courses: {len(all_courses)}")
                break
            
            page += 1
            
            # APIレート制限対策として少し待機
            time.sleep(0.5)
        
        logger.info(f"取得した講座数: {len(all_courses)}")
        return all_courses
        
    except Exception as e:
        logger.error(f"講座データ取得エラー: {str(e)}")
        raise

def fetch_strapi_articles_paginated() -> List[Dict]:
    """Strapiからコラムページデータをページネーション対応で取得（膨大なデータに対応）"""
    if not STRAPI_BASE_URL:
        raise ValueError("STRAPI_BASE_URL is not set")
    
    all_articles = []
    page = 1
    page_size = 50  # ページサイズを増やして効率化（25 → 50）
    max_consecutive_failures = 3  # 連続失敗許容回数
    consecutive_failures = 0
    
    headers = {}
    if STRAPI_API_TOKEN:
        headers['Authorization'] = f'Bearer {STRAPI_API_TOKEN}'
    
    try:
        while True:
            try:
                url = f"{STRAPI_BASE_URL}/api/colum-pages"
                params = {
                    'populate[0]': 'QandA',
                    'populate[1]': 'article',
                    'populate[2]': 'article.paragraph',
                    'pagination[page]': page,
                    'pagination[pageSize]': page_size
                }
                
                logger.info(f"📥 Fetching articles page {page} (size: {page_size}) from: {url}")
                
                response = make_request_with_retry(url, headers, params, max_retries=5, timeout=300)
                data = response.json()
                
                articles_data = data.get('data', [])
                if not articles_data:
                    logger.info(f"✅ No more articles found on page {page}. Fetch complete!")
                    break
                
                all_articles.extend(articles_data)
                consecutive_failures = 0  # 成功したらリセット
                
                logger.info(f"✅ Page {page}: {len(articles_data)} articles fetched. Total: {len(all_articles)}")
                
                # ページネーション情報をチェック
                pagination = data.get('meta', {}).get('pagination', {})
                current_page = pagination.get('page', page)
                page_count = pagination.get('pageCount', 1)
                total_count = pagination.get('total', 0)
                
                logger.info(f"   Progress: Page {current_page}/{page_count}, Articles: {len(all_articles)}/{total_count}")
                
                if current_page >= page_count:
                    logger.info(f"🎉 All pages fetched! Total articles: {len(all_articles)}")
                    break
                
                page += 1
                
                # APIレート制限対策として少し待機（膨大なデータなので少し長めに）
                time.sleep(0.8)
                
            except Exception as page_error:
                consecutive_failures += 1
                logger.error(f"❌ Error fetching page {page} (attempt {consecutive_failures}/{max_consecutive_failures}): {str(page_error)}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"💥 Too many consecutive failures ({consecutive_failures}). Stopping fetch.")
                    logger.info(f"   Partial data retrieved: {len(all_articles)} articles")
                    break
                
                # 失敗したページをスキップして次へ
                logger.warning(f"⚠️  Skipping page {page} and continuing...")
                page += 1
                time.sleep(2)  # 少し長めに待機
        
        logger.info(f"📊 Final result: {len(all_articles)} articles fetched")
        return all_articles
        
    except Exception as e:
        logger.error(f"💥 Critical error in fetch_strapi_articles_paginated: {str(e)}")
        if all_articles:
            logger.info(f"   Returning partial data: {len(all_articles)} articles")
            return all_articles
        raise

def process_course_data(raw_courses: List[Dict]) -> List[Dict]:
    """講座データを処理してBigQuery用に変換"""
    processed_courses = []
    failed_courses = []
    
    for item in raw_courses:
        try:
            attributes = item.get('attributes', {})
            
            # 基本情報
            course_id = str(item.get('id'))
            slug = attributes.get('slug', '')
            name = attributes.get('name', '')  # nameフィールドがある場合
            description = attributes.get('description', '')
            
            # 日時変換
            created_at = None
            updated_at = None
            
            try:
                if attributes.get('createdAt'):
                    created_at = datetime.fromisoformat(attributes['createdAt'].replace('Z', '+00:00'))
                if attributes.get('updatedAt'):
                    updated_at = datetime.fromisoformat(attributes['updatedAt'].replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Date parsing error for course {course_id}: {e}")
            
            # BigQuery用レコード作成
            processed_course = {
                'id': course_id,
                'slug': slug,
                'name': name,  # nameがない場合はslugを使用
                'description': extract_text_from_html(description),
                'total_articles': 0,  # 後で集計
                'total_pageviews': 0,  # 後で集計
                'created_at': created_at.isoformat() if created_at else None,
                'updated_at': updated_at.isoformat() if updated_at else None,
                'last_synced': datetime.utcnow().isoformat()
            }
            
            processed_courses.append(processed_course)
            
        except Exception as e:
            logger.error(f"講座データ処理エラー (ID: {item.get('id')}): {str(e)}")
            failed_courses.append(item.get('id'))
            continue
    
    logger.info(f"処理完了した講座数: {len(processed_courses)}")
    if failed_courses:
        logger.warning(f"処理失敗した講座数: {len(failed_courses)}, IDs: {failed_courses}")
    return processed_courses

def process_article_data(raw_articles: List[Dict]) -> List[Dict]:
    """記事データを処理してBigQuery用に変換（full_content_html生成、エラーに強い）"""
    processed_articles = []
    failed_articles = []
    
    total = len(raw_articles)
    logger.info(f"🔄 Processing {total} articles...")

    for idx, item in enumerate(raw_articles, 1):
        article_id = str(item.get('id', 'unknown'))
        
        if idx % 50 == 0:
            logger.info(f"   Progress: {idx}/{total} articles processed ({idx/total*100:.1f}%)")
        
        try:
            attributes = item.get('attributes', {})
            
            # 基本情報
            title = attributes.get('POST_TITLE', '')
            link = attributes.get('LINK', '')
            koza_id = str(attributes.get('KOZA_ID', ''))
            
            # HTMLコンテンツとプレーンテキストコンテンツを構築
            html_parts = []
            plain_text_parts = []
            
            # 1. 記事タイトル (h1)
            if title:
                html_parts.append(f"<h1>{title}</h1>")
                plain_text_parts.append(title)
            
            # 2. 冒頭説明文 (description)
            description = attributes.get('description', '')
            if description:
                html_parts.append(f"<div class='description'>{description}</div>")
                plain_text_parts.append(extract_text_from_html(description))

            # 3. 記事本文 (article)
            for article_section in attributes.get('article', []):
                section_title = article_section.get('title', '')
                if section_title:
                    html_parts.append(f"<h2>{section_title}</h2>")
                    plain_text_parts.append(section_title)
                
                # textフィールドの処理 (存在する場合)
                section_text = article_section.get('text')
                if section_text:
                    html_parts.append(section_text)
                    plain_text_parts.append(extract_text_from_html(section_text))

                for paragraph in article_section.get('paragraph', []):
                    subtitle = paragraph.get('subtitle', '')
                    if subtitle:
                        html_parts.append(f"<h3>{subtitle}</h3>")
                        plain_text_parts.append(subtitle)
                    
                    paragraph_text = paragraph.get('text', '')
                    if paragraph_text:
                        html_parts.append(paragraph_text)
                        plain_text_parts.append(extract_text_from_html(paragraph_text))

            # 4. Q&Aセクション
            qanda_list = attributes.get('QandA', [])
            if qanda_list:
                html_parts.append("<h2>Q&A</h2>")
                plain_text_parts.append("Q&A")
                qanda_html = "<dl class='qanda-list'>"
                qanda_plain = []
                for qanda in qanda_list:
                    question = qanda.get('Question', '')
                    answer = qanda.get('Answer', '')
                    if question:
                        qanda_html += f"<dt>{question}</dt>"
                        qanda_plain.append(f"Q: {question}")
                    if answer:
                        qanda_html += f"<dd>{answer}</dd>"
                        qanda_plain.append(f"A: {extract_text_from_html(answer)}")
                qanda_html += "</dl>"
                html_parts.append(qanda_html)
                plain_text_parts.append("\n".join(qanda_plain))

            # コンテンツを結合
            full_content_html = "\n".join(html_parts).strip()
            full_content = "\n".join(plain_text_parts).strip()
            qanda_content = "\n".join(qanda_plain) if qanda_list else "" # qanda_contentも更新

            # 日時変換
            created_at = None
            updated_at = None
            try:
                if attributes.get('createdAt'):
                    created_at = datetime.fromisoformat(attributes['createdAt'].replace('Z', '+00:00'))
                if attributes.get('updatedAt'):
                    updated_at = datetime.fromisoformat(attributes['updatedAt'].replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Date parsing error for article {article_id}: {e}")
            
            # BigQuery用レコード作成
            processed_article = {
                'id': article_id,
                'title': title,
                'link': link,
                'koza_id': koza_id,
                'full_content': full_content,
                'full_content_html': full_content_html,
                'qanda_content': qanda_content,
                'content_type': 'article',
                'pageviews': None,
                'content_embedding': [],
                'embedding_model': None,
                'created_at': created_at.isoformat() if created_at else None,
                'updated_at': updated_at.isoformat() if updated_at else None,
                'last_synced': datetime.utcnow().isoformat()
            }
            processed_articles.append(processed_article)
            
        except Exception as e:
            logger.error(f"❌ 記事データ処理エラー (ID: {article_id}): {str(e)}", exc_info=False)
            failed_articles.append(article_id)
            # エラーでも処理を続行
            continue
    
    logger.info(f"✅ 処理完了した記事数: {len(processed_articles)}/{total} ({len(processed_articles)/total*100:.1f}%)")
    if failed_articles:
        logger.warning(f"⚠️  処理失敗した記事数: {len(failed_articles)}, IDs: {failed_articles[:20]}...")
    
    return processed_articles

def update_course_statistics(courses: List[Dict], articles: List[Dict]):
    """講座の統計情報（記事数、総ページビュー数）を更新"""
    course_stats = {}
    
    # 講座別の記事数を集計
    for article in articles:
        koza_id = article.get('koza_id', '')
        if koza_id:
            if koza_id not in course_stats:
                course_stats[koza_id] = {'total_articles': 0, 'total_pageviews': 0}
            course_stats[koza_id]['total_articles'] += 1
            # ページビューがある場合は加算（後でGA4データと統合）
            if article.get('pageviews'):
                course_stats[koza_id]['total_pageviews'] += article['pageviews']
    
    # 講座データに統計情報を設定
    for course in courses:
        course_id = course['id']
        if course_id in course_stats:
            course['total_articles'] = course_stats[course_id]['total_articles']
            course['total_pageviews'] = course_stats[course_id]['total_pageviews']
    
    logger.info(f"講座統計情報を更新: {len(course_stats)}講座")

def create_temp_table_from_records(table_id: str, records: List[Dict], record_type: str) -> str:
    """レコードから一時テーブルを作成（改良版：失敗を減らす）"""
    if not records:
        return None
    
    # 一時テーブル名を生成
    timestamp = int(time.time())
    temp_table_name = f"temp_{table_id}_{timestamp}"
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
    
    logger.info(f"一時テーブル作成中: {temp_table_id}")
    
    # 元のテーブルのスキーマを取得
    original_table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table_id}")
    
    # 一時テーブルを作成
    temp_table = bigquery.Table(temp_table_id, schema=original_table.schema)
    temp_table = client.create_table(temp_table)
    
    # より保守的なバッチサイズ設定
    INITIAL_BATCH_SIZE = 20  # さらに小さく
    MAX_BATCH_SIZE = 50     # 最大値も小さく
    MIN_BATCH_SIZE = 1      # 最小値を1に
    
    current_batch_size = INITIAL_BATCH_SIZE
    total_inserted = 0
    total_failed = 0
    batch_num = 0
    failed_record_ids = []
    
    try:
        i = 0
        while i < len(records):
            batch = records[i:i + current_batch_size]
            batch_num += 1
            
            logger.info(f"バッチ {batch_num} 処理中: {len(batch)}件 (サイズ: {current_batch_size})")
            
            try:
                # データを挿入
                errors = client.insert_rows_json(temp_table, batch)
                
                if errors:
                    logger.error(f"バッチ挿入エラー: {errors}")
                    
                    # 個別レコード処理に切り替え
                    logger.info("個別レコード処理に切り替えます")
                    success_count = 0
                    for record in batch:
                        try:
                            single_errors = client.insert_rows_json(temp_table, [record])
                            if not single_errors:
                                success_count += 1
                            else:
                                logger.error(f"個別レコード挿入エラー ID {record.get('id')}: {single_errors}")
                                failed_record_ids.append(record.get('id'))
                                total_failed += 1
                        except Exception as e:
                            logger.error(f"個別レコード挿入例外 ID {record.get('id')}: {str(e)}")
                            failed_record_ids.append(record.get('id'))
                            total_failed += 1
                    
                    total_inserted += success_count
                    logger.info(f"個別処理で {success_count}/{len(batch)} 件成功")
                    
                    # バッチサイズを最小に調整
                    current_batch_size = MIN_BATCH_SIZE
                else:
                    # 成功した場合
                    total_inserted += len(batch)
                    logger.info(f"バッチ {batch_num} 挿入成功 ({len(batch)}件)")
                    
                    # 成功したらバッチサイズを少し増やす（最大値まで）
                    if current_batch_size < MAX_BATCH_SIZE:
                        current_batch_size = min(MAX_BATCH_SIZE, current_batch_size + 5)
                
                i += len(batch)  # バッチサイズではなく、実際に処理したレコード数で進める
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"バッチ処理エラー: {error_msg}")
                
                # ペイロードサイズエラーの場合
                if "413" in error_msg or "too large" in error_msg.lower() or "payload" in error_msg.lower():
                    if current_batch_size > MIN_BATCH_SIZE:
                        current_batch_size = max(MIN_BATCH_SIZE, current_batch_size // 2)
                        logger.warning(f"ペイロードサイズエラー。バッチサイズを {current_batch_size} に削減")
                        continue  # 同じバッチを小さいサイズで再試行
                    else:
                        # 最小バッチサイズでも413エラーの場合、個別処理
                        logger.warning("個別レコード処理に強制切り替え（大きすぎるフィールドを切り詰め）")
                        for record in batch:
                            try:
                                # レコードサイズを削減（full_content_htmlも含む）
                                minimal_record = record.copy()
                                if 'full_content' in minimal_record and minimal_record.get('full_content') and len(minimal_record['full_content']) > 50000:
                                    logger.warning(f"  Truncating full_content for ID {record.get('id')}: {len(minimal_record['full_content'])} chars")
                                    minimal_record['full_content'] = minimal_record['full_content'][:50000] + "...[truncated]"
                                if 'full_content_html' in minimal_record and minimal_record.get('full_content_html') and len(minimal_record['full_content_html']) > 50000:
                                    logger.warning(f"  Truncating full_content_html for ID {record.get('id')}: {len(minimal_record['full_content_html'])} chars")
                                    minimal_record['full_content_html'] = minimal_record['full_content_html'][:50000] + "...[truncated]"
                                if 'qanda_content' in minimal_record and minimal_record.get('qanda_content') and len(minimal_record['qanda_content']) > 10000:
                                    minimal_record['qanda_content'] = minimal_record['qanda_content'][:10000] + "...[truncated]"
                                if 'description' in minimal_record and minimal_record.get('description') and len(minimal_record['description']) > 5000:
                                    minimal_record['description'] = minimal_record['description'][:5000] + "...[truncated]"
                                
                                single_errors = client.insert_rows_json(temp_table, [minimal_record])
                                if not single_errors:
                                    total_inserted += 1
                                else:
                                    logger.error(f"最小化レコード挿入エラー ID {record.get('id')}: {single_errors}")
                                    failed_record_ids.append(record.get('id'))
                                    total_failed += 1
                            except Exception as minimal_error:
                                logger.error(f"最小化レコード処理例外 ID {record.get('id')}: {str(minimal_error)}")
                                failed_record_ids.append(record.get('id'))
                                total_failed += 1
                        
                        i += len(batch)
                else:
                    # その他のエラー - 個別処理を試行
                    logger.warning("その他のエラーにより個別処理に切り替え")
                    for record in batch:
                        try:
                            single_errors = client.insert_rows_json(temp_table, [record])
                            if not single_errors:
                                total_inserted += 1
                            else:
                                failed_record_ids.append(record.get('id'))
                                total_failed += 1
                        except Exception as individual_error:
                            logger.error(f"個別処理例外 ID {record.get('id')}: {str(individual_error)}")
                            failed_record_ids.append(record.get('id'))
                            total_failed += 1
                    
                    i += len(batch)
        
        logger.info(f"一時テーブル挿入完了: 成功 {total_inserted}/{len(records)} 件, 失敗 {total_failed} 件")
        
        if failed_record_ids:
            logger.warning(f"失敗したレコードID: {failed_record_ids[:20]}...")  # 最初の20件のみ表示
        
        if total_inserted == 0:
            # 一時テーブルを削除
            client.delete_table(temp_table_id)
            raise Exception(f"一時テーブルへのデータ挿入が全て失敗しました")
        
        return temp_table_name
        
    except Exception as e:
        # エラー時は一時テーブルを削除
        try:
            client.delete_table(temp_table_id)
        except:
            pass
        raise Exception(f"一時テーブル作成エラー: {str(e)}")

def check_streaming_buffer(table_id: str) -> bool:
    """Streaming Bufferの状態をチェック"""
    try:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        table = client.get_table(full_table_id)
        
        # Streaming Bufferの情報を確認
        if hasattr(table, 'streaming_buffer') and table.streaming_buffer:
            estimated_rows = table.streaming_buffer.estimated_rows
            oldest_entry_time = table.streaming_buffer.oldest_entry_time
            logger.warning(f"Streaming Buffer検出: {estimated_rows}行, 最古エントリ: {oldest_entry_time}")
            return True
        return False
    except Exception as e:
        logger.warning(f"Streaming Buffer確認エラー: {str(e)}")
        return False

def wait_for_streaming_buffer_clear(table_id: str, max_wait_minutes: int = 5) -> bool:
    """Streaming Bufferがクリアされるまで待機（短時間）"""
    import time
    
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    wait_seconds = 0
    max_wait_seconds = max_wait_minutes * 60
    check_interval = 30  # 30秒間隔でチェック
    
    logger.info(f"Streaming Bufferクリア待機開始（最大{max_wait_minutes}分）")
    
    while wait_seconds < max_wait_seconds:
        try:
            table = client.get_table(full_table_id)
            
            if not hasattr(table, 'streaming_buffer') or not table.streaming_buffer:
                logger.info(f"Streaming Bufferがクリアされました（{wait_seconds}秒後）")
                return True
            
            if table.streaming_buffer:
                estimated_rows = table.streaming_buffer.estimated_rows or 0
                logger.info(f"Streaming Buffer残存: {estimated_rows}行 - {wait_seconds}/{max_wait_seconds}秒経過")
            
            time.sleep(check_interval)
            wait_seconds += check_interval
            
        except Exception as e:
            logger.warning(f"Streaming Buffer確認エラー: {str(e)}")
            time.sleep(check_interval)
            wait_seconds += check_interval
    
    logger.warning(f"Streaming Bufferクリア待機タイムアウト（{max_wait_minutes}分）")
    return False

def force_update_existing_records(table_id: str, records: List[Dict], record_type: str):
    """既存レコードの強制更新（DELETE + INSERT方式）"""
    if not records:
        return
    
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    try:
        # 更新対象のIDリストを作成
        record_ids = [record['id'] for record in records]
        ids_string = "', '".join(record_ids)
        
        logger.info(f"既存{record_type}レコード {len(record_ids)}件の強制更新を開始")
        
        # 1. 既存レコードを削除
        delete_query = f"""
        DELETE FROM `{full_table_id}`
        WHERE id IN ('{ids_string}')
        """
        
        logger.info(f"既存レコード削除中...")
        delete_job = client.query(delete_query)
        delete_result = delete_job.result()
        
        if hasattr(delete_job, 'num_dml_affected_rows'):
            deleted_rows = delete_job.num_dml_affected_rows
            logger.info(f"削除完了: {deleted_rows}行")
        
        # 2. 新しいレコードを挿入
        logger.info(f"新しいレコード挿入中...")
        table = client.get_table(full_table_id)
        
        # バッチサイズを設定
        BATCH_SIZE = 50  # 安全のため小さめに設定
        total_inserted = 0
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"更新バッチ {batch_num}/{total_batches} を処理中... ({len(batch)}件)")
            
            try:
                errors = client.insert_rows_json(table, batch)
                if errors:
                    logger.error(f"バッチ挿入エラー: {errors}")
                    # 個別レコードで再試行
                    for record in batch:
                        try:
                            single_errors = client.insert_rows_json(table, [record])
                            if not single_errors:
                                total_inserted += 1
                            else:
                                logger.error(f"個別レコード挿入エラー ID {record.get('id')}: {single_errors}")
                        except Exception as e:
                            logger.error(f"個別レコード挿入例外 ID {record.get('id')}: {str(e)}")
                else:
                    total_inserted += len(batch)
                    logger.info(f"バッチ {batch_num} 挿入成功 ({len(batch)}件)")
            except Exception as e:
                logger.error(f"バッチ処理エラー: {str(e)}")
        
        logger.info(f"強制更新完了: {total_inserted}件の{record_type}レコードを更新しました")
        
    except Exception as e:
        logger.error(f"強制更新エラー: {str(e)}")
        raise

def upsert_to_bigquery_with_fallback(table_id: str, records: List[Dict], record_type: str):
    """改良されたStreaming Bufferを考慮したフォールバック付きUPSERT"""
    if not records:
        logger.info(f"アップロードする{record_type}データがありません")
        return
    
    if not client:
        raise Exception("BigQuery client is not initialized")
    
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    # Streaming Bufferの確認
    has_streaming_buffer = check_streaming_buffer(table_id)
    
    if has_streaming_buffer:
        logger.warning(f"Streaming Bufferが検出されました。")
        
        # 短時間待機してStreaming Bufferクリアを試行
        if wait_for_streaming_buffer_clear(table_id, max_wait_minutes=3):
            logger.info("Streaming Bufferがクリアされました。MERGE文で処理します。")
            try:
                upsert_to_bigquery_merge(table_id, records, record_type)
                return
            except Exception as e:
                if "streaming buffer" in str(e).lower():
                    logger.warning(f"MERGE文でStreaming Bufferエラーが再発。強制更新モードに切り替えます。")
                else:
                    raise
        
        # Streaming Bufferがクリアされない場合は強制更新モード
        logger.info("強制更新モード（DELETE + INSERT）で処理します")
        
        # 既存データのIDを取得
        existing_ids_query = f"SELECT id FROM `{full_table_id}`"
        existing_ids = set()
        
        try:
            query_job = client.query(existing_ids_query)
            for row in query_job:
                existing_ids.add(row.id)
            logger.info(f"既存{record_type}ID数: {len(existing_ids)}")
        except Exception as e:
            logger.warning(f"既存ID取得エラー: {str(e)}")
        
        # 新規と更新対象に分類
        new_records = []
        update_records = []
        
        for record in records:
            if record['id'] in existing_ids:
                update_records.append(record)
            else:
                new_records.append(record)
        
        logger.info(f"{record_type}データ分析: 新規{len(new_records)}件, 更新対象{len(update_records)}件")
        
        # 新規データの挿入
        if new_records:
            logger.info(f"新規{record_type}データを挿入中...")
            table = client.get_table(full_table_id)
            
            BATCH_SIZE = 50
            total_inserted = 0
            
            for i in range(0, len(new_records), BATCH_SIZE):
                batch = new_records[i:i + BATCH_SIZE]
                try:
                    errors = client.insert_rows_json(table, batch)
                    if not errors:
                        total_inserted += len(batch)
                    else:
                        logger.error(f"新規データ挿入エラー: {errors}")
                except Exception as e:
                    logger.error(f"新規データ挿入例外: {str(e)}")
            
            logger.info(f"新規{record_type}データ {total_inserted}件を挿入しました")
        
        # 既存データの強制更新
        if update_records:
            force_update_existing_records(table_id, update_records, record_type)
        
    else:
        # Streaming Bufferがない場合はMERGE文を使用
        try:
            upsert_to_bigquery_merge(table_id, records, record_type)
        except Exception as e:
            if "streaming buffer" in str(e).lower():
                logger.warning(f"MERGE文でStreaming Bufferエラーが発生。強制更新モードにフォールバック: {str(e)}")
                # 再帰的に呼び出すのではなく、強制更新を直接実行
                force_update_existing_records(table_id, records, record_type)
            else:
                raise

def upsert_to_bigquery_merge(table_id: str, records: List[Dict], record_type: str):
    """改良されたMERGE文を使用したUPSERT処理"""
    temp_table_name = None
    
    try:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        
        # 一時テーブルを作成してデータを挿入
        temp_table_name = create_temp_table_from_records(table_id, records, record_type)
        if not temp_table_name:
            logger.info(f"処理する{record_type}データがありません")
            return
        
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
        
        # テーブルの種類に応じてMERGE文を構築
        if table_id == COURSES_TABLE_ID:
            merge_query = f"""
            MERGE `{full_table_id}` T
            USING `{temp_table_id}` S
            ON T.id = S.id
            WHEN MATCHED THEN
              UPDATE SET
                slug = S.slug,
                name = S.name,
                description = S.description,
                total_articles = S.total_articles,
                total_pageviews = S.total_pageviews,
                updated_at = S.updated_at,
                last_synced = S.last_synced
            WHEN NOT MATCHED THEN
              INSERT (id, slug, name, description, total_articles, total_pageviews, created_at, updated_at, last_synced)
              VALUES (S.id, S.slug, S.name, S.description, S.total_articles, S.total_pageviews, S.created_at, S.updated_at, S.last_synced)
            """
        elif table_id == ARTICLES_TABLE_ID:
            # 記事データのMERGE文を改良（full_content_htmlのみ使用）
            merge_query = f"""
            MERGE `{full_table_id}` T
            USING `{temp_table_id}` S
            ON T.id = S.id
            WHEN MATCHED THEN
              UPDATE SET
                title = S.title,
                link = S.link,
                koza_id = S.koza_id,
                full_content_html = S.full_content_html,
                qanda_content = S.qanda_content,
                content_type = S.content_type,
                pageviews = COALESCE(S.pageviews, T.pageviews),
                content_embedding = CASE 
                  WHEN S.content_embedding IS NOT NULL AND ARRAY_LENGTH(S.content_embedding) > 0 
                  THEN S.content_embedding 
                  ELSE T.content_embedding 
                END,
                embedding_model = COALESCE(S.embedding_model, T.embedding_model),
                updated_at = S.updated_at,
                last_synced = S.last_synced
            WHEN NOT MATCHED THEN
              INSERT (id, title, link, koza_id, full_content_html, qanda_content, content_type, pageviews, content_embedding, embedding_model, created_at, updated_at, last_synced)
              VALUES (S.id, S.title, S.link, S.koza_id, S.full_content_html, S.qanda_content, S.content_type, S.pageviews, S.content_embedding, S.embedding_model, S.created_at, S.updated_at, S.last_synced)
            """
        else:
            raise ValueError(f"サポートされていないテーブル: {table_id}")
        
        logger.info(f"{record_type}データのMERGE処理を実行中...")
        
        # MERGE文を実行
        query_job = client.query(merge_query)
        result = query_job.result()  # 実行完了を待機
        
        # 結果を確認
        if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows is not None:
            affected_rows = query_job.num_dml_affected_rows
            logger.info(f"MERGE処理完了: {affected_rows}行が影響を受けました")
        else:
            logger.info(f"MERGE処理完了: {record_type}データが正常に同期されました")
        
        # 実際の更新・挿入件数をより詳細に確認
        check_query = f"""
        SELECT 
          COUNT(*) as total_count,
          COUNT(CASE WHEN last_synced >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) THEN 1 END) as recent_sync_count
        FROM `{full_table_id}`
        WHERE id IN (SELECT id FROM `{temp_table_id}`)
        """
        
        check_job = client.query(check_query)
        for row in check_job:
            logger.info(f"同期確認: 対象{row.total_count}件中{row.recent_sync_count}件が最近同期されました")
        
        logger.info(f"BigQueryに{len(records)}件の{record_type}データを正常にUPSERTしました")
        
    finally:
        # 一時テーブルを削除
        if temp_table_name:
            try:
                temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{temp_table_name}"
                client.delete_table(temp_table_id)
                logger.info(f"一時テーブルを削除しました: {temp_table_name}")
            except Exception as e:
                logger.warning(f"一時テーブルの削除エラー: {str(e)}")

# 下位互換のためのエイリアス
def upsert_to_bigquery(table_id: str, records: List[Dict], record_type: str):
    """BigQueryにデータをUPSERT（Streaming Buffer対応）"""
    return upsert_to_bigquery_with_fallback(table_id, records, record_type)

@functions_framework.http
def sync_strapi_data(request):
    """Cloud Functions HTTPエントリーポイント - 講座と記事データの同期"""
    try:
        logger.info("=== Strapi データ同期開始（講座・記事） ===")
        logger.info(f"Request method: {request.method}")
        logger.info(f"Request path: {request.path}")
        
        # 環境変数チェック
        if not PROJECT_ID:
            raise ValueError("PROJECT_ID environment variable is required")
        if not STRAPI_BASE_URL:
            raise ValueError("STRAPI_BASE_URL environment variable is required")
        
        logger.info(f"Project ID: {PROJECT_ID}")
        logger.info(f"Dataset ID: {DATASET_ID}")
        logger.info(f"Articles Table ID: {ARTICLES_TABLE_ID}")
        logger.info(f"Courses Table ID: {COURSES_TABLE_ID}")
        logger.info(f"Strapi URL: {STRAPI_BASE_URL}")
        
        # 1. 講座データ取得・処理
        logger.info("=" * 60)
        logger.info("📚 PHASE 1: 講座データ処理")
        logger.info("=" * 60)
        raw_courses = fetch_strapi_courses_paginated()
        processed_courses = process_course_data(raw_courses)
        
        # 2. 記事データ取得・処理
        logger.info("=" * 60)
        logger.info("📝 PHASE 2: 記事データ処理")
        logger.info("=" * 60)
        raw_articles = fetch_strapi_articles_paginated()
        processed_articles = process_article_data(raw_articles)
        
        # 3. 講座統計情報の更新
        logger.info("=" * 60)
        logger.info("📊 PHASE 3: 講座統計情報更新")
        logger.info("=" * 60)
        update_course_statistics(processed_courses, processed_articles)
        
        # 4. BigQueryに保存（講座データ）
        logger.info("=" * 60)
        logger.info("💾 PHASE 4: 講座データ保存")
        logger.info("=" * 60)
        upsert_to_bigquery(COURSES_TABLE_ID, processed_courses, "講座")
        
        # 5. BigQueryに保存（記事データ）
        logger.info("=" * 60)
        logger.info("💾 PHASE 5: 記事データ保存（full_content_html含む）")
        logger.info("=" * 60)
        upsert_to_bigquery(ARTICLES_TABLE_ID, processed_articles, "記事")
        
        result = {
            'status': 'success',
            'message': f'🎉 講座{len(processed_courses)}件、記事{len(processed_articles)}件のデータを同期しました（full_content_html含む）',
            'courses_count': len(processed_courses),
            'articles_count': len(processed_articles),
            'raw_articles_fetched': len(raw_articles),
            'timestamp': datetime.utcnow().isoformat(),
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_ID,
            'courses_table_id': COURSES_TABLE_ID,
            'articles_table_id': ARTICLES_TABLE_ID
        }
        
        logger.info("=" * 60)
        logger.info("🎉 同期完了！")
        logger.info("=" * 60)
        logger.info(f"結果: {result}")
        return result, 200
        
    except Exception as e:
        logger.error(f"=== 同期処理エラー ===: {str(e)}", exc_info=True)
        error_result = {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
        return error_result, 500

@functions_framework.http
def hello_world(request):
    """テスト用の簡単なエンドポイント"""
    return {'message': 'Hello from Cloud Functions!', 'timestamp': datetime.utcnow().isoformat()}, 200