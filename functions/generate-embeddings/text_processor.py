import re
import html
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Tuple, Optional
import unicodedata
from course_mapper import CourseMapper

logger = logging.getLogger(__name__)

class TextProcessor:
    """宅建・資格試験記事に最適化されたテキスト前処理クラス"""
    
    def __init__(self, project_id: str = "seo-optimize-464208"):
        self.max_length = 8000  # Vertex AI Text Embeddings APIの制限
        self.min_length = 50    # 最小テキスト長
        
        # 講座マッパーの初期化
        self.course_mapper = CourseMapper(project_id)
        
        # 法律・資格試験特有のパターン
        self.legal_patterns = {
            # 条文参照パターン
            'article_ref': r'([^\s]+法[第]?\d+条[の]?\d*(?:第\d+項)?)',
            # 重要キーワード
            'important_terms': r'(宅建業法|宅地建物取引士|重要事項説明|営業保証金|供託所|行政書士|社会保険労務士|ファイナンシャルプランナー|簿記|マンション管理士)',
            # 質問回答パターン
            'qa_pattern': r'Q[:：]\s*([^\n]+(?:\n(?!A[:：])[^\n]*)*)\s*A[:：]\s*([^\n]+(?:\n(?!Q[:：])[^\n]*)*)',
        }
        
        # 講座別特化キーワード
        self.course_specific_keywords = {
            "1": ["行政書士法", "許認可", "官公署", "代理", "代行"],  # 行政書士
            "2": ["労働基準法", "社会保険", "労災", "雇用保険", "年金"],  # 社労士
            "3": ["ライフプランニング", "リスク管理", "金融資産", "タックスプランニング", "不動産", "相続"],  # FP
            "4": ["宅建業法", "都市計画法", "建築基準法", "重要事項説明", "媒介契約"],  # 宅建
            "5": ["マンション管理適正化法", "区分所有法", "管理組合", "修繕積立金"],  # マンション管理士
            "6": ["仕訳", "貸借対照表", "損益計算書", "減価償却", "棚卸資産"],  # 簿記
            "26": ["ITパスポート", "情報セキュリティ", "システム開発", "データベース"],  # IT
            "27": ["関税法", "通関業法", "輸出入", "税関"],  # 通関士
            "45": ["司法書士法", "不動産登記", "商業登記", "供託"],  # 司法書士
            "46": ["アルゴリズム", "データ構造", "プログラミング", "システム設計"],  # 基本情報技術者
            "47": ["金融商品取引法", "証券", "投資信託", "デリバティブ"]  # 証券外務員
        }
        
        # 除去対象パターン
        self.removal_patterns = {
            # 不要な装飾文字
            'decorative': r'[◆◇■□●○▲△▼▽★☆※]',
            # 過度な記号
            'excessive_symbols': r'[！？]{2,}|[。、]{2,}',
            # URL
            'urls': r'https?://[^\s]+',
            # メールアドレス
            'emails': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        }
        
        logger.info("TextProcessor初期化完了")
    
    def process_article_content(self, full_content: str, qanda_content: str, 
                              title: str = "", koza_id: str = "") -> str:
        """
        記事コンテンツの前処理（講座情報をBigQueryから動的取得）
        
        Args:
            full_content: 記事の本文
            qanda_content: Q&A内容
            title: 記事タイトル
            koza_id: 講座ID
            
        Returns:
            処理済みテキスト
        """
        try:
            # None値の処理
            full_content = full_content or ""
            qanda_content = qanda_content or ""
            title = title or ""
            
            # 1. HTMLタグの除去と基本クリーニング
            full_text = self._clean_html_and_basic(full_content)
            qanda_text = self._clean_html_and_basic(qanda_content)
            title_text = self._clean_html_and_basic(title)
            
            # 2. 講座情報の取得
            course_info = self.course_mapper.get_course_info(str(koza_id)) if koza_id else {}
            
            # 3. 講座別特化処理
            structured_full = self._structure_course_specific_content(full_text, str(koza_id))
            structured_qanda = self._structure_qanda_content(qanda_text)
            
            # 4. 重要情報の抽出と強調（講座別）
            enhanced_full = self._enhance_course_specific_terms(structured_full, str(koza_id))
            enhanced_qanda = self._enhance_course_specific_terms(structured_qanda, str(koza_id))
            
            # 5. テキストの結合（講座情報含む）
            combined_text = self._combine_content_with_course_info(
                title_text, enhanced_full, enhanced_qanda, course_info
            )
            
            # 6. 最終正規化と長さ調整
            final_text = self._final_normalization(combined_text)
            
            # 7. 品質チェック
            if len(final_text.strip()) < self.min_length:
                logger.warning(f"処理後テキストが短すぎます: {len(final_text)}文字")
                return self._fallback_processing(full_content, qanda_content, course_info.get('name', ''))
            
            logger.info(f"テキスト処理完了: {len(full_content)} + {len(qanda_content)} -> {len(final_text)}文字 (講座: {course_info.get('name', 'Unknown')})")
            return final_text
            
        except Exception as e:
            logger.error(f"テキスト処理エラー: {str(e)}")
            return self._fallback_processing(full_content, qanda_content, "")
    
    def _structure_course_specific_content(self, text: str, course_id: str) -> str:
        """講座別特化コンテンツの構造化"""
        if not text:
            return ""
        
        # 基本的な構造化
        text = self._structure_legal_content(text)
        
        # 講座別キーワードの強調
        if course_id in self.course_specific_keywords:
            keywords = self.course_specific_keywords[course_id]
            for keyword in keywords:
                if keyword in text:
                    text = text.replace(keyword, f'【{keyword}】')
        
        return text
    
    def _enhance_course_specific_terms(self, text: str, course_id: str) -> str:
        """講座別重要用語の強調"""
        if not text:
            return ""
        
        # 基本的な重要用語の強調
        text = self._enhance_important_terms(text)
        
        # 講座別の追加強調
        course_name = self.course_mapper.get_course_name(course_id)
        if course_name and course_name != f"講座{course_id}":
            # 講座名自体も重要用語として強調
            text = text.replace(course_name, f'【{course_name}】')
        
        return text
    
    def _combine_content_with_course_info(self, title: str, full_content: str, 
                                        qanda_content: str, course_info: Dict) -> str:
        """講座情報を含むコンテンツ結合"""
        parts = []
        
        # 1. タイトル（最重要）
        if title.strip():
            parts.append(f"タイトル: {title.strip()}")
        
        # 2. 講座情報（BigQueryから取得）
        if course_info:
            course_name = course_info.get('name', '')
            course_slug = course_info.get('slug', '')
            if course_name:
                parts.append(f"講座: {course_name}")
                if course_slug:
                    parts.append(f"分野: {course_slug}")
        
        # 3. 本文（重要度高）
        if full_content.strip():
            parts.append(f"内容: {full_content.strip()}")
        
        # 4. Q&A（補足情報）
        if qanda_content.strip():
            parts.append(f"Q&A: {qanda_content.strip()}")
        
        return '\n\n'.join(parts)
    
    def _clean_html_and_basic(self, text: str) -> str:
        """HTMLタグ除去と基本クリーニング"""
        if not text:
            return ""
        
        try:
            # BeautifulSoupでHTMLタグを除去
            soup = BeautifulSoup(text, 'html.parser')
            
            # 不要なタグを完全に削除
            for tag in soup(['script', 'style', 'meta', 'link', 'nav', 'footer']):
                tag.decompose()
            
            # テキストのみを抽出
            clean_text = soup.get_text()
            
            # HTMLエンティティのデコード
            clean_text = html.unescape(clean_text)
            
            # Unicode正規化
            clean_text = unicodedata.normalize('NFKC', clean_text)
            
            return clean_text
            
        except Exception as e:
            logger.warning(f"HTML除去エラー: {str(e)}")
            return text
    
    def _structure_legal_content(self, text: str) -> str:
        """法律・資格試験コンテンツの構造化"""
        if not text:
            return ""
        
        # 見出し構造の正規化
        text = re.sub(r'(?:^|\n)([^\n]{1,50}とは[？?]?)\s*(?:\n|$)', r'\n【\1】\n', text, flags=re.MULTILINE)
        
        # 条文参照の正規化
        text = re.sub(self.legal_patterns['article_ref'], r'《\1》', text)
        
        # リスト項目の正規化
        text = re.sub(r'(?:^|\n)・([^\n]+)', r'\n- \1', text, flags=re.MULTILINE)
        text = re.sub(r'(?:^|\n)(\d+)\.([^\n]+)', r'\n\1. \2', text, flags=re.MULTILINE)
        
        # 重要ポイントの強調
        text = re.sub(r'(?:^|\n)(ポイント|重要|注意)[：:]([^\n]+)', r'\n★\1: \2', text, flags=re.MULTILINE)
        
        return text
    
    def _structure_qanda_content(self, text: str) -> str:
        """Q&Aコンテンツの構造化"""
        if not text:
            return ""
        
        # Q&Aペアの抽出と構造化
        qa_matches = re.findall(self.legal_patterns['qa_pattern'], text, re.MULTILINE | re.DOTALL)
        
        if qa_matches:
            structured_qa = []
            for i, (question, answer) in enumerate(qa_matches, 1):
                # 質問と回答のクリーニング
                clean_q = re.sub(r'\s+', ' ', question.strip())
                clean_a = re.sub(r'\s+', ' ', answer.strip())
                
                structured_qa.append(f"Q{i}: {clean_q}")
                structured_qa.append(f"A{i}: {clean_a}")
            
            return '\n'.join(structured_qa)
        
        # Q&Aパターンが見つからない場合は元のテキストを返す
        return text
    
    def _enhance_important_terms(self, text: str) -> str:
        """重要用語の強調"""
        if not text:
            return ""
        
        # 基本的な法律用語の強調
        important_terms = [
            '宅建業法', '宅地建物取引士', '重要事項説明', '営業保証金', '供託所',
            '宅建業者', '媒介契約', '売買契約', '賃貸借契約', '重要事項',
            '契約書面', '37条書面', '35条書面', '免許', '登録', '更新',
            '監督処分', '指示処分', '業務停止', '免許取消', '罰則',
            '行政書士', '社会保険労務士', 'ファイナンシャルプランナー', '簿記',
            'マンション管理士', '管理業務主任者', '司法書士', '通関士'
        ]
        
        for term in important_terms:
            if term in text:
                # 重要用語を強調マークで囲む（埋め込み時に重要度を高める）
                text = text.replace(term, f'【{term}】')
        
        return text
    
    def _final_normalization(self, text: str) -> str:
        """最終正規化処理"""
        if not text:
            return ""
        
        # 不要パターンの除去
        for pattern_name, pattern in self.removal_patterns.items():
            text = re.sub(pattern, '', text)
        
        # 連続する空白・改行の正規化
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # 3つ以上の改行を2つに
        text = re.sub(r'[ \t]+', ' ', text)  # 連続するスペース・タブを1つに
        text = re.sub(r'\n ', '\n', text)  # 行頭のスペースを除去
        
        # 文字数制限の適用
        if len(text) > self.max_length:
            text = self._smart_truncate(text)
        
        return text.strip()
    
    def _smart_truncate(self, text: str) -> str:
        """賢い文字数制限（重要部分を優先保持）"""
        if len(text) <= self.max_length:
            return text
        
        # セクション別に分割
        sections = text.split('\n\n')
        
        # 重要度順にソート
        priority_sections = []
        for section in sections:
            priority = self._calculate_section_priority(section)
            priority_sections.append((priority, section))
        
        priority_sections.sort(key=lambda x: x[0], reverse=True)
        
        # 重要度の高いセクションから順に追加
        result = ""
        for priority, section in priority_sections:
            if len(result + section + '\n\n') <= self.max_length:
                result += section + '\n\n'
            else:
                # 残り文字数で可能な限り追加
                remaining = self.max_length - len(result)
                if remaining > 100:  # 最低100文字は確保
                    truncated_section = section[:remaining-10] + "..."
                    result += truncated_section
                break
        
        return result.strip()
    
    def _calculate_section_priority(self, section: str) -> int:
        """セクションの重要度を計算"""
        priority = 0
        
        # タイトルセクション
        if section.startswith('タイトル:'):
            priority += 100
        
        # 講座情報
        if section.startswith('講座:') or section.startswith('分野:'):
            priority += 90
        
        # 重要キーワードの含有数
        important_keywords = ['宅建業法', '重要事項説明', '契約', '免許', '登録', '行政書士', '社労士', 'FP']
        for keyword in important_keywords:
            priority += section.count(keyword) * 10
        
        # 条文参照の含有数
        article_refs = re.findall(self.legal_patterns['article_ref'], section)
        priority += len(article_refs) * 5
        
        # Q&Aセクション
        if 'Q:' in section or 'A:' in section:
            priority += 20
        
        # セクションの長さ（適度な長さを優先）
        length_score = min(len(section) // 100, 10)
        priority += length_score
        
        return priority
    
    def _fallback_processing(self, full_content: str, qanda_content: str, course_name: str = "") -> str:
        """フォールバック処理（エラー時の簡易処理）"""
        try:
            # 最低限の処理
            parts = []
            if course_name:
                parts.append(f"講座: {course_name}")
            if full_content:
                parts.append(full_content)
            if qanda_content:
                parts.append(qanda_content)
            
            combined = '\n\n'.join(parts)
            
            # HTMLタグの簡易除去
            combined = re.sub(r'<[^>]+>', '', combined)
            
            # 基本的な正規化
            combined = re.sub(r'\s+', ' ', combined)
            
            # 長さ制限
            if len(combined) > self.max_length:
                combined = combined[:self.max_length-3] + "..."
            
            return combined
            
        except Exception as e:
            logger.error(f"フォールバック処理もエラー: {str(e)}")
            return "処理エラーが発生しました"
    
    def get_processing_stats(self, original_full: str, original_qanda: str, processed: str, course_id: str = "") -> Dict:
        """処理統計情報の取得"""
        course_name = self.course_mapper.get_course_name(course_id) if course_id else "Unknown"
        
        return {
            'original_full_length': len(original_full or ''),
            'original_qanda_length': len(original_qanda or ''),
            'processed_length': len(processed),
            'compression_ratio': len(processed) / (len(original_full or '') + len(original_qanda or '') + 1),
            'has_legal_terms': bool(re.search(self.legal_patterns['important_terms'], processed)),
            'has_article_refs': bool(re.search(self.legal_patterns['article_ref'], processed)),
            'qa_count': len(re.findall(r'Q\d+:', processed)),
            'course_id': course_id,
            'course_name': course_name
        }
