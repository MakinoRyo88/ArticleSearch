import re
import html
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Tuple, Optional
import unicodedata
from course_mapper import CourseMapper
import json

logger = logging.getLogger(__name__)

class UniversalTextProcessor:
    """全資格対応汎用テキスト前処理クラス"""
    
    def __init__(self, project_id: str = "seo-optimize-464208"):
        self.max_length = 8000  # Vertex AI Text Embeddings APIの制限
        self.min_length = 50    # 最小テキスト長
        
        # 講座マッパーの初期化
        self.course_mapper = CourseMapper(project_id)
        
        # 汎用的な重要パターン（全資格共通）
        self.universal_patterns = {
            # 法律・規則関連
            'legal_terms': r'([^\s]+(?:法|規則|規定|基準|要件|条例))',
            'articles': r'([第]?\d+条[の]?\d*(?:第\d+項)?)',
            'procedures': r'([^\s]+(?:手続|申請|届出|登録|許可|認定|免許|資格))',
            
            # 質問回答パターン
            'qa_pattern': r'Q[:：]\s*([^\n]+(?:\n(?!A[:：])[^\n]*)*)\s*A[:：]\s*([^\n]+(?:\n(?!Q[:：])[^\n]*)*)',
            
            # 重要な概念
            'important_concepts': r'(とは[？?]?|について|に関して|の定義|の意味)',
            'key_points': r'(ポイント|重要|注意|要点|まとめ|結論)',
        }
        
        # 資格分野別の特化キーワード（動的に更新可能）
        self.field_keywords = self._initialize_field_keywords()
        
        # 除去対象パターン
        self.removal_patterns = {
            'decorative': r'[◆◇■□●○▲△▼▽★☆※]',
            'excessive_symbols': r'[！？]{2,}|[。、]{2,}',
            'urls': r'https?://[^\s]+',
            'emails': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        }
        
        logger.info("UniversalTextProcessor初期化完了")
    
    def _initialize_field_keywords(self) -> Dict[str, List[str]]:
        """資格分野別キーワードの初期化"""
        return {
            # 法律系資格
            "legal": [
                "法律", "条文", "規則", "規定", "手続", "申請", "届出", "登録", "許可", "免許",
                "契約", "権利", "義務", "責任", "損害", "賠償", "処分", "罰則", "違反"
            ],
            
            # 不動産系資格
            "real_estate": [
                "宅建業法", "重要事項説明", "媒介契約", "売買契約", "賃貸借", "都市計画法",
                "建築基準法", "区分所有法", "マンション", "管理組合", "修繕", "供託"
            ],
            
            # 金融・保険系資格
            "finance": [
                "ライフプランニング", "リスク管理", "金融資産", "タックスプランニング", "相続",
                "投資信託", "証券", "保険", "年金", "税金", "控除", "所得", "資産運用"
            ],
            
            # 労務・社会保険系資格
            "labor": [
                "労働基準法", "社会保険", "労災", "雇用保険", "年金", "健康保険", "厚生年金",
                "労働契約", "就業規則", "賃金", "休暇", "解雇", "退職"
            ],
            
            # IT・技術系資格
            "technology": [
                "システム", "データベース", "ネットワーク", "セキュリティ", "プログラミング",
                "アルゴリズム", "データ構造", "開発", "設計", "テスト", "運用", "保守"
            ],
            
            # 会計・簿記系資格
            "accounting": [
                "仕訳", "貸借対照表", "損益計算書", "減価償却", "棚卸資産", "固定資産",
                "流動資産", "負債", "資本", "収益", "費用", "決算", "税務"
            ],
            
            # 貿易・物流系資格
            "trade": [
                "関税法", "通関業法", "輸出入", "税関", "関税", "貿易", "通関", "検査",
                "申告", "許可", "承認", "輸出", "輸入", "原産地"
            ],
            
            # 旅行・観光系資格
            "travel": [
                "旅行業法", "旅行業務", "取扱管理者", "企画旅行", "手配旅行", "旅程管理",
                "添乗", "宿泊", "運送", "観光", "ツアー", "パッケージ"
            ]
        }
    
    def process_article_content(self, full_content: str, qanda_content: str, 
                              title: str = "", koza_id: str = "") -> str:
        """
        記事コンテンツの汎用前処理
        
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
            
            # 3. 資格分野の特定
            field_type = self._identify_field_type(course_info, full_text, title_text)
            
            # 4. 汎用的な構造化処理
            structured_full = self._structure_universal_content(full_text, field_type)
            structured_qanda = self._structure_qanda_content(qanda_text)
            
            # 5. 重要情報の抽出と強調
            enhanced_full = self._enhance_universal_terms(structured_full, field_type)
            enhanced_qanda = self._enhance_universal_terms(structured_qanda, field_type)
            
            # 6. テキストの結合
            combined_text = self._combine_content_with_metadata(
                title_text, enhanced_full, enhanced_qanda, course_info, field_type
            )
            
            # 7. 最終正規化と長さ調整
            final_text = self._final_normalization(combined_text)
            
            # 8. 品質チェック
            if len(final_text.strip()) < self.min_length:
                logger.warning(f"処理後テキストが短すぎます: {len(final_text)}文字")
                return self._fallback_processing(full_content, qanda_content, course_info.get('name', ''))
            
            logger.info(f"テキスト処理完了: {len(full_content)} + {len(qanda_content)} -> {len(final_text)}文字 "
                       f"(講座: {course_info.get('name', 'Unknown')}, 分野: {field_type})")
            return final_text
            
        except Exception as e:
            logger.error(f"テキスト処理エラー: {str(e)}")
            return self._fallback_processing(full_content, qanda_content, "")
    
    def _identify_field_type(self, course_info: Dict, content: str, title: str) -> str:
        """資格分野を特定"""
        
        # 講座名やスラッグから分野を推定
        course_name = course_info.get('name', '').lower()
        course_slug = course_info.get('slug', '').lower()
        
        # 分野マッピング
        field_mapping = {
            'real_estate': ['宅建', '宅地建物', 'マンション管理', '管理業務', 'takken', 'mankan'],
            'finance': ['fp', 'ファイナンシャル', '証券', '年金', 'gaimuin', 'nenkin'],
            'labor': ['社会保険労務士', '社労士', 'sharoushi'],
            'legal': ['行政書士', '司法書士', 'gyosei', 'shoshi'],
            'technology': ['it', '基本情報', 'fe', 'システム'],
            'accounting': ['簿記', 'boki'],
            'trade': ['通関士', 'tsukanshi'],
            'travel': ['旅行', 'ryokou'],
            'safety': ['危険物', 'kikenbutsu']
        }
        
        # 講座情報から分野を特定
        for field, keywords in field_mapping.items():
            for keyword in keywords:
                if keyword in course_name or keyword in course_slug:
                    return field
        
        # コンテンツから分野を推定
        combined_text = (content + ' ' + title).lower()
        
        field_scores = {}
        for field, keywords in self.field_keywords.items():
            score = sum(1 for keyword in keywords if keyword in combined_text)
            if score > 0:
                field_scores[field] = score
        
        if field_scores:
            return max(field_scores, key=field_scores.get)
        
        return 'general'  # デフォルト
    
    def _structure_universal_content(self, text: str, field_type: str) -> str:
        """汎用的なコンテンツ構造化"""
        if not text:
            return ""
        
        # 基本的な構造化
        # 見出し構造の正規化
        text = re.sub(r'(?:^|\n)([^\n]{1,50}とは[？?]?)\s*(?:\n|$)', r'\n【\1】\n', text, flags=re.MULTILINE)
        
        # 条文・規則参照の正規化
        text = re.sub(self.universal_patterns['legal_terms'], r'《\1》', text)
        text = re.sub(self.universal_patterns['articles'], r'《\1》', text)
        
        # リスト項目の正規化
        text = re.sub(r'(?:^|\n)・([^\n]+)', r'\n- \1', text, flags=re.MULTILINE)
        text = re.sub(r'(?:^|\n)(\d+)\.([^\n]+)', r'\n\1. \2', text, flags=re.MULTILINE)
        
        # 重要ポイントの強調
        text = re.sub(r'(?:^|\n)(ポイント|重要|注意|要点|まとめ)[：:]([^\n]+)', r'\n★\1: \2', text, flags=re.MULTILINE)
        
        return text
    
    def _structure_qanda_content(self, text: str) -> str:
        """Q&Aコンテンツの構造化"""
        if not text:
            return ""
        
        # Q&Aペアの抽出と構造化
        qa_matches = re.findall(self.universal_patterns['qa_pattern'], text, re.MULTILINE | re.DOTALL)
        
        if qa_matches:
            structured_qa = []
            for i, (question, answer) in enumerate(qa_matches, 1):
                # 質問と回答のクリーニング
                clean_q = re.sub(r'\s+', ' ', question.strip())
                clean_a = re.sub(r'\s+', ' ', answer.strip())
                
                structured_qa.append(f"Q{i}: {clean_q}")
                structured_qa.append(f"A{i}: {clean_a}")
            
            return '\n'.join(structured_qa)
        
        return text
    
    def _enhance_universal_terms(self, text: str, field_type: str) -> str:
        """汎用的な重要用語の強調"""
        if not text:
            return ""
        
        # 分野特有のキーワードを強調
        if field_type in self.field_keywords:
            keywords = self.field_keywords[field_type]
            for keyword in keywords:
                if keyword in text:
                    text = text.replace(keyword, f'【{keyword}】')
        
        # 汎用的な重要用語の強調
        universal_terms = [
            '重要', '注意', 'ポイント', '必要', '義務', '権利', '責任',
            '手続', '申請', '登録', '許可', '免許', '資格', '試験',
            '法律', '規則', '規定', '基準', '要件', '条件'
        ]
        
        for term in universal_terms:
            if term in text:
                text = text.replace(term, f'【{term}】')
        
        return text
    
    def _combine_content_with_metadata(self, title: str, full_content: str, 
                                     qanda_content: str, course_info: Dict, field_type: str) -> str:
        """メタデータを含むコンテンツ結合"""
        parts = []
        
        # 1. タイトル（最重要）
        if title.strip():
            parts.append(f"タイトル: {title.strip()}")
        
        # 2. 講座・分野情報
        if course_info:
            course_name = course_info.get('name', '')
            if course_name:
                parts.append(f"講座: {course_name}")
        
        if field_type and field_type != 'general':
            field_names = {
                'real_estate': '不動産',
                'finance': '金融・保険',
                'labor': '労務・社会保険',
                'legal': '法律',
                'technology': 'IT・技術',
                'accounting': '会計・簿記',
                'trade': '貿易・物流',
                'travel': '旅行・観光',
                'safety': '安全管理'
            }
            field_name = field_names.get(field_type, field_type)
            parts.append(f"分野: {field_name}")
        
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
        
        # 講座・分野情報
        if section.startswith('講座:') or section.startswith('分野:'):
            priority += 90
        
        # 重要キーワードの含有数
        important_keywords = ['重要', '注意', 'ポイント', '必要', '義務', '権利', '法律', '規則']
        for keyword in important_keywords:
            priority += section.count(f'【{keyword}】') * 10
        
        # 条文参照の含有数
        article_refs = re.findall(self.universal_patterns['articles'], section)
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
    
    def get_processing_stats(self, original_full: str, original_qanda: str, processed: str, 
                           course_id: str = "") -> Dict:
        """処理統計情報の取得"""
        course_info = self.course_mapper.get_course_info(course_id) if course_id else {}
        course_name = course_info.get('name', 'Unknown')
        
        # 分野の特定
        field_type = self._identify_field_type(course_info, original_full or '', '')
        
        return {
            'original_full_length': len(original_full or ''),
            'original_qanda_length': len(original_qanda or ''),
            'processed_length': len(processed),
            'compression_ratio': len(processed) / (len(original_full or '') + len(original_qanda or '') + 1),
            'has_legal_terms': bool(re.search(self.universal_patterns['legal_terms'], processed)),
            'has_article_refs': bool(re.search(self.universal_patterns['articles'], processed)),
            'qa_count': len(re.findall(r'Q\d+:', processed)),
            'course_id': course_id,
            'course_name': course_name,
            'field_type': field_type,
            'enhanced_terms_count': len(re.findall(r'【[^】]+】', processed))
        }
    
    def update_field_keywords(self, field_type: str, new_keywords: List[str]):
        """分野別キーワードの動的更新"""
        if field_type not in self.field_keywords:
            self.field_keywords[field_type] = []
        
        # 重複を避けて追加
        for keyword in new_keywords:
            if keyword not in self.field_keywords[field_type]:
                self.field_keywords[field_type].append(keyword)
        
        logger.info(f"分野 {field_type} のキーワードを更新: {len(new_keywords)}個追加")
