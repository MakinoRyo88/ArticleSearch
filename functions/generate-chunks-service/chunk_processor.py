import logging
import re
import html
import unicodedata
from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger(__name__)

class ChunkProcessor:
    """HTMLコンテンツを意味のあるチャンクに分割するための高機能プロセッサ（HTML保持→分割→テキスト化）"""

    def __init__(self, min_chunk_chars=200, max_chunk_chars=800, overlap_chars=100):
        """
        Args:
            min_chunk_chars (int): チャンクと見なす最小文字数（Vertex AI最適化: 200）。
            max_chunk_chars (int): チャンクの最大目標文字数（Vertex AI最適化: 800）。
            overlap_chars (int): チャンク間でオーバーラップさせる文字数（コンテキスト保持強化: 100）。
        """
        self.min_chunk_chars = min_chunk_chars
        self.max_chunk_chars = max_chunk_chars
        self.overlap_chars = overlap_chars
        self.heading_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']

    def _clean_text(self, text: str) -> str:
        """不要な空白や改行を削除してテキストを整形する"""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _extract_text_from_html(self, html_content: str) -> str:
        """HTMLからテキストを抽出（画像のalt属性も保持）"""
        if not html_content:
            return ""
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 画像のalt属性をテキストとして保持
            for img in soup.find_all('img'):
                if img.get('alt'):
                    img.replace_with(f" {img.get('alt')} ")
            
            # スクリプトやスタイルタグは完全削除
            for tag in soup(['script', 'style', 'meta', 'link', 'nav', 'footer']):
                tag.decompose()
            
            # テキスト抽出
            text = soup.get_text(separator=' ', strip=True)
            
            # HTMLエンティティのデコード
            text = html.unescape(text)
            
            # Unicode正規化
            text = unicodedata.normalize('NFKC', text)
            
            return text
            
        except Exception as e:
            logger.error(f"HTML→テキスト変換エラー: {e}")
            # フォールバック: 単純な正規表現でHTMLタグを削除
            return re.sub(r'<[^>]+>', ' ', str(html_content))

    def split_into_chunks(self, html_content: str) -> list[dict]:
        """
        HTMLコンテンツを階層的な見出しに基づいて意味のあるセクションへ分割し、
        文字数制約とオーバーラップを適用してチャンクを生成する。
        
        処理フロー:
        1. HTMLを見出しタグで分割（HTML構造を保持）
        2. 各セクションからテキストを抽出（この段階でHTML除去）
        3. 短すぎるセクションを結合
        4. 長すぎるセクションを分割
        5. オーバーラップを追加
        """
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 1. 見出しタグを基準にHTMLをセクションに分割（HTML構造を保持）
        sections = self._split_by_headings(soup)
        
        logger.info(f"見出しベース分割: {len(sections)}セクション")

        # 2. 各セクションからテキストを抽出（HTMLタグを除去）
        text_sections = self._extract_text_from_sections(sections)
        
        logger.info(f"テキスト抽出完了: {len(text_sections)}セクション")

        # 3. 短すぎるものを結合
        combined_sections = self._combine_short_sections_v2(text_sections)
        
        logger.info(f"短いセクション結合後: {len(combined_sections)}セクション")

        # 4. 長すぎるセクションをさらに分割
        split_sections = self._split_long_sections(combined_sections)
        
        logger.info(f"長いセクション分割後: {len(split_sections)}セクション")
        
        # 5. チャンクにオーバーラップを追加
        final_chunks = self._apply_overlap(split_sections)

        logger.info(f"最終的なチャンク生成数: {len(final_chunks)}")
        return final_chunks

    def _split_by_headings(self, soup: BeautifulSoup) -> list[dict]:
        """見出しタグを基準にHTMLをセクションに分割する（HTML要素を保持）"""
        sections = []
        current_section = {'title': '冒頭', 'title_html': '', 'elements': []}

        # ルート要素の直下の要素を処理
        for element in soup.children:
            # NavigableString（テキストノード）をスキップ
            if isinstance(element, NavigableString):
                if element.strip():
                    current_section['elements'].append(element)
                continue
            
            if hasattr(element, 'name') and element.name in self.heading_tags:
                # 現在のセクションをリストに追加
                if current_section['elements'] or current_section['title'] != '冒頭':
                    sections.append(current_section)
                
                # 新しいセクションを開始
                current_section = {
                    'title': self._clean_text(element.get_text()),
                    'title_html': str(element),
                    'elements': []
                }
            else:
                current_section['elements'].append(element)
        
        # 最後のセクションを追加
        if current_section['elements'] or current_section['title'] != '冒頭':
            sections.append(current_section)
            
        return sections
    
    def _extract_text_from_sections(self, sections: list[dict]) -> list[dict]:
        """各セクションからテキストを抽出（この段階でHTMLタグを除去）"""
        text_sections = []
        
        for section in sections:
            # 見出しテキスト
            title = section['title']
            
            # 本文HTML
            elements_html = ''.join(str(el) for el in section['elements'])
            
            # HTML→テキスト変換
            text = self._extract_text_from_html(elements_html)
            
            if text.strip() or title != '冒頭':
                text_sections.append({
                    'title': title,
                    'text': text
                })
        
        return text_sections

    def _combine_short_sections_v2(self, sections: list[dict]) -> list[dict]:
        """短すぎるセクションを前のセクションに結合する（テキスト抽出済み）"""
        combined_sections = []
        
        for section in sections:
            text = section['text'].strip()
            title = section['title']
            
            # 短すぎて、かつ結合先のセクションが存在する場合
            if combined_sections and len(text) < self.min_chunk_chars:
                logger.info(f"セクション '{title}' (文字数: {len(text)}) は短すぎるため、前のセクションに結合します。")
                # タイトルも保持（見出しとして）
                combined_sections[-1]['text'] += f"\n\n{title}\n{text}"
            elif len(text) >= self.min_chunk_chars or not combined_sections:
                # 最小文字数以上、または最初のセクション
                combined_sections.append({'title': title, 'text': text})
            else:
                logger.warning(f"セクション '{title}' (文字数: {len(text)}) は短すぎますが、これをチャンクとして保持します。")
                combined_sections.append({'title': title, 'text': text})

        return combined_sections

    def _split_long_sections(self, sections: list[dict]) -> list[dict]:
        """最大文字数を超えるセクションを句点「。」と段落で分割する"""
        final_sections = []
        for section in sections:
            text = section['text']
            if len(text) <= self.max_chunk_chars:
                final_sections.append(section)
                continue

            logger.info(f"セクション '{section['title']}' (文字数: {len(text)}) は長すぎるため、分割します。")

            # まず段落で分割を試行
            paragraphs = text.split('\n')
            if len(paragraphs) > 1:
                paragraphs = [p.strip() for p in paragraphs if p.strip()]
            else:
                # 段落分割がない場合は句点で分割
                paragraphs = [s + '。' for s in text.split('。') if s.strip()]

            current_sub_chunk = ""
            sub_chunk_count = 0

            for paragraph in paragraphs:
                if not paragraph: continue

                # 段落自体が長すぎる場合は文単位で分割
                if len(paragraph) > self.max_chunk_chars:
                    sentences = paragraph.split('。')
                    for sentence in sentences:
                        if not sentence: continue
                        sentence_with_period = sentence + '。' if not sentence.endswith('。') else sentence

                        if len(current_sub_chunk) + len(sentence_with_period) > self.max_chunk_chars:
                            if current_sub_chunk:
                                final_sections.append({
                                    'title': f"{section['title']} (パート{sub_chunk_count + 1})",
                                    'text': current_sub_chunk.strip()
                                })
                                sub_chunk_count += 1
                            current_sub_chunk = sentence_with_period
                        else:
                            current_sub_chunk += " " + sentence_with_period if current_sub_chunk else sentence_with_period
                else:
                    # 通常の段落処理
                    if len(current_sub_chunk) + len(paragraph) > self.max_chunk_chars:
                        if current_sub_chunk:
                            final_sections.append({
                                'title': f"{section['title']} (パート{sub_chunk_count + 1})",
                                'text': current_sub_chunk.strip()
                            })
                            sub_chunk_count += 1
                        current_sub_chunk = paragraph
                    else:
                        current_sub_chunk += "\n" + paragraph if current_sub_chunk else paragraph

            if current_sub_chunk:
                final_sections.append({
                    'title': f"{section['title']} (パート{sub_chunk_count + 1})",
                    'text': current_sub_chunk.strip()
                })

        return final_sections

    def _apply_overlap(self, sections: list[dict]) -> list[dict]:
        """チャンク間により効果的なオーバーラップを追加する"""
        if not sections:
            return []

        overlapped_chunks = []
        for i in range(len(sections)):
            current_chunk = sections[i].copy()  # コピーを作成

            # 前のチャンクの末尾を現在のチャンクの先頭に追加
            if i > 0:
                prev_chunk_text = sections[i-1]['text']
                # 文の境界で切り取るように改善
                sentences = prev_chunk_text.split('。')
                if len(sentences) > 1:
                    # 最後の数文を取得
                    overlap_sentences = sentences[-3:] if len(sentences) >= 3 else sentences[-2:]
                    overlap_text = '。'.join(s for s in overlap_sentences if s.strip()) + '。'
                    if len(overlap_text) <= self.overlap_chars:
                        current_chunk['text'] = overlap_text + " " + current_chunk['text']
                    else:
                        # 長すぎる場合は文字数で切り取り
                        overlap_text = prev_chunk_text[-self.overlap_chars:]
                        current_chunk['text'] = overlap_text + " " + current_chunk['text']
                else:
                    overlap_text = prev_chunk_text[-self.overlap_chars:]
                    current_chunk['text'] = overlap_text + " " + current_chunk['text']

            # 次のチャンクの先頭も少し追加（既存のロジック改善）
            if i < len(sections) - 1:
                next_chunk_text = sections[i+1]['text']
                # 文の境界で切り取り
                sentences = next_chunk_text.split('。')
                if sentences:
                    first_sentence = sentences[0] + '。' if sentences[0] and not sentences[0].endswith('。') else sentences[0]
                    if len(first_sentence) <= self.overlap_chars // 2:  # 半分のサイズまで
                        current_chunk['text'] += " " + first_sentence
                    else:
                        overlap_text = next_chunk_text[:self.overlap_chars // 2]
                        current_chunk['text'] += " " + overlap_text

            overlapped_chunks.append(current_chunk)

        return overlapped_chunks