/**
 * チャンク重要度計算ユーティリティ
 * フェーズ1改善: 加重マッチング率の実装
 */

/**
 * チャンクタイトルから重要度を判定
 * @param {string} chunkTitle - チャンクのタイトル
 * @param {number} chunkIndex - チャンクのインデックス（0始まり）
 * @param {number} totalChunks - 記事の総チャンク数
 * @returns {number} - 重要度（0.5〜5.0）
 */
function getChunkWeight(chunkTitle, chunkIndex, totalChunks) {
  if (!chunkTitle) return 1.0;

  const titleLower = chunkTitle.toLowerCase();

  // タイトル・見出しの判定（最重要）
  if (
    titleLower.includes('タイトル') ||
    titleLower.includes('title') ||
    titleLower.includes('見出し') ||
    chunkIndex === 0 // 最初のチャンクはタイトル相当
  ) {
    return 5.0;
  }

  // 導入部分（重要）
  if (
    titleLower.includes('はじめに') ||
    titleLower.includes('概要') ||
    titleLower.includes('導入') ||
    titleLower.includes('とは') ||
    titleLower.includes('について') ||
    chunkIndex === 1 // 2番目のチャンクは導入部相当
  ) {
    return 3.0;
  }

  // 結論・まとめ（やや重要）
  if (
    titleLower.includes('まとめ') ||
    titleLower.includes('結論') ||
    titleLower.includes('おわりに') ||
    titleLower.includes('ポイント') ||
    chunkIndex === totalChunks - 1 // 最後のチャンクは結論相当
  ) {
    return 2.0;
  }

  // フッター・参考情報（低重要度）
  if (
    titleLower.includes('参考') ||
    titleLower.includes('関連') ||
    titleLower.includes('リンク') ||
    titleLower.includes('注釈') ||
    titleLower.includes('補足')
  ) {
    return 0.5;
  }

  // 通常の本文
  return 1.0;
}

/**
 * 加重マッチング率を計算
 * @param {Array} matchingChunks - マッチングしたチャンクの配列 [{chunk_title, chunk_index}, ...]
 * @param {number} baseTotalChunks - 基点記事の総チャンク数
 * @param {number} similarTotalChunks - 類似記事の総チャンク数
 * @returns {number} - 加重マッチング率（0.0〜1.0）
 */
function calculateWeightedMatchingRatio(matchingChunks, baseTotalChunks, similarTotalChunks) {
  if (!matchingChunks || matchingChunks.length === 0) return 0;

  // マッチングチャンクの重み合計
  const matchedWeight = matchingChunks.reduce((sum, chunk) => {
    const weight = getChunkWeight(
      chunk.base_chunk_title || chunk.chunk_title,
      chunk.base_chunk_index !== undefined ? chunk.base_chunk_index : (chunk.chunk_index || 0),
      baseTotalChunks
    );
    return sum + weight;
  }, 0);

  // 短い方の記事の全チャンクの重み合計（基準）
  const totalChunks = Math.min(baseTotalChunks, similarTotalChunks);
  const totalWeight = Array.from({ length: totalChunks }, (_, i) => 
    getChunkWeight('', i, totalChunks)
  ).reduce((sum, w) => sum + w, 0);

  // 最大1.0に制限
  const ratio = totalWeight > 0 ? matchedWeight / totalWeight : 0;
  return Math.min(ratio, 1.0);
}

/**
 * セマンティック距離ボーナスを計算
 * 連続したチャンクの一致にボーナスを付与
 * @param {Array} matchingChunks - マッチングしたチャンクの配列（ソート済み）
 * @returns {number} - ボーナス率（0.0〜0.15）
 */
function calculateSemanticDistanceBonus(matchingChunks) {
  if (!matchingChunks || matchingChunks.length < 2) return 0;

  // チャンクインデックスでソート
  const sortedChunks = [...matchingChunks].sort(
    (a, b) => {
      const aIndex = a.base_chunk_index !== undefined ? a.base_chunk_index : (a.chunk_index || 0);
      const bIndex = b.base_chunk_index !== undefined ? b.base_chunk_index : (b.chunk_index || 0);
      return aIndex - bIndex;
    }
  );

  let maxConsecutive = 1;
  let currentConsecutive = 1;

  for (let i = 1; i < sortedChunks.length; i++) {
    const prevIndex = sortedChunks[i - 1].base_chunk_index !== undefined 
      ? sortedChunks[i - 1].base_chunk_index 
      : (sortedChunks[i - 1].chunk_index || 0);
    const currIndex = sortedChunks[i].base_chunk_index !== undefined 
      ? sortedChunks[i].base_chunk_index 
      : (sortedChunks[i].chunk_index || 0);

    if (currIndex === prevIndex + 1) {
      currentConsecutive++;
      maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
    } else {
      currentConsecutive = 1;
    }
  }

  // 連続数に応じたボーナス
  if (maxConsecutive >= 5) return 0.15; // 5個以上連続: +15%
  if (maxConsecutive >= 4) return 0.12; // 4個連続: +12%
  if (maxConsecutive >= 3) return 0.09; // 3個連続: +9%
  if (maxConsecutive >= 2) return 0.05; // 2個連続: +5%
  
  return 0;
}

/**
 * メタデータボーナスを計算
 * @param {Object} baseArticle - 基点記事
 * @param {Object} similarArticle - 類似記事
 * @returns {number} - ボーナス率（0.0〜0.20）
 */
function calculateMetadataBonus(baseArticle, similarArticle) {
  let bonus = 0;

  // 同じ講座内: +5%
  if (baseArticle.koza_id === similarArticle.koza_id) {
    bonus += 0.05;
  }

  // 公開日が近い（30日以内）: +3%
  if (baseArticle.created_at && similarArticle.created_at) {
    const daysDiff = Math.abs(
      (new Date(baseArticle.created_at) - new Date(similarArticle.created_at)) / 
      (1000 * 60 * 60 * 24)
    );
    if (daysDiff <= 30) {
      bonus += 0.03;
    }
  }

  // 更新日が近い（30日以内）: +2%
  if (baseArticle.updated_at && similarArticle.updated_at) {
    const daysDiff = Math.abs(
      (new Date(baseArticle.updated_at) - new Date(similarArticle.updated_at)) / 
      (1000 * 60 * 60 * 24)
    );
    if (daysDiff <= 30) {
      bonus += 0.02;
    }
  }

  // キーワードの重複数: 1個あたり+1%（最大5%）
  if (baseArticle.search_keywords && similarArticle.search_keywords) {
    const baseKeywords = new Set(baseArticle.search_keywords);
    const similarKeywords = new Set(similarArticle.search_keywords);
    const overlap = [...baseKeywords].filter(k => similarKeywords.has(k)).length;
    bonus += Math.min(overlap * 0.01, 0.05);
  }

  // 記事タイトルの類似度（簡易版）: 最大5%
  if (baseArticle.title && similarArticle.title) {
    const baseWords = new Set(baseArticle.title.split(/\s+/));
    const similarWords = new Set(similarArticle.title.split(/\s+/));
    const commonWords = [...baseWords].filter(w => similarWords.has(w)).length;
    const totalWords = Math.max(baseWords.size, similarWords.size);
    if (totalWords > 0) {
      const titleSimilarity = commonWords / totalWords;
      bonus += titleSimilarity * 0.05;
    }
  }

  return Math.min(bonus, 0.20); // 最大20%
}

/**
 * 最終類似度スコアを計算（フェーズ1改善版）
 * @param {number} avgSimilarity - 平均類似度
 * @param {Object} matchingInfo - マッチング情報
 * @param {Object} baseArticle - 基点記事
 * @param {Object} similarArticle - 類似記事
 * @returns {Object} - { finalScore, breakdown }
 */
function calculateEnhancedSimilarityScore(avgSimilarity, matchingInfo, baseArticle, similarArticle) {
  const {
    matching_chunks,
    base_total_chunks,
    similar_total_chunks
  } = matchingInfo;

  // 1. 基本スコア: 平均類似度 × 0.60
  const baseScore = avgSimilarity * 0.60;

  // 2. 加重マッチング率: × 0.25
  const weightedRatio = calculateWeightedMatchingRatio(
    matching_chunks,
    base_total_chunks,
    similar_total_chunks
  );
  const weightedRatioScore = weightedRatio * 0.25;

  // 3. セマンティック距離ボーナス: 最大 +0.10
  const semanticBonus = calculateSemanticDistanceBonus(matching_chunks);

  // 4. メタデータボーナス: 最大 +0.05（合計で20%までに制限）
  const metadataBonus = Math.min(
    calculateMetadataBonus(baseArticle, similarArticle) * 0.5, // 50%減算して影響を軽減
    0.05
  );

  // 最終スコア（最大1.0）
  const finalScore = Math.min(
    baseScore + weightedRatioScore + semanticBonus + metadataBonus,
    1.0
  );

  return {
    finalScore,
    breakdown: {
      baseScore: baseScore.toFixed(3),
      weightedRatioScore: weightedRatioScore.toFixed(3),
      semanticBonus: semanticBonus.toFixed(3),
      metadataBonus: metadataBonus.toFixed(3),
      weightedRatio: weightedRatio.toFixed(3)
    }
  };
}

module.exports = {
  getChunkWeight,
  calculateWeightedMatchingRatio,
  calculateSemanticDistanceBonus,
  calculateMetadataBonus,
  calculateEnhancedSimilarityScore
};
