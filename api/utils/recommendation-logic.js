/**
 * 推奨アクション判定ロジック
 * 統一された条件でrecommendation_typeを決定
 */

/**
 * 推奨アクションタイプを決定する統一関数
 * @param {number} score - 類似度スコア (0-1)
 * @param {number} matchingRatio - 一致率 (0-1)
 * @param {boolean} sameCourse - 同一講座かどうか
 * @param {number} basePageviews - 基点記事のPV数
 * @param {number} similarPageviews - 類似記事のPV数
 * @param {number} actualMatchingCount - 実際の一致セクション数
 * @returns {Object} 推奨情報オブジェクト
 */
function determineRecommendation(score, matchingRatio, sameCourse, basePageviews, similarPageviews, actualMatchingCount) {
  let recommendation_type = "MONITOR";
  let explanation_text = "部分的な類似性があります。コンテンツの重複を確認してください。";
  let priority = 0; // 優先度スコア（高いほど優先）

  // 多次元判定ロジック（最適化版）
  // 1. MERGE_CONTENT: コンテンツ統合（最優先）
  // 条件A: 類似度95%以上 AND マッチング率50%以上（講座問わず）
  if (score >= 0.95 && matchingRatio >= 0.5) {
    recommendation_type = "MERGE_CONTENT";
    priority = 100;
    const courseNote = sameCourse ? "同一講座内での統合です。" : "⚠️ 異なる講座ですが、内容がほぼ同一のため統合を推奨します。";
    const pvAdvice = similarPageviews > basePageviews
      ? `💡 アクセス数が多い類似記事（${similarPageviews} PV）に基点記事を統合すると、SEO効果が最大化されます。`
      : `💡 アクセス数が多い基点記事（${basePageviews} PV）に、この類似記事を統合することをお勧めします。`;
    explanation_text = `📊 両記事の内容が極めて似ています（類似度${(score * 100).toFixed(1)}%）。記事の約${(matchingRatio * 100).toFixed(0)}%（${actualMatchingCount}個のセクション）で内容が重複しているため、統合することでSEO評価が向上します。${courseNote} ${pvAdvice}`;
  }
  // 条件B: 類似度92%以上 AND マッチング率30%以上 AND 同一講座
  else if (score >= 0.92 && matchingRatio >= 0.3 && sameCourse) {
    recommendation_type = "MERGE_CONTENT";
    priority = 90;
    explanation_text = `📊 両記事の内容が非常に似ています（類似度${(score * 100).toFixed(1)}%）。${actualMatchingCount}個のセクションで内容が重複しているため、記事を統合することで重複コンテンツを解消し、SEO評価を集約できます。`;
  }

  // 2. REDIRECT_301: 301リダイレクト
  // 条件A: 類似度90%以上（講座問わず）
  else if (score >= 0.90) {
    recommendation_type = "REDIRECT_301";
    priority = 85;
    const courseNote = sameCourse ? "同一講座内でのリダイレクトです。" : "⚠️ 異なる講座ですが、内容が非常に似ているためリダイレクトを推奨します。";
    const redirectDirection = similarPageviews > basePageviews * 2
      ? `アクセス数の多い類似記事（${similarPageviews} PV）に転送`
      : `アクセス数の多い基点記事（${basePageviews} PV）に転送`;
    explanation_text = `🔄 両記事の内容が非常に似ています（類似度${(score * 100).toFixed(1)}%）。${courseNote} 301リダイレクトで${redirectDirection}することで、SEO評価を1つの記事に集約できます。`;
  }
  // 条件B: 類似度88%以上 AND マッチング率20%以上 AND 同一講座
  else if (score >= 0.88 && matchingRatio >= 0.2 && sameCourse) {
    recommendation_type = "REDIRECT_301";
    priority = 80;
    const redirectDirection = similarPageviews > basePageviews * 2
      ? `アクセス数の多い類似記事（${similarPageviews} PV）に転送`
      : `アクセス数の多い基点記事（${basePageviews} PV）に転送`;
    explanation_text = `🔄 両記事は似た内容を扱っています（類似度${(score * 100).toFixed(1)}%）。${actualMatchingCount}個のセクションで内容が重なっているため、301リダイレクトで${redirectDirection}することで、SEO評価を1つの記事に集約できます。`;
  }
  // 条件C: 類似度85%以上 AND 同一講座（マッチング率不問）
  else if (score >= 0.85 && sameCourse) {
    recommendation_type = "REDIRECT_301";
    priority = 70;
    explanation_text = `🔄 両記事の内容に類似性があります（類似度${(score * 100).toFixed(1)}%）。同じトピックを扱っているため、301リダイレクトでアクセスを集約し、SEO評価を向上させることができます。`;
  }

  // 3. CROSS_LINK: 相互リンク
  // 条件: 類似度75%以上 OR (類似度70%以上 AND マッチング率15%以上)
  else if (score >= 0.75 || (score >= 0.70 && matchingRatio >= 0.15)) {
    recommendation_type = "CROSS_LINK";
    priority = 60;
    const courseNote = !sameCourse ? "💡 異なる講座の関連記事として、ユーザーの学習範囲を広げることができます。" : "💡 同じ講座内の関連記事として、理解を深める助けになります。";
    explanation_text = `🔗 両記事は関連するトピックを扱っています（類似度${(score * 100).toFixed(1)}%）。それぞれの記事に相互リンクを設置することで、読者が関連情報にアクセスしやすくなり、サイト内の回遊性が向上します。${courseNote}`;
  }

  // 4. REVIEW: レビュー推奨
  // 条件A: 類似度65%以上 AND 同一講座 AND マッチング率10%以下
  else if (score >= 0.65 && sameCourse && matchingRatio <= 0.1 && matchingRatio > 0) {
    recommendation_type = "REVIEW";
    priority = 50;
    explanation_text = `👀 両記事には部分的な類似性があります（類似度${(score * 100).toFixed(1)}%）。わずか${actualMatchingCount}個のセクションのみが重複しているため、内容を確認して、必要に応じて情報を差別化することをお勧めします。`;
  }
  // 条件B: 類似度65%以上 AND 異なる講座 AND マッチング率15%以下（新規追加）
  else if (score >= 0.65 && !sameCourse && matchingRatio <= 0.15 && matchingRatio > 0) {
    recommendation_type = "REVIEW";
    priority = 45;
    explanation_text = `👀 異なる講座ですが、部分的な類似性があります（類似度${(score * 100).toFixed(1)}%）。${actualMatchingCount}個のセクションで重複が見られるため、内容の差別化や独自性の向上を検討してください。`;
  }

  // 5. MONITOR: 監視のみ
  else if (score >= 0.60) {
    recommendation_type = "MONITOR";
    priority = 40;
    const courseNote = sameCourse ? "同一講座内での軽微な類似性です。" : "異なる講座での軽微な類似性です。";
    explanation_text = `📌 両記事には軽微な類似性があります（類似度${(score * 100).toFixed(1)}%）。${courseNote} 今すぐのアクションは不要ですが、将来的に内容が重複しないよう、定期的に確認することをお勧めします。`;
  }

  return {
    recommendation_type,
    priority,
    explanation_text,
    confidence_score: score * 0.95 + 0.05
  };
}

/**
 * 推奨タイプの設定情報を取得
 */
const RECOMMENDATION_CONFIGS = {
  MERGE_CONTENT: {
    id: "MERGE_CONTENT",
    name: "統合",
    fullName: "コンテンツ統合",
    description: "記事の内容がほぼ同じです。重複を解消するため、2つの記事を1つに統合することを強く推奨します。",
    icon: "🔥",
    className: "bg-red-500 hover:bg-red-600 text-white border-red-500",
    priority: 100,
    minSimilarity: 0.92,
    conditions: "類似度95%以上+一致率50%以上 または 類似度92%以上+一致率30%以上+同講座"
  },
  REDIRECT_301: {
    id: "REDIRECT_301",
    name: "リダイレクト",
    fullName: "301リダイレクト",
    description: "記事の内容が非常に似ています。SEO評価を統合するため、アクセス数の少ない記事から多い記事への301リダイレクトを推奨します。",
    icon: "⚡",
    className: "bg-orange-500 hover:bg-orange-600 text-white border-orange-500",
    priority: 80,
    minSimilarity: 0.85,
    conditions: "類似度90%以上 または 類似度88%以上+一致率20%以上+同講座 または 類似度85%以上+同講座"
  },
  CROSS_LINK: {
    id: "CROSS_LINK",
    name: "相互リンク",
    fullName: "相互リンク",
    description: "記事同士が関連性を持っています。読者の利便性向上のため、記事間の相互リンクを設置することを推奨します。",
    icon: "🔗",
    className: "bg-blue-500 hover:bg-blue-600 text-white border-blue-500",
    priority: 60,
    minSimilarity: 0.70,
    conditions: "類似度75%以上 または 類似度70%以上+一致率15%以上"
  },
  REVIEW: {
    id: "REVIEW",
    name: "レビュー",
    fullName: "レビュー推奨",
    description: "記事の内容に類似点がありますが、差別化の余地があります。内容の見直しや独自性の向上を検討してください。",
    icon: "👁️",
    className: "bg-yellow-500 hover:bg-yellow-600 text-white border-yellow-500",
    priority: 50,
    minSimilarity: 0.65,
    conditions: "類似度65%以上+一致率10%以下+同講座 または 類似度65%以上+一致率15%以下+異講座"
  },
  MONITOR: {
    id: "MONITOR",
    name: "監視",
    fullName: "監視のみ",
    description: "軽微な類似性があります。現時点では特別な対応は不要ですが、定期的な確認を継続してください。",
    icon: "📊",
    className: "bg-gray-500 hover:bg-gray-600 text-white border-gray-500",
    priority: 40,
    minSimilarity: 0.60,
    conditions: "類似度60%以上"
  }
};

/**
 * 推奨タイプ設定を取得
 */
function getRecommendationConfig(type) {
  return RECOMMENDATION_CONFIGS[type] || RECOMMENDATION_CONFIGS.MONITOR;
}

/**
 * 全ての推奨タイプ設定を取得（優先度順）
 */
function getAllRecommendationConfigs() {
  return Object.values(RECOMMENDATION_CONFIGS).sort((a, b) => b.priority - a.priority);
}

module.exports = {
  determineRecommendation,
  getRecommendationConfig,
  getAllRecommendationConfigs,
  RECOMMENDATION_CONFIGS
};