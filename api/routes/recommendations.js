/**
 * 統合提案生成API
 * 類似記事ペアの分析と統合効果予測
 */

const express = require("express")
const { body, validationResult } = require("express-validator")
const BigQueryService = require("../services/bigquery-service")
const VertexAIService = require("../services/vertex-ai-service")
const CacheService = require("../services/cache-service")
const { formatResponse, formatError } = require("../utils/response-formatter")
const { determineRecommendation } = require("../utils/recommendation-logic")
const winston = require("winston")

const router = express.Router()
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

const bigQueryService = new BigQueryService()
const vertexAIService = new VertexAIService()
const cacheService = new CacheService()

// バリデーションルール
const recommendationValidation = [
  body("base_article_id").isString().isLength({ min: 1, max: 100 }),
  body("similar_article_ids").isArray({ min: 1, max: 10 }),
  body("similar_article_ids.*").isString().isLength({ min: 1, max: 100 }),
  body("analysis_type").optional().isIn(["basic", "detailed", "comprehensive"]),
  body("include_traffic_prediction").optional().isBoolean(),
  body("include_seo_analysis").optional().isBoolean(),
]

/**
 * POST /api/recommendations/generate
 * 統合提案生成
 */
router.post("/generate", recommendationValidation, async (req, res) => {
  try {
    // バリデーションエラーチェック
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
      return res.status(400).json(formatError("Validation failed", errors.array()))
    }

    const {
      base_article_id,
      similar_article_ids,
      analysis_type = "detailed",
      include_traffic_prediction = true,
      include_seo_analysis = true,
    } = req.body

    logger.info("統合提案生成開始", {
      base_article_id,
      similar_article_count: similar_article_ids.length,
      analysis_type,
    })

    // キャッシュキー生成
    const cacheKey = `recommendations_${base_article_id}_${Buffer.from(JSON.stringify(similar_article_ids)).toString("base64")}`

    // キャッシュから取得試行
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      logger.info("統合提案をキャッシュから返却", { cacheKey })
      return res.json(formatResponse(cachedResult, "Recommendations retrieved from cache"))
    }

    // 記事データ取得
    const allArticleIds = [base_article_id, ...similar_article_ids]
    const articlesData = await getArticlesData(allArticleIds)

    if (!articlesData[base_article_id]) {
      return res.status(404).json(formatError("Base article not found", `No article found with ID: ${base_article_id}`))
    }

    const baseArticle = articlesData[base_article_id]
    const similarArticles = similar_article_ids.map((id) => articlesData[id]).filter((article) => article !== undefined)

    if (similarArticles.length === 0) {
      return res.status(400).json(formatError("No valid similar articles found"))
    }

    // 統合提案生成
    const recommendations = await generateRecommendations(
      baseArticle,
      similarArticles,
      analysis_type,
      include_traffic_prediction,
      include_seo_analysis,
    )

    const result = {
      base_article: {
        id: baseArticle.id,
        title: baseArticle.title,
        link: baseArticle.link,
        koza_name: baseArticle.koza_name,
        pageviews: baseArticle.pageviews,
        content_type: baseArticle.content_type,
      },
      recommendations,
      analysis_metadata: {
        analysis_type,
        total_similar_articles: similarArticles.length,
        include_traffic_prediction,
        include_seo_analysis,
        generated_at: new Date().toISOString(),
      },
    }

    // 結果をキャッシュ（30分間）
    cacheService.set(cacheKey, result, 1800)

    logger.info("統合提案生成完了", {
      base_article_id,
      recommendations_count: recommendations.length,
      analysis_type,
    })

    res.json(formatResponse(result, `Generated ${recommendations.length} integration recommendations`))
  } catch (error) {
    logger.error("統合提案生成でエラー", {
      error: error.message,
      stack: error.stack,
      base_article_id: req.body.base_article_id,
    })
    res.status(500).json(formatError("Failed to generate recommendations", error.message))
  }
})

/**
 * POST /api/recommendations/batch
 * 複数記事の一括統合提案生成
 */
router.post("/batch", async (req, res) => {
  try {
    const { article_pairs, analysis_type = "basic" } = req.body

    if (!Array.isArray(article_pairs) || article_pairs.length === 0) {
      return res.status(400).json(formatError("Article pairs array is required"))
    }

    if (article_pairs.length > 20) {
      return res.status(400).json(formatError("Maximum 20 article pairs allowed per batch"))
    }

    logger.info("一括統合提案生成開始", { pairs_count: article_pairs.length })

    const batchResults = []
    const errors = []

    // 並列処理で各ペアを処理
    const promises = article_pairs.map(async (pair, index) => {
      try {
        const { base_article_id, similar_article_id } = pair

        if (!base_article_id || !similar_article_id) {
          throw new Error(`Invalid pair at index ${index}: missing article IDs`)
        }

        // 記事データ取得
        const articlesData = await getArticlesData([base_article_id, similar_article_id])
        const baseArticle = articlesData[base_article_id]
        const similarArticle = articlesData[similar_article_id]

        if (!baseArticle || !similarArticle) {
          throw new Error(`Articles not found for pair at index ${index}`)
        }

        // 統合提案生成
        const recommendations = await generateRecommendations(
          baseArticle,
          [similarArticle],
          analysis_type,
          true,
          false, // SEO分析は省略してパフォーマンス向上
        )

        return {
          pair_index: index,
          base_article_id,
          similar_article_id,
          recommendations: recommendations[0] || null,
          success: true,
        }
      } catch (error) {
        logger.error(`一括処理でエラー (index: ${index})`, { error: error.message })
        errors.push({
          pair_index: index,
          error: error.message,
        })
        return null
      }
    })

    const results = await Promise.all(promises)
    const successfulResults = results.filter((result) => result !== null)

    const batchResult = {
      successful_pairs: successfulResults.length,
      total_pairs: article_pairs.length,
      results: successfulResults,
      errors: errors,
      analysis_type,
      generated_at: new Date().toISOString(),
    }

    logger.info("一括統合提案生成完了", {
      successful: successfulResults.length,
      total: article_pairs.length,
      errors: errors.length,
    })

    res.json(formatResponse(batchResult, `Processed ${successfulResults.length}/${article_pairs.length} article pairs`))
  } catch (error) {
    logger.error("一括統合提案生成でエラー", { error: error.message, stack: error.stack })
    res.status(500).json(formatError("Failed to generate batch recommendations", error.message))
  }
})

/**
 * GET /api/recommendations/templates
 * 統合提案テンプレート取得
 */
router.get("/templates", async (req, res) => {
  try {
    const templates = {
      merge_content: {
        name: "コンテンツ統合",
        description: "類似度の高い記事を1つの包括的な記事に統合",
        recommended_similarity: 0.94,
        steps: [
          "両記事の内容を詳細に比較分析",
          "重複部分と独自部分を特定",
          "統合後の記事構成を設計",
          "SEOキーワードの最適化",
          "301リダイレクトの設定",
          "内部リンクの更新",
        ],
        expected_benefits: [
          "ページビューの統合による順位向上",
          "重複コンテンツの解消",
          "ユーザー体験の向上",
          "サイト全体の権威性向上",
        ],
      },
      redirect_301: {
        name: "301リダイレクト",
        description: "低パフォーマンス記事から高パフォーマンス記事への統合",
        recommended_similarity: 0.8,
        steps: [
          "リダイレクト元・先記事の選定",
          "リダイレクト先記事の内容補強",
          "301リダイレクトの実装",
          "Search Consoleでの監視",
          "内部リンクの更新",
        ],
        expected_benefits: [
          "リンクジュースの統合",
          "インデックス効率の向上",
          "ユーザーの迷いの解消",
          "サイト構造の最適化",
        ],
      },
      cross_link: {
        name: "相互リンク強化",
        description: "関連記事間の内部リンクを強化してサイト回遊を向上",
        recommended_similarity: 0.7,
        steps: [
          "関連記事の特定",
          "リンク挿入箇所の選定",
          "自然なアンカーテキストの作成",
          "相互リンクの実装",
          "クリック率の監視",
        ],
        expected_benefits: [
          "サイト滞在時間の向上",
          "ページビューの増加",
          "サイト全体の権威性向上",
          "ユーザーエンゲージメント向上",
        ],
      },
    }

    res.json(formatResponse(templates, "Integration templates retrieved successfully"))
  } catch (error) {
    logger.error("テンプレート取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get templates", error.message))
  }
})

// ヘルパー関数
async function getArticlesData(articleIds) {
  const placeholders = articleIds.map(() => "?").join(",")
  const query = `
    SELECT 
      id, title, link, koza_id, full_content, qanda_content,
      content_type, pageviews, engaged_sessions, avg_engagement_time,
      organic_sessions, search_keywords, created_at, updated_at
    FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\`
    WHERE id IN (${placeholders})
  `

  const results = await bigQueryService.executeQuery(query, articleIds)

  const articlesData = {}
  results.forEach((row) => {
    articlesData[row.id] = {
      id: row.id,
      title: row.title,
      link: row.link,
      koza_id: row.koza_id,
      koza_name: null, // 後でJOINまたは別途取得
      full_content: row.full_content,
      qanda_content: row.qanda_content,
      content_type: row.content_type,
      pageviews: Number.parseInt(row.pageviews) || 0,
      engaged_sessions: Number.parseInt(row.engaged_sessions) || 0,
      avg_engagement_time: Number.parseFloat(row.avg_engagement_time) || 0,
      organic_sessions: Number.parseInt(row.organic_sessions) || 0,
      search_keywords: row.search_keywords || [],
      created_at: row.created_at,
      updated_at: row.updated_at,
    }
  })

  return articlesData
}

async function generateRecommendations(
  baseArticle,
  similarArticles,
  analysisType,
  includeTrafficPrediction,
  includeSeoAnalysis,
) {
  const recommendations = []

  for (const similarArticle of similarArticles) {
    try {
      // 類似度スコア取得（キャッシュから）
      const similarityScore = await getSimilarityScore(baseArticle.id, similarArticle.id)

      // 推奨アクションタイプ決定（新しい統一ロジックを使用）
      const matchingRatio = 0.5 // デフォルト値（実際のマッチング率が不明な場合）
      const sameCourse = baseArticle.koza_id === similarArticle.koza_id
      const actualMatchingCount = 0 // デフォルト値

      const recommendationResult = determineRecommendation(
        similarityScore,
        matchingRatio,
        sameCourse,
        baseArticle.pageviews || 0,
        similarArticle.pageviews || 0,
        actualMatchingCount
      )

      const recommendationType = recommendationResult.recommendation_type

      // トラフィック予測
      let trafficPrediction = null
      if (includeTrafficPrediction) {
        trafficPrediction = calculateTrafficPrediction(baseArticle, similarArticle, similarityScore)
      }

      // SEO分析
      let seoAnalysis = null
      if (includeSeoAnalysis) {
        seoAnalysis = await performSeoAnalysis(baseArticle, similarArticle)
      }

      // 統合効果スコア計算
      const integrationScore = calculateIntegrationScore(
        baseArticle,
        similarArticle,
        similarityScore,
        trafficPrediction,
      )

      const recommendation = {
        similar_article: {
          id: similarArticle.id,
          title: similarArticle.title,
          link: similarArticle.link,
          pageviews: similarArticle.pageviews,
        },
        similarity_score: similarityScore,
        recommendation_type: recommendationType,
        integration_score: integrationScore,
        priority: calculatePriority(integrationScore, similarityScore, trafficPrediction),
        ...(trafficPrediction && { traffic_prediction: trafficPrediction }),
        ...(seoAnalysis && { seo_analysis: seoAnalysis }),
        implementation_steps: getImplementationSteps(recommendationType),
        expected_timeline: getExpectedTimeline(recommendationType),
        risk_assessment: assessRisks(baseArticle, similarArticle, recommendationType),
      }

      recommendations.push(recommendation)
    } catch (error) {
      logger.error("個別推奨生成でエラー", {
        error: error.message,
        similarArticleId: similarArticle.id,
      })
      continue
    }
  }

  // 優先度でソート
  recommendations.sort((a, b) => b.priority - a.priority)

  return recommendations
}

async function getSimilarityScore(baseId, similarId) {
  try {
    const query = `
      SELECT similarity_score
      FROM \`${process.env.GCP_PROJECT}.content_analysis.similarity_cache\`
      WHERE base_article_id = ? AND similar_article_id = ?
        AND expires_at > CURRENT_TIMESTAMP()
      LIMIT 1
    `

    const results = await bigQueryService.executeQuery(query, [baseId, similarId])
    return results.length > 0 ? Number.parseFloat(results[0].similarity_score) : 0.5
  } catch (error) {
    logger.info("類似度スコア取得でエラー", { error: error.message })
    return 0.5 // デフォルト値
  }
}

// 古いdetermineRecommendationTypeは統一関数に置き換えられました
// 新しいロジックは utils/recommendation-logic.js の determineRecommendation を使用

function calculateTrafficPrediction(baseArticle, similarArticle, similarityScore) {
  const basePv = baseArticle.pageviews || 0
  const similarPv = similarArticle.pageviews || 0
  const combinedPv = basePv + similarPv

  // シナジー効果を考慮
  const synergyFactor = 1 + similarityScore * 0.3
  const predictedPv = Math.round(combinedPv * synergyFactor)

  return {
    current_combined_pv: combinedPv,
    predicted_pv: predictedPv,
    expected_increase: predictedPv - combinedPv,
    increase_percentage: combinedPv > 0 ? Math.round(((predictedPv - combinedPv) / combinedPv) * 100) : 0,
    confidence_level: similarityScore > 0.7 ? "high" : similarityScore > 0.5 ? "medium" : "low",
  }
}

async function performSeoAnalysis(baseArticle, similarArticle) {
  try {
    // キーワード重複分析
    const baseKeywords = baseArticle.search_keywords || []
    const similarKeywords = similarArticle.search_keywords || []
    const commonKeywords = baseKeywords.filter((keyword) =>
      similarKeywords.some((sk) => sk.toLowerCase() === keyword.toLowerCase()),
    )

    // コンテンツ長分析
    const baseContentLength = (baseArticle.full_content || "").length
    const similarContentLength = (similarArticle.full_content || "").length

    return {
      keyword_overlap: {
        common_keywords: commonKeywords,
        overlap_percentage:
          baseKeywords.length > 0 ? Math.round((commonKeywords.length / baseKeywords.length) * 100) : 0,
        potential_cannibalization: commonKeywords.length > 3,
      },
      content_analysis: {
        base_content_length: baseContentLength,
        similar_content_length: similarContentLength,
        length_ratio: similarContentLength > 0 ? Math.round((baseContentLength / similarContentLength) * 100) / 100 : 0,
      },
      seo_recommendations: generateSeoRecommendations(commonKeywords.length, baseContentLength, similarContentLength),
    }
  } catch (error) {
    logger.error("SEO分析でエラー", { error: error.message })
    return null
  }
}

function generateSeoRecommendations(keywordOverlap, baseLength, similarLength) {
  const recommendations = []

  if (keywordOverlap > 3) {
    recommendations.push("キーワードカニバリゼーションの可能性があります。統合を検討してください。")
  }

  if (baseLength < 1000 && similarLength < 1000) {
    recommendations.push("両記事とも短いため、統合により充実したコンテンツを作成できます。")
  }

  if (Math.abs(baseLength - similarLength) > 2000) {
    recommendations.push("コンテンツ量に大きな差があります。短い記事の内容を長い記事に統合することを検討してください。")
  }

  return recommendations
}

function calculateIntegrationScore(baseArticle, similarArticle, similarityScore, trafficPrediction) {
  let score = similarityScore * 100 // ベーススコア

  // トラフィック要因
  const totalPv = (baseArticle.pageviews || 0) + (similarArticle.pageviews || 0)
  if (totalPv > 1000) score += 10
  if (totalPv > 5000) score += 10

  // エンゲージメント要因
  const avgEngagement = ((baseArticle.avg_engagement_time || 0) + (similarArticle.avg_engagement_time || 0)) / 2
  if (avgEngagement > 60) score += 5

  // 予測効果要因
  if (trafficPrediction && trafficPrediction.increase_percentage > 20) {
    score += 15
  }

  return Math.min(Math.round(score), 100)
}

function calculatePriority(integrationScore, similarityScore, trafficPrediction) {
  let priority = integrationScore

  // 高類似度ボーナス
  if (similarityScore > 0.8) priority += 10

  // 高トラフィック予測ボーナス
  if (trafficPrediction && trafficPrediction.predicted_pv > 10000) {
    priority += 15
  }

  return Math.min(priority, 100)
}

function getImplementationSteps(recommendationType) {
  const steps = {
    MERGE_CONTENT: [
      "両記事の詳細な内容分析",
      "統合後の記事構成設計",
      "重複部分の整理と独自部分の統合",
      "SEOキーワードの最適化",
      "新記事の作成と公開",
      "301リダイレクトの設定",
      "内部リンクの更新",
    ],
    REDIRECT_301: [
      "リダイレクト先記事の内容確認・補強",
      "301リダイレクトの実装",
      "Search Consoleでの監視設定",
      "内部リンクの更新",
      "リダイレクト効果の測定",
    ],
    CROSS_LINK: [
      "相互リンク箇所の特定",
      "自然なアンカーテキストの作成",
      "リンクの実装",
      "クリック率の監視",
      "ユーザー行動の分析",
    ],
    MONITOR: ["定期的な類似度チェック", "トラフィック動向の監視", "コンテンツ更新の検討", "将来的な統合可能性の評価"],
  }

  return steps[recommendationType] || steps["MONITOR"]
}

function getExpectedTimeline(recommendationType) {
  const timelines = {
    MERGE_CONTENT: "2-4週間",
    REDIRECT_301: "1-2週間",
    CROSS_LINK: "1週間",
    MONITOR: "継続的",
  }

  return timelines[recommendationType] || "未定"
}

function assessRisks(baseArticle, similarArticle, recommendationType) {
  const risks = []

  if (recommendationType === "MERGE_CONTENT") {
    if ((baseArticle.pageviews || 0) > 5000) {
      risks.push("高トラフィック記事の統合によるSEOリスク")
    }
    risks.push("統合作業中の一時的な順位下落の可能性")
  }

  if (recommendationType === "REDIRECT_301") {
    risks.push("リダイレクト実装時の技術的リスク")
    risks.push("短期的なトラフィック減少の可能性")
  }

  if ((baseArticle.organic_sessions || 0) > 1000 || (similarArticle.organic_sessions || 0) > 1000) {
    risks.push("オーガニック検索への影響")
  }

  return risks.length > 0 ? risks : ["リスクは低いと評価されます"]
}

module.exports = router
