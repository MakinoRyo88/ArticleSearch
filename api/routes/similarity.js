/**
 * 類似度計算API（修正版）
 * 基本カラムのみを使用
 */

const express = require("express")
const { param, query, validationResult } = require("express-validator")
const BigQueryService = require("../services/bigquery-service")
const { formatResponse, formatError } = require("../utils/response-formatter")
const winston = require("winston")

const router = express.Router()
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

const bigQueryService = new BigQueryService()

const similarityValidation = [
  param("articleId").isString().isLength({ min: 1, max: 100 }),
  query("limit").optional().isInt({ min: 1, max: 50 }),
  query("threshold").optional().isFloat({ min: 0, max: 1 }),
  query("min_pageviews").optional().isInt({ min: 0 }),
]


/**
 * GET /api/similarity/:articleId
 * リアルタイムで類似記事をベクトル検索
 */
router.get("/:articleId", similarityValidation, async (req, res) => {
  try {
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
      return res.status(400).json(formatError("Validation failed", errors.array()))
    }

    const { articleId } = req.params
    const { limit = 20, threshold = 0.3, min_pageviews = 0 } = req.query

    logger.info("リアルタイム類似記事検索開始", { articleId, limit, threshold })

    const viewName = `\`${process.env.GCP_PROJECT}.content_analysis.articles_with_valid_embeddings\``

    const baseArticleQuery = `
      SELECT id, title, link, koza_id, pageviews, full_content, qanda_content, search_keywords
      FROM ${viewName}
      WHERE id = ?
      LIMIT 1
    `
    const baseArticleResult = await bigQueryService.executeQuery(baseArticleQuery, [articleId])

    if (baseArticleResult.length === 0) {
      return res.status(404).json(formatError("Base article not found or has invalid embedding", `ID: ${articleId}`))
    }
    const baseArticle = baseArticleResult[0]
    
    // ★★★ 最後の修正ポイント：SQL内の不要な「.val」を削除 ★★★
    const directCalculationQuery = `
      WITH base_article AS (
        SELECT content_embedding
        FROM ${viewName}
        WHERE id = ?
      )
      SELECT
        t.id,
        t.title,
        t.link,
        t.koza_id,
        t.pageviews,
        t.engaged_sessions,
        t.search_keywords,
        (
          SELECT
            SUM(base_vector * t_vector) / (SQRT(SUM(base_vector * base_vector)) * SQRT(SUM(t_vector * t_vector)))
          FROM
            UNNEST(b.content_embedding) AS base_vector WITH OFFSET AS base_offset
            JOIN UNNEST(t.content_embedding) AS t_vector WITH OFFSET AS t_offset
            ON base_offset = t_offset
        ) AS similarity_score
      FROM
        ${viewName} AS t,
        base_article AS b
      WHERE
        t.id != ?
        AND t.koza_id = ?
        AND t.pageviews >= ?
      ORDER BY
        similarity_score DESC
      LIMIT ?
    `

    const queryParams = [
      articleId,
      articleId,
      baseArticle.koza_id,
      parseInt(min_pageviews, 10),
      parseInt(limit, 10),
    ]

    const searchResults = await bigQueryService.executeQuery(directCalculationQuery, queryParams)

    // 結果の整形と定型文の生成
    // 結果の整形と定型文の生成
    const similarArticles = searchResults
      .filter((row) => row.similarity_score >= threshold)
      .map((row) => {
        const score = row.similarity_score
        let recommendation_type = "MONITOR"
        let explanation_text = "関連性は低いですが、定期的にパフォーマンスを監視することをお勧めします。"

        if (score >= 0.94) {
          recommendation_type = "MERGE_CONTENT"
          explanation_text = `類似度が ${(score * 100).toFixed(1)}% と極めて高く、内容がほぼ重複しています。SEO評価を集約するため、コンテンツ統合を強く推奨します。`
        } else if (score >= 0.8) {
          recommendation_type = "REDIRECT_301"
          explanation_text = `類似度が ${(score * 100).toFixed(1)}% と非常に高く、主要トピックが共通しています。評価の高い記事へ301リダイレクトすることを検討してください。`
        } else if (score >= 0.7) {
          recommendation_type = "CROSS_LINK"
          explanation_text = `類似度が ${(score * 100).toFixed(1)}% で、関連性が見られます。双方の記事から相互にリンクを設置し、ユーザーの回遊性を高めることを推奨します。`
        }

        return {
          id: row.id,
          title: row.title,
          link: row.link,
          koza_id: row.koza_id,
          koza_name: null,
          pageviews: Number.parseInt(row.pageviews) || 0,
          engaged_sessions: Number.parseInt(row.engaged_sessions) || 0,
          similarity_score: score,
          recommendation_type: recommendation_type,
          explanation_text: explanation_text,
          confidence_score: score * 0.9 + 0.1,
          search_keywords: row.search_keywords || [],
        }
      })
      
    // 最終的なレスポンスの構築
    const baseArticleFormatted = {
        id: baseArticle.id,
        title: baseArticle.title,
        link: baseArticle.link,
        koza_id: baseArticle.koza_id,
        koza_name: null,
        pageviews: Number.parseInt(baseArticle.pageviews) || 0,
        full_content: baseArticle.full_content || null,
        qanda_content: baseArticle.qanda_content || null,
        search_keywords: baseArticle.search_keywords || [],
      }
  
      const result = {
        base_article: baseArticleFormatted,
        similar_articles: similarArticles,
        metadata: {
          total_found: similarArticles.length,
          threshold_used: parseFloat(threshold),
          filters_applied: {
            koza_id: baseArticle.koza_id || null,
            min_pageviews: parseInt(min_pageviews, 10),
          },
          cache_based: false,
        },
      }
  
      logger.info("リアルタイム類似記事検索完了", {
        articleId,
        found_count: similarArticles.length,
      })
  
      res.json(formatResponse(result, `Found ${similarArticles.length} similar articles in real-time`))

  } catch (error) {
    logger.error("リアルタイム類似記事検索でエラー", {
      error: error.message,
      stack: error.stack,
      articleId: req.params.articleId,
    })
    res.status(500).json(formatError("Failed to get similar articles", error.message))
  }
})


/**
 * GET /api/similarity/stats
 * 類似度計算統計情報（基本版）
 */
router.get("/stats", async (req, res) => {
  try {
    const cacheKey = "similarity_stats"

    // キャッシュから取得試行
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      return res.json(formatResponse(cachedResult, "Similarity stats retrieved from cache"))
    }

    logger.info("類似度統計情報取得開始")

    // similarity_cacheテーブルが存在するかチェック
    const cacheTableExistsQuery = `
      SELECT COUNT(*) as table_count
      FROM \`${process.env.GCP_PROJECT}.content_analysis.INFORMATION_SCHEMA.TABLES\`
      WHERE table_name = 'similarity_cache'
    `

    let statsData = {
      total_cache_entries: 0,
      unique_base_articles: 0,
      avg_similarity_score: 0.0,
      max_similarity_score: 0.0,
      min_similarity_score: 0.0,
      recommendations: {
        merge_content: 0,
        redirect_301: 0,
        cross_link: 0,
      },
      valid_entries: 0,
      cache_health: {
        oldest_cache: null,
        newest_cache: null,
        coverage_rate: 0,
      },
    }
    let tableCheck // Declare tableCheck variable here

    try {
      tableCheck = await bigQueryService.executeQuery(cacheTableExistsQuery)

      if (tableCheck[0]?.table_count > 0) {
        const statsQuery = `
          SELECT
            COUNT(*) as total_cache_entries,
            COUNT(DISTINCT base_article_id) as unique_base_articles,
            AVG(similarity_score) as avg_similarity_score,
            MAX(similarity_score) as max_similarity_score,
            MIN(similarity_score) as min_similarity_score,
            COUNT(CASE WHEN recommendation_type = 'MERGE_CONTENT' THEN 1 END) as merge_recommendations,
            COUNT(CASE WHEN recommendation_type = 'REDIRECT_301' THEN 1 END) as redirect_recommendations,
            COUNT(CASE WHEN recommendation_type = 'CROSS_LINK' THEN 1 END) as crosslink_recommendations,
            COUNT(CASE WHEN expires_at > CURRENT_TIMESTAMP() THEN 1 END) as valid_entries,
            MIN(cached_at) as oldest_cache,
            MAX(cached_at) as newest_cache
          FROM \`${process.env.GCP_PROJECT}.content_analysis.similarity_cache\`
        `

        const results = await bigQueryService.executeQuery(statsQuery)
        const stats = results[0]

        statsData = {
          total_cache_entries: Number.parseInt(stats.total_cache_entries) || 0,
          unique_base_articles: Number.parseInt(stats.unique_base_articles) || 0,
          avg_similarity_score: Number.parseFloat(stats.avg_similarity_score) || 0,
          max_similarity_score: Number.parseFloat(stats.max_similarity_score) || 0,
          min_similarity_score: Number.parseFloat(stats.min_similarity_score) || 0,
          recommendations: {
            merge_content: Number.parseInt(stats.merge_recommendations) || 0,
            redirect_301: Number.parseInt(stats.redirect_recommendations) || 0,
            cross_link: Number.parseInt(stats.crosslink_recommendations) || 0,
          },
          valid_entries: Number.parseInt(stats.valid_entries) || 0,
          cache_health: {
            oldest_cache: stats.oldest_cache,
            newest_cache: stats.newest_cache,
            coverage_rate:
              stats.unique_base_articles > 0
                ? Math.round((stats.valid_entries / stats.unique_base_articles) * 100) / 100
                : 0,
          },
        }
      }
    } catch (error) {
      logger.info("similarity_cacheテーブルアクセスでエラー", { error: error.message })
    }

    // 結果をキャッシュ（10分間）
    cacheService.set(cacheKey, statsData, 600)

    logger.info("類似度統計情報取得完了", statsData)

    res.json(formatResponse(statsData, "Similarity statistics retrieved successfully"))
  } catch (error) {
    logger.error("類似度統計情報取得でエラー", { error: error.message, stack: error.stack })
    res.status(500).json(formatError("Failed to get similarity statistics", error.message))
  }
})

module.exports = router
