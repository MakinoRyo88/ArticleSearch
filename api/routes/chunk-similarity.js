/**
 * チャンクベース類似度検索API
 * 記事の各チャンクを使って類似記事を検索
 */

const express = require("express");
const { param, query, validationResult } = require("express-validator");
const BigQueryService = require("../services/bigquery-service");
const { formatResponse, formatError } = require("../utils/response-formatter");
const { calculateEnhancedSimilarityScore } = require("../utils/chunk-weights");
const { determineRecommendation } = require("../utils/recommendation-logic");
const winston = require("winston");

const router = express.Router();
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
});

const bigQueryService = new BigQueryService();

const chunkSimilarityValidation = [
  param("articleId").isString().isLength({ min: 1, max: 100 }),
  query("limit").optional().isInt({ min: 1, max: 50 }),
  query("threshold").optional().isFloat({ min: 0, max: 1 }),
  query("min_pageviews").optional().isInt({ min: 0 }),
  query("top_chunks").optional().isInt({ min: 1, max: 20 }),
];

/**
 * GET /api/chunk-similarity/stats
 * チャンク統計情報
 */
router.get("/stats", async (req, res) => {
  try {
    logger.info("チャンク統計情報取得開始");

    const projectId = process.env.GCP_PROJECT;

    const statsQuery = `
      SELECT
        COUNT(*) as total_chunks,
        COUNT(DISTINCT article_id) as articles_with_chunks,
        AVG(LENGTH(chunk_text)) as avg_chunk_length,
        MIN(LENGTH(chunk_text)) as min_chunk_length,
        MAX(LENGTH(chunk_text)) as max_chunk_length,
        COUNT(CASE WHEN content_embedding IS NOT NULL 
              AND ARRAY_LENGTH(content_embedding) > 0 THEN 1 END) as chunks_with_embeddings,
        AVG(chunk_index) as avg_chunks_per_article
      FROM \`${projectId}.content_analysis.article_chunks\`
    `;

    const results = await bigQueryService.executeQuery(statsQuery);
    const stats = results[0];

    const statsData = {
      total_chunks: parseInt(stats.total_chunks) || 0,
      articles_with_chunks: parseInt(stats.articles_with_chunks) || 0,
      avg_chunk_length: Math.round(parseFloat(stats.avg_chunk_length) || 0),
      min_chunk_length: parseInt(stats.min_chunk_length) || 0,
      max_chunk_length: parseInt(stats.max_chunk_length) || 0,
      chunks_with_embeddings: parseInt(stats.chunks_with_embeddings) || 0,
      avg_chunks_per_article: Math.round(parseFloat(stats.avg_chunks_per_article) || 0),
      coverage_rate: stats.total_chunks > 0
        ? Math.round((stats.chunks_with_embeddings / stats.total_chunks) * 100)
        : 0
    };

    logger.info("チャンク統計情報取得完了", statsData);

    res.json(formatResponse(statsData, "Chunk statistics retrieved successfully"));
  } catch (error) {
    logger.error("チャンク統計情報取得でエラー", {
      error: error.message,
      stack: error.stack
    });
    res.json(
      formatError("Failed to get chunk statistics", error.message)
    );
  }
});

/**
 * GET /api/chunk-similarity/:articleId
 * チャンク単位で類似記事を検索
 * 
 * 処理フロー:
 * 1. 基点記事の全チャンクを取得
 * 2. 各チャンクで類似チャンクを検索
 * 3. 記事IDでグループ化
 * 4. 最大スコアでランキング
 */
router.get("/:articleId", chunkSimilarityValidation, async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json(formatError("Validation failed", errors.array()));
    }

    const { articleId } = req.params;
    const { 
      limit = 20, 
      threshold = 0.5, 
      min_pageviews = 0,
      top_chunks = 10 
    } = req.query;

    logger.info("チャンクベース類似記事検索開始", { 
      articleId, 
      limit, 
      threshold,
      top_chunks 
    });

    const projectId = process.env.GCP_PROJECT;
    
    // ステップ1: 基点記事の情報を取得（メタデータ含む）
    const baseArticleQuery = `
      SELECT 
        a.id,
        a.title,
        a.link,
        a.koza_id,
        a.koza_name,
        a.pageviews,
        a.engaged_sessions,
        a.search_keywords,
        a.created_at,
        a.updated_at,
        COUNT(c.chunk_id) as chunk_count
      FROM \`${projectId}.content_analysis.articles\` a
      LEFT JOIN \`${projectId}.content_analysis.article_chunks\` c
        ON CAST(a.id AS STRING) = c.article_id
      WHERE CAST(a.id AS STRING) = @articleId
      GROUP BY a.id, a.title, a.link, a.koza_id, a.koza_name, 
               a.pageviews, a.engaged_sessions, a.search_keywords,
               a.created_at, a.updated_at
    `;
    
    const baseArticleResult = await bigQueryService.executeQuery(baseArticleQuery, {
      articleId: String(articleId)
    });

    if (baseArticleResult.length === 0) {
      return res.status(404).json(
        formatError("Base article not found", `ID: ${articleId}`)
      );
    }

    const baseArticle = baseArticleResult[0];
    
    if (baseArticle.chunk_count === 0) {
      return res.status(404).json(
        formatError("No chunks found for this article", `ID: ${articleId}`)
      );
    }

    logger.info(`基点記事: ${baseArticle.title}, チャンク数: ${baseArticle.chunk_count}`);

    // ステップ2: 各チャンクで類似検索を実行し、記事単位で集約
    const similarityQuery = `
      WITH base_chunks AS (
        -- 基点記事の全チャンク（インデックス付き）
        SELECT 
          chunk_id,
          chunk_title,
          chunk_index,
          content_embedding
        FROM \`${projectId}.content_analysis.article_chunks\`
        WHERE article_id = CAST(@articleId AS STRING)
          AND content_embedding IS NOT NULL
          AND ARRAY_LENGTH(content_embedding) > 0
      ),
      chunk_similarities AS (
        -- 各基点チャンクに対して類似チャンクを検索
        SELECT
          base.chunk_id as base_chunk_id,
          base.chunk_title as base_chunk_title,
          base.chunk_index as base_chunk_index,
          target.chunk_id as similar_chunk_id,
          target.chunk_title as similar_chunk_title,
          target.chunk_index as similar_chunk_index,
          target.article_id as similar_article_id,
          -- コサイン類似度を計算
          (
            SELECT
              SUM(base_vec * target_vec) / (
                SQRT(SUM(base_vec * base_vec)) * 
                SQRT(SUM(target_vec * target_vec))
              )
            FROM
              UNNEST(base.content_embedding) AS base_vec WITH OFFSET AS base_offset
              JOIN UNNEST(target.content_embedding) AS target_vec WITH OFFSET AS target_offset
                ON base_offset = target_offset
          ) AS similarity_score
        FROM
          base_chunks base,
          \`${projectId}.content_analysis.article_chunks\` target
        WHERE
          target.article_id != CAST(@articleId AS STRING)
          AND target.content_embedding IS NOT NULL
          AND ARRAY_LENGTH(target.content_embedding) > 0
      ),
      ranked_similarities AS (
        -- 各基点チャンクごとに類似チャンクをランキング
        SELECT
          base_chunk_id,
          base_chunk_title,
          base_chunk_index,
          similar_chunk_id,
          similar_chunk_title,
          similar_chunk_index,
          similar_article_id,
          similarity_score,
          ROW_NUMBER() OVER (
            PARTITION BY base_chunk_id 
            ORDER BY similarity_score DESC
          ) as rank_per_base_chunk
        FROM chunk_similarities
        WHERE similarity_score >= @threshold
      ),
      article_aggregated AS (
        -- 記事IDで集約し、加重平均スコアを計算
        SELECT
          similar_article_id,
          -- 加重スコア: 平均類似度 × 0.70 + マッチング率ボーナス × 0.30（最大100%）
          -- マッチング率 = 実際の一致数 / 短い方の記事のチャンク数
          LEAST(
            AVG(similarity_score) * 0.70 + 
            (SAFE_DIVIDE(
              LEAST(COUNT(DISTINCT base_chunk_id), COUNT(DISTINCT similar_chunk_id)),
              LEAST(
                (SELECT COUNT(*) FROM base_chunks),
                (SELECT COUNT(*) FROM \`${projectId}.content_analysis.article_chunks\` 
                 WHERE article_id = similar_article_id AND content_embedding IS NOT NULL)
              )
            ) * 0.30),
            1.0
          ) as max_similarity_score,
          MAX(similarity_score) as peak_similarity_score,
          AVG(similarity_score) as avg_similarity_score,
          COUNT(DISTINCT base_chunk_id) as matching_base_chunks,
          COUNT(DISTINCT similar_chunk_id) as matching_similar_chunks,
          -- マッチング率も計算（実際の一致数を使用）
          SAFE_DIVIDE(
            LEAST(COUNT(DISTINCT base_chunk_id), COUNT(DISTINCT similar_chunk_id)),
            LEAST(
              (SELECT COUNT(*) FROM base_chunks),
              (SELECT COUNT(*) FROM \`${projectId}.content_analysis.article_chunks\` 
               WHERE article_id = similar_article_id AND content_embedding IS NOT NULL)
            )
          ) as matching_ratio,
          ARRAY_AGG(
            STRUCT(
              base_chunk_title,
              base_chunk_index,
              similar_chunk_title,
              similar_chunk_index,
              similarity_score
            )
            ORDER BY similarity_score DESC
            LIMIT @top_chunks
          ) as top_matching_chunks
        FROM ranked_similarities
        WHERE rank_per_base_chunk <= @top_chunks
        GROUP BY similar_article_id
      )
      SELECT
        a.id,
        a.title,
        a.link,
        a.koza_id,
        a.koza_name,
        a.pageviews,
        a.engaged_sessions,
        a.search_keywords,
        a.created_at,
        a.updated_at,
        agg.max_similarity_score,
        agg.peak_similarity_score,
        agg.avg_similarity_score,
        agg.matching_base_chunks,
        agg.matching_similar_chunks,
        agg.matching_ratio,
        agg.top_matching_chunks,
        -- 類似記事の総チャンク数も取得
        (SELECT COUNT(*) FROM \`${projectId}.content_analysis.article_chunks\` 
         WHERE article_id = CAST(a.id AS STRING) AND content_embedding IS NOT NULL) as similar_total_chunks
      FROM article_aggregated agg
      JOIN \`${projectId}.content_analysis.articles\` a
        ON CAST(a.id AS STRING) = agg.similar_article_id
      WHERE a.pageviews >= @min_pageviews
      ORDER BY agg.max_similarity_score DESC
      LIMIT @limit
    `;

    const similarityParams = {
      articleId: String(articleId),
      threshold: parseFloat(threshold),
      top_chunks: parseInt(top_chunks, 10),
      min_pageviews: parseInt(min_pageviews, 10),
      limit: parseInt(limit, 10)
    };

    const searchResults = await bigQueryService.executeQuery(
      similarityQuery, 
      similarityParams
    );

    // 結果の整形と推奨タイプの判定（フェーズ1改善版: 加重マッチング率 + セマンティック距離 + メタデータ）
    const similarArticles = searchResults.map((row) => {
      const originalScore = row.max_similarity_score;
      const peakScore = row.peak_similarity_score;
      const avgScore = row.avg_similarity_score;
      const matchingBaseChunks = parseInt(row.matching_base_chunks);
      const matchingSimilarChunks = parseInt(row.matching_similar_chunks);
      // 実際の一致数は両方のユニークカウントの最小値
      const actualMatchingCount = Math.min(matchingBaseChunks, matchingSimilarChunks);
      const matchingRatio = parseFloat(row.matching_ratio) || 0;
      const similarTotalChunks = parseInt(row.similar_total_chunks) || 0;
      const baseTotalChunks = parseInt(baseArticle.chunk_count) || 0;
      const similarPageviews = parseInt(row.pageviews) || 0;
      const basePageviews = parseInt(baseArticle.pageviews) || 0;
      const sameCourse = row.koza_id === baseArticle.koza_id;

      // フェーズ1改善: 加重スコア計算
      const similarArticle = {
        id: row.id,
        title: row.title,
        koza_id: row.koza_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
        search_keywords: row.search_keywords
      };

      const matchingInfo = {
        matching_chunks: row.top_matching_chunks || [],
        base_total_chunks: baseTotalChunks,
        similar_total_chunks: similarTotalChunks
      };

      const enhancedResult = calculateEnhancedSimilarityScore(
        avgScore,
        matchingInfo,
        baseArticle,
        similarArticle
      );

      const score = enhancedResult.finalScore;
      const scoreBreakdown = enhancedResult.breakdown;

      // 統一された推奨判定ロジックを使用
      const recommendationResult = determineRecommendation(
        score,
        matchingRatio,
        sameCourse,
        basePageviews,
        similarPageviews,
        actualMatchingCount
      );

      const recommendation_type = recommendationResult.recommendation_type;
      const priority = recommendationResult.priority;
      const explanation_text = recommendationResult.explanation_text;

      return {
        id: row.id,
        title: row.title,
        link: row.link,
        koza_id: row.koza_id,
        koza_name: row.koza_name,
        pageviews: parseInt(row.pageviews) || 0,
        engaged_sessions: parseInt(row.engaged_sessions) || 0,
        similarity_score: score,
        original_similarity_score: originalScore, // 旧スコア（比較用）
        peak_similarity_score: peakScore,
        avg_similarity_score: avgScore,
        score_breakdown: scoreBreakdown, // 詳細スコア内訳
        matching_base_chunks: matchingBaseChunks,
        matching_similar_chunks: matchingSimilarChunks,
        actual_matching_count: actualMatchingCount,
        matching_ratio: matchingRatio,
        base_total_chunks: baseTotalChunks,
        similar_total_chunks: similarTotalChunks,
        recommendation_type: recommendation_type,
        recommendation_priority: priority,
        explanation_text: explanation_text,
        confidence_score: recommendationResult.confidence_score,
        search_keywords: row.search_keywords || [],
        top_matching_chunks: row.top_matching_chunks || []
      };
    });

    // 新しいスコアで再ソート（優先度スコアと類似度の組み合わせ）
    similarArticles.sort((a, b) => {
      // 優先度が異なる場合は優先度で比較
      if (a.recommendation_priority !== b.recommendation_priority) {
        return b.recommendation_priority - a.recommendation_priority;
      }
      // 優先度が同じ場合は類似度で比較
      return b.similarity_score - a.similarity_score;
    });

    // 上位N件に制限
    const limitedResults = similarArticles.slice(0, parseInt(limit, 10));

    // レスポンスの構築
    const result = {
      base_article: {
        id: baseArticle.id,
        title: baseArticle.title,
        link: baseArticle.link,
        koza_id: baseArticle.koza_id,
        koza_name: baseArticle.koza_name,
        pageviews: parseInt(baseArticle.pageviews) || 0,
        engaged_sessions: parseInt(baseArticle.engaged_sessions) || 0,
        search_keywords: baseArticle.search_keywords || [],
        chunk_count: parseInt(baseArticle.chunk_count),
        created_at: baseArticle.created_at,
        updated_at: baseArticle.updated_at
      },
      similar_articles: limitedResults,
      metadata: {
        total_found: similarArticles.length,
        returned_count: limitedResults.length,
        threshold_used: parseFloat(threshold),
        top_chunks_per_base: parseInt(top_chunks, 10),
        search_method: "chunk_based_similarity_enhanced_v2",
        algorithm_version: "phase1_improvements",
        improvements: [
          "weighted_chunk_matching",
          "semantic_distance_bonus",
          "metadata_bonus"
        ],
        filters_applied: {
          min_pageviews: parseInt(min_pageviews, 10),
        }
      }
    };

    logger.info("チャンクベース類似記事検索完了（フェーズ1改善版）", {
      articleId,
      found_count: similarArticles.length,
      returned_count: limitedResults.length,
      base_chunk_count: baseArticle.chunk_count,
      algorithm: "phase1_improvements"
    });

    res.json(formatResponse(
      result, 
      `Found ${limitedResults.length} similar articles using enhanced chunk-based search (Phase 1 improvements)`
    ));

  } catch (error) {
    logger.error("チャンクベース類似記事検索でエラー", {
      error: error.message,
      stack: error.stack,
      articleId: req.params.articleId,
    });
    res.status(500).json(
      formatError("Failed to search similar articles", error.message)
    );
  }
});

module.exports = router;
