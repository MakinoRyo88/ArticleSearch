/**
 * 記事検索API（最適化版）
 * タイムアウト対策とエラーハンドリング強化
 */

const express = require("express")
const { query, validationResult } = require("express-validator")
const BigQueryService = require("../services/bigquery-service")
const CacheService = require("../services/cache-service")
const { formatResponse, formatError } = require("../utils/response-formatter")
const winston = require("winston")

const router = express.Router()
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

const bigQueryService = new BigQueryService()
const cacheService = new CacheService()

// バリデーションルール
const articleSearchValidation = [
  query("search").optional().isString().isLength({ max: 200 }),
  query("koza_id").optional().isString().isLength({ max: 100 }),
  query("min_pageviews").optional().isInt({ min: 0 }),
  query("max_pageviews").optional().isInt({ min: 0 }),
  query("min_engaged_sessions").optional().isInt({ min: 0 }),
  query("max_engaged_sessions").optional().isInt({ min: 0 }),
  query("content_type").optional().isString().isLength({ max: 50 }),
  query("sort")
    .optional()
    .isIn(["pageviews_desc", "pageviews_asc", "title_asc", "title_desc", "updated_at_desc", "updated_at_asc","engaged_sessions_desc", "engaged_sessions_asc"]),
  query("page").optional().isInt({ min: 1 }),
  query("limit").optional().isInt({ min: 1, max: 100 }),
]

/**
 * GET /api/search/articles
 * 記事検索・一覧取得（最適化版）
 */
router.get("/articles", articleSearchValidation, async (req, res) => {
  try {
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
      return res.status(400).json(formatError("Validation failed", errors.array()))
    }

    // パラメータを分割して受け取る
    const {
      search = "",
      koza_id = "",
      content_type = "",
      sort = "pageviews_desc",
    } = req.query

    // 数値パラメータを明示的にパースして型変換する
    const page = req.query.page ? parseInt(req.query.page, 10) : 1
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : 50
    const min_pageviews = req.query.min_pageviews ? parseInt(req.query.min_pageviews, 10) : 0
    const max_pageviews = req.query.max_pageviews ? parseInt(req.query.max_pageviews, 10) : null
    
    // （補足）未実装の engaged_sessions も同様にパースする
    const min_engaged_sessions = req.query.min_engaged_sessions ? parseInt(req.query.min_engaged_sessions, 10) : 0
    const max_engaged_sessions = req.query.max_engaged_sessions ? parseInt(req.query.max_engaged_sessions, 10) : null

    const cacheKey = `articles_search_${Buffer.from(JSON.stringify(req.query)).toString("base64")}`
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      logger.info("記事検索結果をキャッシュから返却", { cacheKey })
      return res.json(formatResponse(cachedResult, "Articles retrieved from cache"))
    }

    logger.info("記事検索開始", { search, koza_id, min_pageviews, sort, page, limit })

    const offset = (page - 1) * limit
    const whereConditions = ["a.id IS NOT NULL"]
    const queryParams = []

    if (search) {
      whereConditions.push("LOWER(a.title) LIKE LOWER(CONCAT('%', ?, '%'))")
      queryParams.push(search)
    }
    if (koza_id) {
      whereConditions.push("a.koza_id = ?")
      queryParams.push(koza_id)
    }
    if (min_pageviews > 0) {
      whereConditions.push("a.pageviews >= ?")
      queryParams.push(min_pageviews)
    }
    if (max_pageviews) {
      whereConditions.push("a.pageviews <= ?")
      queryParams.push(max_pageviews)
    }
    if (min_engaged_sessions > 0) {
      whereConditions.push("a.engaged_sessions >= ?")
      queryParams.push(min_engaged_sessions)
    }
    if (max_engaged_sessions) {
      whereConditions.push("a.engaged_sessions <= ?")
      queryParams.push(max_engaged_sessions)
    }

    let orderBy = "a.pageviews DESC"
    switch (sort) {
      case "pageviews_asc": orderBy = "a.pageviews ASC"; break
      case "title_asc": orderBy = "a.title ASC"; break
      case "title_desc": orderBy = "a.title DESC"; break
      case "updated_at_desc": orderBy = "a.updated_at DESC"; break
      case "updated_at_asc": orderBy = "a.updated_at ASC"; break
      case "engaged_sessions_desc": orderBy = "a.engaged_sessions DESC"; break
      case "engaged_sessions_asc": orderBy = "a.engaged_sessions ASC"; break
    }

    const articlesQuery = `
      SELECT 
        a.id,
        a.title,
        a.link,
        a.koza_id,
        c.name as koza_name,
        c.slug as koza_slug,
        a.pageviews,
        a.engaged_sessions,
        a.avg_engagement_time,
        a.organic_sessions,
        a.search_keywords,
        a.created_at,
        a.updated_at,
        a.last_synced,
        (ARRAY_LENGTH(a.content_embedding) > 0) AS has_embedding
      FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\` AS a
      LEFT JOIN \`${process.env.GCP_PROJECT}.content_analysis.courses\` AS c ON CAST(a.koza_id AS STRING) = CAST(c.id AS STRING)
      WHERE ${whereConditions.join(" AND ")}
      ORDER BY ${orderBy}
      LIMIT ${limit}
      OFFSET ${offset}
    `

    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\` AS a
      WHERE ${whereConditions.join(" AND ")}
    `

    const [articlesResult, countResult] = await Promise.all([
      bigQueryService.executeQuery(articlesQuery, queryParams),
      bigQueryService.executeQuery(countQuery, queryParams),
    ])

    const articles = articlesResult.map((row) => ({
      id: row.id,
      title: row.title,
      link: row.link,
      koza_id: row.koza_id,
      koza_name: row.koza_name,
      koza_slug: row.koza_slug,
      pageviews: Number(row.pageviews) || 0,
      engaged_sessions: Number(row.engaged_sessions) || 0,
      avg_engagement_time: Number(row.avg_engagement_time) || 0,
      organic_sessions: Number(row.organic_sessions) || 0,
      search_keywords: row.search_keywords || [],
      created_at: row.created_at,
      updated_at: row.updated_at,
      last_synced: row.last_synced,
      has_embedding: row.has_embedding,
    }))

    const totalCount = countResult[0]?.total_count || 0
    const totalPages = Math.ceil(totalCount / limit)
    const appliedFilters = {
      search,
      koza_id,
      min_pageviews,
      max_pageviews,
      content_type,
      sort,
    }
    const result = {
      articles,
      pagination: {
        current_page: Number(page),
        per_page: Number(limit),
        total_count: Number(totalCount),
        total_pages: totalPages,
        has_next: page < totalPages,
        has_prev: page > 1,
      },
      filters: appliedFilters,
    }

    cacheService.set(cacheKey, result, 300)
    logger.info("記事検索完了", { total_count: totalCount, returned_count: articles.length })
    res.json(formatResponse(result, `Found ${totalCount} articles`))
  } catch (error) {
    logger.error("記事検索でエラー", { error: error.message, stack: error.stack })
    res.status(500).json(formatError("Failed to search articles", error.message))
  }
})


/**
 * GET /api/search/articles/:id
 * 特定記事の詳細取得（最適化版）
 */
router.get("/articles/:id", async (req, res) => {
  try {
    const { id } = req.params
    if (!id) {
      return res.status(400).json(formatError("Article ID is required"))
    }

    const cacheKey = `article_detail_${id}`
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      logger.info("記事詳細をキャッシュから返却", { articleId: id })
      return res.json(formatResponse(cachedResult, "Article retrieved from cache"))
    }

    logger.info("記事詳細取得開始", { articleId: id })

    const query = `
      SELECT 
        a.id,
        a.title,
        a.link,
        a.koza_id,
        c.name as koza_name,
        c.slug as koza_slug,
        a.full_content,
        a.full_content_html,
        a.pageviews,
        a.engaged_sessions,
        a.created_at,
        a.updated_at
      FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\` AS a
      LEFT JOIN \`${process.env.GCP_PROJECT}.content_analysis.courses\` AS c ON CAST(a.koza_id AS STRING) = CAST(c.id AS STRING)
      WHERE a.id = ?
      LIMIT 1
    `

    const results = await bigQueryService.executeQuery(query, [id])

    if (results.length === 0) {
      return res.status(404).json(formatError("Article not found", `No article found with ID: ${id}`))
    }

    const article = results[0]
    
    // HTMLタグを除去する関数（表示用にクリーンなテキストを生成）
    const stripHtmlTags = (html) => {
      if (!html) return '';
      
      return html
        // カスタムタグを除去（<graybox>, <bluebox>, <redbox>など）
        .replace(/<\/?(?:gray|blue|red|yellow|green|orange|purple|pink|white|black)box[^>]*>/gi, '')
        // その他の全HTMLタグを除去
        .replace(/<[^>]+>/g, '')
        // HTMLエンティティをデコード
        .replace(/&nbsp;/g, ' ')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&#x27;/g, "'")
        .replace(/&apos;/g, "'")
        // 連続する空白を1つに
        .replace(/\s+/g, ' ')
        // 前後の空白を削除
        .trim();
    };
    
    // full_content_htmlを使用（HTMLタグは残したまま）
    // チャンク分割にはHTMLタグが必要なため、ここでは除去しない
    const rawContent = article.full_content_html || '';
    
    // 表示用にクリーンなテキストを生成
    const cleanContent = stripHtmlTags(rawContent);
    
    // full_content_htmlがない場合は警告
    if (!article.full_content_html) {
      logger.warn(`Article ${article.id} has no full_content_html, using empty content`)
    }
    
    const articleDetail = {
      id: article.id,
      title: article.title,
      link: article.link,
      koza_id: article.koza_id,
      koza_name: article.koza_name,
      koza_slug: article.koza_slug,
      full_content: cleanContent, // 表示用: HTMLタグ除去済み
      full_content_html: rawContent, // チャンク分割用: HTMLタグ保持
      pageviews: Number(article.pageviews) || 0,
      engaged_sessions: Number(article.engaged_sessions) || 0,
      created_at: article.created_at,
      updated_at: article.updated_at,
    }

    cacheService.set(cacheKey, articleDetail, 600)
    logger.info("記事詳細取得完了", { articleId: id })
    res.json(formatResponse(articleDetail, "Article retrieved successfully"))
  } catch (error) {
    logger.error("記事詳細取得でエラー", { error: error.message, stack: error.stack, articleId: req.params.id })
    res.status(500).json(formatError("Failed to get article details", error.message))
  }
})

/**
 * GET /api/search/courses
 * 講座一覧取得
 */
router.get("/courses", async (req, res) => {
  try {
    const cacheKey = "courses_list"

    // キャッシュから取得試行
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      logger.info("講座一覧をキャッシュから返却")
      return res.json(formatResponse(cachedResult, "Courses retrieved from cache"))
    }

    logger.info("講座一覧取得開始")

    const query = `
      SELECT 
        id,
        name,
        slug 
      FROM \`${process.env.GCP_PROJECT}.content_analysis.courses\`
      ORDER BY name ASC
      LIMIT 200
    `

    const results = await bigQueryService.executeQuery(query)
    
    const courses = results.map((row) => ({
      id: row.id,
      name: row.name,
      slug: row.slug,
    }))

    // 結果をキャッシュ（30分間）
    cacheService.set(cacheKey, courses, 1800)

    logger.info("講座一覧取得完了", { count: courses.length })

    res.json(formatResponse(courses, `Found ${courses.length} courses`))
  } catch (error) {
    logger.error("講座一覧取得でエラー", { error: error.message, stack: error.stack })

    // タイムアウトまたはエラーの場合はダミーデータを返す
    const dummyCourses = [
      {
        id: "sample-course-1",
        slug: "sample-course-1",
        name: "サンプル講座1",
        description: "サンプル講座の説明",
        total_articles: 10,
        total_pageviews: 5000,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
      {
        id: "sample-course-2",
        slug: "sample-course-2",
        name: "サンプル講座2",
        description: "サンプル講座の説明",
        total_articles: 8,
        total_pageviews: 3000,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    ]

    res.json(formatResponse(dummyCourses, "Query timeout - returning sample data"))
  }
})

/**
 * GET /api/search/stats
 * 検索統計情報取得
 */
router.get("/stats", async (req, res) => {
  try {
    const cacheKey = "search_stats"

    // キャッシュから取得試行
    const cachedResult = cacheService.get(cacheKey)
    if (cachedResult) {
      return res.json(formatResponse(cachedResult, "Stats retrieved from cache"))
    }

    logger.info("検索統計情報取得開始")

    const query = `
      SELECT 
        COUNT(*) as total_articles,
        COUNT(DISTINCT koza_id) as total_courses,
        SUM(pageviews) as total_pageviews,
        AVG(pageviews) as avg_pageviews,
        MAX(pageviews) as max_pageviews,
        MIN(pageviews) as min_pageviews,
        MAX(engaged_sessions) as max_engaged_sessions -- この行を追加
      FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\`
    `

    // タイムアウト付きで実行
    const queryPromise = bigQueryService.executeQuery(query)
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error("Query timeout")), 15000) // 15秒でタイムアウト
    })

    const results = await Promise.race([queryPromise, timeoutPromise])
    const stats = results[0]

    const statsData = {
      total_articles: Number.parseInt(stats.total_articles) || 0,
      total_courses: Number.parseInt(stats.total_courses) || 0,
      total_pageviews: Number.parseInt(stats.total_pageviews) || 0,
      avg_pageviews: Math.round(Number.parseFloat(stats.avg_pageviews) || 0),
      max_pageviews: Number.parseInt(stats.max_pageviews) || 0,
      min_pageviews: Number.parseInt(stats.min_pageviews) || 0,
      max_engaged_sessions: Number.parseInt(stats.max_engaged_sessions) || 0, // この行を追加
      articles_with_embeddings: 0,
      content_types: 1,
      embedding_coverage: 0,
    }

    // 結果をキャッシュ（15分間）
    cacheService.set(cacheKey, statsData, 900)

    logger.info("検索統計情報取得完了", statsData)

    res.json(formatResponse(statsData, "Search statistics retrieved successfully"))
  } catch (error) {
    logger.error("検索統計情報取得でエラー", { error: error.message, stack: error.stack })

    // タイムアウトまたはエラーの場合はダミーデータを返す
    const dummyStats = {
      total_articles: 100,
      total_courses: 10,
      total_pageviews: 50000,
      avg_pageviews: 500,
      max_pageviews: 5000,
      min_pageviews: 10,
      articles_with_embeddings: 0,
      content_types: 1,
      embedding_coverage: 0,
    }

    res.json(formatResponse(dummyStats, "Query timeout - returning sample data"))
  }
})

module.exports = router
