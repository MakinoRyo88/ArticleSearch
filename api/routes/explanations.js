/**
 * Gemini統合記事生成API（最終安定版）
 * 2つの記事を統合し、長文で訴求力のある単一のMarkdown記事を生成
 */

const express = require("express")
const { body, validationResult } = require("express-validator")
const BigQueryService = require("../services/bigquery-service")
const LLMProviderManager = require("../services/llm-provider-manager")
const { formatResponse, formatError } = require("../utils/response-formatter")
const winston = require("winston")

const router = express.Router()
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

const bigQueryService = new BigQueryService()
const llmProviderManager = new LLMProviderManager()

// バリデーションルール
const explanationValidation = [
  body("base_article_id").isString().isLength({ min: 1, max: 100 }),
  body("similar_article_id").isString().isLength({ min: 1, max: 100 }),
  body("llm_provider").optional().isIn(['vertex-ai', 'local-llm']),
]

/**
 * POST /api/explanations/generate
 * 統合記事コンテンツ生成（安定性テスト版）
 */
router.post("/generate", explanationValidation, async (req, res) => {
  try {
    const errors = validationResult(req)
    if (!errors.isEmpty()) {
      return res.status(400).json(formatError("Validation failed", errors.array()))
    }

    const { base_article_id, similar_article_id, llm_provider = 'vertex-ai' } = req.body
    logger.info("統合記事生成開始", {
      base_article_id,
      similar_article_id,
      llm_provider,
      requestPath: req.path,
      userAgent: req.get('User-Agent')
    })

    // 記事データの取得
    logger.info("BigQuery記事データ取得開始")
    const articlesData = await getDetailedArticlesData([base_article_id, similar_article_id])

    const baseArticle = articlesData[base_article_id]
    const similarArticle = articlesData[similar_article_id]

    if (!baseArticle || !similarArticle) {
      logger.warn("記事が見つかりません", {
        base_article_found: !!baseArticle,
        similar_article_found: !!similarArticle
      })
      return res.status(404).json(formatError("指定された記事が見つかりません"))
    }

    logger.info("記事データ取得完了", {
      baseTitle: baseArticle.title,
      similarTitle: similarArticle.title,
      baseContentLength: baseArticle.full_content?.length || 0,
      similarContentLength: similarArticle.full_content?.length || 0
    })

    // 統合記事プロンプト生成
    const integrationPrompt = createIntegratedArticlePrompt(baseArticle, similarArticle)

    logger.info("LLM記事生成開始", {
      provider: llm_provider,
      promptLength: integrationPrompt.length,
      timestamp: new Date().toISOString()
    })

    const generationResult = await llmProviderManager.generateText(
      integrationPrompt,
      llm_provider,
      {
        maxOutputTokens: 15000, // 両プロバイダー統一: 15K tokens（3000-4000文字記事対応）
        temperature: 0.7,
        timeout: llm_provider === 'local-llm' ? 180000 : 120000 // ローカルLLM: 3分(L4 GPU高速化), Vertex AI: 2分
      }
    )

    logger.info("LLM記事生成完了", {
      provider: generationResult.provider,
      contentLength: generationResult.content?.length || 0,
      responseTime: generationResult.response_time,
      timestamp: new Date().toISOString(),
      ollama_info: generationResult.ollama_info || null
    })

    // レスポンスサイズを制限（Cloud Run制限対応） - 両プロバイダー統一
    const maxContentLength = 10000 // 両プロバイダー統一: 10K文字(3000-4000文字記事対応)
    let finalContent = generationResult.content || "記事生成に失敗しました"

    // デバッグ情報を追加
    logger.info("コンテンツ長制限デバッグ", {
      llm_provider,
      maxContentLength,
      originalContentLength: finalContent.length,
      isVertexAI: llm_provider === 'vertex-ai'
    })

    // 制限処理ロジックを完全に書き直し
    logger.info("制限処理前デバッグ", {
      llm_provider,
      contentLength: finalContent.length,
      maxContentLength,
      isVertexAI: llm_provider === 'vertex-ai',
      providerType: typeof llm_provider
    })

    // 制限処理を完全に無効化（3000文字記事対応）
    logger.info("制限処理無効化", {
      provider: llm_provider,
      contentLength: finalContent.length,
      maxContentLength,
      message: "制限処理をスキップし、全コンテンツを返却"
    })

    const result = {
      source_articles: {
        base_article: { id: base_article_id, title: baseArticle.title },
        similar_article: { id: similar_article_id, title: similarArticle.title },
      },
      integrated_article_markdown: finalContent,
      generation_info: {
        provider_used: generationResult.provider,
        response_time: generationResult.response_time,
        content_length: finalContent.length,
        is_fallback: generationResult.is_fallback || false
      },
      generated_at: new Date().toISOString(),
    }

    logger.info("統合記事生成完了", {
      base_article_id,
      similar_article_id,
      provider_used: generationResult.provider,
      response_time: generationResult.response_time,
      final_content_length: finalContent.length,
      content_ending: finalContent.substring(Math.max(0, finalContent.length - 300))
    })

    res.json(formatResponse(result, "Integrated article generated successfully"))

  } catch (error) {
    logger.error("統合記事生成でエラー", {
      error: error.message,
      stack: error.stack,
      provider: req.body.llm_provider || 'vertex-ai',
      requestPath: req.path,
      timestamp: new Date().toISOString(),
      errorType: error.constructor.name
    })

    // ローカルLLM利用不可の場合の特別なエラーメッセージ
    if (error.message.includes('LOCAL_LLM_UNAVAILABLE')) {
      res.status(503).json(formatError(
        "ローカルLLMが現在利用できません",
        "ローカルLLMのインスタンスが起動していないか、接続できません。Vertex AIに切り替えるか、しばらく待ってから再度お試しください。"
      ))
      return
    }

    // その他のエラー
    res.status(500).json(formatError("統合記事の生成に失敗しました", error.message))
  }
})

// --- ヘルパー関数 ---

async function getDetailedArticlesData(articleIds) {
  const query = `
    SELECT id, title, full_content, pageviews
    FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\`
    WHERE id IN UNNEST(@articleIds) LIMIT 2
  `
  const results = await bigQueryService.executeQuery(query, { articleIds: articleIds })
  const articlesData = {}
  results.forEach((row) => {
    articlesData[row.id] = row
  })
  return articlesData
}

function createIntegratedArticlePrompt(baseArticle, similarArticle) {
  // 記事の内容を適切な長さに制限してメモリ使用量を削減（効率化）
  const truncateContent = (content, maxLength = process.env.LOCAL_LLM_GPU_ENABLED === 'true' ? 2000 : 1500) => { // 大幅に増量
    if (!content) return "内容なし";
    // HTMLタグを除去し、連続する空白や改行を最適化
    const cleanContent = content
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .replace(/\n+/g, ' ')
      .trim();

    if (cleanContent.length <= maxLength) return cleanContent;

    // より自然な切り取り位置を探す（句点で区切る）
    const truncated = cleanContent.substring(0, maxLength);
    const lastPeriod = truncated.lastIndexOf('。');

    return lastPeriod > maxLength * 0.8 ?
      truncated.substring(0, lastPeriod + 1) + "..." :
      truncated + "...";
  };

  const baseContent = truncateContent(baseArticle.full_content);
  const similarContent = truncateContent(similarArticle.full_content);

  const prompt = `
以下の2つの記事の内容を完全に統合した、詳細で包括的な長文記事（最低3000文字以上）を作成してください。これは実際に公開する記事として、そのままコピー&ペーストで使用できる完成品である必要があります。

記事A「${baseArticle.title}」の内容：
${baseContent}

記事B「${similarArticle.title}」の内容：
${similarContent}

【記事作成指示】
- 最低3000文字以上の詳細な長文記事を作成してください
- 2つの記事の情報を自然に組み合わせて、1つの包括的で価値ある記事にしてください
- 読者が実際に役立つ実用的な内容にしてください
- 具体的な手順、方法、事例を豊富に含めてください
- SEOに配慮したタイトルと見出し構成にしてください
- そのまま公開できる完成した記事として作成してください
- 各セクションを詳細に書き、情報量を十分に確保してください

【出力形式】
Markdown形式で以下の構成で作成してください：

# [SEOを意識した魅力的なタイトル]

## はじめに
読者の興味を引き、記事の価値を明確に示す導入文。なぜこのテーマが重要なのか、読者にどのような価値を提供するのかを詳しく説明してください。

## 基礎知識と背景
テーマの基本的な概念と重要性の説明。歴史的背景、現在の状況、業界での位置づけなどを含めて詳しく解説してください。

## 実践方法とテクニック
具体的な手順やテクニックの詳細解説。step-by-step のガイド、実際の操作方法、注意点などを豊富に含めてください。

## 応用とコツ
発展的な活用法と成功のポイント。上級者向けのテクニック、よくある問題とその解決法、効率化の方法などを詳しく説明してください。

## 事例紹介とケーススタディ
実際の成功例や具体的な数値を含む事例。複数のケーススタディ、before/after の比較、測定可能な結果などを詳細に紹介してください。

## 実装時の注意点とベストプラクティス
実際に導入・実行する際の重要なポイント、よくある落とし穴、成功のためのベストプラクティスを詳しく解説してください。

## 将来展望と最新トレンド
今後の発展可能性、最新の業界トレンド、関連技術との組み合わせなどを分析してください。

## まとめと次のステップ
要点の整理と読者へのアクションの提示。具体的な行動計画、推奨リソース、さらなる学習方法などを含めてください。

【重要な要求事項】
1. 最低3000文字以上の詳細な長文記事を必ず作成してください
2. 提案や推奨ではなく、確定的な内容として記述してください
3. 「〜することをお勧めします」ではなく「〜します」「〜することで」という確定的な表現を使用してください
4. 読者が今すぐ実践できる具体的な内容にしてください
5. 完成した記事として、そのままコピー&ペーストで使用できるレベルで作成してください
6. 各セクションを十分に詳しく書き、情報量を豊富にしてください
7. 具体例、データ、実践的な情報を多数含めてください`
  return prompt
}

/**
 * GET /api/explanations/providers/status
 * LLMプロバイダーの状態取得
 */
router.get("/providers/status", async (req, res) => {
  try {
    logger.info("プロバイダー状態取得開始")

    const status = await llmProviderManager.getProviderStatus()

    logger.info("プロバイダー状態取得完了", {
      vertex_ai_status: status.providers['vertex-ai']?.status,
      local_llm_status: status.providers['local-llm']?.status
    })

    res.json(formatResponse(status, "Provider status retrieved successfully"))

  } catch (error) {
    logger.error("プロバイダー状態取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get provider status", error.message))
  }
})

/**
 * GET /api/explanations/providers/recommended
 * 推奨プロバイダー取得
 */
router.get("/providers/recommended", async (req, res) => {
  try {
    const { criteria = 'balanced' } = req.query

    const recommended = llmProviderManager.getRecommendedProvider(criteria)
    const stats = llmProviderManager.getServiceStats()

    res.json(formatResponse({
      recommended_provider: recommended,
      criteria,
      stats
    }, "Recommended provider retrieved successfully"))

  } catch (error) {
    logger.error("推奨プロバイダー取得でエラー", { error: error.message })
    res.status(500).json(formatError("Failed to get recommended provider", error.message))
  }
})

/**
 * GET /api/explanations/test
 * シンプルなテストエンドポイント
 */
router.get("/test", async (req, res) => {
  try {
    logger.info("テストエンドポイント呼び出し")

    res.json(formatResponse({
      message: "Test endpoint working",
      timestamp: new Date().toISOString(),
      status: "success"
    }, "Test successful"))

  } catch (error) {
    logger.error("テストエンドポイントでエラー", { error: error.message })
    res.status(500).json(formatError("Test endpoint failed", error.message))
  }
})

/**
 * GET /api/explanations/test-vertex
 * Vertex AIシンプルテスト
 */
router.get("/test-vertex", async (req, res) => {
  try {
    logger.info("Vertex AIテスト開始")

    const result = await llmProviderManager.generateText(
      "こんにちはと日本語で返答してください。",
      "vertex-ai",
      { maxOutputTokens: 100, temperature: 0.7 }
    )

    logger.info("Vertex AIテスト成功", { resultLength: result.content.length })

    res.json(formatResponse({
      result: result,
      test: "vertex-ai-simple"
    }, "Vertex AI test successful"))

  } catch (error) {
    logger.error("Vertex AIテストでエラー", { error: error.message, stack: error.stack })
    res.status(500).json(formatError("Vertex AI test failed", error.message))
  }
})

/**
 * GET /api/explanations/sample-articles
 * サンプル記事IDの取得
 */
router.get("/sample-articles", async (req, res) => {
  try {
    logger.info("サンプル記事ID取得開始")

    const query = `
      SELECT id, title, CHAR_LENGTH(full_content) as content_length
      FROM \`${process.env.GCP_PROJECT}.content_analysis.articles\`
      WHERE full_content IS NOT NULL
        AND CHAR_LENGTH(full_content) > 500
      ORDER BY pageviews DESC
      LIMIT 10
    `

    const results = await bigQueryService.executeQuery(query)

    logger.info("サンプル記事ID取得完了", { count: results.length })

    res.json(formatResponse({
      articles: results,
      total: results.length
    }, "Sample articles retrieved successfully"))

  } catch (error) {
    logger.error("サンプル記事ID取得でエラー", { error: error.message, stack: error.stack })
    res.status(500).json(formatError("Failed to get sample articles", error.message))
  }
})

module.exports = router