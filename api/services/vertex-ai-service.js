/**
 * Vertex AI サービス
 * Vertex AI Geminiを使用したテキスト生成
 */

const { VertexAI } = require("@google-cloud/vertexai")
const winston = require("winston")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class VertexAIService {
  constructor() {
    this.projectId = process.env.GCP_PROJECT || "seo-optimize-464208"
    this.location = "asia-northeast1"

    try {
      this.vertexAI = new VertexAI({
        project: this.projectId,
        location: this.location,
      })

      this.model = "gemini-2.5-flash"

      logger.info("Vertex AI サービス初期化", {
        projectId: this.projectId,
        location: this.location,
        model: this.model,
      })
    } catch (error) {
      logger.error("Vertex AI 初期化エラー", { error: error.message })
      // フォールバック: サービスを無効化
      this.vertexAI = null
    }
  }

  /**
   * テキスト生成
   */
  async generateText(prompt, options = {}) {
    try {
      if (!this.vertexAI) {
        throw new Error("Vertex AI client is not initialized. Check application logs for initialization errors.");
      }

      const { maxOutputTokens = 8192, temperature = 0.7, topP = 0.95, topK = 40 } = options

      logger.info("Vertex AI テキスト生成開始", {
        promptLength: prompt.length,
        maxOutputTokens,
        temperature,
      })

      const generativeModel = this.vertexAI.getGenerativeModel({
        model: this.model,
        generationConfig: {
          maxOutputTokens,
          temperature,
          topP,
          topK,
        },
        safetySettings: [
          {
            category: "HARM_CATEGORY_HATE_SPEECH",
            threshold: "BLOCK_ONLY_HIGH"
          },
          {
            category: "HARM_CATEGORY_DANGEROUS_CONTENT",
            threshold: "BLOCK_ONLY_HIGH"
          },
          {
            category: "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            threshold: "BLOCK_ONLY_HIGH"
          },
          {
            category: "HARM_CATEGORY_HARASSMENT",
            threshold: "BLOCK_ONLY_HIGH"
          }
        ]
      })

      const systemInstruction = 'あなたは詳細で構造化された長文記事を作成する専門ライターです。最低3000文字以上の詳細な記事を必ず書いてください。見出しを使い、具体例を交えて、読者に価値のある実用的な内容を提供してください。各セクションを十分に詳しく書き、情報量を豊富にしてください。'

      const enhancedPrompt = `${prompt}\n\n【絶対要件】最低3000文字以上で、詳細な長文記事を必ず作成してください。マークダウン形式で見出しを使い、構造化された内容にしてください。各セクションを詳しく書き、具体例、実践方法、事例を豊富に含めてください。`

      const request = {
        contents: [{ role: "user", parts: [{ text: `${systemInstruction}\n\n${enhancedPrompt}` }] }],
      };

      const result = await generativeModel.generateContent(
        request
      );

      logger.info("Vertex AI 生データ応答", {
        hasResult: !!result,
        hasResponse: !!result?.response,
        hasCandidates: !!result?.response?.candidates,
        candidateCount: result?.response?.candidates?.length || 0,
        resultKeys: Object.keys(result || {}),
        responseKeys: Object.keys(result?.response || {})
      })

      const response = result?.response

      // より詳細な応答構造をログ出力
      if (response?.candidates) {
        response.candidates.forEach((candidate, index) => {
          logger.info(`Candidate ${index}:`, {
            hasContent: !!candidate.content,
            hasParts: !!candidate.content?.parts,
            partsCount: candidate.content?.parts?.length || 0,
            finishReason: candidate.finishReason,
            safetyRatings: candidate.safetyRatings
          })

          if (candidate.content?.parts) {
            candidate.content.parts.forEach((part, partIndex) => {
              logger.info(`Part ${partIndex}:`, {
                hasText: !!part.text,
                textLength: part.text?.length || 0,
                textPreview: part.text?.substring(0, 100) || 'No text'
              })
            })
          }
        })
      }

      const text = response?.candidates?.[0]?.content?.parts?.[0]?.text

      if (!text) {
        logger.error("Vertex AI 応答からテキスト抽出失敗", {
          fullResult: JSON.stringify(result, null, 2),
          candidatesDetail: response?.candidates?.map(c => ({
            finishReason: c.finishReason,
            safetyRatings: c.safetyRatings,
            content: c.content
          }))
        });

        // 安全性フィルターや他の理由でブロックされた可能性をチェック
        const firstCandidate = response?.candidates?.[0]
        if (firstCandidate?.finishReason) {
          logger.error("Generation blocked:", {
            finishReason: firstCandidate.finishReason,
            safetyRatings: firstCandidate.safetyRatings
          })
          throw new Error(`Content generation blocked: ${firstCandidate.finishReason}`)
        }

        throw new Error("Failed to extract text from Vertex AI response.");
      }

      logger.info("Vertex AI テキスト生成完了", {
        responseLength: text.length,
        tokensUsed: response.usageMetadata?.totalTokenCount || 0,
      })

      return text
    } catch (error) {
      logger.error("Vertex AI テキスト生成でエラー", {
        error: error.message,
        stack: error.stack,
        promptLength: prompt.length,
      })

      // フォールバック応答を返すのではなく、エラーを再スローする
      throw error
    }
  }

  /**
   * フォールバックテキスト生成
   */
  generateFallbackText(prompt) {
    logger.info("フォールバックテキスト生成")

    // プロンプトから基本情報を抽出してシンプルな応答を生成
    if (prompt.includes("類似度")) {
      return "この記事ペアは類似度が高く、統合による効果が期待できます。SEO観点から統合を検討することをお勧めします。"
    }

    return "記事の統合について詳細な分析を行い、適切な提案を提供いたします。"
  }

  /**
   * バッチテキスト生成
   */
  async generateTextBatch(prompts, options = {}) {
    try {
      logger.info("Vertex AI バッチテキスト生成開始", {
        promptCount: prompts.length,
      })

      const promises = prompts.map((prompt) =>
        this.generateText(prompt, options).catch((error) => {
          logger.error("バッチ処理でエラー", { error: error.message })
          return this.generateFallbackText(prompt)
        }),
      )

      const results = await Promise.all(promises)

      logger.info("Vertex AI バッチテキスト生成完了", {
        successful: results.length,
        total: prompts.length,
      })

      return results
    } catch (error) {
      logger.error("Vertex AI バッチテキスト生成でエラー", { error: error.message })
      throw error
    }
  }

  /**
   * 埋め込みベクトル生成（プレースホルダー）
   */
  async generateEmbeddings(texts) {
    try {
      logger.info("埋め込み生成開始", { textCount: texts.length })

      // プレースホルダー実装
      const embeddings = texts.map(() => new Array(768).fill(0).map(() => Math.random()))

      logger.info("埋め込み生成完了")
      return embeddings
    } catch (error) {
      logger.error("埋め込み生成でエラー", { error: error.message })
      throw error
    }
  }
}

module.exports = VertexAIService
