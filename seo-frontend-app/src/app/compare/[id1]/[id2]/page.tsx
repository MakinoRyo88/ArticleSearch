"use client"

import React, { useState, useEffect, useCallback, useRef } from "react"
import { useParams, useRouter } from "next/navigation"
import { ArrowLeft, Bot, ExternalLink, Sparkles, BarChart2, Key, FileText } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts"
import ReactMarkdown from 'react-markdown';
import { LLMProviderSelector } from "@/components/ui/llm-provider-selector";
import { getArticleUrl, getExplanationsGenerateUrl } from "@/lib/api-config";

// --- Utility Functions ---
const stripHtmlTags = (html: string): string => {
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
}

// --- Type Definitions ---
interface ArticleDetail {
  id: string
  title: string
  link: string
  koza_id: string
  koza_name: string | null
  koza_slug: string | null
  full_content: string
  pageviews: number
  engaged_sessions: number
  search_keywords?: string[]
  updated_at: string
}

interface GeminiSuggestion {
  markdownContent: string;
  generationInfo?: {
    provider_used: string;
    cost_estimate: number;
    response_time: number;
    is_fallback?: boolean;
  };
}

// --- Main Component ---
export default function ArticleComparePage() {
  const router = useRouter()
  const params = useParams()
  const { id1, id2 } = params

  const [article1, setArticle1] = useState<ArticleDetail | null>(null)
  const [article2, setArticle2] = useState<ArticleDetail | null>(null)
  const [geminiSuggestion, setGeminiSuggestion] = useState<GeminiSuggestion | null>(null)

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [isGenerating, setIsGenerating] = useState(false)
  const [selectedLLMProvider, setSelectedLLMProvider] = useState<'vertex-ai' | 'local-llm'>('vertex-ai')


  // --- Data Fetching Logic ---
  const fetchArticleDetails = useCallback(async () => {
    if (!id1 || !id2) return
    setLoading(true)
    setError(null)
    try {
      const [res1, res2] = await Promise.all([
        fetch(getArticleUrl(id1 as string)),
        fetch(getArticleUrl(id2 as string)),
      ])
      const data1 = await res1.json()
      const data2 = await res2.json()
      if (!data1.success || !data2.success) throw new Error("記事データの取得に失敗しました。")
      setArticle1(data1.data)
      setArticle2(data2.data)
    } catch (err: any) {
      setError(err.message || "不明なエラーが発生しました。")
    } finally {
      setLoading(false)
    }
  }, [id1, id2])

  useEffect(() => {
    fetchArticleDetails()
  }, [fetchArticleDetails])
  

  // --- AI Suggestion Generation ---
  const handleGenerateSuggestion = async () => {
    if (!article1 || !article2) return;
    setIsGenerating(true);
    setError(null);
    setGeminiSuggestion(null);

    try {
      const response = await fetch(getExplanationsGenerateUrl(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          base_article_id: article1.id,
          similar_article_id: article2.id,
          llm_provider: selectedLLMProvider
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error?.message || `APIエラー: ${response.status}`);
      }

      const responseData = await response.json();

      if (responseData.success && typeof responseData.data.integrated_article_markdown === 'string') {
        const markdownContent = responseData.data.integrated_article_markdown;
        console.log('Received markdown content length:', markdownContent.length);
        console.log('Markdown content preview:', markdownContent.substring(0, 200) + '...');
        console.log('Markdown content ending:', markdownContent.substring(markdownContent.length - 200));

        setGeminiSuggestion({
          markdownContent,
          generationInfo: responseData.data.generation_info
        });
      } else {
        throw new Error("APIからの応答データ構造が正しくありません。");
      }
    } catch (err: any) {
      // ローカルLLM関連のエラーの場合、より具体的なエラーメッセージを表示
      if (err.message.includes('ローカルLLMが現在利用できません')) {
        if (err.message.includes('外部IPアドレスの取得に失敗')) {
          setError(`ローカルLLMエラー: インスタンスは稼働中ですが、ネットワーク接続に問題があります。プロバイダー選択画面で「再起動」ボタンを押してください。`);
        } else if (err.message.includes('再起動が必要です')) {
          setError(`ローカルLLMエラー: インスタンスにネットワーク問題があります。プロバイダー選択画面で「再起動」ボタンを押すか、Vertex AIを使用してください。`);
        } else {
          setError(`ローカルLLMエラー: ${err.message} 画面上部のプロバイダー選択でVertex AIに切り替えることをお勧めします。`);
        }
      } else if (err.message.includes('timeout') || err.message.includes('タイムアウト')) {
        setError(`処理タイムアウト: ローカルLLMの応答に時間がかかりすぎています。Vertex AIプロバイダーを使用することをお勧めします。`);
      } else {
        setError(`AI提案の生成中にエラーが発生しました: ${err.message}`);
      }
    } finally {
      setIsGenerating(false);
    }
  };

  // --- Helper Function ---
  const getFullArticleUrl = (article: { link: string; koza_slug: string | null }) => {
    if (!article.koza_slug) {
      return article.link.startsWith("http") ? article.link : `https://www.foresight.jp/column/${article.link}`
    }
    const formattedLink = article.link.startsWith("/") ? article.link.substring(1) : article.link
    return `https://www.foresight.jp/${article.koza_slug}/column/${formattedLink}`
  }

  // --- Render Logic ---
  if (loading) return <div className="flex h-screen items-center justify-center">読み込み中...</div>
  if (error) return <div className="flex h-screen items-center justify-center text-red-500">エラー: {error}</div>
  if (!article1 || !article2) return <div className="flex h-screen items-center justify-center">記事データが見つかりません。</div>

  const keywords1 = article1.search_keywords || [];
  const keywords2 = article2.search_keywords || [];
  const commonKeywords = keywords1.filter(kw => keywords2.includes(kw));
  const uniqueKeywords1 = keywords1.filter(kw => !keywords2.includes(kw));
  const uniqueKeywords2 = keywords2.filter(kw => !keywords1.includes(kw));

  const chartData = [
    { name: 'PV数', [article1.title]: article1.pageviews, [article2.title]: article2.pageviews },
    { name: 'エンゲージメント', [article1.title]: article1.engaged_sessions, [article2.title]: article2.engaged_sessions },
  ];

  return (
    <div className="flex min-h-screen w-full flex-col bg-slate-50">
      <main className="flex-1 p-4 md:p-6 lg:p-8">
        <div className="flex items-center gap-4 mb-6">
          <Button variant="outline" size="icon" className="h-8 w-8" onClick={() => router.back()}>
            <ArrowLeft className="h-4 w-4" />
            <span className="sr-only">戻る</span>
          </Button>
          <h1 className="text-2xl font-bold tracking-tight">記事詳細比較</h1>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ArticleCard article={article1} onScroll={() => {}} />
          <ArticleCard article={article2} onScroll={() => {}} />
        </div>
        
        <Card className="mt-6">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Sparkles className="h-5 w-5 text-amber-500" />分析サマリー</CardTitle>
            <CardDescription>2つの記事のパフォーマンス指標とキーワードを比較します。</CardDescription>
          </CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="font-semibold mb-2 flex items-center gap-2"><BarChart2 className="h-4 w-4" />パフォーマンス比較</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey={article1.title} fill="#3b82f6" />
                  <Bar dataKey={article2.title} fill="#84cc16" />
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div>
              <h3 className="font-semibold mb-2 flex items-center gap-2"><Key className="h-4 w-4" />キーワード分析</h3>
              <div className="space-y-3">
                <div>
                  <h4 className="text-sm font-medium text-slate-600">共通キーワード</h4>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {commonKeywords.length > 0 ? commonKeywords.map(kw => <Badge key={kw} variant="default">{kw}</Badge>) : <span className="text-sm text-slate-500">なし</span>}
                  </div>
                </div>
                <Separator />
                <div>
                  <h4 className="text-sm font-medium text-slate-600">{`「${article1.title.substring(0,15)}...」の独自キーワード`}</h4>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {uniqueKeywords1.length > 0 ? uniqueKeywords1.map(kw => <Badge key={kw} variant="outline" className="border-blue-500 text-blue-700">{kw}</Badge>) : <span className="text-sm text-slate-500">なし</span>}
                  </div>
                </div>
                 <Separator />
                <div>
                  <h4 className="text-sm font-medium text-slate-600">{`「${article2.title.substring(0,15)}...」の独自キーワード`}</h4>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {uniqueKeywords2.length > 0 ? uniqueKeywords2.map(kw => <Badge key={kw} variant="outline" className="border-lime-500 text-lime-700">{kw}</Badge>) : <span className="text-sm text-slate-500">なし</span>}
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* LLMプロバイダー選択 */}
        <LLMProviderSelector
          selectedProvider={selectedLLMProvider}
          onProviderChange={setSelectedLLMProvider}
          disabled={isGenerating}
          className="mt-6"
        />

        <Card className="mt-6">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Bot className="h-5 w-5 text-violet-500" />AIによる統合提案</CardTitle>
            <CardDescription>2つの記事の長所を活かし、SEO効果を最大化する統合案をAIが生成します。</CardDescription>
          </CardHeader>
          <CardContent>
  {isGenerating ? (
    <div className="flex items-center justify-center p-8">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-slate-900"></div>
      <div className="ml-4">
        <p>AIが提案を生成中です...</p>
        <p className="text-sm text-gray-600 mt-1">
          {selectedLLMProvider === 'local-llm'
            ? '（ローカルLLMでは最大5分程度かかることがあります）'
            : '（最大2分程度かかることがあります）'}
        </p>
      </div>
    </div>
  ) : geminiSuggestion ? (
    <div>
      {/* 生成情報とコピーボタン */}
      {geminiSuggestion.generationInfo && (
        <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4 text-sm">
              <span className="flex items-center gap-1">
                <strong>プロバイダー:</strong>
                {geminiSuggestion.generationInfo.provider_used === 'vertex-ai' ? 'Vertex AI' : 'ローカルLLM'}
                {geminiSuggestion.generationInfo.is_fallback && (
                  <Badge variant="outline" className="ml-1 text-xs">
                    フォールバック
                  </Badge>
                )}
              </span>
              <span>
                <strong>コスト:</strong> ¥{geminiSuggestion.generationInfo.cost_estimate}
              </span>
              <span>
                <strong>生成時間:</strong> {Math.round(geminiSuggestion.generationInfo.response_time / 1000)}秒
              </span>
            </div>
            <button
              onClick={async (event) => {
                try {
                  await navigator.clipboard.writeText(geminiSuggestion.markdownContent);
                  // フィードバック表示
                  const button = event.currentTarget as HTMLButtonElement;
                  if (button) {
                    const originalText = button.textContent || '記事をコピー';
                    const originalClassName = button.className;
                    button.textContent = 'コピー完了!';
                    button.className = 'px-3 py-1 bg-green-600 text-white rounded-md text-sm font-medium transition-colors';
                    setTimeout(() => {
                      button.textContent = originalText;
                      button.className = originalClassName;
                    }, 2000);
                  }
                } catch (err) {
                  console.error('コピーに失敗しました:', err);
                  alert('コピーに失敗しました');
                }
              }}
              className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-medium transition-colors"
            >
              記事をコピー
            </button>
          </div>
        </div>
      )}

      <div className="bg-slate-50 p-4 sm:p-6 rounded-lg max-w-none">
        <div className="prose prose-slate max-w-none">
          <ReactMarkdown
            skipHtml={false}
            components={{
              h1: ({node, ...props}) => <h1 className="text-2xl sm:text-3xl font-bold mb-4 pb-2 border-b-2 border-slate-300" {...props} />,
              h2: ({node, ...props}) => <h2 className="text-xl sm:text-2xl font-semibold mt-8 mb-4 pb-1 border-b border-slate-200" {...props} />,
              h3: ({node, ...props}) => <h3 className="text-lg sm:text-xl font-semibold mt-6 mb-3" {...props} />,
              p: ({node, ...props}) => <p className="mb-4 leading-relaxed text-slate-700" {...props} />,
              ul: ({node, ...props}) => <ul className="list-disc list-inside mb-4 pl-4 space-y-2" {...props} />,
              ol: ({node, ...props}) => <ol className="list-decimal list-inside mb-4 pl-4 space-y-2" {...props} />,
              li: ({node, ...props}) => <li className="text-slate-700" {...props} />,
              strong: ({node, ...props}) => <strong className="font-bold text-slate-800" {...props} />,
              a: ({node, ...props}) => <a className="text-blue-600 hover:underline" {...props} />,
            }}
          >
            {geminiSuggestion.markdownContent}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  ) : (
      <div className="text-center py-4">
        <Button onClick={handleGenerateSuggestion} disabled={isGenerating}>
        <Sparkles className="mr-2 h-4 w-4" />
        統合提案を生成する
      </Button>
      </div>
  )}
  {geminiSuggestion && !isGenerating && (
      <div className="mt-6 text-center border-t pt-4">
          <Button onClick={handleGenerateSuggestion} disabled={isGenerating}>
              <Sparkles className="mr-2 h-4 w-4" />
              提案を再生成する
          </Button>
      </div>
  )}
</CardContent>
        </Card>
      </main>
    </div>
  )
}

// --- Child Component: Article Display Card ---
interface ArticleCardProps {
  article: ArticleDetail;
  onScroll: () => void;
}

const ArticleCard: React.FC<ArticleCardProps> = ({ article, onScroll }) => {
    const getFullArticleUrl = (article: { link: string; koza_slug: string | null }) => {
      if (!article.koza_slug) {
        return article.link.startsWith("http") ? article.link : `https://www.foresight.jp/column/${article.link}`
      }
      const formattedLink = article.link.startsWith("/") ? article.link.substring(1) : article.link
      return `https://www.foresight.jp/${article.koza_slug}/column/${formattedLink}`
    }

    return (
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle className="text-lg">{article.title}</CardTitle>
          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <Badge variant="secondary">{article.koza_name}</Badge>
            <a
              href={getFullArticleUrl(article)}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1 hover:text-blue-600"
            >
              元の記事 <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        </CardHeader>
        <CardContent>
          <h3 className="font-semibold mb-2 flex items-center gap-2 text-base"><FileText className="h-4 w-4" />記事全文</h3>
          <ScrollArea className="h-96 rounded-md border p-4">
            <div className="prose prose-sm max-w-none whitespace-pre-wrap leading-relaxed">
              {stripHtmlTags(article.full_content)}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>
    )
}
