"use client"

import { useState, useEffect, useMemo, useCallback } from "react"
import { useParams, useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Slider } from "@/components/ui/slider"
import { Checkbox } from "@/components/ui/checkbox"
import { Label } from "@/components/ui/label"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  ExternalLink,
  ArrowLeft,
  Users,
  FileText,
  Lightbulb,
  Eye,
  Search,
  ListFilter,
  ArrowDown,
  ArrowUp,
  SortAsc,
  SortDesc,
  Layers,
  HelpCircle,
} from "lucide-react"
import { searchSimilarArticles, getArticle, getCourses, type Article, type SimilarArticle, type Course } from "@/lib/api"

type SortKey = "similarity_score" | "pageviews" | "recommendation_type" | "matching_chunks"
type SortDirection = "asc" | "desc"

// 推奨タイプの設定（統一されたロジックと整合性を保つ）
const RECOMMENDATION_TYPES = [
  {
    id: "MERGE_CONTENT",
    name: "統合",
    fullName: "コンテンツ統合",
    description: "記事の内容がほぼ同じです。重複を解消するため、2つの記事を1つに統合することを強く推奨します。",
    icon: "🔥",
    variant: "default" as const,
    className: "bg-red-500 hover:bg-red-600 text-white border-red-500",
    priority: 100,
    minSimilarity: 0.92,
    conditions: "類似度95%以上+一致率50%以上 または 類似度92%以上+一致率30%以上+同講座"
  },
  {
    id: "REDIRECT_301",
    name: "リダイレクト",
    fullName: "301リダイレクト",
    description: "記事の内容が非常に似ています。SEO評価を統合するため、アクセス数の少ない記事から多い記事への301リダイレクトを推奨します。",
    icon: "⚡",
    variant: "default" as const,
    className: "bg-orange-500 hover:bg-orange-600 text-white border-orange-500",
    priority: 80,
    minSimilarity: 0.85,
    conditions: "類似度90%以上 または 類似度88%以上+一致率20%以上+同講座 または 類似度85%以上+同講座"
  },
  {
    id: "CROSS_LINK",
    name: "相互リンク",
    fullName: "相互リンク",
    description: "記事同士が関連性を持っています。読者の利便性向上のため、記事間の相互リンクを設置することを推奨します。",
    icon: "🔗",
    variant: "default" as const,
    className: "bg-blue-500 hover:bg-blue-600 text-white border-blue-500",
    priority: 60,
    minSimilarity: 0.70,
    conditions: "類似度75%以上 または 類似度70%以上+一致率15%以上"
  },
  {
    id: "REVIEW",
    name: "レビュー",
    fullName: "レビュー推奨",
    description: "記事の内容に類似点がありますが、差別化の余地があります。内容の見直しや独自性の向上を検討してください。",
    icon: "👁️",
    variant: "default" as const,
    className: "bg-yellow-500 hover:bg-yellow-600 text-white border-yellow-500",
    priority: 50,
    minSimilarity: 0.65,
    conditions: "類似度65%以上+一致率10%以下+同講座 または 類似度65%以上+一致率15%以下+異講座"
  },
  {
    id: "MONITOR",
    name: "監視",
    fullName: "監視のみ",
    description: "軽微な類似性があります。現時点では特別な対応は不要ですが、定期的な確認を継続してください。",
    icon: "📊",
    variant: "secondary" as const,
    className: "bg-gray-500 hover:bg-gray-600 text-white border-gray-500",
    priority: 40,
    minSimilarity: 0.60,
    conditions: "類似度60%以上"
  },
]

export default function ArticleAnalyzePage() {
  const router = useRouter()
  const params = useParams()
  const articleId = params.id as string

  // --- State管理 ---
  // 元データ
  const [baseArticle, setBaseArticle] = useState<Article | null>(null)
  const [allSimilarArticles, setAllSimilarArticles] = useState<SimilarArticle[]>([])
  const [courses, setCourses] = useState<Course[]>([])
  
  // UI状態
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // フィルターとソートの状態
  const [searchTerm, setSearchTerm] = useState("")
  const [debouncedSearchTerm, setDebouncedSearchTerm] = useState("")
  const [similarityRange, setSimilarityRange] = useState<[number, number]>([50, 100])
  const [pageviewsRange, setPageviewsRange] = useState<[number, number]>([0, 1000])
  const [maxPageviews, setMaxPageviews] = useState(1000)
  const [selectedRecommendations, setSelectedRecommendations] = useState<string[]>([])
  const [selectedCourseIds, setSelectedCourseIds] = useState<string[]>([])
  const [sameCourseOnly, setSameCourseOnly] = useState(false)
  const [sortConfig, setSortConfig] = useState<{ key: SortKey; direction: SortDirection }>({
    key: "similarity_score",
    direction: "desc",
  })

  // 検索用デバウンス処理
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearchTerm(searchTerm)
    }, 300)

    return () => clearTimeout(timer)
  }, [searchTerm])

  // 記事データと類似記事データを取得する関数
  const fetchArticleData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      // 基点記事の詳細情報を取得
      const baseArticleRes = await getArticle(articleId)

      if (!baseArticleRes.success || !baseArticleRes.data) {
        throw new Error(baseArticleRes.error?.message || "基点記事の取得に失敗しました。")
      }
      setBaseArticle(baseArticleRes.data)

      // チャンクベース類似記事を取得（新API）
      const similarArticlesRes = await searchSimilarArticles(articleId, {
        limit: 50,
        threshold: 0.5,
        min_pageviews: 0,
        top_chunks: 10
      })

      if (!similarArticlesRes.success || !similarArticlesRes.data) {
        throw new Error(similarArticlesRes.error?.message || "類似記事の取得に失敗しました。")
      }

      // 講座データを取得
      const coursesRes = await getCourses()

      if (!coursesRes.success || !coursesRes.data) {
        console.error(
          "Failed to fetch courses, proceeding without slugs for links:",
          coursesRes.error || "Unknown error",
        )
      } else {
        setCourses(coursesRes.data)
      }

      // 類似記事データにkoza_slugとkoza_nameを付与
      const fetchedSimilarArticles = similarArticlesRes.data.similar_articles.map((sa) => {
        const correspondingCourse = coursesRes.data?.find((c) => c.id === sa.koza_id)
        return {
          ...sa,
          koza_slug: correspondingCourse ? correspondingCourse.slug : null,
          koza_name: correspondingCourse ? correspondingCourse.name : null,
        }
      })
      setAllSimilarArticles(fetchedSimilarArticles)
      
      // PV数の最大値を計算してスライダーの上限を設定
      if (fetchedSimilarArticles.length > 0) {
        const maxPv = Math.max(...fetchedSimilarArticles.map((a) => a.pageviews), 0)
        setMaxPageviews(maxPv > 0 ? maxPv : 1000)
        setPageviewsRange([0, maxPv > 0 ? maxPv : 1000])
      }
    } catch (err: any) {
      setError(err.message || "データの読み込み中にエラーが発生しました。")
    } finally {
      setLoading(false)
    }
  }, [articleId])

  useEffect(() => {
    if (articleId) {
      fetchArticleData()
    }
  }, [articleId, fetchArticleData])

  // --- 表示用データの動的生成 (フィルタリング & ソート) ---
  const displayedArticles = useMemo(() => {
    let filtered = [...allSimilarArticles]

    // 1. タイトル検索（デバウンス適用）
    if (debouncedSearchTerm) {
      filtered = filtered.filter((article) =>
        article.title.toLowerCase().includes(debouncedSearchTerm.toLowerCase()),
      )
    }

    // 2. 推奨タイプ
    if (selectedRecommendations.length > 0) {
      filtered = filtered.filter((article) =>
        selectedRecommendations.includes(article.recommendation_type),
      )
    }

    // 3. 類似度範囲
    filtered = filtered.filter(
      (article) =>
        article.similarity_score * 100 >= similarityRange[0] &&
        article.similarity_score * 100 <= similarityRange[1],
    )

    // 4. PV数範囲
    filtered = filtered.filter(
      (article) =>
        article.pageviews >= pageviewsRange[0] && article.pageviews <= pageviewsRange[1],
    )

    // 5. 講座フィルター
    if (sameCourseOnly && baseArticle) {
      // 基点記事と同じ講座のみ表示
      filtered = filtered.filter((article) => article.koza_id === baseArticle.koza_id)
    } else if (selectedCourseIds.length > 0) {
      // 選択された講座のみ表示
      filtered = filtered.filter((article) => selectedCourseIds.includes(article.koza_id))
    }

    // 6. ソート
    filtered.sort((a, b) => {
      let aValue, bValue
      
      switch (sortConfig.key) {
        case "similarity_score":
          aValue = a.similarity_score
          bValue = b.similarity_score
          break
        case "pageviews":
          aValue = a.pageviews
          bValue = b.pageviews
          break
        case "matching_chunks":
          aValue = a.matching_base_chunks
          bValue = b.matching_base_chunks
          break
        case "recommendation_type":
          // 推奨タイプの優先度でソート（MERGE_CONTENT > REDIRECT_301 > CROSS_LINK > MONITOR）
          const typeOrder = { "MERGE_CONTENT": 0, "REDIRECT_301": 1, "CROSS_LINK": 2, "MONITOR": 3 }
          aValue = typeOrder[a.recommendation_type as keyof typeof typeOrder] ?? 999
          bValue = typeOrder[b.recommendation_type as keyof typeof typeOrder] ?? 999
          break
        default:
          return 0
      }

      if (aValue < bValue) {
        return sortConfig.direction === "asc" ? -1 : 1
      }
      if (aValue > bValue) {
        return sortConfig.direction === "asc" ? 1 : -1
      }
      return 0
    })

    return filtered
  }, [allSimilarArticles, debouncedSearchTerm, selectedRecommendations, similarityRange, pageviewsRange, selectedCourseIds, sameCourseOnly, baseArticle?.koza_id, sortConfig])

  // --- ヘルパー関数 ---
  const handleSort = (key: SortKey) => {
    setSortConfig((prev) => {
      const direction: SortDirection = prev.key === key && prev.direction === "desc" ? "asc" : "desc"
      return { key, direction }
    })
  }

  const getRecommendationProps = (type: string) => {
    const config = RECOMMENDATION_TYPES.find(t => t.id === type)
    return config || {
      name: "未知",
      fullName: "未知のタイプ",
      variant: "secondary" as const,
      className: "bg-gray-500 text-white border-gray-500"
    }
  }

  const handleRecommendationToggle = (typeId: string) => {
    setSelectedRecommendations((prev) =>
      prev.includes(typeId)
        ? prev.filter((item) => item !== typeId)
        : [...prev, typeId]
    )
  }

  const handleCourseToggle = (courseId: string) => {
    setSelectedCourseIds((prev) =>
      prev.includes(courseId)
        ? prev.filter((id) => id !== courseId)
        : [...prev, courseId]
    )
  }

  const handleSameCourseToggle = () => {
    setSameCourseOnly((prev) => {
      if (!prev) {
        // 基点記事と同じ講座のみにする場合、他の講座フィルターをクリア
        setSelectedCourseIds([])
      }
      return !prev
    })
  }

  // 完全な記事URLを生成するヘルパー関数
  const getFullArticleUrl = (article: { link: string; koza_slug: string | null }) => {
    if (!article.koza_slug) {
      return article.link.startsWith("http") ? article.link : `https://www.foresight.jp/column/${article.link}`
    }

    const formattedLink = article.link.startsWith("/") ? article.link.substring(1) : article.link
    return `https://www.foresight.jp/${article.koza_slug}/column/${formattedLink}`
  }

  // --- レンダリング ---
  if (loading) {
    return (
      <div className="flex min-h-screen w-full flex-col items-center justify-center bg-muted/40 p-4">
        <div className="flex items-center space-x-4">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
          <p className="text-lg">記事データを読み込み中...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex min-h-screen w-full flex-col items-center justify-center bg-muted/40 p-4">
        <Card className="w-full max-w-2xl">
          <CardHeader>
            <CardTitle className="text-red-500">エラー</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground mb-4">{error}</p>
            <div className="flex gap-2">
              <Button onClick={fetchArticleData} variant="default">
                再試行
              </Button>
              <Button onClick={() => router.back()} variant="outline">
                <ArrowLeft className="h-4 w-4 mr-2" /> 戻る
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (!baseArticle) {
    return (
      <div className="flex min-h-screen w-full flex-col items-center justify-center bg-muted/40 p-4">
        <Card className="w-full max-w-2xl">
          <CardHeader>
            <CardTitle>記事が見つかりません</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground mb-4">指定されたIDの記事は見つかりませんでした。</p>
            <Button onClick={() => router.back()} variant="outline">
              <ArrowLeft className="h-4 w-4 mr-2" /> 戻る
            </Button>
          </CardContent>
        </Card>
      </div>
    )
  }

  const baseArticleCourse = courses.find((c) => c.id === baseArticle.koza_id)
  const displayBaseArticle: Article = {
    ...baseArticle,
    koza_name: baseArticleCourse?.name || `講座 ${baseArticle.koza_id}`,
    koza_slug: baseArticleCourse?.slug || null,
  }

  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40">
      <main className="flex-1 space-y-4 sm:space-y-6 p-3 sm:p-4 md:p-6 lg:p-8">
        {/* ヘッダー */}
        <div className="flex items-center gap-2 sm:gap-4">
          <Button 
            variant="outline" 
            size="icon" 
            className="h-8 w-8 shrink-0" 
            onClick={() => router.back()}
          >
            <ArrowLeft className="h-4 w-4" />
            <span className="sr-only">戻る</span>
          </Button>
          <div className="flex-1 min-w-0">
            <h1 className="text-lg sm:text-xl md:text-2xl font-semibold tracking-tight">
              記事分析
            </h1>
            <p className="text-xs sm:text-sm text-muted-foreground truncate">
              {displayBaseArticle.title}
            </p>
          </div>
        </div>

        {/* 基点記事カード */}
        <Card>
          <CardHeader className="flex flex-col sm:flex-row sm:items-center justify-between space-y-2 sm:space-y-0 pb-2">
            <CardTitle className="text-base font-medium flex items-center gap-2">
              <FileText className="h-4 w-4 text-muted-foreground" />
              基点記事
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div>
                <h3 className="text-lg sm:text-xl md:text-2xl font-bold leading-tight">
                  {displayBaseArticle.title}
                </h3>
                <p className="text-xs sm:text-sm text-muted-foreground mt-1">
                  {displayBaseArticle.koza_name}
                </p>
              </div>
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div className="flex items-center space-x-4">
                  <div className="flex items-center">
                    <Eye className="mr-2 h-4 w-4 text-muted-foreground" />
                    <span className="font-semibold text-sm sm:text-base">
                      {displayBaseArticle.pageviews.toLocaleString()}
                    </span>
                    <span className="text-xs text-muted-foreground ml-1">PV</span>
                  </div>
                  {displayBaseArticle.chunk_count !== undefined && (
                    <div className="flex items-center">
                      <Layers className="mr-2 h-4 w-4 text-muted-foreground" />
                      <span className="font-semibold text-sm sm:text-base">
                        {displayBaseArticle.chunk_count}
                      </span>
                      <span className="text-xs text-muted-foreground ml-1">チャンク</span>
                    </div>
                  )}
                </div>
                <a 
                  href={getFullArticleUrl(displayBaseArticle)} 
                  target="_blank" 
                  rel="noopener noreferrer" 
                  className="text-sm text-primary hover:underline flex items-center gap-1 shrink-0"
                >
                  記事を開く <ExternalLink className="h-3 w-3" />
                </a>
              </div>
            </div>
          </CardContent>
        </Card>
        
        {/* フィルターカード */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <ListFilter className="h-5 w-5" />
              フィルター
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 sm:space-y-6">
            {/* 検索フィールド */}
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input 
                placeholder="記事タイトルで検索..." 
                className="pl-8" 
                value={searchTerm} 
                onChange={(e) => setSearchTerm(e.target.value)} 
              />
            </div>

            {/* 類似度とPV数の範囲スライダー */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* 類似度スライダー */}
              <div className="space-y-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium">類似度の範囲</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">範囲:</span>
                    <span className="font-mono text-sm font-semibold bg-blue-100 text-blue-800 px-2 py-1 rounded-md">
                      {similarityRange[0]}% - {similarityRange[1]}%
                    </span>
                  </div>
                </div>
                <div className="px-3 py-4 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-2 border-blue-200">
                  <div className="relative">
                    <Slider 
                      value={similarityRange} 
                      onValueChange={(value) => setSimilarityRange(value as [number, number])} 
                      min={0} 
                      max={100} 
                      step={1} 
                      className="w-full [&>span[data-orientation=horizontal]]:h-3 [&>span[data-orientation=horizontal]]:bg-gray-200 [&>span>span]:bg-blue-500 [&>span>span]:h-3 [&_[role=slider]]:h-5 [&_[role=slider]]:w-5 [&_[role=slider]]:bg-white [&_[role=slider]]:border-2 [&_[role=slider]]:border-blue-600 [&_[role=slider]]:shadow-md"
                    />
                    {/* 選択範囲の視覚的表示 */}
                    <div className="flex justify-between items-center mt-1 text-xs">
                      <span className="text-blue-600 font-medium">選択範囲</span>
                      <div className="flex items-center gap-1">
                        <div className="w-4 h-2 bg-blue-500 rounded-full"></div>
                        <span className="text-blue-600">範囲内</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex justify-between text-xs text-blue-600 mt-2 font-medium">
                    <span>0%</span>
                    <span>50%</span>
                    <span>100%</span>
                  </div>
                </div>
              </div>
              
              {/* PV数スライダー */}
              <div className="space-y-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium">PV数の範囲</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">範囲:</span>
                    <span className="font-mono text-sm font-semibold bg-green-100 text-green-800 px-2 py-1 rounded-md">
                      {pageviewsRange[0].toLocaleString()} - {pageviewsRange[1].toLocaleString()}
                    </span>
                  </div>
                </div>
                <div className="px-3 py-4 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-2 border-green-200">
                  <div className="relative">
                    <Slider 
                      value={pageviewsRange} 
                      onValueChange={(value) => setPageviewsRange(value as [number, number])} 
                      min={0} 
                      max={maxPageviews} 
                      step={Math.max(1, Math.floor(maxPageviews / 100))}
                      className="w-full [&>span[data-orientation=horizontal]]:h-3 [&>span[data-orientation=horizontal]]:bg-gray-200 [&>span>span]:bg-green-500 [&>span>span]:h-3 [&_[role=slider]]:h-5 [&_[role=slider]]:w-5 [&_[role=slider]]:bg-white [&_[role=slider]]:border-2 [&_[role=slider]]:border-green-600 [&_[role=slider]]:shadow-md"
                    />
                    {/* 選択範囲の視覚的表示 */}
                    <div className="flex justify-between items-center mt-1 text-xs">
                      <span className="text-green-600 font-medium">選択範囲</span>
                      <div className="flex items-center gap-1">
                        <div className="w-4 h-2 bg-green-500 rounded-full"></div>
                        <span className="text-green-600">範囲内</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex justify-between text-xs text-green-600 mt-2 font-medium">
                    <span>0</span>
                    <span>{Math.floor(maxPageviews / 2).toLocaleString()}</span>
                    <span>{maxPageviews.toLocaleString()}</span>
                  </div>
                </div>
              </div>
            </div>

            {/* 推奨タイプ選択 */}
            <div className="space-y-3">
              <label className="text-sm font-medium">推奨タイプ</label>
              <div className="flex flex-wrap gap-2">
                {RECOMMENDATION_TYPES.map((type) => (
                  <TooltipProvider key={type.id}>
                    <Tooltip delayDuration={300}>
                      <TooltipTrigger asChild>
                        <Button
                          variant={selectedRecommendations.includes(type.id) ? "default" : "outline"}
                          size="sm"
                          className={`${selectedRecommendations.includes(type.id) ? type.className : ""} transition-all duration-200`}
                          onClick={() => handleRecommendationToggle(type.id)}
                        >
                          <span className="block lg:hidden">{type.name}</span>
                          <span className="hidden lg:block">{type.fullName}</span>
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent className="bg-slate-900 text-white border-slate-700">
                        <p className="text-xs font-medium">{type.fullName}</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                ))}
              </div>
            </div>

            {/* 講座フィルター */}
            <div className="space-y-3">
              <label className="text-sm font-medium">講座で絞り込み</label>

              {/* 基点記事と同じ講座のみ表示 */}
              {baseArticle && (
                <div className="space-y-3 p-3 bg-blue-50 rounded-lg border border-blue-200">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="same-course-only"
                      checked={sameCourseOnly}
                      onCheckedChange={handleSameCourseToggle}
                    />
                    <Label htmlFor="same-course-only" className="text-sm font-medium cursor-pointer">
                      基点記事と同じ講座のみ（{baseArticle.koza_name || "未分類"}）
                    </Label>
                  </div>
                </div>
              )}

              {/* 講座選択（基点記事と同じ講座のみがOFFの場合のみ表示） */}
              {!sameCourseOnly && (
                <div className="space-y-2">
                  <div className="text-xs text-muted-foreground">複数選択可能（全講座から選択）</div>
                  <div className="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto">
                    {courses.map((course) => (
                      <div key={course.id} className="flex items-center space-x-2">
                        <Checkbox
                          id={`course-${course.id}`}
                          checked={selectedCourseIds.includes(course.id)}
                          onCheckedChange={() => handleCourseToggle(course.id)}
                        />
                        <Label
                          htmlFor={`course-${course.id}`}
                          className="text-xs cursor-pointer leading-tight"
                        >
                          {course.name}
                        </Label>
                      </div>
                    ))}
                  </div>
                  {selectedCourseIds.length > 0 && (
                    <div className="text-xs text-muted-foreground">
                      {selectedCourseIds.length}講座を選択中
                    </div>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
        
        {/* 類似記事リスト */}
        <div className="space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <h2 className="text-lg sm:text-xl font-semibold tracking-tight">
              類似記事と統合提案 ({displayedArticles.length}件)
            </h2>
            
            {/* ソート機能 */}
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm text-muted-foreground shrink-0">並び順:</span>
              <div className="flex flex-wrap gap-1">
                <Button 
                  variant={sortConfig.key === 'similarity_score' ? 'default' : 'ghost'} 
                  size="sm" 
                  onClick={() => handleSort('similarity_score')}
                  className="text-xs sm:text-sm"
                >
                  類似度
                  {sortConfig.key === 'similarity_score' && (
                    sortConfig.direction === 'asc' ? 
                      <ArrowUp className="ml-1 h-3 w-3" /> : 
                      <ArrowDown className="ml-1 h-3 w-3" />
                  )}
                </Button>
                <Button 
                  variant={sortConfig.key === 'pageviews' ? 'default' : 'ghost'} 
                  size="sm" 
                  onClick={() => handleSort('pageviews')}
                  className="text-xs sm:text-sm"
                >
                  PV数
                  {sortConfig.key === 'pageviews' && (
                    sortConfig.direction === 'asc' ? 
                      <ArrowUp className="ml-1 h-3 w-3" /> : 
                      <ArrowDown className="ml-1 h-3 w-3" />
                  )}
                </Button>
                <Button 
                  variant={sortConfig.key === 'recommendation_type' ? 'default' : 'ghost'} 
                  size="sm" 
                  onClick={() => handleSort('recommendation_type')}
                  className="text-xs sm:text-sm"
                >
                  推奨
                  {sortConfig.key === 'recommendation_type' && (
                    sortConfig.direction === 'asc' ? 
                      <ArrowUp className="ml-1 h-3 w-3" /> : 
                      <ArrowDown className="ml-1 h-3 w-3" />
                  )}
                </Button>
                <Button 
                  variant={sortConfig.key === 'matching_chunks' ? 'default' : 'ghost'} 
                  size="sm" 
                  onClick={() => handleSort('matching_chunks')}
                  className="text-xs sm:text-sm"
                >
                  <Layers className="mr-1 h-3 w-3" />
                  一致数
                  {sortConfig.key === 'matching_chunks' && (
                    sortConfig.direction === 'asc' ? 
                      <ArrowUp className="ml-1 h-3 w-3" /> : 
                      <ArrowDown className="ml-1 h-3 w-3" />
                  )}
                </Button>
              </div>
            </div>
          </div>

          {displayedArticles.length === 0 ? (
            <div className="text-center py-12 sm:py-16 text-muted-foreground bg-background rounded-lg">
              <p>条件に一致する類似記事は見つかりませんでした。</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4 sm:gap-6">
              {displayedArticles.map((article) => {
                const recommendationConfig = getRecommendationProps(article.recommendation_type)
                
                return (
                  <Card key={article.id} className="flex flex-col hover:shadow-lg transition-shadow h-full">
                    <CardHeader className="pb-3">
                      <div className="space-y-2">
                        <CardTitle className="text-sm sm:text-base leading-snug min-h-[3rem]">
                          <a 
                            href={getFullArticleUrl(article)} 
                            target="_blank" 
                            rel="noopener noreferrer" 
                            className="hover:text-primary transition-colors line-clamp-3"
                          >
                            {article.title}
                          </a>
                        </CardTitle>
                        <CardDescription className="text-xs">
                          {article.koza_name}
                        </CardDescription>
                      </div>
                    </CardHeader>
                    
                    <CardContent className="flex-1 flex flex-col justify-between gap-4 pt-0 min-h-0">
                      {/* 推奨タイプ（上部に移動・専用エリア） */}
                      <div className="border-b pb-3 mb-2">
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-muted-foreground font-medium">推奨アクション</span>
                          <TooltipProvider>
                            <Tooltip delayDuration={200}>
                              <TooltipTrigger asChild>
                                <HelpCircle className="h-3 w-3 text-muted-foreground cursor-help" />
                              </TooltipTrigger>
                              <TooltipContent
                                side="right"
                                align="start"
                                className="max-w-[280px] bg-slate-900 text-white border-slate-600 shadow-xl z-50"
                                sideOffset={5}
                              >
                                <div className="space-y-3 p-2">
                                  <div className="flex items-center gap-2">
                                    <span className="text-lg">{recommendationConfig.icon}</span>
                                    <span className="font-semibold text-sm text-white">
                                      {recommendationConfig.fullName}
                                    </span>
                                  </div>
                                  <div className="border-t border-slate-600 pt-2">
                                    <p className="text-xs text-gray-200 leading-relaxed">
                                      {recommendationConfig.description}
                                    </p>
                                  </div>
                                  <div className="bg-slate-800/50 rounded p-2 space-y-2">
                                    <div className="flex items-center gap-1">
                                      <div className={`w-3 h-3 rounded-full ${
                                        recommendationConfig.priority >= 80 ? 'bg-red-500' :
                                        recommendationConfig.priority >= 60 ? 'bg-orange-500' :
                                        recommendationConfig.priority >= 50 ? 'bg-blue-500' :
                                        recommendationConfig.priority >= 45 ? 'bg-yellow-500' :
                                        'bg-gray-500'
                                      }`}></div>
                                      <span className="text-xs text-gray-300">
                                        優先度: {
                                          recommendationConfig.priority >= 80 ? '高' :
                                          recommendationConfig.priority >= 60 ? '中高' :
                                          recommendationConfig.priority >= 50 ? '中' :
                                          recommendationConfig.priority >= 45 ? '中低' :
                                          '低'
                                        }
                                      </span>
                                    </div>
                                    {recommendationConfig.conditions && (
                                      <div className="border-t border-slate-600 pt-1">
                                        <div className="text-xs text-gray-400">判定条件:</div>
                                        <div className="text-xs text-gray-300 font-mono">
                                          {recommendationConfig.conditions}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              </TooltipContent>
                            </Tooltip>
                          </TooltipProvider>
                        </div>
                        <div className="mt-1">
                          <Badge className={`text-xs font-medium px-2 py-1 ${recommendationConfig.className}`}>
                            {recommendationConfig.fullName}
                          </Badge>
                        </div>
                      </div>

                      {/* メトリクス（推奨を除く3つのみ） */}
                      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
                        <div>
                          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
                            類似度
                            {article.peak_similarity_score !== undefined && (
                              <TooltipProvider>
                                <Tooltip delayDuration={200}>
                                  <TooltipTrigger asChild>
                                    <HelpCircle className="h-3 w-3 text-muted-foreground cursor-help" />
                                  </TooltipTrigger>
                                  <TooltipContent
                                    side="right"
                                    align="start"
                                    className="max-w-[240px] bg-slate-900 text-white border-slate-600 shadow-xl z-50"
                                    sideOffset={10}
                                  >
                                    <div className="space-y-2 p-1">
                                      <div className="text-center">
                                        <div className="text-2xl font-bold text-blue-400 mb-1">
                                          {Math.min((article.similarity_score * 100), 100).toFixed(1)}%
                                        </div>
                                        <div className="w-full bg-gray-700 rounded-full h-2 mb-2">
                                          <div
                                            className="bg-gradient-to-r from-blue-500 to-green-500 h-2 rounded-full"
                                            style={{width: `${Math.min((article.similarity_score * 100), 100)}%`}}
                                          ></div>
                                        </div>
                                        <p className="text-gray-300 text-xs">両記事の内容がどのくらい似ているか</p>
                                      </div>

                                      <div className="bg-slate-800/50 rounded p-2 text-xs">
                                        <div className="space-y-1">
                                          <div className="flex justify-between">
                                            <span>90%以上</span>
                                            <span className="text-red-400">ほぼ同じ</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>70-89%</span>
                                            <span className="text-yellow-400">よく似ている</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>50-69%</span>
                                            <span className="text-blue-400">やや似ている</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>50%未満</span>
                                            <span className="text-gray-400">あまり似ていない</span>
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </div>
                          <div className="font-bold text-sm sm:text-base">
                            {(article.similarity_score * 100).toFixed(1)}%
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
                            <Layers className="h-3 w-3" />
                            一致率
                            {article.matching_ratio !== undefined && (
                              <TooltipProvider>
                                <Tooltip delayDuration={200}>
                                  <TooltipTrigger asChild>
                                    <HelpCircle className="h-3 w-3 text-muted-foreground cursor-help" />
                                  </TooltipTrigger>
                                  <TooltipContent
                                    side="right"
                                    align="start"
                                    className="max-w-[280px] bg-slate-900 text-white border-slate-600 shadow-xl z-50"
                                    sideOffset={10}
                                  >
                                    <div className="space-y-2 p-1">
                                      <div className="text-center">
                                        <div className="text-2xl font-bold text-orange-400 mb-1">
                                          {Math.min(Math.round((article.matching_ratio || 0) * 100), 100)}%
                                        </div>
                                        <div className="w-full bg-gray-700 rounded-full h-2 mb-2">
                                          <div
                                            className="bg-gradient-to-r from-orange-500 to-red-500 h-2 rounded-full"
                                            style={{width: `${Math.min(Math.round((article.matching_ratio || 0) * 100), 100)}%`}}
                                          ></div>
                                        </div>
                                        <p className="text-gray-300 text-xs">実際に重複している部分の割合</p>
                                      </div>

                                      <div className="bg-slate-800/50 rounded p-2 text-xs space-y-2">
                                        <div>
                                          <p className="text-blue-300 mb-1">📄 この記事: {article.base_total_chunks}個の章</p>
                                          <p className="text-green-300 mb-1">📄 比較記事: {article.similar_total_chunks}個の章</p>
                                          <p className="text-orange-300">🔗 実際の重複: {article.actual_matching_count || Math.min(article.matching_base_chunks, article.matching_similar_chunks)}個</p>
                                        </div>

                                        <div className="border-t border-slate-600 pt-2 space-y-1">
                                          <div className="flex justify-between">
                                            <span>50%以上</span>
                                            <span className="text-red-400">かなり重複</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>30-49%</span>
                                            <span className="text-yellow-400">ある程度重複</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>10-29%</span>
                                            <span className="text-blue-400">部分的に重複</span>
                                          </div>
                                          <div className="flex justify-between">
                                            <span>10%未満</span>
                                            <span className="text-gray-400">ほとんど重複なし</span>
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </div>
                          <div className="font-semibold text-sm sm:text-base">
                            {article.matching_ratio !== undefined && article.matching_ratio !== null
                              ? `${Math.min(Math.round(article.matching_ratio * 100), 100)}%`
                              : `${article.matching_base_chunks}個`}
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-muted-foreground">PV数</div>
                          <div className="font-semibold text-sm sm:text-base">
                            {article.pageviews.toLocaleString()}
                          </div>
                        </div>
                      </div>

                      {/* 説明 */}
                      {article.explanation_text && (
                        <div className="flex items-start gap-3 p-3 bg-muted/50 rounded-lg min-h-0">
                          <Lightbulb className="h-4 w-4 mt-0.5 text-primary shrink-0"/>
                          <p className="text-xs sm:text-sm text-muted-foreground leading-relaxed break-words overflow-hidden">
                            {article.explanation_text}
                          </p>
                        </div>
                      )}

                      {/* アクションボタン */}
                      <Button 
                        variant="outline" 
                        size="sm" 
                        className="w-full text-xs sm:text-sm" 
                        onClick={() => router.push(`/compare/${baseArticle.id}/${article.id}`)}
                      >
                        <Users className="mr-2 h-3 w-3 sm:h-4 sm:w-4" /> 
                        詳細比較
                      </Button>
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          )}
        </div>
      </main>
    </div>
  )
}