"use client";

import type React from "react";
import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationPrevious,
  PaginationLink,
  PaginationNext,
} from "@/components/ui/pagination";
import { Badge } from "@/components/ui/badge";
import { getArticlesListUrl, getCoursesUrl, getStatsUrl } from "@/lib/api-config";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Slider } from "@/components/ui/slider";
import {
  ExternalLink,
  Search,
  Filter,
  ListFilter,
  HelpCircle,
  Eye,
  Users,
  FileText,
  ArrowUp,
  ArrowDown,
  SortAsc,
  SortDesc,
  AlertCircle,
} from "lucide-react";

interface Article {
  id: string;
  title: string;
  link: string;
  koza_id: string;
  koza_name: string;
  koza_slug: string | null;
  pageviews: number;
  engaged_sessions: number;
  avg_engagement_time: number;
  organic_sessions: number;
  search_keywords: string[];
  created_at: string | { value: string };
  updated_at: string | { value: string };
  last_synced: string | { value: string };
  has_embedding: boolean;
}

interface Course {
  id: string;
  slug: string;
  name: string;
  description: string;
  total_articles: number;
  total_pageviews: number;
  created_at: string;
  updated_at: string;
}

interface PaginationInfo {
  current_page: number;
  per_page: number;
  total_count: number;
  total_pages: number;
  has_next: boolean;
  has_prev: boolean;
}

interface FiltersInfo {
  search: string;
  koza_id: string;
  min_pageviews: number;
  max_pageviews: number | null;
  min_engaged_sessions: number;
  max_engaged_sessions: number | null;
  content_type: string;
  sort: string;
}

interface ArticleSearchResponse {
  articles: Article[];
  pagination: PaginationInfo;
  filters: FiltersInfo;
}

type SortKey = "pageviews" | "title" | "updated_at" | "engaged_sessions";
type SortDirection = "asc" | "desc";

export default function ArticlesPage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  // =================================================================
  // == 1. データとUI状態の管理 ==
  // =================================================================
  const [articles, setArticles] = useState<Article[]>([]);
  const [pagination, setPagination] = useState<PaginationInfo | null>(null);
  const [courses, setCourses] = useState<Course[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // サーバーから取得するスライダーの最大値
  const [initialMaxPageviews, setInitialMaxPageviews] = useState(1000000);
  const [maxEngagedSessions, setMaxEngagedSessions] = useState(100000);

  // =================================================================
  // == 2. URLを唯一の真実とする状態導出 (useMemo) ==
  // =================================================================
  // これらの値はURLから直接算出され、UIの表示に使われる
  const searchTerm = useMemo(
    () => searchParams.get("search") || "",
    [searchParams],
  );
  const selectedKoza = useMemo(
    () => searchParams.get("koza_id") || "all",
    [searchParams],
  );
  const sortConfig = useMemo(() => {
    const sort = searchParams.get("sort") || "pageviews_desc";
    const lastUnderscore = sort.lastIndexOf("_");
    const key = sort.substring(0, lastUnderscore) as SortKey;
    const direction = sort.substring(lastUnderscore + 1) as SortDirection;
    return { key, direction };
  }, [searchParams]);

  const pageviewsRange = useMemo(() => {
    const min = Number(searchParams.get("min_pageviews")) || 0;
    const max =
      Number(searchParams.get("max_pageviews")) || initialMaxPageviews;
    return [min, max] as [number, number];
  }, [searchParams, initialMaxPageviews]);

  const engagedSessionsRange = useMemo(() => {
    const min = Number(searchParams.get("min_engaged_sessions")) || 0;
    const max =
      Number(searchParams.get("max_engaged_sessions")) || maxEngagedSessions;
    return [min, max] as [number, number];
  }, [searchParams, maxEngagedSessions]);

  // =================================================================
  // == 3. URLを更新するための単一の司令塔関数 ==
  // =================================================================
  const updateFilters = useCallback(
    (newFilters: Record<string, string | number | null>) => {
      const newSearchParams = new URLSearchParams(searchParams.toString());
      Object.entries(newFilters).forEach(([key, value]) => {
        // 値がnull, 空, 'all', または最小値系のフィルターで0の場合はURLから削除
        if (
          value === null ||
          value === "" ||
          value === "all" ||
          (key.startsWith("min_") && value === 0)
        ) {
          newSearchParams.delete(key);
        } else {
          newSearchParams.set(key, String(value));
        }
      });
      newSearchParams.set("page", "1"); // フィルター変更時は1ページ目に戻す
      router.push(`/articles?${newSearchParams.toString()}`);
    },
    [router, searchParams],
  );

  // フィルターが現在適用されているかどうかを判断する
  const areFiltersActive = useMemo(() => {
    const params = new URLSearchParams(searchParams.toString());
    // ページ番号以外のパラメータが存在するかどうかで判断
    params.delete("page");
    return params.toString() !== "";
  }, [searchParams]);

  // =================================================================
  // == 4. ユーザー操作をハンドリングするローカル状態と副作用 ==
  // =================================================================

  // --- 検索入力の制御 ---
  const [localSearch, setLocalSearch] = useState(searchTerm);
  useEffect(() => {
    setLocalSearch(searchTerm);
  }, [searchTerm]); // URL変更時にローカルにも反映
  useEffect(() => {
    const timer = setTimeout(() => {
      if (localSearch !== searchTerm) {
        updateFilters({ search: localSearch });
      }
    }, 500);
    return () => clearTimeout(timer);
  }, [localSearch, searchTerm, updateFilters]);

  // --- スライダー入力の制御 ---
  const [localPageviewsRange, setLocalPageviewsRange] =
    useState(pageviewsRange);
  const [localEngagedSessionsRange, setLocalEngagedSessionsRange] =
    useState(engagedSessionsRange);
  useEffect(() => {
    setLocalPageviewsRange(pageviewsRange);
  }, [pageviewsRange]); // URL変更時にローカルにも反映
  useEffect(() => {
    setLocalEngagedSessionsRange(engagedSessionsRange);
  }, [engagedSessionsRange]); // URL変更時にローカルにも反映

  useEffect(() => {
    const timer = setTimeout(() => {
      if (
        localPageviewsRange[0] !== pageviewsRange[0] ||
        localPageviewsRange[1] !== pageviewsRange[1]
      ) {
        updateFilters({
          min_pageviews: localPageviewsRange[0],
          max_pageviews:
            localPageviewsRange[1] >= initialMaxPageviews
              ? null
              : localPageviewsRange[1],
        });
      }
    }, 500);
    return () => clearTimeout(timer);
  }, [localPageviewsRange, pageviewsRange, initialMaxPageviews, updateFilters]);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (
        localEngagedSessionsRange[0] !== engagedSessionsRange[0] ||
        localEngagedSessionsRange[1] !== engagedSessionsRange[1]
      ) {
        updateFilters({
          min_engaged_sessions: localEngagedSessionsRange[0],
          max_engaged_sessions:
            localEngagedSessionsRange[1] >= maxEngagedSessions
              ? null
              : localEngagedSessionsRange[1],
        });
      }
    }, 500);
    return () => clearTimeout(timer);
  }, [
    localEngagedSessionsRange,
    engagedSessionsRange,
    maxEngagedSessions,
    updateFilters,
  ]);

  // =================================================================
  // == 5. データ取得と各種アクションハンドラ ==
  // =================================================================

  // --- データ取得ロジック ---
  const fetchArticlesAndCourses = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(getArticlesListUrl(searchParams));
      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);

      const result = await response.json();
      if (result.success) {
        setArticles(result.data.articles);
        setPagination(result.data.pagination);
      } else {
        throw new Error(result.error?.message || "Failed to fetch articles");
      }
    } catch (err) {
      console.error("データ取得エラー:", err);
      setError(
        err instanceof Error ? err.message : "An unknown error occurred",
      );
    } finally {
      setLoading(false);
    }
  }, [searchParams]);

  useEffect(() => {
    fetchArticlesAndCourses();
  }, [fetchArticlesAndCourses]);

  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        // 講座一覧の取得
        const coursesRes = await fetch(getCoursesUrl());
        const coursesData = await coursesRes.json();
        if (coursesData.success) setCourses(coursesData.data);

        // 統計情報（最大値など）の取得
        const statsRes = await fetch(getStatsUrl());
        const statsData = await statsRes.json();
        if (statsData.success) {
          setInitialMaxPageviews(statsData.data.max_pageviews || 1000000);
          setMaxEngagedSessions(statsData.data.max_engaged_sessions || 100000);
        }
      } catch (err) {
        console.error("初期データ取得エラー:", err);
      }
    };
    fetchInitialData();
  }, []);

  // --- アクションハンドラ ---
  const handleSort = useCallback(
    (key: SortKey) => {
      const newDirection =
        sortConfig.key === key && sortConfig.direction === "desc"
          ? "asc"
          : "desc";
      updateFilters({ sort: `${key}_${newDirection}` });
    },
    [sortConfig, updateFilters],
  );

  const handlePageChange = useCallback(
    (page: number) => {
      if (!pagination || page < 1 || page > pagination.total_pages) return;
      const newSearchParams = new URLSearchParams(searchParams.toString());
      newSearchParams.set("page", String(page));
      router.push(`/articles?${newSearchParams.toString()}`);
    },
    [pagination, searchParams, router],
  );

  const navigateToAnalyze = useCallback(
    (articleId: string) => {
      router.push(`/articles/${articleId}/analyze`);
    },
    [router],
  );

  const getFullArticleUrl = useCallback(
    (article: { link: string; koza_slug: string | null }) => {
      if (!article.koza_slug || article.link.startsWith("http"))
        return article.link;
      const formattedLink = article.link.startsWith("/")
        ? article.link.substring(1)
        : article.link;
      return `https://www.foresight.jp/${article.koza_slug}/column/${formattedLink}`;
    },
    [],
  );

  const resetFilters = useCallback(() => {
    setLoading(true);
    router.push("/articles");
  }, [router]);

  const getSortIcon = (key: SortKey) => {
    if (sortConfig.key !== key)
      return <SortAsc className="h-3 w-3 text-muted-foreground ml-2" />;
    return sortConfig.direction === "asc" ? (
      <ArrowUp className="h-3 w-3 ml-2" />
    ) : (
      <ArrowDown className="h-3 w-3 ml-2" />
    );
  };

  // どのような形式の日付データでも安全にフォーマットする関数
  const formatDate = (
    dateValue: string | { value: string } | null | undefined,
  ): string => {
    // データがない場合はハイフンを返す
    if (!dateValue) {
      return "---";
    }

    // オブジェクト形式（{ value: '...' }）の場合
    if (
      typeof dateValue === "object" &&
      dateValue !== null &&
      "value" in dateValue
    ) {
      // 無効な日付の場合はハイフンを返す
      const date = new Date(dateValue.value);
      return isNaN(date.getTime()) ? "---" : date.toLocaleDateString();
    }

    // 文字列形式の場合
    const date = new Date(String(dateValue));
    return isNaN(date.getTime()) ? "---" : date.toLocaleDateString();
  };

  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40">
      <div className="flex flex-col sm:gap-4 sm:py-4 sm:pl-14">
        <main className="grid flex-1 items-start gap-4 p-4 sm:px-6 sm:py-0 md:gap-8">
          {/* ヘッダー */}
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div>
              <h1 className="text-2xl font-semibold">記事一覧</h1>
              <p className="text-sm text-muted-foreground">
                {pagination &&
                  `全 ${pagination.total_count.toLocaleString()} 件中 ${((pagination.current_page - 1) * pagination.per_page + 1).toLocaleString()}-${Math.min(pagination.current_page * pagination.per_page, pagination.total_count).toLocaleString()} 件を表示`}
              </p>
            </div>
          </div>

          {/* フィルターカード */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2 text-base">
                  <ListFilter className="h-5 w-5" />
                  検索・フィルター
                </CardTitle>
                <CardDescription>
                  記事を検索・絞り込んで表示します
                </CardDescription>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={resetFilters}
                disabled={loading || !areFiltersActive}
                className="text-xs sm:text-sm"
              >
                リセット
              </Button>
            </CardHeader>
            <CardContent className="space-y-4 sm:space-y-6">
              {/* 検索バー */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  type="text"
                  placeholder="記事タイトルで検索..."
                  value={localSearch} // searchTerm -> localSearch
                  onChange={(e) => setLocalSearch(e.target.value)} // setSearchTerm -> setLocalSearch
                  className="pl-10"
                  disabled={loading}
                />
              </div>

              {/* 講座選択とソート */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">講座</label>
                  <Select
                    value={selectedKoza}
                    onValueChange={(value) => updateFilters({ koza_id: value })} // handleFilterChangeを直接呼び出す
                    disabled={loading}
                  >
                    <SelectTrigger className="relative">
                      <SelectValue placeholder="講座を選択" />
                    </SelectTrigger>
                    <SelectContent className="z-[100] max-h-[200px] overflow-y-auto bg-white border border-gray-200 shadow-lg">
                      <SelectItem value="all">全ての講座</SelectItem>
                      {courses
                        .filter(
                          (course) => course.id && course.id.trim() !== "",
                        )
                        .map((course) => (
                          <SelectItem key={course.id} value={course.id}>
                            {course.name}
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium">並び順</label>
                  <Select
                    value={`${sortConfig.key}_${sortConfig.direction}`}
                    onValueChange={(value) => updateFilters({ sort: value })}
                    disabled={loading}
                  >
                    <SelectTrigger className="relative">
                      <SelectValue placeholder="並び替え" />
                    </SelectTrigger>
                    <SelectContent className="z-[100] max-h-[200px] overflow-y-auto bg-white border border-gray-200 shadow-lg">
                      <SelectItem value="pageviews_desc">
                        PV数 (降順)
                      </SelectItem>
                      <SelectItem value="pageviews_asc">PV数 (昇順)</SelectItem>
                      <SelectItem value="engaged_sessions_desc">
                        エンゲージメント (降順)
                      </SelectItem>
                      <SelectItem value="engaged_sessions_asc">
                        エンゲージメント (昇順)
                      </SelectItem>
                      <SelectItem value="title_asc">タイトル (昇順)</SelectItem>
                      <SelectItem value="title_desc">
                        タイトル (降順)
                      </SelectItem>
                      <SelectItem value="updated_at_desc">
                        更新日 (新しい順)
                      </SelectItem>
                      <SelectItem value="updated_at_asc">
                        更新日 (古い順)
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* PV数範囲スライダー */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">PV数の範囲</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">範囲:</span>
                    <span className="font-mono text-sm font-semibold bg-blue-100 text-blue-800 px-2 py-1 rounded-md">
                      {localPageviewsRange[0].toLocaleString()} -{" "}
                      {localPageviewsRange[1].toLocaleString()}
                    </span>
                  </div>
                </div>
                <div className="px-3 py-4 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-2 border-blue-200">
                  <Slider
                    value={localPageviewsRange}
                    onValueChange={(value) => {
                      const [v1, v2] = value;
                      setLocalPageviewsRange([
                        Math.min(v1, v2),
                        Math.max(v1, v2),
                      ]);
                    }}
                    min={0}
                    max={initialMaxPageviews}
                    step={1}
                    className="[&>span>span]:bg-blue-500"
                  />
                  <div className="flex justify-between text-xs text-blue-600 mt-2 font-medium">
                    <span>0</span>
                    <span>
                      {Math.floor(initialMaxPageviews / 2).toLocaleString()}
                    </span>
                    <span>{initialMaxPageviews.toLocaleString()}</span>
                  </div>
                </div>
              </div>

              {/* エンゲージメント数範囲スライダー */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">
                    エンゲージメント数の範囲
                  </span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">範囲:</span>
                    <span className="font-mono text-sm font-semibold bg-green-100 text-green-800 px-2 py-1 rounded-md">
                      {localEngagedSessionsRange[0].toLocaleString()} -{" "}
                      {localEngagedSessionsRange[1].toLocaleString()}
                    </span>
                  </div>
                </div>
                <div className="px-3 py-4 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-2 border-green-200">
                  <Slider
                    value={localEngagedSessionsRange}
                    onValueChange={(value) => {
                      const [v1, v2] = value;
                      setLocalEngagedSessionsRange([
                        Math.min(v1, v2),
                        Math.max(v1, v2),
                      ]);
                    }}
                    min={0}
                    max={maxEngagedSessions}
                    step={1}
                    className="[&>span>span]:bg-green-500"
                  />
                  <div className="flex justify-between text-xs text-green-600 mt-2 font-medium">
                    <span>0</span>
                    <span>
                      {Math.floor(maxEngagedSessions / 2).toLocaleString()}
                    </span>
                    <span>{maxEngagedSessions.toLocaleString()}</span>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground">
                  ※
                  エンゲージメント数の最大値は現在表示されている記事データから自動計算されています。フィルター変更は自動で適用されます。
                </p>
              </div>
            </CardContent>
          </Card>

          {/* 記事リストカード */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileText className="h-5 w-5" />
                記事データ
              </CardTitle>
              <CardDescription>
                システムに登録されている記事の一覧と分析情報です
              </CardDescription>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="flex items-center space-x-4">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
                    <p className="text-lg">データを読み込み中...</p>
                  </div>
                </div>
              ) : error ? (
                <div className="text-center py-8">
                  <p className="text-red-500 mb-4">エラー: {error}</p>
                  <Button onClick={fetchArticlesAndCourses} variant="outline">
                    再試行
                  </Button>
                </div>
              ) : articles.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <FileText className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p className="text-lg mb-2">
                    該当する記事が見つかりませんでした
                  </p>
                  <p className="text-sm">検索条件を変更してお試しください</p>
                </div>
              ) : (
                <>
                  {/* デスクトップ用テーブル */}
                  <div className="hidden md:block">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleSort("title")}
                              className="h-auto p-0 font-semibold justify-start"
                              disabled={loading}
                            >
                              タイトル
                              {getSortIcon("title")}
                            </Button>
                          </TableHead>
                          <TableHead>講座</TableHead>
                          <TableHead className="text-right">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleSort("pageviews")}
                              className="h-auto p-0 font-semibold justify-end"
                              disabled={loading}
                            >
                              PV数
                              {getSortIcon("pageviews")}
                            </Button>
                          </TableHead>
                          <TableHead className="text-right">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleSort("engaged_sessions")}
                              className="h-auto p-0 font-semibold justify-end"
                              disabled={loading}
                            >
                              エンゲージメント
                              {getSortIcon("engaged_sessions")}
                            </Button>
                          </TableHead>
                          <TableHead className="text-center">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleSort("updated_at")}
                              className="h-auto p-0 font-semibold"
                              disabled={loading}
                            >
                              最終更新
                              {getSortIcon("updated_at")}
                            </Button>
                          </TableHead>
                          <TableHead className="text-center">
                            アクション
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {articles.map((article) => (
                          <TableRow key={article.id}>
                            <TableCell className="font-medium">
                              <a
                                href={getFullArticleUrl(article)}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="hover:underline flex items-center gap-2 max-w-md"
                              >
                                <span className="line-clamp-2">
                                  {article.title}
                                </span>
                                <ExternalLink className="h-4 w-4 text-muted-foreground shrink-0" />
                              </a>
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline">
                                {article.koza_name || "N/A"}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right">
                              <div className="flex items-center justify-end gap-1">
                                <Eye className="h-3 w-3 text-muted-foreground" />
                                {article.pageviews.toLocaleString()}
                              </div>
                            </TableCell>
                            <TableCell className="text-right">
                              <div className="flex items-center justify-end gap-1">
                                <Users className="h-3 w-3 text-muted-foreground" />
                                {article.engaged_sessions.toLocaleString()}
                              </div>
                            </TableCell>
                            <TableCell className="text-center text-sm">
                              {formatDate(article.updated_at)}
                            </TableCell>
                            <TableCell className="text-center">
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <div>
                                      <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() =>
                                          navigateToAnalyze(article.id)
                                        }
                                        disabled={!article.has_embedding}
                                        className="disabled:cursor-not-allowed"
                                      >
                                        分析
                                      </Button>
                                    </div>
                                  </TooltipTrigger>
                                  {!article.has_embedding && (
                                    <TooltipContent>
                                      <p>
                                        ベクトルデータがないため分析できません
                                      </p>
                                    </TooltipContent>
                                  )}
                                </Tooltip>
                              </TooltipProvider>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>

                  {/* モバイル用カード表示 */}
                  <div className="md:hidden space-y-4">
                    {articles.map((article) => (
                      <Card key={article.id} className="p-4">
                        <div className="space-y-3">
                          <div className="space-y-2">
                            <a
                              href={getFullArticleUrl(article)}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-sm font-medium hover:underline line-clamp-3 block"
                            >
                              {article.title}
                            </a>
                            <Badge variant="outline" className="text-xs">
                              {article.koza_name || "N/A"}
                            </Badge>
                          </div>

                          <div className="grid grid-cols-2 gap-4 text-sm">
                            <div className="flex items-center gap-2">
                              <Eye className="h-4 w-4 text-muted-foreground" />
                              <span className="font-semibold">
                                {article.pageviews.toLocaleString()}
                              </span>
                              <span className="text-muted-foreground">PV</span>
                            </div>
                            <div className="flex items-center gap-2">
                              <Users className="h-4 w-4 text-muted-foreground" />
                              <span className="font-semibold">
                                {article.engaged_sessions.toLocaleString()}
                              </span>
                            </div>
                          </div>

                          <div className="flex items-center justify-between pt-2 border-t">
                            <span className="text-xs text-muted-foreground">
                              {formatDate(article.updated_at)}
                            </span>
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <div>
                                    <Button
                                      variant="outline"
                                      size="sm"
                                      onClick={() =>
                                        navigateToAnalyze(article.id)
                                      }
                                      disabled={!article.has_embedding}
                                      className="disabled:cursor-not-allowed text-xs"
                                    >
                                      分析
                                    </Button>
                                  </div>
                                </TooltipTrigger>
                                {!article.has_embedding && (
                                  <TooltipContent>
                                    <p>
                                      ベクトルデータがないため分析できません
                                    </p>
                                  </TooltipContent>
                                )}
                              </Tooltip>
                            </TooltipProvider>
                          </div>
                        </div>
                      </Card>
                    ))}
                  </div>

                  {/* ページネーション */}
                  {pagination && (
                    <div className="flex flex-col sm:flex-row items-center justify-between gap-4 mt-6">
                      <div className="text-sm text-muted-foreground">
                        {pagination.total_count} 件中{" "}
                        {(pagination.current_page - 1) * pagination.per_page +
                          1}{" "}
                        -{" "}
                        {Math.min(
                          pagination.current_page * pagination.per_page,
                          pagination.total_count,
                        )}{" "}
                        件を表示
                      </div>
                      <Pagination>
                        <PaginationContent>
                          <PaginationItem>
                            <PaginationPrevious
                              href="#"
                              onClick={(e) => {
                                e.preventDefault();
                                handlePageChange(pagination.current_page - 1);
                              }}
                              aria-disabled={!pagination.has_prev}
                              className={
                                !pagination.has_prev
                                  ? "pointer-events-none opacity-50"
                                  : ""
                              }
                            />
                          </PaginationItem>
                          {Array.from(
                            { length: Math.min(5, pagination.total_pages) },
                            (_, i) => {
                              const page = i + 1;
                              return (
                                <PaginationItem key={page}>
                                  <PaginationLink
                                    isActive={page === pagination.current_page}
                                    onClick={(e) => {
                                      e.preventDefault();
                                      handlePageChange(page);
                                    }}
                                  >
                                    {page}
                                  </PaginationLink>
                                </PaginationItem>
                              );
                            },
                          )}
                          <PaginationItem>
                            <PaginationNext
                              onClick={(e) => {
                                e.preventDefault();
                                handlePageChange(pagination.current_page + 1);
                              }}
                              aria-disabled={!pagination.has_next}
                              className={
                                !pagination.has_next
                                  ? "pointer-events-none opacity-50"
                                  : ""
                              }
                            />
                          </PaginationItem>
                        </PaginationContent>
                      </Pagination>
                    </div>
                  )}
                </>
              )}
            </CardContent>
          </Card>
        </main>
      </div>
    </div>
  );
}
