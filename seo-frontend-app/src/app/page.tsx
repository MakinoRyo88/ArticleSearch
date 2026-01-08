import Link from "next/link"
import { Search, BarChart3, Activity, FileText } from "lucide-react"

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="text-center">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            SEO記事統合支援システム
          </h1>
          <p className="text-xl text-gray-600 mb-8">
            AIを活用した記事の類似性分析とSEO最適化提案
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mt-12">
          {/* 記事検索 */}
          <Link href="/articles" className="group">
            <div className="bg-white p-8 rounded-2xl shadow-lg border border-gray-200 hover:shadow-xl transition-all duration-200 group-hover:border-blue-300">
              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-100 rounded-full mb-6">
                  <Search className="h-8 w-8 text-blue-600" />
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-3">記事検索・閲覧</h3>
                <p className="text-gray-600">
                  記事の検索・一覧表示・詳細閲覧機能
                </p>
              </div>
            </div>
          </Link>

          {/* 類似記事比較 */}
          <Link href="/compare" className="group">
            <div className="bg-white p-8 rounded-2xl shadow-lg border border-gray-200 hover:shadow-xl transition-all duration-200 group-hover:border-green-300">
              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-green-100 rounded-full mb-6">
                  <BarChart3 className="h-8 w-8 text-green-600" />
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-3">類似記事分析</h3>
                <p className="text-gray-600">
                  AIによる記事の類似性分析とSEO改善提案
                </p>
              </div>
            </div>
          </Link>

          {/* 監視ダッシュボード */}
          <Link href="/monitoring" className="group">
            <div className="bg-white p-8 rounded-2xl shadow-lg border border-gray-200 hover:shadow-xl transition-all duration-200 group-hover:border-purple-300">
              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-purple-100 rounded-full mb-6">
                  <Activity className="h-8 w-8 text-purple-600" />
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-3">監視ダッシュボード</h3>
                <p className="text-gray-600">
                  LLMプロバイダーのパフォーマンス・コスト監視
                </p>
              </div>
            </div>
          </Link>
        </div>

        {/* 機能説明 */}
        <div className="mt-16 bg-white rounded-2xl shadow-lg p-8">
          <div className="text-center mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">主な機能</h2>
            <p className="text-gray-600">高度なAI技術を活用したSEO最適化支援機能</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-blue-100 rounded-full mb-4">
                <FileText className="h-6 w-6 text-blue-600" />
              </div>
              <h3 className="font-semibold text-gray-900 mb-2">チャンクベース分析</h3>
              <p className="text-sm text-gray-600">記事をチャンクに分割して精密な類似性を分析</p>
            </div>

            <div className="text-center">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-green-100 rounded-full mb-4">
                <Activity className="h-6 w-6 text-green-600" />
              </div>
              <h3 className="font-semibold text-gray-900 mb-2">ローカルLLM統合</h3>
              <p className="text-sm text-gray-600">コスト削減とプライバシー保護を両立</p>
            </div>

            <div className="text-center">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-purple-100 rounded-full mb-4">
                <BarChart3 className="h-6 w-6 text-purple-600" />
              </div>
              <h3 className="font-semibold text-gray-900 mb-2">リアルタイム監視</h3>
              <p className="text-sm text-gray-600">システムパフォーマンスとコストを監視</p>
            </div>

            <div className="text-center">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-yellow-100 rounded-full mb-4">
                <Search className="h-6 w-6 text-yellow-600" />
              </div>
              <h3 className="font-semibold text-gray-900 mb-2">高度な検索機能</h3>
              <p className="text-sm text-gray-600">カテゴリ・PV数・エンゲージメントでフィルタリング</p>
            </div>
          </div>
        </div>

        {/* フッター */}
        <div className="mt-16 text-center text-gray-600">
          <p className="text-sm">
            &copy; 2024 SEO記事統合支援システム. AI技術でSEO最適化を支援します。
          </p>
        </div>
      </div>
    </div>
  )
}
