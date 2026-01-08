/** @type {import('next').NextConfig} */
const nextConfig = {
  // output: 'standalone',
  typescript: {
    ignoreBuildErrors: true,
  },
  eslint: {
    ignoreDuringBuilds: true,
  },
  // APIプロキシ設定を削除（直接Cloud Run APIに接続）
};

export default nextConfig;
