#!/usr/bin/env python3
"""
Strapi同期処理のローカルテスト
膨大なデータの取得をテストします
"""

import os
import sys
from unittest.mock import Mock

# 環境変数を設定
os.environ['PROJECT_ID'] = 'seo-optimize-464208'
os.environ['DATASET_ID'] = 'content_analysis'
os.environ['ARTICLES_TABLE_ID'] = 'articles'
os.environ['COURSES_TABLE_ID'] = 'courses'
os.environ['STRAPI_BASE_URL'] = input("Strapi Base URL: ").strip()
os.environ['STRAPI_API_TOKEN'] = input("Strapi API Token (optional, press Enter to skip): ").strip()

# メイン関数をインポート
from main import fetch_strapi_articles_paginated, process_article_data

def test_fetch_articles():
    """記事取得のテスト"""
    print("\n" + "="*60)
    print("📥 Strapiから記事データを取得中...")
    print("="*60)
    
    try:
        raw_articles = fetch_strapi_articles_paginated()
        
        print(f"\n✅ 取得完了: {len(raw_articles)}件の記事")
        
        if raw_articles:
            print(f"\n📊 サンプルデータ（最初の記事）:")
            first = raw_articles[0]
            print(f"   ID: {first.get('id')}")
            print(f"   Title: {first.get('attributes', {}).get('POST_TITLE', 'N/A')[:50]}...")
            print(f"   Link: {first.get('attributes', {}).get('LINK', 'N/A')}")
            
            # データサイズをチェック
            import json
            data_size = len(json.dumps(raw_articles))
            print(f"\n💾 データサイズ: {data_size:,} bytes ({data_size/1024/1024:.2f} MB)")
        
        print("\n" + "="*60)
        print("🔄 記事データを処理中...")
        print("="*60)
        
        processed_articles = process_article_data(raw_articles)
        
        print(f"\n✅ 処理完了: {len(processed_articles)}件")
        
        if processed_articles:
            # full_content_htmlが正しく生成されているかチェック
            sample = processed_articles[0]
            print(f"\n📝 サンプル（ID: {sample['id']}）:")
            print(f"   タイトル: {sample['title'][:50]}...")
            print(f"   full_content: {len(sample.get('full_content', ''))} 文字")
            print(f"   full_content_html: {len(sample.get('full_content_html', ''))} 文字")
            print(f"   qanda_content: {len(sample.get('qanda_content', ''))} 文字")
            
            # HTMLタグが含まれているかチェック
            has_html = '<h' in sample.get('full_content_html', '') or '<p>' in sample.get('full_content_html', '')
            print(f"   HTMLタグ存在: {'✅ YES' if has_html else '❌ NO'}")
            
            if has_html:
                html_preview = sample.get('full_content_html', '')[:200]
                print(f"   HTML例: {html_preview}...")
        
        print("\n" + "="*60)
        print("🎉 テスト完了！")
        print("="*60)
        print(f"\n📊 結果サマリー:")
        print(f"   取得: {len(raw_articles)}件")
        print(f"   処理成功: {len(processed_articles)}件")
        print(f"   処理失敗: {len(raw_articles) - len(processed_articles)}件")
        
        return True
        
    except Exception as e:
        print(f"\n❌ エラー: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_fetch_articles()
    sys.exit(0 if success else 1)
