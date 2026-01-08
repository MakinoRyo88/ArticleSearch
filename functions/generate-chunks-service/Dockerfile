# Python 3.11をベースイメージとして使用
FROM python:3.11

# 環境変数設定
ENV PYTHONUNBUFFERED True
# PYTHONPATH: /app をPythonの検索パスに追加し、モジュールを見つけられるようにする
ENV PYTHONPATH /app

# 作業ディレクトリを/appに設定
WORKDIR /app

# 最初にrequirements.txtをコピー
COPY requirements.txt .

# 必要なライブラリをインストール
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションのソースコードを全てコピー
COPY . .

# コンテナのメイン実行プログラムをGunicornに指定
# main:app は、main.pyファイルの中のappという名前のFlaskインスタンスを指す
# Cloud Runが指定するPORT環境変数をリッスンする
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
