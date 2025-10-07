# Node.js の公式 LTS (長期サポート) イメージを使用
FROM node:22-slim

# 作業ディレクトリを設定
WORKDIR /usr/src/app

# package.json と package-lock.json をコピーして依存関係をインストール
# これにより、キャッシュが効きやすくなります
COPY package*.json ./

# npm install を実行
# ここで依存関係のダウンロードとインストールが完結します
RUN npm install

# アプリケーションのコードをコピー (index.js)
COPY . .

# Cloud Run (Functions Framework) のエントリポイントを定義
# functions-framework を使って、processReceipt 関数を待ち受けます
# (Cloud Runが自動で8080ポートでリッスンします)
CMD ["npx", "@google-cloud/functions-framework", "--target=processReceipt"]