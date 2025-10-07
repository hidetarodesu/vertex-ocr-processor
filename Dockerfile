# Node.js の公式 LTS (長期サポート) イメージを使用
FROM node:22-slim

# 作業ディレクトリを設定
WORKDIR /usr/src/app

# package.json と package-lock.json をコピーして依存関係をインストール
COPY package*.json ./

# npm install を実行
RUN npm install

# アプリケーションのコードをコピー (index.js)
COPY . .

# ★★★ この行に置き換えてください ★★★
ENTRYPOINT ["/usr/local/bin/npx", "@google-cloud/functions-framework", "--target=processReceipt"]