// ====================================================================
// GCP Cloud Functions x Google Gen AI - 請求書/領収書 OCR スクリプト
// ====================================================================

// 必要なライブラリのインポート (package.json に依存)
const { Storage } = require('@google/cloud-storage');
const { VertexAI } = require('@google/cloud-vertexai');
const stringify = require('csv-stringify').stringify;

// 設定値
// CSVファイルを出力するバケット名 (入力バケットと同じ)
const BUCKET_NAME = 'receipt-input-data-2025-1005'; // ★プロジェクトに合わせて変更してください★
const CSV_FILE_NAME = 'receipt_data.csv';
const TARGET_MODEL = 'gemini-2.5-flash';

// クライアントの初期化 (認証はサービスアカウントで自動)
const storage = new Storage();
const ai = new VertexAI({}); 

/**
 * Cloud Storageへのファイルアップロードをトリガーとして実行されるメイン関数
 * @param {object} file - アップロードされたファイルの情報
 */
exports.processReceipt = async (file) => {
    const fileName = file.name;
    const mimeType = file.contentType;

    // 既に処理済みのファイルまたはCSVファイル自体はスキップ
    if (fileName.includes('[PROCESSED]') || fileName === CSV_FILE_NAME) {
        console.log(`Skipping file: ${fileName}`);
        return;
    }

    console.log(`Processing file: ${fileName} (${file.id})`);

    try {
        // 1. ファイルをBase64にエンコード
        const base64Data = await encodeFileToBase64(BUCKET_NAME, fileName);
        if (!base64Data) return;

        // 2. Gemini API呼び出し
        const extractedData = await callGeminiApi(base64Data, mimeType);
        if (!extractedData) return;

        // 3. CSVファイルに追記
        await appendDataToCsvFile(extractedData, fileName);

        // 4. 処理後、ファイル名を変更して二重処理を防ぐ (リネーム)
        await renameFile(BUCKET_NAME, fileName);

        console.log(`Successfully processed and renamed ${fileName}`);

    } catch (error) {
        console.error(`Error processing ${fileName}: ${error.message}`);
        // 処理失敗時も、エラーの詳細をログに残す
    }
};


// ====================================================================
// ヘルパー関数 (ロジック本体)
// ====================================================================

/**
 * Cloud Storageからファイルを取得し、Base64形式にエンコードします。
 */
async function encodeFileToBase64(bucketName, fileName) {
    try {
        const file = storage.bucket(bucketName).file(fileName);
        const [data] = await file.download();
        
        // Base64エンコード
        return data.toString('base64');
    } catch (e) {
        console.error(`Error downloading or encoding file: ${e.message}`);
        return null;
    }
}

/**
 * Gemini APIを呼び出し、画像から情報を抽出します。
 */
async function callGeminiApi(base64Data, mimeType) {
    const prompt = `
あなたは経理の専門家です。この領収書または請求書の画像から、以下の4項目を正確に抽出し、日本語でJSON形式で出力してください。
- invoice_number (請求書番号または領収書番号): 番号がない場合は「N/A」
- issuing_company (発行元企業名): 企業名がわからない場合は「N/A」
- issue_date (発行日): YYYY-MM-DD形式に変換。日付がない場合は「N/A」
- total_amount (合計金額): 数字のみを抽出し、小数点以下は切り捨て。金額が不明な場合は 0

抽出結果は、指定されたJSONスキーマに従って出力してください。他のコメントや説明は一切含めないでください。
`;

    try {
        const response = await ai.models.generateContent({
            model: TARGET_MODEL,
            contents: [
                { role: "user", parts: [
                    { text: prompt },
                    { inlineData: { mimeType: mimeType, data: base64Data } }
                ]}
            ],
            config: {
                // Geminiの応答形式をJSONに固定
                responseMimeType: "application/json",
                responseSchema: {
                    type: "OBJECT",
                    properties: {
                        invoice_number: { type: "STRING" },
                        issuing_company: { type: "STRING" },
                        issue_date: { type: "STRING" },
                        total_amount: { type: "NUMBER" }
                    }
                }
            }
        });
        
        // 応答テキストからJSON部分を抽出してパース
        const jsonText = response.response.candidates[0].content.parts[0].text;
        const extractedData = JSON.parse(jsonText.trim()); 
        
        return extractedData;
    } catch (e) {
        console.error("Gemini API呼び出しまたはJSONパースエラー: " + e.message);
        return null;
    }
}

/**
 * 抽出されたデータをCSVファイルに追記します。
 */
async function appendDataToCsvFile(data, sourceFileName) {
    const bucket = storage.bucket(BUCKET_NAME);
    const csvFile = bucket.file(CSV_FILE_NAME);

    // CSVヘッダーの定義
    const columns = [
        'source_file', 'invoice_number', 'issuing_company', 'issue_date', 'total_amount'
    ];
    
    // データ行の準備
    const dataRow = [{
        source_file: sourceFileName,
        ...data
    }];
    
    // CSV文字列に変換
    const csvData = await new Promise((resolve, reject) => {
        stringify(dataRow, { header: false, columns: columns }, (err, output) => {
            if (err) return reject(err);
            resolve(output);
        });
    });

    try {
        // CSVファイルの存在確認
        const [exists] = await csvFile.exists();
        
        if (!exists) {
            // ファイルが存在しない場合、ヘッダーを作成
            const headerRow = columns.join(',') + '\n';
            await csvFile.save(headerRow + csvData);
            console.log(`Created new CSV file and added data for ${sourceFileName}.`);
        } else {
            // ファイルが存在する場合、追記
            const appendedData = '\n' + csvData.trim();
            await csvFile.append(appendedData);
            console.log(`Appended data to existing CSV file for ${sourceFileName}.`);
        }
    } catch (e) {
        console.error(`Error writing to CSV file: ${e.message}`);
        throw new Error('CSV書き込みエラー');
    }
}

/**
 * 処理済みのファイル名を変更します。
 */
async function renameFile(bucketName, fileName) {
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);
    const newFileName = `[PROCESSED]${fileName}`;
    
    await file.rename(newFileName);
    console.log(`Renamed ${fileName} to ${newFileName}`);
}