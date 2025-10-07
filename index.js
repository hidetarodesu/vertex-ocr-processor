// ===========================================
// 環境設定
// ===========================================

// 処理対象の入力バケット名 (GCS)
const INPUT_BUCKET_NAME = 'receipt-input-data-2025-1005';
// CSV出力先のバケット名（入力バケットと同じにして、ファイル名を output.csv とします）
const OUTPUT_CSV_FILE = 'output.csv';

// サービス アカウントに権限 (Storage, Vertex AI User) が必要です。

// ===========================================
// 依存関係のインポート
// ===========================================
const { Storage } = require('@google/cloud-storage');
const { VertexAI } = require('@google/cloud-vertexai');
const { stringify } = require('csv-stringify');

const storage = new Storage();

// Vertex AI クライアントの初期化
// リージョンは Cloud Run と同じ 'us-central1' を推奨
const vertex_ai = new VertexAI({project: process.env.GCLOUD_PROJECT, location: 'us-central1'});
const model = 'gemini-2.5-flash'; 

// ===========================================
// メイン関数 (GCSイベントを処理)
// ===========================================

/**
 * GCSにファイルがアップロードされたイベントを処理します。
 * @param {object} event The Cloud Storage event payload.
 * @param {object} context The Cloud Functions context.
 */
exports.processReceipt = async (event, context) => {
    // 最終切り分けのために追加したログ
    console.log("FUNCTION START: Received event and beginning execution.");

    // GCSイベントデータからファイル名とバケット名を取得
    const file = event.data;
    const fileName = file.name;
    const bucketName = file.bucket;

    if (!fileName || !bucketName) {
        console.error('Invalid GCS event data: Missing file name or bucket name.');
        return;
    }
    
    // 処理済みファイルを除外
    if (fileName.startsWith('[PROCESSED]') || fileName === OUTPUT_CSV_FILE) {
        console.log(`Skipping processed file or CSV file: ${fileName}`);
        return;
    }

    try {
        console.log(`Processing receipt: ${fileName} from bucket: ${bucketName}`);
        
        // 1. ファイルをストリームで読み取り、base64 エンコード
        const base64Image = await encodeFileToBase64(bucketName, fileName);

        // 2. Gemini API で OCR 処理を実行
        const jsonOutput = await analyzeImageWithGemini(base64Image);
        
        // 3. データを CSV に変換
        const csvData = convertJsonToCsvData(jsonOutput, fileName);

        // 4. CSV ファイルに追記
        await appendDataToCsvFile(csvData, OUTPUT_CSV_FILE);
        
        // 5. 処理済みとしてファイル名を変更
        await renameFile(bucketName, fileName);

        console.log(`OCR processing complete for ${fileName}. Data appended to ${OUTPUT_CSV_FILE}.`);

    } catch (error) {
        console.error(`Error processing file ${fileName}:`, error);
        // エラーが発生しても、ファイル名を変更して無限ループを防ぐ
        try {
             await renameFile(bucketName, fileName, true);
        } catch (renameError) {
             console.error(`FATAL: Could not rename file after error: ${fileName}`, renameError);
        }
        throw new Error(`OCR Process Failed for ${fileName}`);
    }
};

// ===========================================
// ヘルパー関数
// ===========================================

/**
 * GCSファイルの内容を読み取り、Base64文字列にエンコードします。
 * @param {string} bucketName GCSバケット名
 * @param {string} fileName ファイル名
 * @returns {Promise<string>} Base64エンコードされたファイル文字列
 */
async function encodeFileToBase64(bucketName, fileName) {
    console.log(`Downloading file: ${fileName}`);
    const file = storage.bucket(bucketName).file(fileName);
    const [contents] = await file.download();
    return contents.toString('base64');
}

/**
 * Gemini APIを使用して画像の内容を分析し、指定されたJSON形式で出力を求めます。
 * @param {string} base64Image Base64エンコードされた画像文字列
 * @returns {Promise<object>} Geminiから返されたJSONオブジェクト
 */
async function analyzeImageWithGemini(base64Image) {
    console.log('Calling Gemini API for analysis...');
    
    const prompt = `
        You are an expert receipt and invoice data extractor. 
        Analyze the image and extract the following information into a single JSON object.
        1. store_name: The name of the store or merchant.
        2. total_amount: The total amount paid (numeric value).
        3. transaction_date: The date of the transaction in YYYY-MM-DD format.

        If any field is missing, use "N/A" for strings and 0 for numbers.
    `;
    
    const imagePart = {
        inlineData: {
            data: base64Image,
            mimeType: 'image/jpeg' // PNGやJPEGなど、実際のMIMEタイプに合わせて変更
        },
    };

    const request = {
        contents: [
            { role: 'user', parts: [imagePart, { text: prompt }] }
        ],
        config: {
            responseMimeType: "application/json",
        },
    };

    const response = await vertex_ai.generateContent(request);
    
    // Geminiの応答は文字列として返されるため、JSONとしてパース
    const textResponse = response.candidates[0].content.parts[0].text.trim();
    return JSON.parse(textResponse);
}

/**
 * Geminiから返されたJSONデータとファイル名をCSV形式の配列に変換します。
 * @param {object} jsonOutput GeminiのJSON出力
 * @param {string} sourceFileName 元のファイル名
 * @returns {Array<string[]>} CSVに書き込むための配列データ
 */
function convertJsonToCsvData(jsonOutput, sourceFileName) {
    // CSVのヘッダーは、初回書き込み時にのみ使用します。
    // jsonOutputはキーと値のみを抽出し、元のファイル名を先頭に追加
    const data = [
        sourceFileName,
        jsonOutput.store_name,
        jsonOutput.total_amount,
        jsonOutput.transaction_date
    ];
    return [data];
}

/**
 * GCS上のCSVファイルにデータを追記します。
 * @param {Array<string[]>} data CSVに書き込むデータ
 * @param {string} csvFileName CSVファイル名
 */
async function appendDataToCsvFile(data, csvFileName) {
    const bucket = storage.bucket(INPUT_BUCKET_NAME);
    const file = bucket.file(csvFileName);
    
    let isNewFile = false;
    try {
        await file.getMetadata();
    } catch (e) {
        if (e.code === 404) {
            isNewFile = true;
        } else {
            throw e;
        }
    }

    const stringifier = stringify({ header: isNewFile, columns: ['SourceFile', 'StoreName', 'TotalAmount', 'TransactionDate'] });
    let csvString = '';
    
    // 配列をCSV文字列に変換
    for (const record of data) {
        stringifier.write(record);
    }
    stringifier.end();

    for await (const chunk of stringifier) {
        csvString += chunk;
    }

    try {
        // GCSファイルに追記 (append)
        await file.createWriteStream({
            metadata: { contentType: 'text/csv' },
            resumable: false,
            // 既存ファイルの場合は追記、新規の場合は最初から
            offset: isNewFile ? 0 : (await file.getMetadata())[0].size,
        }).end(csvString);

        console.log(`Appended data to CSV file: ${csvFileName}.`);
    } catch (e) {
        console.error('Error writing to CSV file:', e.message);
        throw new Error('CSV書き込みエラー');
    }
}

/**
 * 処理済みのファイル名を変更します。
 * @param {string} bucketName GCSバケット名
 * @param {string} fileName ファイル名
 * @param {boolean} isError 処理がエラーで完了したかどうか
 */
async function renameFile(bucketName, fileName, isError = false) {
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);
    
    const prefix = isError ? '[ERROR_PROCESSED]' : '[PROCESSED]';
    const newFileName = `${prefix}${fileName}`;

    await file.rename(newFileName);
    console.log(`Renamed ${fileName} to ${newFileName}`);
}