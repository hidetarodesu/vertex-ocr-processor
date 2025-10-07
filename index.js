// ===========================================
// 環境設定
// ===========================================

// 処理対象の入力バケット名 (GCS)
const INPUT_BUCKET_NAME = 'receipt-input-data-2025-1005';
// CSV出力先のバケット名
const OUTPUT_CSV_FILE = 'output.csv';

// ===========================================
// 依存関係のインポート
// ===========================================
const { Storage } = require('@google/cloud-storage');
const { VertexAI } = require('@google/cloud-vertexai');
const { stringify } = require('csv-stringify');

// ★★★ 外部での初期化を削除します ★★★
// const storage = new Storage();
// const vertex_ai = new VertexAI({project: process.env.GCLOUD_PROJECT, location: 'us-central1'}); 

// ===========================================
// メイン関数 (GCSイベントを処理)
// ===========================================

/**
 * GCSにファイルがアップロードされたイベントを処理します。
 * @param {object} event The Cloud Storage event payload.
 * @param {object} context The Cloud Functions context.
 */
exports.processReceipt = async (event, context) => {
    // ★★★ クライアントの初期化を関数の内側に移動します ★★★
    const storage = new Storage();
    const vertex_ai = new VertexAI({project: process.env.GCLOUD_PROJECT, location: 'us-central1'}); 
    const model = 'gemini-2.5-flash'; 

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
        const base64Image = await encodeFileToBase64(bucketName, fileName, storage); // storageを渡す

        // 2. Gemini API で OCR 処理を実行
        const jsonOutput = await analyzeImageWithGemini(base64Image, vertex_ai, model); // vertex_aiとmodelを渡す
        
        // 3. データを CSV に変換
        const csvData = convertJsonToCsvData(jsonOutput, fileName);

        // 4. CSV ファイルに追記
        await appendDataToCsvFile(csvData, OUTPUT_CSV_FILE, storage); // storageを渡す
        
        // 5. 処理済みとしてファイル名を変更
        await renameFile(bucketName, fileName, storage); // storageを渡す

        console.log(`OCR processing complete for ${fileName}. Data appended to ${OUTPUT_CSV_FILE}.`);

    } catch (error) {
        console.error(`Error processing file ${fileName}:`, error);
        // エラーが発生しても、ファイル名を変更して無限ループを防ぐ
        try {
             await renameFile(bucketName, fileName, storage, true); // storageを渡す
        } catch (renameError) {
             console.error(`FATAL: Could not rename file after error: ${fileName}`, renameError);
        }
        throw new Error(`OCR Process Failed for ${fileName}`);
    }
};

// ===========================================
// ヘルパー関数 (storage, vertex_aiを引数として受け取るように修正)
// ===========================================

/**
 * GCSファイルの内容を読み取り、Base64文字列にエンコードします。
 */
async function encodeFileToBase64(bucketName, fileName, storage) {
    console.log(`Downloading file: ${fileName}`);
    const file = storage.bucket(bucketName).file(fileName);
    const [contents] = await file.download();
    return contents.toString('base64');
}

/**
 * Gemini APIを使用して画像の内容を分析し、指定されたJSON形式で出力を求めます。
 */
async function analyzeImageWithGemini(base64Image, vertex_ai, model) {
    console.log('Calling Gemini API for analysis...');
    
    // ... (プロンプトとリクエストは省略せずに元のコードをそのまま使ってください)
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
    
    const textResponse = response.candidates[0].content.parts[0].text.trim();
    return JSON.parse(textResponse);
}

/**
 * GCS上のCSVファイルにデータを追記します。
 */
async function appendDataToCsvFile(data, csvFileName, storage) {
    const bucket = storage.bucket(INPUT_BUCKET_NAME);
    const file = bucket.file(csvFileName);
    
    // ... (CSV書き込みロジックは省略せずに元のコードをそのまま使ってください)

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
 */
async function renameFile(bucketName, fileName, storage, isError = false) {
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);
    
    const prefix = isError ? '[ERROR_PROCESSED]' : '[PROCESSED]';
    const newFileName = `${prefix}${fileName}`;

    await file.rename(newFileName);
    console.log(`Renamed ${fileName} to ${newFileName}`);
}

// ... (convertJsonToCsvData 関数は変更不要ですが、前の回答のコードには含まれていました)
function convertJsonToCsvData(jsonOutput, sourceFileName) {
    const data = [
        sourceFileName,
        jsonOutput.store_name,
        jsonOutput.total_amount,
        jsonOutput.transaction_date
    ];
    return [data];
}