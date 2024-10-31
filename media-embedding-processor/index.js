const crypto = require('crypto');
const { Spanner } = require('@google-cloud/spanner');
const { Storage } = require('@google-cloud/storage');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

// Your Spanner and GCS configuration
const spanner = new Spanner({
  projectId: 'your-project-id',
});
const instance = spanner.instance('vector-db');
const database = instance.database('vectordb');
const tableName = 'Images';

const storage = new Storage({
  projectId: 'your-project-id',
});
const bucketName = 'your-bucket-name';
const bucket = storage.bucket(bucketName);

const tsvFilePaths = [
  'cc12m_part00.tsv',
  'cc12m_part01.tsv',
  'cc12m_part02.tsv',
  'cc12m_part03.tsv',
  'cc12m_part04.tsv',
  'cc12m_part05.tsv',
  'cc12m_part06.tsv',
  'cc12m_part07.tsv',
  'cc12m_part08.tsv',
  'cc12m_part09.tsv',
  'cc12m_part10.tsv',
  'cc12m_part11.tsv',
  'cc12m_part12.tsv',
  'cc12m_part13.tsv',
  'cc12m_part14.tsv',
  'cc12m_part15.tsv',
  'cc12m_part16.tsv',
  'cc12m_part17.tsv',
  'cc12m_part18.tsv',
  'cc12m_part19.tsv',
  'cc12m_part20.tsv',
  'cc12m_part21.tsv',
  'cc12m_part22.tsv',
  'cc12m_part23.tsv',
  'cc12m_part24.tsv',
  'cc12m_part25.tsv'
];

function logErrorToFile(error) {
  const logFilePath = path.join(__dirname, 'error.log');
  const errorMessage = `${new Date().toISOString()} - ${error.message}\n${error.stack}\n\n`;
  fs.appendFile(logFilePath, errorMessage, (err) => {
    if (err) {
      console.error('Failed to write error to log file:', err);
    }
  });
}

// Function to generate a hash from the description
function generateHash(description) {
  return crypto.createHash('md5').update(description).digest('hex');
}

async function downloadImage(url, filePath) {
  const client = url.startsWith('https') ? https : http;

  return new Promise((resolve, reject) => {
    const request = client.get(url, { timeout: 5000 }, (response) => {
      if (response.statusCode !== 200) {
        return reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
      }

      const writer = fs.createWriteStream(filePath);
      response.pipe(writer);
      writer.on('finish', resolve);
      writer.on('error', reject);
    });

    request.on('error', reject);
    request.on('timeout', () => {
      request.destroy();
      reject(new Error('Request timed out'));
    });
  });
}

async function uploadImageToGCS(filePath, destFileName) {
  await bucket.upload(filePath, {
    destination: destFileName,
  });
  console.log(`Uploaded ${filePath} to ${bucketName}/${destFileName}`);
}

async function processFile(tsvFilePath) {
  const fileStream = fs.createReadStream(tsvFilePath);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  for await (const row of rl) {
    if (row.trim() !== '') {
      try {
        const [imageUrl, description] = row.split('\t');
              // console.log(`${tsvFilePath} : ${imageUrl} at ${new Date().toString()}`);
        const id = generateHash(description); // Use hash of the description as the ID

        // Download the image
        const fileName = `${id}.jpg`;
        const tempFilePath = `/tmp/${fileName}`;
        await downloadImage(imageUrl, tempFilePath);

        // Upload to GCS
        await uploadImageToGCS(tempFilePath, fileName);

        // Delete the temporary file
        fs.unlink(tempFilePath, (err) => {
          if (err) {
            console.error(`Error deleting temporary file ${tempFilePath}:`, err);
          } else {
          //  console.log(`Temporary file ${tempFilePath} deleted successfully`);
          }
        });

        // Insert into Spanner
        const query = `INSERT INTO ${tableName} (id, image_url, description, gcs_image_path) VALUES (@id, @imageUrl, @description, @gcsImagePath)`;
        const params = {
          id: id,
          imageUrl: imageUrl,
          description: description,
          gcsImagePath: `gs://${bucketName}/${fileName}`
        };

        await database.runTransactionAsync(async (transaction) => {
          try {
            // console.log('Process the image from: ', params.imageUrl);
            const [rowCount] = await transaction.runUpdate({
              sql: query,
              params: params
            });
            // console.log(`Update affected ${rowCount} rows`);
            await transaction.commit();
            console.log(`# ${tsvFilePath} : ${imageUrl} inserted into DB at ${new Date().toString()}`);
          } catch (err) {
            logErrorToFile(err);
            // console.error('Error inserting into Spanner:', err);
            // Consider rolling back the transaction on error:
            // await transaction.rollback();
          }
        });

      } catch (err) {
        logErrorToFile(err);
        //console.error('Error processing row:', err);
      }
    }
}
}

async function importFilesInParallel() {
  const concurrencyLimit = 10; // Adjust concurrency limit as needed
  const promises = tsvFilePaths.map(filePath => {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, { workerData: filePath });
      worker.on('message', resolve);
      worker.on('error', reject);
      worker.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Worker stopped with exit code ${code}`));
        }
      });
    });
  });

  for (let i = 0; i < promises.length; i += concurrencyLimit) {
    await Promise.all(promises.slice(i, i + concurrencyLimit));
  }

  console.log('All files have been processed.');
}

if (isMainThread) {
  importFilesInParallel().catch(logErrorToFile);
} else {
  processFile(workerData).then(() => parentPort.postMessage('done')).catch(logErrorToFile);
}