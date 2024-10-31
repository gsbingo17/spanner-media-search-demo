const { Spanner } = require('@google-cloud/spanner');
const { GoogleAuth } = require('google-auth-library');

// Your GCP project ID
const projectId = 'your-project-id'; // Replace with your project ID

// Spanner configuration
const spanner = new Spanner({
  projectId: projectId,
});
const instanceId = 'vector-db'; // Replace with your Spanner instance ID
const databaseId = 'vectordb'; // Replace with your Spanner database ID

const gcsUri = 'gs://gb-cc12m/BigBuckBunny.mp4'
// Extract video name from gcsUri
const videoName = gcsUri.split('/').pop().split('.')[0];

// Vertex AI Endpoint
// https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-multimodal-embeddings#img-txt-vid-embedding
const endpoint = `https://us-central1-aiplatform.googleapis.com/v1/projects/${projectId}/locations/us-central1/publishers/google/models/multimodalembedding@001:predict`; // Replace with your endpoint ID

function convertGCSToHttps(gcsUri) {
  // Check if the input starts with 'gs://'
  if (!gcsUri.startsWith('gs://')) {
    throw new Error('Invalid GCS URI format. Must start with "gs://".');
  }

  // Replace 'gs://' with 'https://storage.mtls.cloud.google.com/'
  // https://storage.googleapis.com/gb-cc12m/BigBuckBunny.mp4
  const httpsUri = gcsUri.replace('gs://', 'https://storage.googleapis.com/');

  return httpsUri;
}

async function getMultimodalEmbeddings(gcsUri) {
    try {
      const auth = new GoogleAuth({scopes: "https://www.googleapis.com/auth/cloud-platform",});
      const client = await auth.getClient();
      const accessToken = await client.getAccessToken();

      const requestBody = {
        instances: [
          {
            video: {
              gcsUri: gcsUri,
            },
          },
        ],
      };

      const response = await client.request({
        url: endpoint,
        method: 'POST',
        data: requestBody,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      return response.data.predictions[0].videoEmbeddings;
    } catch (error) {
      console.error('Error getting multimodal embeddings:', error);
      throw error;
    }
  }

  async function insertVideoEmbeddings(dbClient) { 
    try {
      const videoEmbeddings = await getMultimodalEmbeddings(gcsUri);

      await dbClient.runTransactionAsync(async (transaction) => {
        for (const embedding of videoEmbeddings) {
          const {startOffsetSec, endOffsetSec, embedding: embeddingArray} = embedding;
  
          const insertQuery = {
            sql: `INSERT INTO Images (id, image_url,gcs_image_path, description, imageNewEmbedding) VALUES (@id, @image_url, @gcs_image_path, @description, @imageNewEmbedding)`,
            params: {
              id: `${videoName}_${startOffsetSec}_${endOffsetSec}`,
              image_url: convertGCSToHttps(gcsUri),
              gcs_image_path: gcsUri,
              description: 'Bin takes a video to do the test',
              imageNewEmbedding: embeddingArray,
            },
          };
  
          await transaction.runUpdate(insertQuery);
          console.log(`Inserted embedding for ID=${videoName}, startOffsetSec=${startOffsetSec}, endOffsetSec=${endOffsetSec}`);
        }
        await transaction.commit();
      });
    } catch (error) {
      console.error(`Error processing row ID=${videoName}:`, error);
    }
  }

  async function processEmbeddings() {
    const dbClient = await spanner.instance(instanceId).database(databaseId);
    await insertVideoEmbeddings(dbClient);
  }

  processEmbeddings();


