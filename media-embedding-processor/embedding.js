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

// Vertex AI Endpoint
const endpoint = `https://us-central1-aiplatform.googleapis.com/v1/projects/${projectId}/locations/us-central1/publishers/google/models/multimodalembedding@001:predict`; // Replace with your endpoint ID

async function getMultimodalEmbeddings(gcsUri) {
  try {
    const auth = new GoogleAuth({scopes: "https://www.googleapis.com/auth/cloud-platform",});
    const client = await auth.getClient();
    const accessToken = await client.getAccessToken();

    const requestBody = {
      instances: [
        {
          image: {
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

    return response.data.predictions[0].imageEmbedding;
  } catch (error) {
    console.log('!!!Error getting multimodal embeddings:');
    throw error;
  }
}

async function updateImageEmbeddings(dbClient, concurrency = 30) { 
  try {
    const query = {
            sql: `SELECT id, gcs_image_path FROM Images where imageNewEmbedding IS NULL`,
    };

    const stream = dbClient.runStream(query);

    // Create a Promise queue to manage concurrency
    const queue = [];
    let activeProcesses = 0;

    stream.on('data', async (row) => {
      // Pause the stream if we've reached the concurrency limit
      if (activeProcesses >= concurrency) {
        stream.pause();
      }

      const rowObject = Object.fromEntries(row.map(item => [item.name, item.value]));
      const { id, gcs_image_path } = rowObject;

      const processPromise = (async () => {
        try {
          activeProcesses++; 
//          console.log(`Processing image with ID: ${id}`);
          const embeddings = await getMultimodalEmbeddings(gcs_image_path);

          await dbClient.runTransactionAsync(async (transaction) => {
            const updateQuery = `UPDATE Images SET imageNewEmbedding = @embeddings WHERE id = @id`;
            const updateParams = {
              embeddings: embeddings,
              id: id,
            };
            await transaction.runUpdate({
              sql: updateQuery,
              params: updateParams,
            });
            await transaction.commit();
          });

          console.log(`Updated embeddings for image with ID: ${id}`);
        } catch (err) {
          console.log(`!!!Error processing image with ID ${id}:`);
        } finally {
          activeProcesses--; 
          // Resume the stream if it was paused and there's room for more
          if (stream.isPaused() && activeProcesses < concurrency) {
            stream.resume();
          }
        }
      })();

      queue.push(processPromise);
    });

    stream.on('error', (error) => {
      console.log('Error reading from Spanner stream:');
    });

    // Wait for all promises in the queue to resolve
    await Promise.all(queue);

    console.log('All images processed.');
  } catch (error) {
    console.log('Error processing embeddings:');
  }
}

async function processEmbeddings() {
  const dbClient = await spanner.instance(instanceId).database(databaseId);
  await updateImageEmbeddings(dbClient);
}

processEmbeddings();