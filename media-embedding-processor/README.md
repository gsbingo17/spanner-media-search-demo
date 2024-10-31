# Media Embedding Processor

This project processes media embeddings from images and videos stored in Google Cloud Storage (GCS) and stores the embedding information in a Google Cloud Spanner database.

## Prerequisites

- Node.js
- Google Cloud SDK
- Google Cloud Spanner instance and database
- Google Cloud Storage bucket with videos

## Setup

1. Install the dependencies

   ```sh
   cd media-embedding-processor
   npm install
   ```

2. Download and split the CC12M datasets

  ```sh
  wget https://storage.googleapis.com/conceptual_12m/cc12m.tsv
  split -b 100m cc12m.tsv cc12m_part -d
  ```

3. Prepare Cloud Spanner: Create a Google Cloud Spanner instance and database. Then, create the table for storing the embedding information:

```SQL
CREATE TABLE Images (
  id STRING(MAX) NOT NULL,
  image_url STRING(MAX) NOT NULL,
  description STRING(MAX),
  gcs_image_path STRING(MAX),
  imageNewEmbedding ARRAY<FLOAT64>(vector_length=>1408),
  descriptionTokens TOKENLIST AS (TOKENIZE_FULLTEXT(description)) HIDDEN,
) PRIMARY KEY(id);;
```

4. Download and Upload Images: Download the images from CC12M databsets, and upload them to GCS; Using the hash value of description as the ID to prevent data duplication

```sh
node index.js
```

Step 4 : Downloading images from the internet can be time-consuming due to network latency and potential broken URLs.

5. Generate and Store Embeddings: Query the gcs_image_path for each image and invoke the embedding SDK to generate and store the embedding value in the imageNewEmbedding column.

```sh
node embedding.js

// In improve the performance, you can use pm2 to execute the embedding.js with multiple tasks.
pm2 start embedding.js -i 10
```

Step 5 : Generating embeddings using the API can be slow due to request quota limits. Consider increasing the quota or using multiple endpoints in parallel to expedite this step.

6. Create Vector Index: After processing all the embeddings, create a vector index in the table Images. 

```SQL
CREATE VECTOR INDEX ImageEmbeddingIndexCos ON Images(imageNewEmbedding) WHERE imageNewEmbedding IS NOT NULL OPTIONS (
  tree_depth = 2,
  num_leaves = 1500,
  distance_type = 'COSINE'
);
```

7. Process Videos: The script embedding-video.js can be used to process the videos stored in GCS.

```sh
node embedding-video.js
```
