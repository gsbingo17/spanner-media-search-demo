import gradio as gr
from google.cloud import spanner

import vertexai
from vertexai.vision_models import Image, MultiModalEmbeddingModel

from urllib.parse import quote

# Initialize Spanner client and database connection
spanner_client = spanner.Client()
instance_id = "vector-db"
database_id = "vectordb"
database = spanner_client.instance(instance_id).database(database_id)

# Set up Vertex AI client and load the multimodal embedding model
PROJECT_ID = "your-project-id"
vertexai.init(project=PROJECT_ID,location="us-central1")
model = MultiModalEmbeddingModel.from_pretrained("multimodalembedding")

# Function to convert GCS URI to authenticated URL
def gcs_to_authenticated_url(gcs_uri):
  """Converts a GCS URI to an authenticated URL.

  Args:
    gcs_uri: The GCS URI in the format "gs://bucket-name/object-name".

  Returns:
    The authenticated URL for the GCS object.
  """
  bucket_name, object_name = gcs_uri[5:].split("/", 1)
  return f"https://storage.googleapis.com/{bucket_name}/{quote(object_name)}"


# Function to perform vector search
def vector_search(query_vector, top_k=4):
    # Construct the SQL query with parameterized vector comparison
    sql = f"""
        SELECT gcs_image_path, imageNewEmbedding
        FROM Images@{{FORCE_INDEX=ImageEmbeddingIndexCos}}
        where imageNewEmbedding IS NOT NULL
        ORDER BY APPROX_COSINE_DISTANCE(
        {query_vector}, imageNewEmbedding,
        options => JSON'{{"num_leaves_to_search": 1000}}')
        LIMIT {top_k}
    """
    with database.snapshot() as snapshot:
        results = list(snapshot.execute_sql(sql))
    return results

# Function to generate embeddings using a Gemini Vertex AI multimodal embedding API
def generate_embedding(input_data, data_type="image"):
    """Generates embeddings using a Gemini Vertex AI multimodal embedding API.

    Args:
        input_data: The input data for embedding generation.
                     - If data_type is "image", this should be the image path or image bytes.
                     - If data_type is "text", this should be the text string.
        data_type:  The type of input data ("image" or "text"). Defaults to "image".

    Returns:
        list: The generated embedding vector.
    """

    if data_type == "image":
        image = Image.load_from_file(input_data)
        embeddings = model.get_embeddings(
            image=image,
            dimension=1408,  # Adjust dimension if needed
        )
        return embeddings.image_embedding
    elif data_type == "text":
        embeddings = model.get_embeddings(
            contextual_text=input_data,
            dimension=1408,  # Adjust dimension if needed
        )
        return embeddings.text_embedding
    else:
        raise ValueError("Invalid data_type. Must be 'image' or 'text'.")


# Function to handle media upload and search
def media_search(image):
    # Implement image embedding generation using your preferred model
    image_embedding = generate_embedding(image, data_type="image")
    
    # Perform vector search
    results = vector_search(image_embedding)

    # Extract gcs path from results
    image_urls = [gcs_to_authenticated_url(result[0]) for result in results]

    # Separate image and video URLs
    image_urls_with_none = [url for url in image_urls if url.endswith(('.jpg', '.jpeg', '.png', '.gif'))]
    video_urls_with_none = [url for url in image_urls if url.endswith(('.mp4', '.avi', '.mov', '.mkv'))]
    # Ensure the output is always a list of tuples with two elements
    if not image_urls_with_none:
        image_urls_with_none = []
    if not video_urls_with_none:
        video_urls_with_none = [None, None]
    return image_urls_with_none, video_urls_with_none[0]

# Function to handle text search
def text_search(query):
    # Implement text embedding generation using your preferred model
    text_embedding = generate_embedding(query, data_type="text")
    # Perform vector search
    results = vector_search(text_embedding)
    # Extract image URLs from results
    image_urls = [gcs_to_authenticated_url(result[0]) for result in results]
    # Separate image and video URLs
    image_urls_with_none = [url for url in image_urls if url.endswith(('.jpg', '.jpeg', '.png', '.gif'))]
    video_urls_with_none = [(url, '') for url in image_urls if url.endswith(('.mp4', '.avi', '.mov', '.mkv'))]
    # Ensure the output is always a list of tuples with two elements
    if not image_urls_with_none:
        image_urls_with_none = []
    if not video_urls_with_none:
        video_urls_with_none = [None, None]
    return image_urls_with_none, video_urls_with_none

# Define the Gradio interface
with gr.Blocks(css=".image-gallery img { width: 360px; height: 360px; object-fit: cover; }") as demo:
    gr.Markdown("## Media Search App")

    with gr.Tab("Media Search"):
        image_input = gr.Image(label="Upload Image", type="filepath")
        image_output = gr.Gallery(label="Similar Images")
        video_output = gr.Video(label="Similar Videos", height=360, width=640)
        image_button = gr.Button("Search")
        image_button.click(fn=media_search, inputs=image_input, outputs=[image_output, video_output])

    with gr.Tab("Text Search"):
        text_input = gr.Textbox(label="Enter Search Query")
        text_output = gr.Gallery(label="Search Results")
        video_output = gr.Video(label="Similar Videos", height=360, width=640)
        text_button = gr.Button("Search")
        text_button.click(fn=text_search, inputs=text_input, outputs=[text_output, video_output])

# Launch the Gradio app
demo.launch()