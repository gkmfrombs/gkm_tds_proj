import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()


# Retrieve your API token from the environment
AIPROXY_TOKEN = os.getenv("AIPROXY_TOKEN")
print(AIPROXY_TOKEN[:5])
if not AIPROXY_TOKEN:
    raise EnvironmentError("AIPROXY_TOKEN is not set in environment variables.")

def chat_completion(prompt: str):
    url = "https://aiproxy.sanand.workers.dev/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AIPROXY_TOKEN}"
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 100
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()  # Ensure that the API response is correctly returned.
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error during API request: {str(e)}")


#################################################################
def generate_embeddings(texts:str):
    """
    Given a list of texts, generate embeddings using the AI Proxy service.

    Args:
        texts (list): A list of text strings to generate embeddings for.

    Returns:
        list: A list of embedding vectors for the input texts.
    """
    # print("Generating embeddings for ...: ", texts[:15])
    url = "https://aiproxy.sanand.workers.dev/openai/v1/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AIPROXY_TOKEN}"
    }
    payload = {
        "model": "text-embedding-3-small",
        "input": texts
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error during API request: {str(e)}")
