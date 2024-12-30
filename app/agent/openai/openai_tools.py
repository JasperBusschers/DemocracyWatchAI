import requests
from config.settings import Config

conf= Config()

def get_embedding(text):
    url = "https://api.openai.com/v1/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {conf.OPEN_AI_KEY}"
    }
    data = {
        "input": text,
        "model": "text-embedding-3-small"
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()['data'][0]['embedding']  # Return the full response, including the embedding
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

def generate_response(system, user, model='gpt-3.5-turbo',max_tokens=1000):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {conf.OPEN_AI_KEY}"
    }
    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "max_tokens": max_tokens,  # Adjust as needed
        "temperature": 0  # Adjust as needed for more creative responses
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()['choices'][0]['message']['content']
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

