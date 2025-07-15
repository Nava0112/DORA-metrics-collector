# github_auth.py
import os
import jwt
import time
import requests

def generate_jwt():
    app_id = os.getenv("GITHUB_APP_ID")
    private_key_path = os.getenv("GITHUB_PRIVATE_KEY_PATH")

    with open(private_key_path, "r") as f:
        private_key = f.read()

    payload = {
        'iat': int(time.time()) - 60,
        'exp': int(time.time()) + (10 * 60),
        'iss': app_id
    }

    token = jwt.encode(payload, private_key, algorithm='RS256')
    return token

def get_installation_token():
    jwt_token = generate_jwt()
    installation_id = os.getenv("GITHUB_INSTALLATION_ID")

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json"
    }

    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]
