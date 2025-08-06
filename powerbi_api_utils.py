# powerbi_api_utils.py

import requests
import json
import time
import os
import logging
import httpx # Use httpx for async requests
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Loading (No changes needed) ---
def load_config():
    """Loads configuration from config.json."""
    try:
        script_dir = os.path.dirname(__file__)
        config_path = os.path.join(script_dir, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Error: config.json not found at {config_path}. Please create the file.")
        raise
    except json.JSONDecodeError:
        logging.error("Error: Invalid JSON in config.json. Please check the file's syntax.")
        raise

def get_api_constants(tenant_id):
    """Returns Power BI API constants based on tenant ID."""
    AUTHORITY = f"https://login.microsoftonline.com/{tenant_id}"
    SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    ADMIN_BASE_URL = "https://api.powerbi.com/v1.0/myorg/admin"
    return AUTHORITY, SCOPE, BASE_URL, ADMIN_BASE_URL

# --- Power BI API Authentication (No changes needed) ---
def get_access_token(client_id, client_secret, authority, scope):
    """Obtains an AAD access token using client credentials flow."""
    token_url = f"{authority}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": " ".join(scope),
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    logging.info(f"Requesting access token from {token_url}...")
    try:
        response = requests.post(token_url, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("Access token not found in the response.")
        logging.info("Access token obtained successfully.")
        return access_token
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error getting access token: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred getting access token: {e}")
        raise

# --- Asynchronous Power BI API Data Fetching ---
# FIX: Increased max_retries and backoff_factor to better handle throttling (HTTP 429)
async def get_paginated_data_async(client: httpx.AsyncClient, url: str, headers: dict, params: dict = None, max_retries: int = 5, backoff_factor: float = 2.0):
    """
    Asynchronously fetches data from Power BI API endpoints that support pagination,
    with more robust retries for throttling.
    """
    all_data = []
    next_page_url = url
    
    while next_page_url:
        for attempt in range(max_retries):
            try:
                current_params = params if next_page_url == url else None
                response = await client.get(next_page_url, headers=headers, params=current_params, timeout=45.0)
                response.raise_for_status()
                data = response.json()
                
                value = data.get('value', [])
                if isinstance(value, list):
                    all_data.extend(value)
                else:
                    all_data.append(value)
                    next_page_url = None
                    break

                next_page_url = data.get('@odata.nextLink')
                break

            except httpx.HTTPStatusError as e:
                # Retry on rate limiting (429) or common server errors (5xx)
                if e.response.status_code in [429, 502, 503, 504] and attempt < max_retries - 1:
                    sleep_time = backoff_factor * (2 ** attempt)
                    logging.warning(f"HTTP error {e.response.status_code} on {url}. Retrying in {sleep_time:.2f}s...")
                    await asyncio.sleep(sleep_time)
                else:
                    logging.error(f"HTTP error fetching {url}: {e.response.status_code} - {e.response.text}")
                    raise
            except Exception as e:
                logging.error(f"An unexpected error occurred fetching {url}: {e}")
                raise
        else:
             logging.error(f"Max retries exceeded for {url}.")
             break

    return all_data