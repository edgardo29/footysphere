import os
import sys
import requests

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../config")))

from credentials import API_KEYS

def test_api_connection():
    """
    Tests the connection to the football API using the API key.
    Sends a request to a basic endpoint and prints the response status.
    """
    api_key = API_KEYS.get("api_football")
    if not api_key:
        print("API key not found in credentials.")
        return

    # Define the test endpoint
    api_url = "https://v3.football.api-sports.io/status"  # Example endpoint
    headers = {"x-rapidapi-key": api_key}

    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            print("API connection successful!")
            print("Response:", response.json())
        else:
            print(f"API connection failed with status code: {response.status_code}")
            print("Response:", response.text)
    except Exception as e:
        print("Error connecting to the API:", e)

if __name__ == "__main__":
    print("Testing API connection...")
    test_api_connection()
