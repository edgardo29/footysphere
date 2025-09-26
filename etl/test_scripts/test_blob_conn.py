import os
import sys
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../config")))

from credentials import AZURE_STORAGE

def test_blob_connection():
    """
    Tests the connection to Azure Blob Storage and lists available containers.
    """
    try:
        # Create a BlobServiceClient using the account name and key
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

        # List all containers in the storage account
        containers = blob_service_client.list_containers()
        print("Connected to Azure Blob Storage. Containers:")
        for container in containers:
            print(f"- {container['name']}")
    except AzureError as e:
        print("Error connecting to Azure Blob Storage:", e)

if __name__ == "__main__":
    print("Testing Azure Blob Storage connection...")
    test_blob_connection()
