import os
from datetime import datetime
from typing import Optional
import hashlib
from urllib.parse import urljoin

from config import get_settings
from azure.storage.blob import ContainerClient
settings = get_settings()


class AzureBlobStorage:
    """
    Azure Blob Storage backend using SAS token authentication.
    Provides methods for uploading, downloading, and managing blobs.
    """
    
    def __init__(self, sas_url: str, sas_token: str, container_name: str):
        """
        Initialize Azure Blob Storage with SAS authentication.
        
        Args:
            sas_url: Full SAS URL with token for the container
            sas_token: SAS token for authentication
            container_name: Name of the blob container
        """
        
        self.sas_url = sas_url
        self.sas_token = sas_token
        self.container_name = container_name
        
        # Initialize container client using SAS URL
        self.container_client = ContainerClient.from_container_url(sas_url)
    
    def upload(self, file_path: str, data: bytes) -> str:
        """
        Upload file to Azure Blob.
        
        Args:
            file_path: Path within the container to store the file
            data: File content as bytes
            
        Returns:
            The blob path within the container
        """
        blob_client = self.container_client.get_blob_client(file_path)
        blob_client.upload_blob(data, overwrite=True)
        return file_path
    
    def upload_from_file(self, source_path: str, dest_path: str) -> str:
        """
        Upload from an existing local file to Azure Blob.
        
        Args:
            source_path: Local file path
            dest_path: Destination path in blob storage
            
        Returns:
            The blob path within the container
        """
        with open(source_path, 'rb') as f:
            return self.upload(dest_path, f.read())
    
    def get_url(self, file_path: str, expiry_hours: int = 24) -> str:
        """
        Get the full URL for accessing a blob.
        Uses the container SAS token for authentication.
        
        Args:
            file_path: Path to the blob within the container
            expiry_hours: Not used for container-level SAS (token expiry is fixed)
            
        Returns:
            Full URL with SAS token for accessing the blob
        """
        # Parse the base URL from SAS URL (remove query params)
        base_url = self.sas_url.split('?')[0]
        
        # Construct the full blob URL with SAS token
        blob_url = f"{base_url}/{file_path}?{self.sas_token}"
        return blob_url
    
    def delete(self, file_path: str) -> bool:
        """
        Delete blob from Azure.
        
        Args:
            file_path: Path to the blob to delete
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            blob_client = self.container_client.get_blob_client(file_path)
            blob_client.delete_blob()
            return True
        except Exception:
            return False
    
    def exists(self, file_path: str) -> bool:
        """
        Check if blob exists.
        
        Args:
            file_path: Path to the blob to check
            
        Returns:
            True if exists, False otherwise
        """
        blob_client = self.container_client.get_blob_client(file_path)
        return blob_client.exists()
    
    def download(self, file_path: str) -> bytes:
        """
        Download blob content as bytes.
        
        Args:
            file_path: Path to the blob to download
            
        Returns:
            Blob content as bytes
        """
        blob_client = self.container_client.get_blob_client(file_path)
        return blob_client.download_blob().readall()

    def download_to_file(self, file_path: str, dest_path: str) -> str:
        """
        Download blob to a local file.
        
        Args:
            file_path: Path to the blob to download
            dest_path: Local destination path
            
        Returns:
            Local path where file was saved
        """
        # Ensure destination directory exists
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        
        blob_client = self.container_client.get_blob_client(file_path)
        with open(dest_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        
        return dest_path
    
    def list_blobs(self, prefix: Optional[str] = None) -> list:
        """
        List blobs in the container.
        
        Args:
            prefix: Optional prefix to filter blobs
            
        Returns:
            List of blob names
        """
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blobs]


def get_storage() -> AzureBlobStorage:
    """
    Factory function to get Azure Blob Storage backend.
    Configured via environment variables.
    """
    return AzureBlobStorage(
        sas_url=settings.sas_url,
        sas_token=settings.sas_token,
        container_name=settings.azure_blob_name
    )


def generate_storage_path(original_url: str, prefix: str = "processed") -> str:
    """
    Generate organized storage path from original URL.
    Format: processed/{year}/{month}/{day}/{hash}.jpg
    
    Args:
        original_url: Original image URL to hash
        prefix: Prefix for the storage path
        
    Returns:
        Organized storage path
    """
    now = datetime.utcnow()
    url_hash = hashlib.md5(original_url.encode()).hexdigest()
    
    return f"{prefix}/{now.year}/{now.month:02d}/{now.day:02d}/{url_hash}.jpg"
