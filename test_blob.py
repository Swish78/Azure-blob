import os
import time
from storage import get_storage, AzureBlobStorage

def run_test():
    print("----------------------------------------------------------------")
    print("Starting Azure Blob Storage Test")
    print("----------------------------------------------------------------")
    
    # 1. Initialize Storage
    try:
        store = get_storage()
        print("[SUCCESS] Initialized AzureBlobStorage")
    except Exception as e:
        print(f"[FAILED] Could not initialize storage: {e}")
        return

    # 2. Create a dummy file
    test_content = b"Hello, Azure Blob World! This is a test file."
    local_test_file = "temp_test_file.txt"
    with open(local_test_file, "wb") as f:
        f.write(test_content)
    print(f"[INFO] Created local test file: {local_test_file}")

    # 3. Define blob path (simulating folder structure)
    # folder/subfolder/filename
    blob_path = "test_folder/subfolder/hello_blob.txt"
    
    # 4. Upload
    try:
        print(f"[INFO] Uploading to: {blob_path}")
        uploaded_path = store.upload_from_file(local_test_file, blob_path)
        print(f"[SUCCESS] Uploaded to: {uploaded_path}")
    except Exception as e:
        print(f"[FAILED] Upload failed: {e}")
        # Clean up local file even if upload fails
        if os.path.exists(local_test_file):
            os.remove(local_test_file)
        return

    # 5. Verify Exists
    if store.exists(blob_path):
        print(f"[SUCCESS] Blob exists: {blob_path}")
    else:
        print(f"[FAILED] Blob does not exist after upload: {blob_path}")

    # 6. Download to new file
    downloaded_file = "downloaded_test_file.txt"
    try:
        print(f"[INFO] Downloading to: {downloaded_file}")
        store.download_to_file(blob_path, downloaded_file)
        print(f"[SUCCESS] Downloaded file.")
        
        # Verify content
        with open(downloaded_file, "rb") as f:
            content = f.read()
            if content == test_content:
                print("[SUCCESS] Content match verified!")
            else:
                print(f"[FAILED] Content mismatch! Expected: {test_content}, Got: {content}")
    except Exception as e:
        print(f"[FAILED] Download failed: {e}")

    # 7. Get URL (just to check)
    try:
        url = store.get_url(blob_path)
        print(f"[INFO] Generated SAS URL: {url}")
    except Exception as e:
        print(f"[FAILED] URL generation failed: {e}")

    # 8. Clean up
    print("[INFO] Cleaning up...")
    try:
        # Delete blob
        if store.delete(blob_path):
            print(f"[SUCCESS] Deleted blob: {blob_path}")
        else:
            print(f"[FAILED] Could not delete blob: {blob_path}")
            
        # Delete local files
        if os.path.exists(local_test_file):
            os.remove(local_test_file)
        if os.path.exists(downloaded_file):
            os.remove(downloaded_file)
        print("[SUCCESS] Local files cleaned up.")
        
    except Exception as e:
        print(f"[FAILED] Cleanup failed: {e}")

    print("----------------------------------------------------------------")
    print("Test Completed")
    print("----------------------------------------------------------------")

if __name__ == "__main__":
    run_test()
