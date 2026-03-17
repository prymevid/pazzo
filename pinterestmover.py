#!/usr/bin/env python3
"""
Move a random file from tiktok/store to tiktok/await/1.mp4
All configurable settings are at the top of this file.
"""

import os
import sys
import random
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# =============================================================================
# CONFIGURATION - EDIT THESE VALUES
# =============================================================================

# R2 Cloudflare Configuration
R2_CONFIG = {
    "access_key": "cce1f2c14e7a447616404e8a2e885265",
    "secret_key": "33fa236d4d58c8d4c8f507888deccd0c667a95eac9c818c3499b7a4a60595ccb",
    "account_id": "0d0a0a287282172b39fb04d9334d8346",
    "bucket_name": "store",
    "region": "auto"  # Default region for R2
}

# Folder paths (don't add leading/trailing slashes)
SOURCE_CONFIG = {
    "base_folder": "tiktok/store",  # Source folder to pick random file from
    "dest_folder": "tiktok/await",  # Destination folder to clean and copy to
    "dest_filename": "1.mp4",       # Filename for the copied file
    "clean_dest_before_copy": True   # Whether to delete all files in dest folder before copying
}

# File naming and filtering
FILE_CONFIG = {
    "skip_placeholders": True,  # Skip folder placeholder files (ending with /)
    "allowed_extensions": [],    # Empty list means allow all extensions
                                # Example: ['.mp4', '.jpg', '.png']
    "min_file_size_bytes": 0     # Minimum file size in bytes (0 = no minimum)
}

# Output and logging
OUTPUT_CONFIG = {
    "save_results_json": True,   # Save results to result.json
    "results_filename": "result.json",
    "verbose_logging": True      # Print detailed progress
}

# =============================================================================
# END OF CONFIGURATION
# =============================================================================

def log(message, level="info"):
    """Log message if verbose logging is enabled."""
    if OUTPUT_CONFIG["verbose_logging"] or level == "error":
        print(message)

def get_r2_client():
    """Get R2 client."""
    try:
        return boto3.client(
            's3',
            endpoint_url=f'https://{R2_CONFIG["account_id"]}.r2.cloudflarestorage.com',
            aws_access_key_id=R2_CONFIG["access_key"],
            aws_secret_access_key=R2_CONFIG["secret_key"],
            region_name=R2_CONFIG["region"]
        )
    except Exception as e:
        log(f"❌ Error creating R2 client: {e}", "error")
        return None

def list_files_in_folder(client, folder_path):
    """List all files in a specific folder with filtering."""
    if not client:
        return []
    
    try:
        prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
        
        list_args = {
            'Bucket': R2_CONFIG["bucket_name"],
            'Prefix': prefix
        }
        
        files = []
        continuation_token = None
        
        while True:
            if continuation_token:
                list_args['ContinuationToken'] = continuation_token
            
            response = client.list_objects_v2(**list_args)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    size = obj.get('Size', 0)
                    
                    # Skip folder placeholders if configured
                    if FILE_CONFIG["skip_placeholders"] and key.endswith('/'):
                        continue
                    
                    # Filter by file size
                    if size < FILE_CONFIG["min_file_size_bytes"]:
                        continue
                    
                    # Filter by extension if configured
                    if FILE_CONFIG["allowed_extensions"]:
                        ext = os.path.splitext(key)[1].lower()
                        if ext not in FILE_CONFIG["allowed_extensions"]:
                            continue
                    
                    files.append(key)
            
            if not response.get('IsTruncated', False):
                break
            
            continuation_token = response.get('NextContinuationToken')
        
        log(f"📂 Found {len(files)} files in {folder_path}")
        return files
    
    except ClientError as e:
        log(f"❌ Error listing files: {e}", "error")
        return []

def delete_file(client, key):
    """Delete a file."""
    if not client:
        return False
    
    try:
        client.delete_object(Bucket=R2_CONFIG["bucket_name"], Key=key)
        log(f"✅ Deleted: {key}")
        return True
    except ClientError as e:
        log(f"❌ Error deleting {key}: {e}", "error")
        return False

def copy_file(client, source_key, destination_key):
    """Copy a file."""
    if not client:
        return False
    
    try:
        copy_source = {'Bucket': R2_CONFIG["bucket_name"], 'Key': source_key}
        client.copy_object(
            CopySource=copy_source,
            Bucket=R2_CONFIG["bucket_name"],
            Key=destination_key
        )
        log(f"✅ Copied: {source_key} → {destination_key}")
        return True
    except ClientError as e:
        log(f"❌ Error copying {source_key}: {e}", "error")
        return False

def clean_folder(client, folder_path):
    """Delete all files in a folder."""
    if not client:
        return 0
    
    files = list_files_in_folder(client, folder_path)
    count = 0
    
    for file in files:
        if delete_file(client, file):
            count += 1
    
    return count

def save_result(result):
    """Save result to JSON file if configured."""
    if not OUTPUT_CONFIG["save_results_json"]:
        return
    
    try:
        with open(OUTPUT_CONFIG["results_filename"], 'w') as f:
            json.dump(result, f, indent=2)
        log(f"📝 Result saved to {OUTPUT_CONFIG['results_filename']}")
    except Exception as e:
        log(f"❌ Error saving result: {e}", "error")

def test_connection(client):
    """Test the R2 connection."""
    if not client:
        return False
    
    try:
        client.head_bucket(Bucket=R2_CONFIG["bucket_name"])
        log("✅ Connected to R2 bucket successfully")
        return True
    except ClientError as e:
        log(f"❌ Error connecting to bucket: {e}", "error")
        return False

def main():
    """Main function."""
    print("="*60)
    print("🚀 R2 File Mover Started")
    print("="*60)
    
    # Display current configuration
    print("\n📋 Current Configuration:")
    print(f"  Source: {SOURCE_CONFIG['base_folder']}")
    print(f"  Destination: {SOURCE_CONFIG['dest_folder']}/{SOURCE_CONFIG['dest_filename']}")
    print(f"  Clean destination before copy: {SOURCE_CONFIG['clean_dest_before_copy']}")
    if FILE_CONFIG["allowed_extensions"]:
        print(f"  Allowed extensions: {', '.join(FILE_CONFIG['allowed_extensions'])}")
    print("-" * 60)
    
    result = {
        "success": False,
        "message": "",
        "selected_file": None,
        "deleted_from": None,
        "copied_to": None,
        "cleaned_count": 0,
        "config": {
            "source": SOURCE_CONFIG["base_folder"],
            "destination": f"{SOURCE_CONFIG['dest_folder']}/{SOURCE_CONFIG['dest_filename']}",
            "bucket": R2_CONFIG["bucket_name"]
        },
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        # Initialize client
        log("\n🔧 Initializing R2 client...")
        client = get_r2_client()
        
        # Test connection
        if not test_connection(client):
            result["message"] = "Failed to connect to R2 bucket"
            save_result(result)
            sys.exit(1)
        
        # Step 1: List files in source folder
        log("\n📁 Step 1: Listing source folder...")
        files = list_files_in_folder(client, SOURCE_CONFIG["base_folder"])
        
        if not files:
            result["message"] = f"No files found in {SOURCE_CONFIG['base_folder']} folder"
            log("⚠️ No files found in source folder")
            save_result(result)
            sys.exit(0)
        
        # Step 2: Select random file
        log("\n🎲 Step 2: Selecting random file...")
        selected_file = random.choice(files)
        result["selected_file"] = selected_file
        log(f"Selected: {selected_file}")
        
        # Step 3: Clean destination folder if configured
        if SOURCE_CONFIG["clean_dest_before_copy"]:
            log("\n🧹 Step 3: Cleaning destination folder...")
            result["cleaned_count"] = clean_folder(client, SOURCE_CONFIG["dest_folder"])
            log(f"Cleaned {result['cleaned_count']} files")
        else:
            log("\n⏭️ Step 3: Skipping destination folder cleanup")
        
        # Step 4: Copy to destination
        log("\n📋 Step 4: Copying file...")
        destination_key = f"{SOURCE_CONFIG['dest_folder']}/{SOURCE_CONFIG['dest_filename']}"
        if copy_file(client, selected_file, destination_key):
            result["copied_to"] = destination_key
        else:
            raise Exception("Failed to copy file")
        
        # Step 5: Delete original
        log("\n🗑️ Step 5: Deleting original...")
        if delete_file(client, selected_file):
            result["deleted_from"] = selected_file
        else:
            raise Exception("Failed to delete original file")
        
        result["success"] = True
        result["message"] = f"✅ Successfully moved {selected_file} to {destination_key}"
        log(f"\n✅ {result['message']}")
        
    except Exception as e:
        error_msg = str(e)
        log(f"\n❌ Error: {error_msg}", "error")
        result["message"] = f"Error: {error_msg}"
    
    finally:
        # Always save the result if configured
        if OUTPUT_CONFIG["save_results_json"]:
            log("\n💾 Saving result...")
            save_result(result)
        
        print("\n" + "="*60)
        print("📊 FINAL RESULT:")
        print(json.dumps(result, indent=2))
        print("="*60)
        
        # Exit with appropriate code
        if result["success"]:
            sys.exit(0)
        else:
            sys.exit(1)

if __name__ == "__main__":
    main()
