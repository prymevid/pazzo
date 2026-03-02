#!/usr/bin/env python3
"""
Move a random file from instagram/store to instagram/await/1.mp4
"""

import os
import sys
import random
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Hardcoded R2 Configuration
R2_ACCESS_KEY = "cce1f2c14e7a447616404e8a2e885265"
R2_SECRET_KEY = "33fa236d4d58c8d4c8f507888deccd0c667a95eac9c818c3499b7a4a60595ccb"
R2_ACCOUNT_ID = "0d0a0a287282172b39fb04d9334d8346"
R2_BUCKET_NAME = "store"

def get_r2_client():
    """Get R2 client."""
    try:
        return boto3.client(
            's3',
            endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto'
        )
    except Exception as e:
        print(f"❌ Error creating R2 client: {e}", file=sys.stderr)
        return None

def list_files_in_folder(client, folder_path):
    """List all files in a specific folder."""
    if not client:
        return []
    
    try:
        prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
        
        list_args = {
            'Bucket': R2_BUCKET_NAME,
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
                    
                    # Skip folder placeholders
                    if key.endswith('/'):
                        continue
                    
                    files.append(key)
            
            if not response.get('IsTruncated', False):
                break
            
            continuation_token = response.get('NextContinuationToken')
        
        print(f"📂 Found {len(files)} files in {folder_path}")
        return files
    
    except ClientError as e:
        print(f"❌ Error listing files: {e}", file=sys.stderr)
        return []

def delete_file(client, key):
    """Delete a file."""
    if not client:
        return False
    
    try:
        client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
        print(f"✅ Deleted: {key}")
        return True
    except ClientError as e:
        print(f"❌ Error deleting {key}: {e}", file=sys.stderr)
        return False

def copy_file(client, source_key, destination_key):
    """Copy a file."""
    if not client:
        return False
    
    try:
        copy_source = {'Bucket': R2_BUCKET_NAME, 'Key': source_key}
        client.copy_object(
            CopySource=copy_source,
            Bucket=R2_BUCKET_NAME,
            Key=destination_key
        )
        print(f"✅ Copied: {source_key} → {destination_key}")
        return True
    except ClientError as e:
        print(f"❌ Error copying {source_key}: {e}", file=sys.stderr)
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
    """Save result to JSON file."""
    try:
        with open('result.json', 'w') as f:
            json.dump(result, f, indent=2)
        print(f"📝 Result saved to result.json")
    except Exception as e:
        print(f"❌ Error saving result: {e}", file=sys.stderr)

def test_connection(client):
    """Test the R2 connection."""
    if not client:
        return False
    
    try:
        client.head_bucket(Bucket=R2_BUCKET_NAME)
        print("✅ Connected to R2 bucket successfully")
        return True
    except ClientError as e:
        print(f"❌ Error connecting to bucket: {e}", file=sys.stderr)
        return False

def main():
    """Main function."""
    print("="*60)
    print("🚀 R2 File Mover Started")
    print("="*60)
    
    result = {
        "success": False,
        "message": "",
        "selected_file": None,
        "deleted_from": None,
        "copied_to": None,
        "cleaned_count": 0,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        # Initialize client
        print("\n🔧 Initializing R2 client...")
        client = get_r2_client()
        
        # Test connection
        if not test_connection(client):
            result["message"] = "Failed to connect to R2 bucket"
            save_result(result)
            sys.exit(1)
        
        # Step 1: List files in source folder
        print("\n📁 Step 1: Listing source folder...")
        source_prefix = "tiktok/store/"
        files = list_files_in_folder(client, source_prefix)
        
        if not files:
            result["message"] = "No files found in instagram/store folder"
            print("⚠️ No files found in source folder")
            save_result(result)
            sys.exit(0)
        
        # Step 2: Select random file
        print("\n🎲 Step 2: Selecting random file...")
        selected_file = random.choice(files)
        result["selected_file"] = selected_file
        print(f"Selected: {selected_file}")
        
        # Step 3: Clean await folder
        print("\n🧹 Step 3: Cleaning destination folder...")
        await_prefix = "tiktok/await/"
        result["cleaned_count"] = clean_folder(client, await_prefix)
        print(f"Cleaned {result['cleaned_count']} files")
        
        # Step 4: Copy to destination
        print("\n📋 Step 4: Copying file...")
        destination_key = "tiktok/await/1.mp4"
        if copy_file(client, selected_file, destination_key):
            result["copied_to"] = destination_key
        else:
            raise Exception("Failed to copy file")
        
        # Step 5: Delete original
        print("\n🗑️ Step 5: Deleting original...")
        if delete_file(client, selected_file):
            result["deleted_from"] = selected_file
        else:
            raise Exception("Failed to delete original file")
        
        result["success"] = True
        result["message"] = f"✅ Successfully moved {selected_file} to {destination_key}"
        print(f"\n✅ {result['message']}")
        
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ Error: {error_msg}", file=sys.stderr)
        result["message"] = f"Error: {error_msg}"
    
    finally:
        # Always save the result
        print("\n💾 Saving result...")
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
