#!/usr/bin/env python3
"""
Move a random file from instagram/store to instagram/await/1.mp4
"""

import os
import sys
import random
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Configuration from environment variables
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'store')

def get_r2_client():
    """Get R2 client from environment variables."""
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )

def list_files_in_folder(client, folder_path):
    """List all files in a specific folder."""
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
        
        return files
    
    except ClientError as e:
        print(f"Error listing files: {e}", file=sys.stderr)
        return []

def delete_file(client, key):
    """Delete a file."""
    try:
        client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        print(f"Error deleting {key}: {e}", file=sys.stderr)
        return False

def copy_file(client, source_key, destination_key):
    """Copy a file."""
    try:
        copy_source = {'Bucket': R2_BUCKET_NAME, 'Key': source_key}
        client.copy_object(
            CopySource=copy_source,
            Bucket=R2_BUCKET_NAME,
            Key=destination_key
        )
        return True
    except ClientError as e:
        print(f"Error copying {source_key}: {e}", file=sys.stderr)
        return False

def clean_folder(client, folder_path):
    """Delete all files in a folder."""
    files = list_files_in_folder(client, folder_path)
    count = 0
    
    for file in files:
        if delete_file(client, file):
            count += 1
            print(f"Deleted: {file}")
    
    return count

def main():
    """Main function."""
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
        # Validate environment variables
        if not all([R2_ACCESS_KEY, R2_SECRET_KEY, R2_ACCOUNT_ID]):
            missing = []
            if not R2_ACCESS_KEY: missing.append("R2_ACCESS_KEY_ID")
            if not R2_SECRET_KEY: missing.append("R2_SECRET_ACCESS_KEY")
            if not R2_ACCOUNT_ID: missing.append("R2_ACCOUNT_ID")
            
            result["message"] = f"Missing environment variables: {', '.join(missing)}"
            print(json.dumps(result))
            sys.exit(1)
        
        # Initialize client
        client = get_r2_client()
        
        # Step 1: List files in source folder
        source_prefix = "instagram/store/"
        files = list_files_in_folder(client, source_prefix)
        
        if not files:
            result["message"] = "No files found in instagram/store folder"
            print(json.dumps(result))
            sys.exit(0)
        
        print(f"Found {len(files)} files in {source_prefix}")
        
        # Step 2: Select random file
        selected_file = random.choice(files)
        result["selected_file"] = selected_file
        print(f"Selected: {selected_file}")
        
        # Step 3: Clean await folder
        await_prefix = "instagram/await/"
        result["cleaned_count"] = clean_folder(client, await_prefix)
        print(f"Cleaned {result['cleaned_count']} files from {await_prefix}")
        
        # Step 4: Copy to destination
        destination_key = "instagram/await/1.mp4"
        if copy_file(client, selected_file, destination_key):
            result["copied_to"] = destination_key
            print(f"Copied to: {destination_key}")
        else:
            raise Exception("Failed to copy file")
        
        # Step 5: Delete original
        if delete_file(client, selected_file):
            result["deleted_from"] = selected_file
            print(f"Deleted original: {selected_file}")
        else:
            raise Exception("Failed to delete original file")
        
        result["success"] = True
        result["message"] = f"Successfully moved {selected_file} to {destination_key}"
        
    except Exception as e:
        result["message"] = f"Error: {str(e)}"
        print(f"Error: {e}", file=sys.stderr)
    
    # Output result as JSON
    print(json.dumps(result, indent=2))
    
    # Exit with appropriate code
    if result["success"]:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    import json
    main()
