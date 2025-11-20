# -*- coding: utf-8 -*-
"""
S3 Downloader utility for ETL pipeline.

Handles downloading and reading files directly from AWS S3.
Supports both direct memory reading and temporary file downloads.
"""

import os
import json
import gzip
from pathlib import Path
from typing import Optional, Dict, Any
from io import BytesIO
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3Downloader:
    """Manages downloads from S3 with direct memory reading support."""
    
    def __init__(self, credentials_file: str = "aws_credentials.json"):
        """
        Initialize S3 downloader with credentials.
        
        Args:
            credentials_file: Path to AWS credentials JSON file
        """
        self.credentials = self._load_credentials(credentials_file)
        self.s3_client = self._create_s3_client()
        self.bucket_name = self.credentials.get('bucket_name')
        
    def _load_credentials(self, credentials_file: str) -> Dict:
        """Load AWS credentials from JSON file or environment variables."""
        try:
            creds_path = Path(credentials_file)
            
            # Try to load from file first
            if creds_path.exists():
                with open(creds_path, 'r', encoding='utf-8') as f:
                    creds = json.load(f)
                    
                # Validate credentials
                if creds.get('aws_access_key_id') == 'YOUR_ACCESS_KEY_ID_HERE':
                    raise ValueError("AWS credentials not configured in file")
                    
                return creds
            
            # Try to load from environment variables
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            
            if not aws_access_key or not aws_secret_key:
                raise ValueError("AWS credentials not found")
            
            return {
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key,
                'region': os.getenv('AWS_REGION', 'us-east-1'),
                'bucket_name': os.getenv('S3_BUCKET_NAME', 'unidades-proyecto-documents')
            }
                
        except Exception as e:
            print(f"⚠️ Error loading S3 credentials: {e}")
            raise
    
    def _create_s3_client(self):
        """Create boto3 S3 client with credentials."""
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.credentials['aws_access_key_id'],
                aws_secret_access_key=self.credentials['aws_secret_access_key'],
                region_name=self.credentials['region']
            )
        except Exception as e:
            print(f"⚠️ Error creating S3 client: {e}")
            raise
    
    def read_json_from_s3(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Read JSON file directly from S3 into memory (no download).
        
        Args:
            s3_key: S3 object key (path in bucket)
            
        Returns:
            Parsed JSON data or None if failed
        """
        try:
            print(f"📥 Reading from S3: s3://{self.bucket_name}/{s3_key}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Read content
            content = response['Body'].read()
            
            # Handle gzip compression
            if s3_key.endswith('.gz'):
                content = gzip.decompress(content)
            
            # Parse JSON
            data = json.loads(content.decode('utf-8'))
            
            # Get file size
            file_size = len(content) / 1024  # KB
            print(f"✓ Successfully read {file_size:.1f} KB from S3")
            
            return data
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"❌ File not found in S3: {s3_key}")
            else:
                print(f"❌ AWS Error reading from S3: {e}")
            return None
        except NoCredentialsError:
            print(f"❌ AWS credentials not found")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in S3 file: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error reading from S3: {e}")
            return None
    
    def download_to_temp(self, s3_key: str, temp_dir: Path = None) -> Optional[Path]:
        """
        Download file from S3 to temporary location.
        Use this only when direct reading is not possible.
        
        Args:
            s3_key: S3 object key
            temp_dir: Temporary directory (default: ./temp_downloads)
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            # Create temp directory
            if temp_dir is None:
                temp_dir = Path("temp_downloads")
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate local filename
            local_filename = Path(s3_key).name
            local_path = temp_dir / local_filename
            
            print(f"📥 Downloading from S3: s3://{self.bucket_name}/{s3_key}")
            print(f"   → {local_path}")
            
            # Download file
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
            
            # Get file size
            file_size = local_path.stat().st_size / 1024  # KB
            print(f"✓ Downloaded {file_size:.1f} KB")
            
            # Decompress if gzipped
            if local_path.suffix == '.gz':
                decompressed_path = local_path.with_suffix('')
                with gzip.open(local_path, 'rb') as f_in:
                    with open(decompressed_path, 'wb') as f_out:
                        f_out.write(f_in.read())
                local_path.unlink()  # Remove compressed file
                local_path = decompressed_path
                print(f"✓ Decompressed to {decompressed_path}")
            
            return local_path
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"❌ File not found in S3: {s3_key}")
            else:
                print(f"❌ AWS Error downloading from S3: {e}")
            return None
        except NoCredentialsError:
            print(f"❌ AWS credentials not found")
            return None
        except Exception as e:
            print(f"❌ Unexpected error downloading from S3: {e}")
            return None
    
    def list_files(self, prefix: str) -> list:
        """
        List files in S3 bucket with given prefix.
        
        Args:
            prefix: S3 key prefix to filter
            
        Returns:
            List of file keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except Exception as e:
            print(f"❌ Error listing S3 files: {e}")
            return []
    
    def cleanup_temp_files(self, temp_dir: Path = None):
        """
        Clean up temporary downloaded files.
        
        Args:
            temp_dir: Temporary directory to clean
        """
        try:
            if temp_dir is None:
                temp_dir = Path("temp_downloads")
            
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir)
                print(f"✓ Cleaned up temporary directory: {temp_dir}")
            
        except Exception as e:
            print(f"⚠️ Error cleaning up temp files: {e}")


def main():
    """Test the S3 downloader."""
    try:
        # Initialize downloader
        downloader = S3Downloader("aws_credentials.json")
        
        # Test reading GeoJSON directly from S3
        s3_key = "up-geodata/unidades_proyecto.geojson"
        data = downloader.read_json_from_s3(s3_key)
        
        if data:
            print(f"\n✓ Successfully read data from S3")
            if data.get('type') == 'FeatureCollection':
                features = data.get('features', [])
                print(f"  - Type: {data['type']}")
                print(f"  - Features: {len(features)}")
        else:
            print(f"\n❌ Failed to read data from S3")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
