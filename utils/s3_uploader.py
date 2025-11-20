# -*- coding: utf-8 -*-
"""
S3 Uploader utility for ETL pipeline.

Handles uploading transformed data, logs, and reports to AWS S3.
Includes versioning, compression, and error handling.

Author: AI Assistant
Version: 1.0
"""

import os
import json
import gzip
import shutil
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3Uploader:
    """Manages uploads to S3 with versioning and compression."""
    
    def __init__(self, credentials_file: str = "aws_credentials.json"):
        """
        Initialize S3 uploader with credentials.
        
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
                    print("\n" + "="*80)
                    print("⚠️  CONFIGURACIÓN DE CREDENCIALES AWS REQUERIDA")
                    print("="*80)
                    print(f"\nEl archivo '{credentials_file}' existe pero contiene valores de ejemplo.")
                    print("\nPara configurar las credenciales AWS:")
                    print(f"1. Edita el archivo: {creds_path.absolute()}")
                    print("2. Reemplaza 'YOUR_ACCESS_KEY_ID_HERE' con tu AWS Access Key ID")
                    print("3. Reemplaza 'YOUR_SECRET_ACCESS_KEY_HERE' con tu AWS Secret Access Key")
                    print("4. Ajusta la región si es necesario (actual: us-east-1)")
                    print(f"5. El bucket configurado es: {creds.get('bucket_name', 'unidades-proyecto-documents')}")
                    print("\n⚠️  IMPORTANTE: Nunca subas este archivo al repositorio Git")
                    print("="*80 + "\n")
                    raise ValueError("AWS credentials not configured")
                    
                return creds
            
            # Try to load from environment variables
            print(f"\n⚠️  Archivo de credenciales no encontrado: {creds_path.absolute()}")
            print("Intentando cargar desde variables de entorno...")
            
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            
            if not aws_access_key or not aws_secret_key:
                print("\n" + "="*80)
                print("⚠️  CONFIGURACIÓN DE CREDENCIALES AWS REQUERIDA")
                print("="*80)
                print("\nNo se encontraron credenciales AWS. Tienes dos opciones:")
                print("\nOpción 1 - Archivo de configuración (RECOMENDADO):")
                print(f"1. Copia el archivo de ejemplo: aws_credentials.example.json")
                print(f"2. Renómbralo a: aws_credentials.json")
                print("3. Edita el archivo y completa tus credenciales AWS:")
                print("   - aws_access_key_id")
                print("   - aws_secret_access_key")
                print("   - region (default: us-east-1)")
                print("   - bucket_name (default: unidades-proyecto-documents)")
                print("\nOpción 2 - Variables de entorno:")
                print("1. Configura las siguientes variables de entorno:")
                print("   - AWS_ACCESS_KEY_ID")
                print("   - AWS_SECRET_ACCESS_KEY")
                print("   - AWS_REGION (opcional, default: us-east-1)")
                print("   - S3_BUCKET_NAME (opcional, default: unidades-proyecto-documents)")
                print("\n⚠️  IMPORTANTE: Nunca subas credenciales al repositorio Git")
                print("="*80 + "\n")
                raise ValueError("AWS credentials not found")
            
            return {
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key,
                'region': os.getenv('AWS_REGION', 'us-east-1'),
                'bucket_name': os.getenv('S3_BUCKET_NAME', 'unidades-proyecto-documents')
            }
                
        except ValueError:
            # Re-raise ValueError for credential issues
            raise
        except Exception as e:
            print(f"⚠ Error loading credentials: {e}")
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
            print(f"⚠ Error creating S3 client: {e}")
            raise
    
    def compress_file(self, file_path: Path) -> Path:
        """
        Compress file using gzip.
        
        Args:
            file_path: Path to file to compress
            
        Returns:
            Path to compressed file
        """
        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
        
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        original_size = file_path.stat().st_size
        compressed_size = compressed_path.stat().st_size
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        print(f"  ✓ Compressed: {original_size/1024:.2f}KB → {compressed_size/1024:.2f}KB ({compression_ratio:.1f}% reduction)")
        
        return compressed_path
    
    def upload_file(
        self, 
        local_path: Path, 
        s3_key: str,
        compress: bool = True,
        metadata: Optional[Dict] = None,
        delete_after_upload: bool = False
    ) -> bool:
        """
        Upload file to S3.
        
        Args:
            local_path: Local file path
            s3_key: S3 object key (path in bucket)
            compress: Whether to compress before upload
            metadata: Optional metadata to attach to object
            delete_after_upload: Whether to delete local file after successful upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Compress if requested and file is large enough
            upload_path = local_path
            if compress and local_path.stat().st_size > 10240:  # > 10KB
                upload_path = self.compress_file(local_path)
                s3_key += '.gz'
            
            # Prepare metadata
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = {k: str(v) for k, v in metadata.items()}
            
            # Set content type based on file extension
            if local_path.suffix == '.json':
                extra_args['ContentType'] = 'application/json'
            elif local_path.suffix == '.geojson':
                extra_args['ContentType'] = 'application/geo+json'
            elif local_path.suffix == '.md':
                extra_args['ContentType'] = 'text/markdown'
            
            # Upload
            print(f"  Uploading to s3://{self.bucket_name}/{s3_key}")
            self.s3_client.upload_file(
                str(upload_path),
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            # Clean up compressed file if created
            if compress and upload_path != local_path:
                upload_path.unlink()
            
            # Delete original file if requested
            if delete_after_upload:
                try:
                    local_path.unlink()
                    print(f"  ✓ Uploaded successfully (local file deleted)")
                except Exception as e:
                    print(f"  ✓ Uploaded successfully (warning: could not delete local file: {e})")
            else:
                print(f"  ✓ Uploaded successfully")
            
            return True
            
        except FileNotFoundError:
            print(f"  ✗ File not found: {local_path}")
            return False
        except NoCredentialsError:
            print(f"  ✗ AWS credentials not found")
            return False
        except ClientError as e:
            print(f"  ✗ AWS Error: {e}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            return False
    
    def upload_transformed_data(
        self, 
        geojson_path: Path,
        archive: bool = True,
        delete_after_upload: bool = True
    ) -> Dict[str, bool]:
        """
        Upload transformed GeoJSON data to S3 with versioning.
        
        Args:
            geojson_path: Path to GeoJSON file
            archive: Whether to also save to archive with timestamp
            delete_after_upload: Whether to delete local file after successful upload (default: True)
            
        Returns:
            Dictionary with upload results
        """
        results = {}
        
        print("\n" + "="*60)
        print("UPLOADING TRANSFORMED DATA TO S3")
        print("="*60)
        
        # Extract base name without extension for folder structure
        base_name = geojson_path.stem  # e.g., 'unidades_proyecto_transformed'
        
        # Upload to current/ folder (always overwrite)
        print("\n📦 Uploading to CURRENT folder...")
        current_key = f"up-geodata/{base_name}/current/{geojson_path.name}"
        results['current'] = self.upload_file(
            geojson_path, 
            current_key,
            compress=True,  # Compress current version
            metadata={
                'upload_timestamp': datetime.now().isoformat(),
                'content_type': 'application/geo+json',
                'version': 'current'
            }
        )
        
        # Upload to archive/ folder with timestamp
        if archive:
            print("\n📚 Uploading to ARCHIVE folder...")
            timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
            archive_name = f"{geojson_path.stem}_{timestamp}{geojson_path.suffix}"
            archive_key = f"up-geodata/{base_name}/archive/{archive_name}"
            results['archive'] = self.upload_file(
                geojson_path,
                archive_key,
                compress=True,
                metadata={
                    'upload_timestamp': datetime.now().isoformat(),
                    'content_type': 'application/geo+json',
                    'version': 'archive'
                }
            )
        
        # Also upload uncompressed version to root for backward compatibility
        print("\n📄 Uploading uncompressed version (legacy)...")
        root_key = f"up-geodata/{geojson_path.name}"
        results['root'] = self.upload_file(
            geojson_path, 
            root_key,
            compress=False,
            metadata={
                'upload_timestamp': datetime.now().isoformat(),
                'content_type': 'application/geo+json'
            }
        )
        
        # Delete local file if all uploads were successful and deletion is requested
        if delete_after_upload and all(results.values()):
            try:
                geojson_path.unlink()
                print(f"\n🗑️  Local GeoJSON file deleted: {geojson_path.name}")
            except Exception as e:
                print(f"\n⚠️  Warning: Could not delete local file: {e}")
        
        return results
    
    def upload_logs(self, logs_dir: Path, delete_after_upload: bool = True) -> Dict[str, bool]:
        """
        Upload all log files from logs directory.
        
        Args:
            logs_dir: Directory containing log files
            delete_after_upload: Whether to delete local files after successful upload (default: True)
            
        Returns:
            Dictionary with upload results per file
        """
        results = {}
        
        print("\n" + "="*60)
        print("UPLOADING LOGS TO S3")
        print("="*60)
        
        if not logs_dir.exists():
            print(f"⚠ Logs directory not found: {logs_dir}")
            return results
        
        # Get all JSON log files
        log_files = list(logs_dir.glob("*.json"))
        
        for log_file in log_files:
            # Upload to /logs directory
            s3_key = f"logs/{log_file.name}"
            
            results[log_file.name] = self.upload_file(
                log_file,
                s3_key,
                compress=True,
                metadata={
                    'upload_timestamp': datetime.now().isoformat(),
                    'type': 'transformation_log'
                },
                delete_after_upload=delete_after_upload
            )
        
        return results
    
    def upload_reports(self, reports_dir: Path, delete_after_upload: bool = True) -> Dict[str, bool]:
        """
        Upload all report files.
        
        Args:
            reports_dir: Directory containing report files
            delete_after_upload: Whether to delete local files after successful upload (default: True)
            
        Returns:
            Dictionary with upload results per file
        """
        results = {}
        
        print("\n" + "="*60)
        print("UPLOADING REPORTS TO S3")
        print("="*60)
        
        if not reports_dir.exists():
            print(f"⚠ Reports directory not found: {reports_dir}")
            return results
        
        # Upload both JSON and MD reports
        report_files = list(reports_dir.glob("*.json")) + list(reports_dir.glob("*.md"))
        
        for report_file in report_files:
            # Upload to /reports directory
            s3_key = f"reports/{report_file.name}"
            results[report_file.name] = self.upload_file(
                report_file,
                s3_key,
                compress=False,  # Keep reports uncompressed for easy viewing
                metadata={
                    'upload_timestamp': datetime.now().isoformat(),
                    'type': 'quality_report'
                },
                delete_after_upload=delete_after_upload
            )
        
        return results
    
    def upload_all_outputs(
        self, 
        output_dir: Path,
        upload_data: bool = True,
        upload_logs: bool = True,
        upload_reports: bool = True,
        delete_data_after_upload: bool = True,
        delete_logs_after_upload: bool = True,
        delete_reports_after_upload: bool = True
    ) -> Dict[str, Dict]:
        """
        Upload all outputs from ETL pipeline.
        
        Args:
            output_dir: Base output directory
            upload_data: Whether to upload transformed data
            upload_logs: Whether to upload logs
            upload_reports: Whether to upload reports
            delete_data_after_upload: Whether to delete local GeoJSON files after upload (default: True)
            delete_logs_after_upload: Whether to delete local log files after upload (default: True)
            delete_reports_after_upload: Whether to delete local report files after upload (default: True)
            
        Returns:
            Dictionary with all upload results
        """
        all_results = {}
        
        print("\n" + "="*70)
        print("UPLOADING ALL ETL OUTPUTS TO S3")
        print("="*70)
        
        # Upload transformed data
        if upload_data:
            geojson_files = list(output_dir.glob("*.geojson"))
            if geojson_files:
                all_results['data'] = {}
                for geojson_file in geojson_files:
                    all_results['data'][geojson_file.name] = self.upload_transformed_data(
                        geojson_file, 
                        delete_after_upload=delete_data_after_upload
                    )
        
        # Upload logs
        if upload_logs:
            logs_dir = output_dir / 'logs'
            if logs_dir.exists():
                all_results['logs'] = self.upload_logs(logs_dir, delete_after_upload=delete_logs_after_upload)
        
        # Upload reports
        if upload_reports:
            reports_dir = output_dir / 'reports'
            if reports_dir.exists():
                all_results['reports'] = self.upload_reports(reports_dir, delete_after_upload=delete_reports_after_upload)
        
        # Print summary
        print("\n" + "="*70)
        print("UPLOAD SUMMARY")
        print("="*70)
        
        total_uploads = 0
        successful_uploads = 0
        
        for category, results in all_results.items():
            print(f"\n{category.upper()}:")
            if isinstance(results, dict):
                for file_name, result in results.items():
                    total_uploads += 1
                    if isinstance(result, dict):
                        for upload_type, success in result.items():
                            if success:
                                successful_uploads += 1
                                print(f"  ✓ {file_name} ({upload_type})")
                            else:
                                print(f"  ✗ {file_name} ({upload_type})")
                    else:
                        if result:
                            successful_uploads += 1
                            print(f"  ✓ {file_name}")
                        else:
                            print(f"  ✗ {file_name}")
        
        print(f"\n✓ Upload completed: {successful_uploads}/{total_uploads} successful")
        
        return all_results


def main():
    """Test the S3 uploader."""
    try:
        # Initialize uploader
        uploader = S3Uploader("aws_credentials.json")
        
        # Upload all outputs
        output_dir = Path("app_outputs")
        results = uploader.upload_all_outputs(output_dir)
        
        print("\n✓ S3 Upload test completed")
        return results
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
