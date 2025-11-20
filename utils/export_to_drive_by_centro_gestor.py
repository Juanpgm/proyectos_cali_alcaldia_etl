# -*- coding: utf-8 -*-
"""
Export unidades_proyecto data from Firebase to Excel files grouped by centro_gestor
and upload them to Google Drive.

This module reads data from the Firebase "unidades_proyecto" collection,
groups records by "nombre_centro_gestor", creates individual Excel files,
and uploads them to a specified Google Drive folder.

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import pandas as pd
import io
from typing import Dict, List, Optional
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, get_drive_service


def fetch_unidades_proyecto_from_firebase(collection_name: str = "unidades_proyecto") -> Optional[pd.DataFrame]:
    """
    Fetch all documents from the unidades_proyecto collection in Firebase.
    
    Args:
        collection_name: Name of the Firebase collection
        
    Returns:
        DataFrame with all documents or None if failed
    """
    print(f"\n{'='*80}")
    print(f"FETCHING DATA FROM FIREBASE")
    print(f"{'='*80}")
    print(f"Collection: {collection_name}")
    
    try:
        # Get Firestore client
        db = get_firestore_client()
        if not db:
            print("❌ Failed to get Firestore client")
            return None
        
        # Fetch all documents
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()
        
        # Convert to list of dicts
        data = []
        for doc in docs:
            doc_dict = doc.to_dict()
            doc_dict['upid'] = doc.id  # Ensure upid is included
            data.append(doc_dict)
        
        if not data:
            print(f"⚠️  No documents found in collection '{collection_name}'")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        print(f"✅ Fetched {len(df):,} documents from Firebase")
        print(f"   Columns: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching data from Firebase: {e}")
        import traceback
        traceback.print_exc()
        return None


def clean_filename(text: str) -> str:
    """
    Clean a text string to make it suitable for a filename.
    Removes invalid characters and replaces spaces with underscores.
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text suitable for filename
    """
    if not text:
        return "sin_nombre"
    
    # Remove or replace invalid filename characters
    invalid_chars = '<>:"/\\|?*'
    cleaned = str(text).strip()
    
    for char in invalid_chars:
        cleaned = cleaned.replace(char, '_')
    
    # Replace spaces with underscores
    cleaned = cleaned.replace(' ', '_')
    
    # Remove multiple consecutive underscores
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    
    # Limit length (Windows has 255 char limit)
    if len(cleaned) > 100:
        cleaned = cleaned[:100]
    
    return cleaned


def group_by_centro_gestor(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Group DataFrame by nombre_centro_gestor field.
    
    Args:
        df: DataFrame with unidades_proyecto data
        
    Returns:
        Dictionary mapping centro_gestor name to its DataFrame
    """
    print(f"\n{'='*80}")
    print(f"GROUPING DATA BY CENTRO GESTOR")
    print(f"{'='*80}")
    
    if 'nombre_centro_gestor' not in df.columns:
        print("⚠️  Column 'nombre_centro_gestor' not found in data")
        print(f"   Available columns: {', '.join(df.columns)}")
        return {}
    
    # Group by nombre_centro_gestor
    grouped = {}
    
    for centro_gestor in df['nombre_centro_gestor'].unique():
        # Skip null/empty values
        if pd.isna(centro_gestor) or str(centro_gestor).strip() == '':
            continue
        
        # Filter records for this centro_gestor
        df_centro = df[df['nombre_centro_gestor'] == centro_gestor].copy()
        grouped[centro_gestor] = df_centro
    
    print(f"✅ Grouped into {len(grouped)} centro gestores:")
    for centro_gestor, df_centro in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"   - {centro_gestor}: {len(df_centro)} registros")
    
    return grouped


def dataframe_to_excel_buffer(df: pd.DataFrame, sheet_name: str = "Datos") -> Optional[io.BytesIO]:
    """
    Convert a DataFrame to an Excel file in memory (BytesIO buffer).
    
    Args:
        df: DataFrame to convert
        sheet_name: Name for the Excel sheet
        
    Returns:
        BytesIO buffer with Excel file or None if failed
    """
    try:
        # Create a BytesIO buffer
        buffer = io.BytesIO()
        
        # Remove geometry column if present (can't be serialized to Excel)
        df_export = df.copy()
        if 'geometry' in df_export.columns:
            df_export = df_export.drop(columns=['geometry'])
        
        # Convert datetime columns to strings for Excel compatibility
        for col in df_export.columns:
            if df_export[col].dtype == 'datetime64[ns]':
                df_export[col] = df_export[col].astype(str)
        
        # Write to Excel using openpyxl engine
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_export.to_excel(writer, index=False, sheet_name=sheet_name)
        
        # Reset buffer position to beginning
        buffer.seek(0)
        
        return buffer
        
    except Exception as e:
        print(f"❌ Error creating Excel buffer: {e}")
        return None


def upload_excel_to_drive(
    excel_buffer: io.BytesIO,
    filename: str,
    folder_id: str,
    mime_type: str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    user_email: Optional[str] = None
) -> Optional[str]:
    """
    Upload an Excel file (from BytesIO buffer) to Google Drive.
    If file exists, it will be replaced (updated).
    
    Args:
        excel_buffer: BytesIO buffer with Excel file content
        filename: Name for the file in Drive
        folder_id: Google Drive folder ID where to upload
        mime_type: MIME type for Excel files
        user_email: Email del usuario de Google Workspace para Domain-Wide Delegation (opcional)
        
    Returns:
        File ID of uploaded file or None if failed
    """
    try:
        from googleapiclient.http import MediaIoBaseUpload
        
        service = get_drive_service(user_email=user_email)
        if not service:
            print("❌ Failed to get Drive service")
            return None
        
        # Check if file already exists in the folder
        existing_file_id = None
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            if files:
                existing_file_id = files[0]['id']
                print(f"   📝 File exists, will update: {filename}")
        except Exception as e:
            print(f"   ⚠️  Could not check for existing file: {e}")
        
        # Create MediaIoBaseUpload from buffer
        excel_buffer.seek(0)  # Reset buffer position
        media = MediaIoBaseUpload(
            excel_buffer,
            mimetype=mime_type,
            resumable=True
        )
        
        if existing_file_id:
            # Update existing file
            file = service.files().update(
                fileId=existing_file_id,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            print(f"   ✅ Updated: {filename}")
        else:
            # Create new file
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            print(f"   ✅ Uploaded: {filename}")
        
        file_id = file.get('id')
        web_link = file.get('webViewLink', 'N/A')
        
        if web_link != 'N/A':
            print(f"      Link: {web_link}")
        
        return file_id
        
    except Exception as e:
        print(f"   ❌ Error uploading '{filename}': {e}")
        import traceback
        traceback.print_exc()
        return None


def export_and_upload_by_centro_gestor(
    collection_name: str = "unidades_proyecto",
    drive_folder_id: str = "1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-",
    temp_dir: Optional[str] = None,
    user_email: Optional[str] = None
) -> Dict[str, any]:
    """
    Main function to export unidades_proyecto data grouped by centro_gestor
    and upload Excel files to Google Drive.
    
    Args:
        collection_name: Firebase collection name
        drive_folder_id: Google Drive folder ID for uploads
        temp_dir: Optional local directory to save files (for backup)
        user_email: Email del usuario de Google Workspace para Domain-Wide Delegation (opcional)
        
    Returns:
        Dictionary with execution results
    """
    print(f"\n{'='*80}")
    print(f"EXPORT UNIDADES PROYECTO BY CENTRO GESTOR TO DRIVE")
    print(f"{'='*80}")
    print(f"Collection: {collection_name}")
    print(f"Drive Folder ID: {drive_folder_id}")
    
    results = {
        'success': False,
        'total_records': 0,
        'total_grupos': 0,
        'files_created': 0,
        'files_uploaded': 0,
        'errors': []
    }
    
    try:
        # Step 1: Fetch data from Firebase
        df = fetch_unidades_proyecto_from_firebase(collection_name)
        if df is None or df.empty:
            results['errors'].append("No data fetched from Firebase")
            return results
        
        results['total_records'] = len(df)
        
        # Step 2: Group by centro_gestor
        grouped = group_by_centro_gestor(df)
        if not grouped:
            results['errors'].append("No valid centro_gestor groups found")
            return results
        
        results['total_grupos'] = len(grouped)
        
        # Step 3: Create and upload Excel files
        print(f"\n{'='*80}")
        print(f"CREATING AND UPLOADING EXCEL FILES")
        print(f"{'='*80}")
        
        # Create temp directory if specified
        if temp_dir:
            temp_path = Path(temp_dir)
            temp_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Backup directory: {temp_path}")
        
        for centro_gestor, df_centro in grouped.items():
            try:
                # Clean centro_gestor name for filename
                safe_filename = clean_filename(centro_gestor)
                excel_filename = f"{safe_filename}.xlsx"
                
                print(f"\n📊 Processing: {centro_gestor}")
                print(f"   Records: {len(df_centro)}")
                print(f"   Filename: {excel_filename}")
                
                # Create Excel buffer
                excel_buffer = dataframe_to_excel_buffer(df_centro, sheet_name=centro_gestor[:31])
                if not excel_buffer:
                    results['errors'].append(f"Failed to create Excel for '{centro_gestor}'")
                    continue
                
                results['files_created'] += 1
                
                # Save to local temp directory if specified
                if temp_dir:
                    local_file = temp_path / excel_filename
                    with open(local_file, 'wb') as f:
                        excel_buffer.seek(0)
                        f.write(excel_buffer.read())
                    print(f"   💾 Saved locally: {local_file}")
                    excel_buffer.seek(0)  # Reset for upload
                
                # Upload to Google Drive
                file_id = upload_excel_to_drive(excel_buffer, excel_filename, drive_folder_id, user_email=user_email)
                if file_id:
                    results['files_uploaded'] += 1
                else:
                    results['errors'].append(f"Failed to upload '{excel_filename}'")
                
            except Exception as e:
                error_msg = f"Error processing '{centro_gestor}': {e}"
                print(f"   ❌ {error_msg}")
                results['errors'].append(error_msg)
        
        # Mark as successful if at least one file was uploaded
        results['success'] = results['files_uploaded'] > 0
        
        return results
        
    except Exception as e:
        print(f"❌ Critical error in export process: {e}")
        import traceback
        traceback.print_exc()
        results['errors'].append(f"Critical error: {e}")
        return results


def print_results_summary(results: Dict[str, any]):
    """
    Print a summary of the export and upload results.
    
    Args:
        results: Results dictionary from export_and_upload_by_centro_gestor
    """
    print(f"\n{'='*80}")
    print(f"EXPORT RESULTS SUMMARY")
    print(f"{'='*80}")
    
    print(f"\n📊 Data Processing:")
    print(f"   Total records: {results['total_records']:,}")
    print(f"   Centro gestores: {results['total_grupos']}")
    
    print(f"\n📁 File Operations:")
    print(f"   Files created: {results['files_created']}")
    print(f"   Files uploaded: {results['files_uploaded']}")
    
    if results['errors']:
        print(f"\n⚠️  Errors ({len(results['errors'])}):")
        for i, error in enumerate(results['errors'][:10], 1):
            print(f"   {i}. {error}")
        if len(results['errors']) > 10:
            print(f"   ... and {len(results['errors']) - 10} more errors")
    
    print(f"\n{'='*80}")
    if results['success']:
        print(f"✅ EXPORT COMPLETED SUCCESSFULLY")
    else:
        print(f"❌ EXPORT FAILED OR PARTIALLY COMPLETED")
    print(f"{'='*80}")


def main():
    """
    Main execution function for testing.
    """
    # Default Drive folder ID (from user request)
    # https://drive.google.com/drive/folders/1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-
    drive_folder_id = "1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-"
    
    # Optional: Save files locally as backup
    project_root = Path(__file__).parent.parent
    temp_dir = project_root / "app_outputs" / "excel_by_centro_gestor"
    
    # Execute export and upload
    results = export_and_upload_by_centro_gestor(
        collection_name="unidades_proyecto",
        drive_folder_id=drive_folder_id,
        temp_dir=str(temp_dir)
    )
    
    # Print summary
    print_results_summary(results)
    
    return results['success']


if __name__ == "__main__":
    """
    Entry point for script execution.
    """
    success = main()
    sys.exit(0 if success else 1)
