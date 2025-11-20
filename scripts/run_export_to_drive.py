# -*- coding: utf-8 -*-
"""
CLI script for exporting unidades_proyecto data to Google Drive grouped by centro_gestor.

Usage:
    python scripts/run_export_to_drive.py [options]

Options:
    --collection NAME       Firebase collection name (default: unidades_proyecto)
    --folder-id ID          Google Drive folder ID (default: 1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-)
    --temp-dir PATH         Local directory to save backup files (optional)
    --dry-run               Test without uploading to Drive
    --help                  Show this help message

Examples:
    # Basic usage (uses defaults)
    python scripts/run_export_to_drive.py

    # With custom folder
    python scripts/run_export_to_drive.py --folder-id 1ABC...XYZ

    # Save local backup
    python scripts/run_export_to_drive.py --temp-dir app_outputs/backup

    # Test mode (no upload)
    python scripts/run_export_to_drive.py --dry-run
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.export_to_drive_by_centro_gestor import (
    export_and_upload_by_centro_gestor,
    print_results_summary,
    fetch_unidades_proyecto_from_firebase,
    group_by_centro_gestor,
    dataframe_to_excel_buffer,
    clean_filename
)


def run_dry_run(collection_name: str, temp_dir: str = None):
    """
    Run in dry-run mode: test everything except upload to Drive.
    
    Args:
        collection_name: Firebase collection name
        temp_dir: Optional local directory to save files
    """
    print("\n" + "="*80)
    print("DRY RUN MODE - No files will be uploaded to Drive")
    print("="*80)
    
    # Fetch data
    df = fetch_unidades_proyecto_from_firebase(collection_name)
    if df is None or df.empty:
        print("❌ Failed to fetch data from Firebase")
        return False
    
    # Group data
    grouped = group_by_centro_gestor(df)
    if not grouped:
        print("❌ Failed to group data")
        return False
    
    # Create temp directory if specified
    if temp_dir:
        temp_path = Path(temp_dir)
        temp_path.mkdir(parents=True, exist_ok=True)
        print(f"\n📁 Files will be saved to: {temp_path}")
    
    # Process each group
    print("\n" + "="*80)
    print("CREATING EXCEL FILES (DRY RUN)")
    print("="*80)
    
    success_count = 0
    for centro_gestor, df_centro in grouped.items():
        safe_filename = clean_filename(centro_gestor)
        excel_filename = f"{safe_filename}.xlsx"
        
        print(f"\n📊 {centro_gestor}")
        print(f"   Records: {len(df_centro)}")
        print(f"   Filename: {excel_filename}")
        
        # Create Excel buffer
        excel_buffer = dataframe_to_excel_buffer(df_centro, sheet_name=centro_gestor[:31])
        
        if excel_buffer:
            buffer_size = len(excel_buffer.getvalue())
            print(f"   ✅ Excel created: {buffer_size / 1024:.2f} KB")
            
            # Save to disk if temp_dir specified
            if temp_dir:
                temp_path = Path(temp_dir)
                file_path = temp_path / excel_filename
                with open(file_path, 'wb') as f:
                    excel_buffer.seek(0)
                    f.write(excel_buffer.read())
                print(f"   💾 Saved: {file_path}")
            
            success_count += 1
        else:
            print(f"   ❌ Failed to create Excel")
    
    # Summary
    print("\n" + "="*80)
    print("DRY RUN SUMMARY")
    print("="*80)
    print(f"Total records: {len(df):,}")
    print(f"Centro gestores: {len(grouped)}")
    print(f"Excel files created: {success_count}/{len(grouped)}")
    
    if temp_dir:
        print(f"\n📂 Files saved to: {Path(temp_dir).absolute()}")
    
    print("\n✅ Dry run completed successfully")
    print("   To upload to Drive, run without --dry-run flag")
    
    return True


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Export unidades_proyecto data to Google Drive grouped by centro_gestor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --folder-id 1ABC...XYZ
  %(prog)s --temp-dir app_outputs/backup
  %(prog)s --dry-run
  %(prog)s --user-email admin@tu-dominio.com

For more information, see utils/README_EXPORT_TO_DRIVE.md
        """
    )
    
    parser.add_argument(
        '--collection',
        type=str,
        default='unidades_proyecto',
        help='Firebase collection name (default: unidades_proyecto)'
    )
    
    parser.add_argument(
        '--folder-id',
        type=str,
        default='1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-',
        help='Google Drive folder ID (default: 1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-)'
    )
    
    parser.add_argument(
        '--temp-dir',
        type=str,
        default=None,
        help='Local directory to save backup files (optional)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test mode - create files but do not upload to Drive'
    )
    
    parser.add_argument(
        '--user-email',
        type=str,
        default=None,
        help='Email del usuario de Google Workspace para Domain-Wide Delegation (opcional)'
    )
    
    args = parser.parse_args()
    
    # Leer user_email desde variable de entorno si no se proporciona
    user_email = args.user_email or os.getenv('GOOGLE_WORKSPACE_USER_EMAIL')
    
    # Show configuration
    print("\n" + "="*80)
    print("EXPORT UNIDADES PROYECTO TO DRIVE")
    print("="*80)
    print(f"Collection: {args.collection}")
    print(f"Drive Folder ID: {args.folder_id}")
    print(f"Temp Directory: {args.temp_dir or 'None (files will not be saved locally)'}")
    print(f"Mode: {'DRY RUN (no upload)' if args.dry_run else 'FULL (with upload)'}")
    if user_email:
        print(f"Domain-Wide Delegation: {user_email}")
    
    # Execute
    try:
        if args.dry_run:
            # Dry run mode
            success = run_dry_run(args.collection, args.temp_dir)
        else:
            # Full mode with upload
            results = export_and_upload_by_centro_gestor(
                collection_name=args.collection,
                drive_folder_id=args.folder_id,
                temp_dir=args.temp_dir,
                user_email=user_email
            )
            
            print_results_summary(results)
            success = results['success']
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        return 130
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
