"""
Main entry point for the Intelligent ETL System.

This script provides the primary interface for running the ETL system
with automatic data loading, schema generation, and database management.
"""

import os
import sys
from pathlib import Path

# Add the core module to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from core import (
    get_database_config,
    create_etl_system,
    run_etl_from_config
)

import logging
import argparse
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_main.log')
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main function to run the ETL system."""
    parser = argparse.ArgumentParser(
        description='Intelligent ETL System for Santiago de Cali Projects',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --test-config           # Test database configuration and connection
  python main.py --diagnose              # Diagnose system health
  python main.py --init                  # Initialize database
  python main.py --load                  # Load data from outputs
  python main.py --run                   # Run complete ETL process
  python main.py --status                # Show system status
        """
    )
    
    # Command options
    parser.add_argument('--test-config', action='store_true',
                       help='Test database configuration and connection')
    parser.add_argument('--diagnose', action='store_true',
                       help='Diagnose system health and issues')
    parser.add_argument('--repair', action='store_true',
                       help='Attempt to repair system issues')
    parser.add_argument('--init', action='store_true',
                       help='Initialize database and setup')
    parser.add_argument('--load', action='store_true',
                       help='Load data from transformation outputs')
    parser.add_argument('--run', action='store_true',
                       help='Run complete ETL process')
    parser.add_argument('--status', action='store_true',
                       help='Show current system status')
    
    # Configuration options
    parser.add_argument('--data-dir', type=str,
                       default='transformation_app/app_outputs',
                       help='Data directory path (default: transformation_app/app_outputs)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--config-file', type=str,
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Load configuration
        config = get_database_config()
        
        # Create ETL system
        etl_system = create_etl_system(config)
        
        # Execute requested command
        if args.test_config:
            print("🧪 Testing database configuration...")
            from core.config import test_connection, validate_config
            
            # Test configuration validity
            if validate_config(config):
                print("✅ Configuration is valid")
            
            # Test actual connection
            if test_connection(config):
                print("🎉 Database connection successful!")
                return 0
            else:
                print("❌ Database connection failed!")
                return 1
                
        elif args.diagnose:
            print("🔍 Diagnosing system health...")
            diagnosis = etl_system.diagnose_system()
            print(f"System Health: {diagnosis['system_health']}")
            
            if diagnosis['system_health'] in ['poor', 'critical']:
                print("⚠️  Issues found. Consider running --repair")
            
        elif args.repair:
            print("🔧 Attempting system repairs...")
            repair_result = etl_system.repair_system()
            
            if repair_result['overall_success']:
                print("✅ System repair completed successfully!")
            else:
                print("⚠️  System repair completed with some issues.")
            
        elif args.init:
            print("🔧 Initializing database...")
            if etl_system.initialize_database():
                print("✅ Database initialization completed!")
            else:
                print("❌ Database initialization failed!")
                sys.exit(1)
            
        elif args.load:
            print(f"📥 Loading data from: {args.data_dir}")
            data_path = Path(args.data_dir)
            
            if not data_path.exists():
                print(f"❌ Data directory not found: {data_path}")
                sys.exit(1)
            
            result = etl_system.process_data_directory(data_path)
            
            if result.success:
                print("✅ Data loading completed successfully!")
                print(f"Tables Created: {result.tables_created}")
                print(f"Records Loaded: {result.records_loaded}")
            else:
                print("⚠️  Data loading completed with issues.")
                for error in result.errors:
                    print(f"Error: {error}")
            
        elif args.run:
            print("🚀 Running complete ETL process...")
            data_path = Path(args.data_dir)
            
            result = etl_system.run_full_etl(data_path)
            
            if result.success:
                print("✅ ETL process completed successfully!")
            else:
                print("⚠️  ETL process completed with issues.")
            
            print(f"Final Statistics:")
            print(f"  Tables Created: {result.tables_created}")
            print(f"  Records Loaded: {result.records_loaded}")
            print(f"  Execution Time: {result.execution_time:.2f}s")
            
        elif args.status:
            print("📊 Checking system status...")
            status = etl_system.get_system_status()
            
            print(f"Database: {status['config']['host']}:{status['config']['port']}")
            print(f"Connected: {'✅ Yes' if status['database_connected'] else '❌ No'}")
            print(f"Health: {status['diagnosis']['system_health']}")
            
        else:
            # Default action: run quick status check
            print("🔍 Quick system check...")
            print("Use --help for available commands")
            
            status = etl_system.get_system_status()
            health = status['diagnosis']['system_health']
            
            print(f"System Health: {health}")
            
            if health == 'excellent':
                print("✅ System is ready for ETL operations!")
                print("Run with --load to load data or --run for complete ETL")
            elif health in ['poor', 'critical']:
                print("⚠️  System needs attention. Run with --diagnose for details")
            
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 INTELLIGENT ETL SYSTEM - SANTIAGO DE CALI")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    main()
    
    print()
    print("✅ Process completed!")
    print("=" * 60)