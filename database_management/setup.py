#!/usr/bin/env python3
"""
Quick setup script for the Intelligent ETL System.

This script automates the initial setup process.
"""

import os
import sys
import subprocess
from pathlib import Path
import shutil


def print_header():
    """Print setup header."""
    print("=" * 60)
    print("🚀 INTELLIGENT ETL SYSTEM - QUICK SETUP")
    print("   Santiago de Cali - Database Management")
    print("=" * 60)
    print()


def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ required. Current version:", sys.version)
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} detected")
    return True


def check_postgresql():
    """Check if PostgreSQL is available."""
    try:
        result = subprocess.run(['psql', '--version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PostgreSQL detected: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        pass
    
    print("⚠️  PostgreSQL not found in PATH")
    print("   Please install PostgreSQL 12+ before continuing")
    return False


def setup_environment():
    """Setup Python environment and dependencies."""
    print("\n📦 Setting up Python environment...")
    
    try:
        # Install requirements
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                      check=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False


def setup_config():
    """Setup configuration files."""
    print("\n⚙️  Setting up configuration...")
    
    env_example = Path('.env.example')
    env_file = Path('.env')
    
    if env_example.exists() and not env_file.exists():
        shutil.copy(env_example, env_file)
        print("✅ Created .env file from template")
        print("⚠️  Please edit .env with your database credentials")
        return True
    elif env_file.exists():
        print("✅ .env file already exists")
        return True
    else:
        print("❌ .env.example not found")
        return False


def test_system():
    """Test the ETL system."""
    print("\n🔍 Testing ETL system...")
    
    try:
        # Import and test core modules
        sys.path.insert(0, str(Path.cwd()))
        
        from core import get_database_config, create_etl_system
        
        # Test configuration
        config = get_database_config()
        print(f"✅ Configuration loaded: {config.database}@{config.host}")
        
        # Test ETL system creation
        etl_system = create_etl_system(config)
        print("✅ ETL system created successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ System test failed: {e}")
        return False


def show_next_steps():
    """Show next steps for user."""
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Edit .env file with your PostgreSQL credentials")
    print("2. Run: python main.py --init")
    print("3. Run: python main.py --run")
    print("\n💡 Quick commands:")
    print("   python main.py --diagnose    # Check system health")
    print("   python main.py --status      # Show current status")
    print("   python etl_cli.py --help     # Interactive CLI")
    print("\n📖 Documentation: README.md")


def main():
    """Main setup function."""
    print_header()
    
    # Check requirements
    if not check_python_version():
        sys.exit(1)
    
    postgresql_available = check_postgresql()
    
    # Setup environment
    if not setup_environment():
        sys.exit(1)
    
    # Setup configuration
    if not setup_config():
        sys.exit(1)
    
    # Test system
    if not test_system():
        print("⚠️  System test failed, but setup may still work")
    
    # Show next steps
    show_next_steps()
    
    if not postgresql_available:
        print("\n⚠️  WARNING: PostgreSQL not detected")
        print("   Please install PostgreSQL before running the ETL system")


if __name__ == '__main__':
    main()