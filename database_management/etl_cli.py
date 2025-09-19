"""
Command Line Interface for the Intelligent ETL System.

Provides easy-to-use commands for database management and data loading.
"""

import click
import logging
from pathlib import Path
from typing import Optional
import json

from core import get_database_config, create_etl_system, IntelligentETL
from core.database_manager import get_database_stats

logger = logging.getLogger(__name__)


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--config-file', '-c', type=click.Path(exists=True), help='Configuration file path')
@click.pass_context
def cli(ctx, verbose, config_file):
    """Intelligent ETL System for Santiago de Cali projects."""
    
    # Setup logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Store configuration in context
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['config_file'] = config_file


@cli.command()
@click.pass_context
def test_config(ctx):
    """Test database configuration and connection."""
    click.echo("🧪 Testing database configuration and connection...")
    
    try:
        from core.config import test_connection, validate_config
        
        # Load configuration
        config = get_database_config()
        
        # Test configuration validity
        click.echo("\n1️⃣ Validating configuration...")
        if validate_config(config):
            click.echo("✅ Configuration is valid")
        else:
            click.echo("❌ Configuration is invalid")
            return
        
        # Test actual connection
        click.echo("\n2️⃣ Testing database connection...")
        if test_connection(config):
            click.echo("🎉 Database connection successful!")
            click.echo("✅ System ready for ETL operations")
        else:
            click.echo("❌ Database connection failed!")
            click.echo("💡 Check that PostgreSQL is running and credentials are correct")
            
    except Exception as e:
        click.echo(f"❌ Error testing configuration: {e}")


@cli.command()
@click.pass_context
def diagnose(ctx):
    """Diagnose system health and database status."""
    click.echo("🔍 Diagnosing system health...")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        diagnosis = etl_system.diagnose_system()
        
        # Display results
        click.echo(f"\n📊 System Health: {diagnosis['system_health'].upper()}")
        
        # Database status
        db_status = diagnosis.get('database', {})
        click.echo(f"🔗 Database Connection: {'✅ Connected' if db_status.get('connection') else '❌ Failed'}")
        
        if config.enable_postgis:
            click.echo(f"🗺️  PostGIS: {'✅ Installed' if db_status.get('postgis') else '❌ Missing'}")
        
        # Issues
        issues = db_status.get('issues', [])
        if issues:
            click.echo(f"\n⚠️  Issues Found ({len(issues)}):")
            for issue in issues:
                click.echo(f"  • {issue}")
        
        # Recommendations
        recommendations = diagnosis.get('recommendations', [])
        if recommendations:
            click.echo(f"\n💡 Recommendations:")
            for rec in recommendations:
                click.echo(f"  • {rec}")
        
        # Data files
        data_files = diagnosis.get('data_files', {})
        if 'total_files' in data_files:
            click.echo(f"\n📁 Data Files: {data_files['total_files']} files found")
            
            for dir_info in data_files.get('directories', []):
                click.echo(f"  📂 {dir_info['name']}: {len(dir_info['files'])} files")
        
        if ctx.obj.get('verbose'):
            click.echo(f"\n🔧 Full Diagnosis (JSON):")
            click.echo(json.dumps(diagnosis, indent=2, ensure_ascii=False))
    
    except Exception as e:
        click.echo(f"❌ Diagnosis failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.pass_context
def repair(ctx):
    """Attempt to repair system issues."""
    click.echo("🔧 Attempting system repairs...")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        repair_result = etl_system.repair_system()
        
        # Display results
        if repair_result['overall_success']:
            click.echo("✅ System repair completed successfully!")
        else:
            click.echo("⚠️  System repair completed with some issues.")
        
        # Actions performed
        actions = repair_result.get('actions_performed', [])
        if actions:
            click.echo(f"\n🔨 Actions Performed:")
            for action in actions:
                click.echo(f"  • {action}")
        
        # Successes
        successes = repair_result.get('successes', [])
        if successes:
            click.echo(f"\n✅ Successful Repairs:")
            for success in successes:
                click.echo(f"  • {success}")
        
        # Failures
        failures = repair_result.get('failures', [])
        if failures:
            click.echo(f"\n❌ Failed Repairs:")
            for failure in failures:
                click.echo(f"  • {failure}")
        
        if ctx.obj.get('verbose'):
            click.echo(f"\n🔧 Full Repair Result (JSON):")
            click.echo(json.dumps(repair_result, indent=2, ensure_ascii=False))
    
    except Exception as e:
        click.echo(f"❌ Repair failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--data-dir', '-d', type=click.Path(exists=True), 
              default='transformation_app/app_outputs',
              help='Data directory path')
@click.pass_context
def load(ctx, data_dir):
    """Load data from transformation outputs to database."""
    click.echo(f"📥 Loading data from: {data_dir}")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        data_path = Path(data_dir)
        result = etl_system.process_data_directory(data_path)
        
        # Display results
        if result.success:
            click.echo("✅ Data loading completed successfully!")
        else:
            click.echo("⚠️  Data loading completed with some issues.")
        
        click.echo(f"📊 Statistics:")
        click.echo(f"  • Tables Created: {result.tables_created}")
        click.echo(f"  • Records Loaded: {result.records_loaded}")
        click.echo(f"  • Execution Time: {result.execution_time:.2f}s")
        
        if result.errors:
            click.echo(f"\n❌ Errors ({len(result.errors)}):")
            for error in result.errors:
                click.echo(f"  • {error}")
        
        if result.warnings:
            click.echo(f"\n⚠️  Warnings ({len(result.warnings)}):")
            for warning in result.warnings:
                click.echo(f"  • {warning}")
        
        if ctx.obj.get('verbose'):
            click.echo(f"\n📊 Full Result (JSON):")
            click.echo(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    
    except Exception as e:
        click.echo(f"❌ Data loading failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--data-dir', '-d', type=click.Path(exists=True),
              default='transformation_app/app_outputs',
              help='Data directory path')
@click.pass_context
def run(ctx, data_dir):
    """Run complete ETL process (diagnose, repair, load)."""
    click.echo("🚀 Running complete ETL process...")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        data_path = Path(data_dir) if data_dir else None
        result = etl_system.run_full_etl(data_path)
        
        # Display results
        if result.success:
            click.echo("✅ ETL process completed successfully!")
        else:
            click.echo("⚠️  ETL process completed with some issues.")
        
        click.echo(f"📊 Final Statistics:")
        click.echo(f"  • Tables Created: {result.tables_created}")
        click.echo(f"  • Records Loaded: {result.records_loaded}")
        click.echo(f"  • Total Execution Time: {result.execution_time:.2f}s")
        
        if result.errors:
            click.echo(f"\n❌ Errors ({len(result.errors)}):")
            for error in result.errors:
                click.echo(f"  • {error}")
        
        if result.warnings:
            click.echo(f"\n⚠️  Warnings ({len(result.warnings)}):")
            for warning in result.warnings:
                click.echo(f"  • {warning}")
    
    except Exception as e:
        click.echo(f"❌ ETL process failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.pass_context
def status(ctx):
    """Show current system status."""
    click.echo("📊 Checking system status...")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        status = etl_system.get_system_status()
        
        # Display status
        click.echo(f"🕐 Timestamp: {status['timestamp']}")
        click.echo(f"🔗 Database: {status['config']['host']}:{status['config']['port']}")
        click.echo(f"📊 Schema: {status['config']['schema']}")
        click.echo(f"🔌 Connected: {'✅ Yes' if status['database_connected'] else '❌ No'}")
        
        # Get database stats if connected
        if status['database_connected']:
            try:
                stats = get_database_stats(etl_system.db_manager)
                
                click.echo(f"📈 Database Statistics:")
                click.echo(f"  • Total Tables: {stats.get('table_count', 'Unknown')}")
                
                table_stats = stats.get('table_stats', {})
                if table_stats:
                    click.echo(f"  • Table Records:")
                    for table, count in table_stats.items():
                        click.echo(f"    - {table}: {count}")
            
            except Exception as e:
                click.echo(f"⚠️  Could not get database statistics: {e}")
        
        if ctx.obj.get('verbose'):
            click.echo(f"\n📊 Full Status (JSON):")
            click.echo(json.dumps(status, indent=2, ensure_ascii=False))
    
    except Exception as e:
        click.echo(f"❌ Status check failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize database and setup basic structures."""
    click.echo("🔧 Initializing database...")
    
    try:
        config = get_database_config()
        etl_system = create_etl_system(config)
        
        if etl_system.initialize_database():
            click.echo("✅ Database initialization completed successfully!")
            
            # Show connection info
            click.echo(f"📊 Connection Info:")
            click.echo(f"  • Host: {config.host}")
            click.echo(f"  • Port: {config.port}")
            click.echo(f"  • Database: {config.database}")
            click.echo(f"  • Schema: {config.schema}")
            if config.enable_postgis:
                click.echo(f"  • PostGIS: Enabled")
        else:
            click.echo("❌ Database initialization failed!")
            raise click.Abort()
    
    except Exception as e:
        click.echo(f"❌ Initialization failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def export_config(ctx, output):
    """Export current configuration to file."""
    try:
        config = get_database_config()
        
        config_data = {
            "database": config.connection_info,
            "connection_string": config.connection_string,
            "postgis_enabled": config.enable_postgis
        }
        
        if output:
            output_path = Path(output)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            click.echo(f"✅ Configuration exported to: {output_path}")
        else:
            click.echo("📊 Current Configuration:")
            click.echo(json.dumps(config_data, indent=2, ensure_ascii=False))
    
    except Exception as e:
        click.echo(f"❌ Configuration export failed: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()