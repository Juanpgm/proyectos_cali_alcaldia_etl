#!/usr/bin/env python3
"""
Script simple para probar conexión a Railway PostgreSQL desde GitHub Actions
"""
import os
import sys
import psycopg2

def test_railway_connection():
    """Test connection to Railway PostgreSQL"""
    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print('❌ DATABASE_URL not configured')
            return False
        
        print('🔍 Testing Railway PostgreSQL connection...')
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute('SELECT version();')
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f'✅ Database connection successful!')
        print(f'📊 PostgreSQL version: {version[:50]}...')
        return True
        
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
        return False

if __name__ == '__main__':
    success = test_railway_connection()
    
    # Write to GitHub output
    github_output = os.getenv('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            if success:
                f.write('healthy=true\n')
            else:
                f.write('healthy=false\n')
    
    sys.exit(0 if success else 1)