#!/bin/bash
# deploy-etl-server.sh
# Script para deployment en servidor Ubuntu/Debian

# 1. Instalar dependencias del sistema
sudo apt update
sudo apt install -y python3 python3-pip git cron postgresql-client

# 2. Clonar repositorio
git clone https://github.com/Juanpgm/proyectos_cali_alcaldia_etl.git
cd proyectos_cali_alcaldia_etl

# 3. Crear entorno virtual
python3 -m venv etl_env
source etl_env/bin/activate

# 4. Instalar dependencias Python
pip install -r requirements.txt

# 5. Configurar variables de entorno
echo "DATABASE_URL=postgresql://postgres:password@host:port/railway" > .env

# 6. Configurar cron job para ejecución diaria a las 2 AM
echo "0 2 * * * cd /path/to/proyectos_cali_alcaldia_etl && source etl_env/bin/activate && python load_app/bulk_load_data.py --data-type all >> /var/log/etl.log 2>&1" | crontab -

# 7. Ejecutar test inicial
python load_app/bulk_load_data.py --data-type all

echo "✅ ETL deployment completado!"
echo "📊 El ETL se ejecutará diariamente a las 2 AM"
echo "📝 Logs disponibles en /var/log/etl.log"