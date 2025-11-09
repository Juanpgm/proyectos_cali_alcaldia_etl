import requests
import json

# Consultar endpoint
response = requests.get('https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all', timeout=30)
print(f'Status: {response.status_code}')

data = response.json()
print(f'\nTotal registros: {len(data.get("data", []))}')

# Mostrar estructura del primer registro
if data.get('data'):
    print('\n=== PRIMER REGISTRO ===')
    print(json.dumps(data['data'][0], indent=2, default=str))
    
    # Mostrar campos disponibles
    print('\n=== CAMPOS DISPONIBLES ===')
    for key in sorted(data['data'][0].keys()):
        print(f"  - {key}")
