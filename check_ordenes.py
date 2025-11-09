import requests
import json

# Consultar endpoint
response = requests.get('https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all', timeout=30)
data = response.json()['data']

# Detectar órdenes de compra
ordenes = [c for c in data if 'numero_orden' in c or c.get('tipo_documento') == 'ORDEN_COMPRA_TVEC' or c.get('tipo_contrato') == 'Orden de Compra']

print(f'Total contratos: {len(data)}')
print(f'Órdenes de compra detectadas: {len(ordenes)}')
print(f'\nCampos disponibles en primer contrato: {list(data[0].keys())}')
print(f'\nPrimer contrato:')
print(f'  - tipo_documento: {data[0].get("tipo_documento")}')
print(f'  - tipo_contrato: {data[0].get("tipo_contrato")}')
print(f'  - referencia_contrato: {data[0].get("referencia_contrato")}')

if ordenes:
    print(f'\n\n📋 PRIMERA ORDEN DE COMPRA DETECTADA:')
    print(json.dumps(ordenes[0], indent=2))
else:
    print('\n\n⚠️  NO SE DETECTARON ÓRDENES DE COMPRA')
    print('\nMuestra de 5 contratos con sus tipos:')
    for i, c in enumerate(data[:5]):
        print(f'\n{i+1}. {c.get("referencia_contrato", "N/A")}:')
        print(f'   tipo_documento: {c.get("tipo_documento", "N/A")}')
        print(f'   tipo_contrato: {c.get("tipo_contrato", "N/A")}')
        print(f'   numero_orden: {c.get("numero_orden", "N/A")}')
