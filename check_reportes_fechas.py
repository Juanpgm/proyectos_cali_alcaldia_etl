from database.config import get_firestore_client
from datetime import datetime

db = get_firestore_client()

# Obtener reportes
reportes = list(db.collection('reportes_contratos').limit(10).stream())
print(f'Total reportes (muestra): {len(reportes)}\n')

for i, r in enumerate(reportes[:5]):
    data = r.to_dict()
    fecha_reporte = data.get('fecha_reporte')
    ref_contrato = data.get('referencia_contrato')
    
    print(f'{i+1}. Reporte {r.id}:')
    print(f'   referencia_contrato: {ref_contrato}')
    print(f'   fecha_reporte: {fecha_reporte} (tipo: {type(fecha_reporte).__name__})')
    print(f'   avance_financiero: {data.get("avance_financiero", "N/A")}')
    print(f'   avance_fisico: {data.get("avance_fisico", "N/A")}\n')

# Contar reportes con fecha válida
print("\n📊 ANÁLISIS DE FECHAS:")
reportes_all = list(db.collection('reportes_contratos').stream())
total = len(reportes_all)
con_fecha = sum(1 for r in reportes_all if r.to_dict().get('fecha_reporte'))
sin_fecha = total - con_fecha

print(f'Total reportes: {total}')
print(f'Con fecha_reporte: {con_fecha}')
print(f'Sin fecha_reporte: {sin_fecha}')
