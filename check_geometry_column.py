import json

# Load data
with open('transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total records: {len(data)}")
print(f"\nColumns: {list(data[0].keys())}")

print(f"\nGeometry column samples (first 3):")
for i, rec in enumerate(data[:3]):
    geom = rec.get('geom', rec.get('geometry', rec.get('geometria', 'N/A')))
    print(f"  [{i+1}] geom: {str(geom)[:150]}")
    
print(f"\nChecking for geometry-related columns:")
geom_cols = [col for col in data[0].keys() if 'geo' in col.lower() or 'coord' in col.lower()]
print(f"  Found: {geom_cols}")
