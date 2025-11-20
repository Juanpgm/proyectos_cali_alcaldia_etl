# -*- coding: utf-8 -*-
"""
Test script to upload a single LineString document to Firebase for debugging.
"""

import os
import json
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

# Load one feature from the GeoJSON
with open('context/unidades_proyecto.geojson', 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

feature = geojson_data['features'][0]

print("Original feature:")
print(json.dumps(feature, indent=2))

# Extract geometry and properties
geometry = feature.get('geometry')
properties = feature.get('properties', {})

# Clean coordinates to 2D
coords = geometry.get('coordinates', [])
clean_coords = [[c[0], c[1]] if len(c) > 2 else c for c in coords]

# Prepare document
document_data = {}

# Add all properties (serialize values)
for key, value in properties.items():
    if not key or not isinstance(key, str) or not key.strip():
        continue
    
    # Convert NaN to None
    if value is None or (isinstance(value, float) and (str(value) == 'nan' or str(value) == 'NaN')):
        document_data[key.strip()] = None
    else:
        document_data[key.strip()] = str(value) if not isinstance(value, (int, float, bool)) else value

# Add tipo_equipamiento
document_data['tipo_equipamiento'] = 'Vias'

# Add geometry - Firebase no acepta arrays anidados profundos
# Serializar coordinates como JSON string
document_data['geometry'] = {
    'type': geometry.get('type'),
    'coordinates': json.dumps(clean_coords)  # Serializar como string
}

# Add metadata
document_data['has_geometry'] = True
document_data['geometry_type'] = geometry.get('type')
document_data['created_at'] = datetime.now().isoformat()
document_data['updated_at'] = datetime.now().isoformat()

print("\nPrepared document:")
print(json.dumps(document_data, indent=2, default=str))

# Try to upload
try:
    db = get_firestore_client()
    doc_ref = db.collection('unidades_proyecto').document('TEST-LINESTRING-001')
    doc_ref.set(document_data)
    print("\n✅ Document uploaded successfully!")
except Exception as e:
    print(f"\n❌ Error uploading document: {e}")
    import traceback
    traceback.print_exc()
