import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
env_path = Path(__file__).parent / '.env.prod'
load_dotenv(env_path)

sys.path.insert(0, 'a:/programing_workspace/proyectos_cali_alcaldia_etl/utils')
from google_maps_geocoder import GoogleMapsGeocoder

# Test
geocoder = GoogleMapsGeocoder(use_adc=True)

# Test address in Cali
test_address = "Colombia, Valle del Cauca, Cali, Av. 4 Oe. #1-152"

print(f"Testing address: {test_address}")
print("="*80)

# Get coordinates
coords = geocoder.get_coordinates_from_address(test_address)

if coords:
    lat, lon = coords
    print(f"\n✓ get_coordinates_from_address() returned:")
    print(f"  lat = {lat}")
    print(f"  lon = {lon}")
    print(f"  Tuple: ({lat}, {lon})")
    
    # Now test what process_forward_geocoding would create
    import json
    
    geometry_geojson = {
        "type": "Point",
        "coordinates": [lat, lon]
    }
    
    json_str = json.dumps(geometry_geojson)
    
    print(f"\n✓ JSON created:")
    print(f"  {json_str}")
    
    # Parse it back
    parsed = json.loads(json_str)
    print(f"\n✓ Parsed back:")
    print(f"  coords[0] = {parsed['coordinates'][0]} ({'LAT' if 3 <= parsed['coordinates'][0] <= 4 else 'LON'})")
    print(f"  coords[1] = {parsed['coordinates'][1]} ({'LON' if -77 <= parsed['coordinates'][1] <= -76 else 'LAT'})")
    
    if 3 <= parsed['coordinates'][0] <= 4 and -77 <= parsed['coordinates'][1] <= -76:
        print(f"\n✅ FORMATO CORRECTO: [lat, lon]")
    else:
        print(f"\n❌ FORMATO INCORRECTO")
else:
    print("❌ No se pudieron obtener coordenadas")
