# -*- coding: utf-8 -*-
"""
Script para corregir el encoding de los archivos GeoJSON de basemaps.
"""

import geopandas as gpd
import json

def fix_encoding(text):
    """Corrige caracteres mal codificados en español."""
    if not isinstance(text, str):
        return text
    
    # Mapeo de caracteres corruptos comunes
    replacements = {
        'Benjam�n': 'Benjamín',
        'Urbanizaci�n': 'Urbanización',
        'Campi�a': 'Campiña',
        'Eli�cer': 'Eliécer',
        'Gait�n': 'Gaitán',
        'Su�rez': 'Suárez',
        'M�nica': 'Mónica',
        'Berl�n': 'Berlín',
        'F�tima': 'Fátima',
        'A�rea': 'Aérea',
        'Nicol�s': 'Nicolás',
        'Juanamb�': 'Juanambú',
        'Ca�a': 'Caña',
        'Pe��n': 'Peñón',
        'Am�ricas': 'Américas',
        'Tr�bol': 'Trébol',
        'B�rbara': 'Bárbara',
        'Belalc�zar': 'Belalcázar',
        'Sim�n': 'Simón',
        'Bol�var': 'Bolívar',
        'Morti�al': 'Mortiñal',
        'F�': 'Fé',
        'Breta�a': 'Bretaña',
        'Jos�': 'José',
        'Marroqu�n': 'Marroquín',
        'Beltr�n': 'Beltrán',
        'Jun�n': 'Junín',
        'Balc�zar': 'Balcázar',
        'Crist�bal': 'Cristóbal',
        'Para�so': 'Paraíso',
        'Bel�n': 'Belén',
        'Ol�mpico': 'Olímpico',
        'Jard�n': 'Jardín',
        'Rinc�n': 'Rincón',
        'Mar�a': 'María',
        'C�rdoba': 'Córdoba',
        'Col�n': 'Colón',
        'Le�n': 'León',
        'Arag�n': 'Aragón',
        'C�mbulos': 'Cómbulos',
        'Nari�o': 'Nariño',
        'Orqu�deas': 'Orquídeas',
        'Uni�n': 'Unión',
        'G�mez': 'Gómez',
        'Ca�averalejo': 'Cañaveralejo',
        'Ca�averal': 'Cañaveral',
        'Rep�blica': 'República',
        'Holgu�n': 'Holguín',
        'Garc�s': 'Garcés',
        'Alf�rez': 'Alférez',
        'Ram�rez': 'Ramírez',
        'N�poles': 'Nápoles',
        'Mel�ndez': 'Meléndez',
        'Jord�n': 'Jordán',
        'Silo�': 'Siloé',
    }
    
    result = text
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def fix_basemap(input_file, output_file, column_name):
    """Corrige el encoding de un archivo GeoJSON."""
    print(f"Corrigiendo {input_file}...")
    
    # Leer el archivo
    gdf = gpd.read_file(input_file)
    print(f"  Total registros: {len(gdf)}")
    
    # Verificar caracteres corruptos antes
    corrupted_before = gdf[gdf[column_name].astype(str).str.contains('�', na=False)]
    print(f"  Registros con caracteres corruptos: {len(corrupted_before)}")
    
    # Aplicar corrección
    gdf[column_name] = gdf[column_name].apply(fix_encoding)
    
    # Verificar después
    corrupted_after = gdf[gdf[column_name].astype(str).str.contains('�', na=False)]
    print(f"  Registros corruptos después: {len(corrupted_after)}")
    
    # Guardar con encoding UTF-8
    gdf.to_file(output_file, driver='GeoJSON', encoding='utf-8')
    print(f"  ✓ Guardado en {output_file}")
    
    return gdf


if __name__ == "__main__":
    print("="*80)
    print("CORRIGIENDO ENCODING DE BASEMAPS")
    print("="*80)
    print()
    
    # Corregir barrios_veredas
    gdf_barrios = fix_basemap(
        'basemaps/barrios_veredas.geojson',
        'basemaps/barrios_veredas.geojson',
        'barrio_vereda'
    )
    
    print()
    
    # Corregir comunas_corregimientos
    gdf_comunas = fix_basemap(
        'basemaps/comunas_corregimientos.geojson',
        'basemaps/comunas_corregimientos.geojson',
        'comuna_corregimiento'
    )
    
    print()
    print("="*80)
    print("VERIFICACIÓN")
    print("="*80)
    
    # Verificar Benjamín Herrera
    benjamins = gdf_barrios[gdf_barrios['barrio_vereda'].str.contains('Benjam', na=False)]
    if len(benjamins) > 0:
        print(f"Benjamín Herrera: {benjamins['barrio_vereda'].unique()[0]}")
    
    # Mostrar algunos ejemplos
    print("\nEjemplos de barrios corregidos:")
    examples = gdf_barrios[gdf_barrios['barrio_vereda'].str.contains('[áéíóúñ]', case=False, na=False, regex=True)]
    if len(examples) > 0:
        for name in examples['barrio_vereda'].unique()[:10]:
            print(f"  - {name}")
    
    print("\n✓ Corrección completada")
