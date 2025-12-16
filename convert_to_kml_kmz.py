import geopandas as gpd
import fiona
import os
from pathlib import Path
import zipfile
import shutil

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

def convert_geojson_to_kml_kmz(geojson_path, output_dir=None):
    """
    Convert GeoJSON to KML and KMZ formats for Google My Maps
    
    Parameters:
    -----------
    geojson_path : str
        Path to the input GeoJSON file
    output_dir : str, optional
        Directory for output files. If None, uses same directory as input
    
    Returns:
    --------
    tuple : Paths to the created KML and KMZ files
    """
    
    print("="*70)
    print("Converting GeoJSON to KML/KMZ for Google My Maps")
    print("="*70)
    
    # Validate input file exists
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")
    
    print(f"\nReading GeoJSON file: {geojson_path}")
    
    # Read the GeoJSON file
    gdf = gpd.read_file(geojson_path)
    
    print(f"✓ Loaded {len(gdf)} features")
    print(f"  Geometry types: {gdf.geometry.geom_type.unique()}")
    print(f"  CRS: {gdf.crs}")
    
    # Ensure CRS is WGS84 (required for KML)
    if gdf.crs is None:
        print("  Setting CRS to EPSG:4326 (WGS84)")
        gdf.set_crs("EPSG:4326", inplace=True)
    elif gdf.crs != "EPSG:4326":
        print(f"  Converting from {gdf.crs} to EPSG:4326 (WGS84)")
        gdf = gdf.to_crs("EPSG:4326")
    
    # Prepare output paths
    input_path = Path(geojson_path)
    if output_dir is None:
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    kml_path = output_dir / f"{input_path.stem}.kml"
    kmz_path = output_dir / f"{input_path.stem}.kmz"
    
    # Include all requested columns for Google My Maps
    important_cols = [
        'referencia_proceso', 'referencia_contrato', 'bpin', 
        'identificador', 'tipo_equipamiento', 'fuente_financiacion',
        'nombre_up', 'nombre_up_detalle', 'comuna_corregimiento',
        'tipo_intervencion', 'unidad', 'cantidad', 'direccion',
        'barrio_vereda', 'estado', 'presupuesto_base', 'avance_obra',
        'ano', 'fecha_inicio', 'fecha_fin', 'plataforma', 'url_proceso',
        'clase_obra', 'descripcion_intervencion', 'nombre_centro_gestor',
        'geometry'
    ]
    
    # Filter to only existing columns
    cols_to_keep = [col for col in important_cols if col in gdf.columns]
    gdf_export = gdf[cols_to_keep].copy()
    
    # Add lat/lon columns from geometry
    gdf_export['lon'] = gdf_export.geometry.x
    gdf_export['lat'] = gdf_export.geometry.y
    
    # Create a "Name" field for better display in Google My Maps
    if 'nombre_up' in gdf_export.columns:
        gdf_export['Name'] = gdf_export['nombre_up']
    elif 'identificador' in gdf_export.columns:
        gdf_export['Name'] = gdf_export['identificador']
    else:
        gdf_export['Name'] = [f"Feature {i+1}" for i in range(len(gdf_export))]
    
    # Create a "Description" field with ALL information
    def create_description(row):
        desc_parts = []
        
        # Información de Identificación
        desc_parts.append("<b>=== IDENTIFICACIÓN ===</b>")
        if 'referencia_proceso' in row and row['referencia_proceso']:
            desc_parts.append(f"<b>Ref. Proceso:</b> {row['referencia_proceso']}")
        if 'referencia_contrato' in row and row['referencia_contrato']:
            desc_parts.append(f"<b>Ref. Contrato:</b> {row['referencia_contrato']}")
        if 'bpin' in row and row['bpin']:
            desc_parts.append(f"<b>BPIN:</b> {row['bpin']}")
        if 'identificador' in row and row['identificador']:
            desc_parts.append(f"<b>Identificador:</b> {row['identificador']}")
        
        # Información del Proyecto
        desc_parts.append("<br><b>=== PROYECTO ===</b>")
        if 'nombre_up' in row and row['nombre_up']:
            desc_parts.append(f"<b>Nombre:</b> {row['nombre_up']}")
        if 'nombre_up_detalle' in row and row['nombre_up_detalle']:
            desc_parts.append(f"<b>Detalle:</b> {row['nombre_up_detalle']}")
        if 'tipo_equipamiento' in row and row['tipo_equipamiento']:
            desc_parts.append(f"<b>Tipo Equip.:</b> {row['tipo_equipamiento']}")
        if 'clase_obra' in row and row['clase_obra']:
            desc_parts.append(f"<b>Clase Obra:</b> {row['clase_obra']}")
        if 'tipo_intervencion' in row and row['tipo_intervencion']:
            desc_parts.append(f"<b>Tipo Interv.:</b> {row['tipo_intervencion']}")
        if 'descripcion_intervencion' in row and row['descripcion_intervencion']:
            desc_parts.append(f"<b>Descripción:</b> {row['descripcion_intervencion']}")
        
        # Ubicación
        desc_parts.append("<br><b>=== UBICACIÓN ===</b>")
        if 'direccion' in row and row['direccion']:
            desc_parts.append(f"<b>Dirección:</b> {row['direccion']}")
        if 'comuna_corregimiento' in row and row['comuna_corregimiento']:
            desc_parts.append(f"<b>Comuna:</b> {row['comuna_corregimiento']}")
        if 'barrio_vereda' in row and row['barrio_vereda']:
            desc_parts.append(f"<b>Barrio:</b> {row['barrio_vereda']}")
        if 'lat' in row and row['lat']:
            desc_parts.append(f"<b>Latitud:</b> {row['lat']:.6f}")
        if 'lon' in row and row['lon']:
            desc_parts.append(f"<b>Longitud:</b> {row['lon']:.6f}")
        
        # Estado y Avance
        desc_parts.append("<br><b>=== ESTADO ===</b>")
        if 'estado' in row and row['estado']:
            desc_parts.append(f"<b>Estado:</b> {row['estado']}")
        if 'avance_obra' in row and row['avance_obra']:
            desc_parts.append(f"<b>Avance:</b> {row['avance_obra']}%")
        
        # Información Financiera
        desc_parts.append("<br><b>=== FINANCIERO ===</b>")
        if 'presupuesto_base' in row and row['presupuesto_base']:
            try:
                presupuesto = f"${float(row['presupuesto_base']):,.0f}"
                desc_parts.append(f"<b>Presupuesto:</b> {presupuesto}")
            except:
                pass
        if 'fuente_financiacion' in row and row['fuente_financiacion']:
            desc_parts.append(f"<b>Fuente:</b> {row['fuente_financiacion']}")
        
        # Cantidades
        if 'cantidad' in row and row['cantidad']:
            unidad = row.get('unidad', '')
            desc_parts.append(f"<b>Cantidad:</b> {row['cantidad']} {unidad}")
        
        # Fechas
        desc_parts.append("<br><b>=== FECHAS ===</b>")
        if 'ano' in row and row['ano']:
            desc_parts.append(f"<b>Año:</b> {row['ano']}")
        if 'fecha_inicio' in row and row['fecha_inicio']:
            desc_parts.append(f"<b>Inicio:</b> {row['fecha_inicio']}")
        if 'fecha_fin' in row and row['fecha_fin']:
            desc_parts.append(f"<b>Fin:</b> {row['fecha_fin']}")
        
        # Gestión
        desc_parts.append("<br><b>=== GESTIÓN ===</b>")
        if 'nombre_centro_gestor' in row and row['nombre_centro_gestor']:
            desc_parts.append(f"<b>Centro Gestor:</b> {row['nombre_centro_gestor']}")
        if 'plataforma' in row and row['plataforma']:
            desc_parts.append(f"<b>Plataforma:</b> {row['plataforma']}")
        if 'url_proceso' in row and row['url_proceso']:
            desc_parts.append(f"<b>URL:</b> <a href='{row['url_proceso']}' target='_blank'>Ver proceso</a>")
        
        return "<br>".join(desc_parts) if desc_parts else "Sin descripción"
    
    gdf_export['Description'] = gdf_export.apply(create_description, axis=1)
    
    # Move Name and Description to the front
    cols = ['Name', 'Description'] + [col for col in gdf_export.columns if col not in ['Name', 'Description', 'geometry']] + ['geometry']
    cols = [col for col in cols if col in gdf_export.columns]
    gdf_export = gdf_export[cols]
    
    print(f"\n{'='*70}")
    print("Exporting to KML...")
    print(f"{'='*70}")
    
    # Save as KML
    print(f"Creating KML file: {kml_path}")
    gdf_export.to_file(kml_path, driver='KML')
    print(f"✓ KML file created successfully!")
    print(f"  File size: {os.path.getsize(kml_path) / 1024:.2f} KB")
    
    # Create KMZ (compressed KML) manually using zip
    print(f"\nCreating KMZ file: {kmz_path}")
    try:
        # KMZ is just a zipped KML file with .kmz extension
        with zipfile.ZipFile(kmz_path, 'w', zipfile.ZIP_DEFLATED) as kmz:
            # Add the KML file to the zip (must be named doc.kml inside the KMZ)
            kmz.write(kml_path, arcname='doc.kml')
        
        print(f"✓ KMZ file created successfully!")
        print(f"  File size: {os.path.getsize(kmz_path) / 1024:.2f} KB")
    except Exception as e:
        print(f"⚠ Warning: Could not create KMZ file: {e}")
        print(f"  You can still use the KML file for Google My Maps")
    
    print(f"\n{'='*70}")
    print("CONVERSION COMPLETE")
    print(f"{'='*70}")
    print(f"Total features: {len(gdf_export)}")
    print(f"\nOutput files:")
    print(f"  KML: {kml_path}")
    print(f"  KMZ: {kmz_path}")
    print(f"\n📍 These files are ready to upload to Google My Maps!")
    print(f"   Visit: https://www.google.com/mymaps")
    print(f"{'='*70}")
    
    return str(kml_path), str(kmz_path)


if __name__ == "__main__":
    # Input GeoJSON file
    geojson_file = r"app_outputs\unidades_proyecto_pulmon_oriente.geojson"
    
    # Output directory (optional - will use same as input if not specified)
    output_directory = r"app_outputs"
    
    try:
        # Convert to KML and KMZ
        kml_file, kmz_file = convert_geojson_to_kml_kmz(geojson_file, output_directory)
        
    except Exception as e:
        print(f"\n❌ Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()
