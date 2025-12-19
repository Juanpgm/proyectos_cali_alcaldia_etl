"""
Script para ejecutar Quality Control sobre el GeoJSON transformado
y generar reporte HTML completo con métricas visuales
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.quality_control import validate_geojson


def validate_administrative_values(geojson_path):
    """Valida que los valores administrativos coincidan con los basemaps"""
    base_dir = Path(geojson_path).parent.parent
    
    # Load GeoJSON
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Load basemaps
    barrios_path = base_dir / 'basemaps' / 'barrios_veredas.geojson'
    comunas_path = base_dir / 'basemaps' / 'comunas_corregimientos.geojson'
    
    barrio_basemap = set()
    comuna_basemap = set()
    
    if barrios_path.exists():
        with open(barrios_path, 'r', encoding='utf-8') as f:
            barrios_data = json.load(f)
        barrio_basemap = set(f['properties']['barrio_vereda'] for f in barrios_data['features'])
    
    if comunas_path.exists():
        with open(comunas_path, 'r', encoding='utf-8') as f:
            comunas_data = json.load(f)
        comuna_basemap = set(f['properties']['comuna_corregimiento'] for f in comunas_data['features'])
    
    # Extract values from GeoJSON
    comunas_geojson = set()
    barrios_geojson = set()
    
    for feature in data['features']:
        props = feature['properties']
        if props.get('comuna_corregimiento'):
            comunas_geojson.add(props['comuna_corregimiento'])
        if props.get('barrio_vereda'):
            barrios_geojson.add(props['barrio_vereda'])
    
    # Calculate matches
    comunas_correct = comunas_geojson.intersection(comuna_basemap)
    comunas_not_in_basemap = comunas_geojson - comuna_basemap
    
    barrios_correct = barrios_geojson.intersection(barrio_basemap)
    barrios_not_in_basemap = barrios_geojson - barrio_basemap
    
    # Coverage
    total_features = len(data['features'])
    features_with_comuna = sum(1 for f in data['features'] if f['properties'].get('comuna_corregimiento'))
    features_with_barrio = sum(1 for f in data['features'] if f['properties'].get('barrio_vereda'))
    
    return {
        'comunas': {
            'total': len(comunas_geojson),
            'basemap_total': len(comuna_basemap),
            'correct': len(comunas_correct),
            'not_in_basemap': len(comunas_not_in_basemap),
            'not_in_basemap_list': sorted(list(comunas_not_in_basemap)),
            'match_percentage': (len(comunas_correct) / len(comunas_geojson) * 100) if comunas_geojson else 0
        },
        'barrios': {
            'total': len(barrios_geojson),
            'basemap_total': len(barrio_basemap),
            'correct': len(barrios_correct),
            'not_in_basemap': len(barrios_not_in_basemap),
            'not_in_basemap_list': sorted(list(barrios_not_in_basemap))[:20],  # Only first 20
            'match_percentage': (len(barrios_correct) / len(barrios_geojson) * 100) if barrios_geojson else 0
        },
        'coverage': {
            'total_features': total_features,
            'with_comuna': features_with_comuna,
            'with_barrio': features_with_barrio,
            'comuna_percentage': (features_with_comuna / total_features * 100) if total_features else 0,
            'barrio_percentage': (features_with_barrio / total_features * 100) if total_features else 0
        }
    }

def generate_html_report(validation_result, geojson_path):
    """Genera un reporte HTML completo con las métricas de calidad"""
    
    stats = validation_result['statistics']
    
    # Colores por severidad
    severity_colors = {
        'CRITICAL': '#dc3545',
        'HIGH': '#fd7e14',
        'MEDIUM': '#ffc107',
        'LOW': '#0d6efd',
        'INFO': '#6c757d'
    }
    
    severity_icons = {
        'CRITICAL': '🔴',
        'HIGH': '🟠',
        'MEDIUM': '🟡',
        'LOW': '🔵',
        'INFO': '⚪'
    }
    
    # Preparar datos para gráficos
    severity_data = [(k, v) for k, v in stats['by_severity'].items()]
    severity_data.sort(key=lambda x: list(severity_colors.keys()).index(x[0]))
    
    dimension_data = [(k, v) for k, v in stats['by_dimension'].items()]
    dimension_data.sort(key=lambda x: x[1], reverse=True)
    
    top_issues_data = list(stats['top_issues'].items())[:10]
    
    # HTML Template
    html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Calidad - Unidades de Proyecto</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            color: #2d3748;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header .subtitle {{
            color: #718096;
            font-size: 1.1em;
        }}
        
        .header .metadata {{
            margin-top: 15px;
            padding-top: 15px;
            border-top: 2px solid #e2e8f0;
            color: #718096;
            font-size: 0.9em;
        }}
        
        .score-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            padding: 40px;
            margin-bottom: 20px;
            text-align: center;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .score-card .score {{
            font-size: 5em;
            font-weight: bold;
            margin: 20px 0;
        }}
        
        .score-card .rating {{
            font-size: 2em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 2px;
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(0,0,0,0.15);
        }}
        
        .card h3 {{
            color: #2d3748;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 15px;
            font-weight: 600;
        }}
        
        .card .value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }}
        
        .card .label {{
            color: #718096;
            font-size: 0.9em;
        }}
        
        .section {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        
        .section h2 {{
            color: #2d3748;
            font-size: 1.5em;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}
        
        .severity-bar {{
            margin-bottom: 15px;
        }}
        
        .severity-bar .label {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
            font-size: 0.9em;
        }}
        
        .severity-bar .bar {{
            height: 30px;
            border-radius: 5px;
            position: relative;
            overflow: hidden;
            background: #e2e8f0;
        }}
        
        .severity-bar .fill {{
            height: 100%;
            transition: width 0.5s ease;
            display: flex;
            align-items: center;
            padding-left: 10px;
            color: white;
            font-weight: 600;
        }}
        
        .issue-item {{
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 8px;
            background: #f7fafc;
            border-left: 4px solid #667eea;
        }}
        
        .issue-item .rule {{
            font-weight: 600;
            color: #2d3748;
            margin-bottom: 5px;
        }}
        
        .issue-item .count {{
            color: #667eea;
            font-weight: bold;
        }}
        
        .issue-item .severity {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 600;
            color: white;
            margin-left: 10px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        
        th {{
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        
        td {{
            padding: 12px;
            border-bottom: 1px solid #e2e8f0;
        }}
        
        tr:hover {{
            background: #f7fafc;
        }}
        
        .badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        
        .badge-critical {{ background: #dc3545; color: white; }}
        .badge-high {{ background: #fd7e14; color: white; }}
        .badge-medium {{ background: #ffc107; color: #000; }}
        .badge-low {{ background: #0d6efd; color: white; }}
        .badge-info {{ background: #6c757d; color: white; }}
        
        .progress-circle {{
            width: 200px;
            height: 200px;
            border-radius: 50%;
            background: conic-gradient(
                #667eea 0deg {stats['quality_score'] * 3.6}deg,
                #e2e8f0 {stats['quality_score'] * 3.6}deg 360deg
            );
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 20px auto;
            position: relative;
        }}
        
        .progress-circle::before {{
            content: '';
            position: absolute;
            width: 160px;
            height: 160px;
            border-radius: 50%;
            background: white;
        }}
        
        .progress-circle .score-text {{
            position: relative;
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }}
        
        .alert {{
            padding: 15px 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid;
        }}
        
        .alert-danger {{
            background: #fee;
            border-color: #dc3545;
            color: #721c24;
        }}
        
        .alert-warning {{
            background: #fff3cd;
            border-color: #ffc107;
            color: #856404;
        }}
        
        .alert-success {{
            background: #d4edda;
            border-color: #28a745;
            color: #155724;
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            
            .container {{
                max-width: 100%;
            }}
            
            .card:hover {{
                transform: none;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>📊 Reporte de Calidad de Datos</h1>
            <div class="subtitle">Unidades de Proyecto - Sistema ETL Cali</div>
            <div class="metadata">
                <strong>Archivo:</strong> {os.path.basename(geojson_path)}<br>
                <strong>Fecha de validación:</strong> {datetime.fromisoformat(validation_result['validated_at']).strftime('%d/%m/%Y %H:%M:%S')}<br>
                <strong>Estándar:</strong> ISO 19157 - Calidad de Datos Geográficos
            </div>
        </div>
        
        <!-- Score Card -->
        <div class="score-card">
            <div style="font-size: 1.5em; margin-bottom: 10px;">Puntuación Global de Calidad</div>
            <div class="score">{stats['quality_score']}<span style="font-size: 0.5em;">/100</span></div>
            <div class="rating">{stats['quality_rating']}</div>
            <div style="margin-top: 20px; font-size: 1.1em; opacity: 0.9;">
                {_get_quality_message(stats['quality_score'])}
            </div>
        </div>
        
        <!-- Quick Stats -->
        <div class="grid">
            <div class="card">
                <h3>Unidades de Proyecto</h3>
                <div class="value">{validation_result['total_unidades']:,}</div>
                <div class="label">Con {validation_result['total_intervenciones']:,} intervenciones</div>
            </div>
            
            <div class="card">
                <h3>Unidades con Problemas</h3>
                <div class="value" style="color: #fd7e14;">{validation_result['unidades_with_issues']:,}</div>
                <div class="label">{validation_result['unidades_with_issues']/validation_result['total_unidades']*100:.1f}% del total</div>
            </div>
            
            <div class="card">
                <h3>Intervenciones con Problemas</h3>
                <div class="value" style="color: #fd7e14;">{validation_result['intervenciones_with_issues']:,}</div>
                <div class="label">{validation_result['intervenciones_with_issues']/validation_result['total_intervenciones']*100:.1f}% del total</div>
            </div>
            
            <div class="card">
                <h3>Problemas Críticos</h3>
                <div class="value" style="color: #dc3545;">{stats['critical_issues']}</div>
                <div class="label">Requieren atención inmediata</div>
            </div>
        </div>
        
        <!-- Alertas -->
        {_generate_alerts_html(stats)}
        
        <!-- Distribución por Severidad -->
        <div class="section">
            <h2>📈 Distribución por Severidad</h2>
            {_generate_severity_bars_html(severity_data, validation_result['total_issues'], severity_colors, severity_icons)}
        </div>
        
        <!-- Dimensiones ISO 19157 -->
        <div class="section">
            <h2>🎯 Dimensiones de Calidad (ISO 19157)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Dimensión</th>
                        <th>Problemas</th>
                        <th>Porcentaje</th>
                        <th>Barra</th>
                    </tr>
                </thead>
                <tbody>
                    {_generate_dimension_rows_html(dimension_data, validation_result['total_issues'])}
                </tbody>
            </table>
        </div>
        
        <!-- Top 10 Problemas -->
        <div class="section">
            <h2>🔍 Top 10 Problemas Más Frecuentes</h2>
            {_generate_top_issues_html(top_issues_data, severity_colors)}
        </div>
        
        <!-- Problemas por Campo -->
        <div class="section">
            <h2>📋 Problemas por Campo</h2>
            <table>
                <thead>
                    <tr>
                        <th>Campo</th>
                        <th>Problemas</th>
                        <th>Tipos de Problemas</th>
                    </tr>
                </thead>
                <tbody>
                    {_generate_field_rows_html(stats['by_field'])}
                </tbody>
            </table>
        </div>
        
        <!-- Registros Duplicados -->
        {_generate_duplicates_section_html(validation_result)}
        
        <!-- Valores Administrativos -->
        {_generate_administrative_validation_html(geojson_path)}
        
        <!-- Footer -->
        <div class="section" style="text-align: center; color: #718096;">
            <p>Reporte generado automáticamente por el Sistema ETL de Unidades de Proyecto</p>
            <p style="margin-top: 10px; font-size: 0.9em;">
                © 2024-2025 Alcaldía de Cali - Todos los derechos reservados
            </p>
        </div>
    </div>
</body>
</html>
"""
    
    return html_content


def _get_quality_message(score):
    """Retorna un mensaje descriptivo según el score de calidad"""
    if score >= 90:
        return "🎉 Excelente calidad de datos. El conjunto cumple con altos estándares de calidad."
    elif score >= 75:
        return "✅ Buena calidad de datos. Pocos problemas que no afectan significativamente la usabilidad."
    elif score >= 60:
        return "⚠️ Calidad aceptable. Se recomienda revisar y corregir los problemas detectados."
    elif score >= 40:
        return "🔧 Calidad regular. Es necesario tomar acciones correctivas para mejorar la calidad."
    else:
        return "❌ Calidad deficiente. Se requiere una revisión exhaustiva y corrección de problemas."


def _generate_alerts_html(stats):
    """Genera alertas según las métricas"""
    alerts = []
    
    if stats['critical_issues'] > 0:
        alerts.append(f'''
        <div class="alert alert-danger">
            <strong>⚠️ Atención:</strong> Se detectaron {stats['critical_issues']} problemas críticos que requieren corrección inmediata.
        </div>
        ''')
    
    if stats['actionable_issues'] > 50:
        alerts.append(f'''
        <div class="alert alert-warning">
            <strong>📌 Nota:</strong> Hay {stats['actionable_issues']} problemas de alta prioridad (críticos + altos) que deben ser atendidos.
        </div>
        ''')
    
    if stats['quality_score'] >= 90:
        alerts.append('''
        <div class="alert alert-success">
            <strong>✨ ¡Felicitaciones!</strong> Los datos cumplen con excelentes estándares de calidad.
        </div>
        ''')
    
    return ''.join(alerts)


def _generate_severity_bars_html(severity_data, total_issues, severity_colors, severity_icons):
    """Genera las barras de severidad"""
    html = ""
    for severity, count in severity_data:
        percentage = (count / total_issues * 100) if total_issues > 0 else 0
        color = severity_colors.get(severity, '#6c757d')
        icon = severity_icons.get(severity, '')
        
        html += f'''
        <div class="severity-bar">
            <div class="label">
                <span><strong>{icon} {severity}</strong></span>
                <span>{count:,} ({percentage:.1f}%)</span>
            </div>
            <div class="bar">
                <div class="fill" style="width: {percentage}%; background: {color};">
                    {count if percentage > 10 else ''}
                </div>
            </div>
        </div>
        '''
    
    return html


def _generate_dimension_rows_html(dimension_data, total_issues):
    """Genera las filas de dimensiones ISO"""
    html = ""
    for dimension, count in dimension_data:
        percentage = (count / total_issues * 100) if total_issues > 0 else 0
        html += f'''
        <tr>
            <td><strong>{dimension}</strong></td>
            <td>{count:,}</td>
            <td>{percentage:.1f}%</td>
            <td>
                <div style="background: #e2e8f0; border-radius: 5px; height: 20px; overflow: hidden;">
                    <div style="width: {percentage}%; background: #667eea; height: 100%;"></div>
                </div>
            </td>
        </tr>
        '''
    
    return html


def _generate_top_issues_html(top_issues_data, severity_colors):
    """Genera la lista de top problemas"""
    html = ""
    for i, (rule_id, info) in enumerate(top_issues_data, 1):
        severity_class = f"badge-{info['severity'].lower()}"
        html += f'''
        <div class="issue-item">
            <div class="rule">
                {i}. {rule_id} - {info['name']}
                <span class="badge {severity_class}">{info['severity']}</span>
            </div>
            <div>
                <span class="count">{info['count']:,} ocurrencias</span>
                <span style="color: #718096; margin-left: 15px;">Dimensión: {info['dimension']}</span>
            </div>
        </div>
        '''
    
    return html


def _generate_field_rows_html(by_field):
    """Genera las filas de problemas por campo"""
    html = ""
    # Ordenar por cantidad de problemas
    sorted_fields = sorted(by_field.items(), key=lambda x: x[1]['count'], reverse=True)
    
    for field, data in sorted_fields[:20]:  # Top 20 campos
        html += f'''
        <tr>
            <td><code>{field}</code></td>
            <td><strong>{data['count']:,}</strong></td>
            <td><span style="color: #718096; font-size: 0.9em;">{', '.join(data['issues'][:5])}</span></td>
        </tr>
        '''
    
    return html


def _generate_duplicates_section_html(validation_result):
    """Genera la sección de duplicados si existen"""
    if validation_result['duplicate_groups'] == 0:
        return ""
    
    html = f'''
    <div class="section">
        <h2>🔄 Registros Duplicados</h2>
        <div class="alert alert-warning">
            Se detectaron <strong>{validation_result['duplicate_groups']}</strong> grupos de registros duplicados, 
            afectando a <strong>{validation_result['duplicate_records']}</strong> registros en total.
        </div>
        <table>
            <thead>
                <tr>
                    <th>Grupo</th>
                    <th>Tamaño</th>
                    <th>UPIDs Duplicados</th>
                </tr>
            </thead>
            <tbody>
    '''
    
    for i, group in enumerate(validation_result['duplicate_details'][:10], 1):  # Primeros 10 grupos
        upids = ', '.join([dup['upid'] for dup in group])
        html += f'''
        <tr>
            <td>Grupo {i}</td>
            <td><strong>{len(group)}</strong> registros</td>
            <td><code>{upids}</code></td>
        </tr>
        '''
    
    html += '''
            </tbody>
        </table>
    </div>
    '''
    
    return html


def _generate_administrative_validation_html(geojson_path):
    """Genera la sección de validación de valores administrativos"""
    try:
        admin_validation = validate_administrative_values(geojson_path)
    except Exception as e:
        return f'''
        <div class="section">
            <h2>🗺️ Validación de Valores Administrativos</h2>
            <div class="alert alert-warning">
                ⚠️ No se pudo validar los valores administrativos: {str(e)}
            </div>
        </div>
        '''
    
    comunas = admin_validation['comunas']
    barrios = admin_validation['barrios']
    coverage = admin_validation['coverage']
    
    # Determinar color y mensaje según % de coincidencia
    comuna_color = '#28a745' if comunas['match_percentage'] >= 90 else '#ffc107' if comunas['match_percentage'] >= 70 else '#dc3545'
    barrio_color = '#28a745' if barrios['match_percentage'] >= 90 else '#ffc107' if barrios['match_percentage'] >= 70 else '#dc3545'
    
    html = f'''
    <div class="section">
        <h2>🗺️ Validación de Valores Administrativos</h2>
        <p style="color: #718096; margin-bottom: 20px;">
            Verificación de que los valores de comuna y barrio en el GeoJSON coincidan exactamente con los basemaps oficiales.
        </p>
        
        <!-- Grid de métricas administrativas -->
        <div class="grid" style="margin-bottom: 30px;">
            <div class="card">
                <h3>Cobertura Comuna</h3>
                <div class="value" style="color: {comuna_color};">{coverage['comuna_percentage']:.1f}%</div>
                <div class="label">{coverage['with_comuna']:,} de {coverage['total_features']:,} unidades</div>
            </div>
            
            <div class="card">
                <h3>Cobertura Barrio</h3>
                <div class="value" style="color: {barrio_color};">{coverage['barrio_percentage']:.1f}%</div>
                <div class="label">{coverage['with_barrio']:,} de {coverage['total_features']:,} unidades</div>
            </div>
            
            <div class="card">
                <h3>Comunas Coincidentes</h3>
                <div class="value" style="color: {comuna_color};">{comunas['match_percentage']:.1f}%</div>
                <div class="label">{comunas['correct']} de {comunas['total']} valores</div>
            </div>
            
            <div class="card">
                <h3>Barrios Coincidentes</h3>
                <div class="value" style="color: {barrio_color};">{barrios['match_percentage']:.1f}%</div>
                <div class="label">{barrios['correct']} de {barrios['total']} valores</div>
            </div>
        </div>
        
        <!-- Tabla de comunas -->
        <h3 style="margin-bottom: 15px;">📍 Comunas/Corregimientos</h3>
        <table style="margin-bottom: 30px;">
            <thead>
                <tr>
                    <th>Métrica</th>
                    <th>Valor</th>
                    <th>Detalles</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Valores únicos en GeoJSON</strong></td>
                    <td>{comunas['total']}</td>
                    <td>Valores distintos encontrados</td>
                </tr>
                <tr>
                    <td><strong>Valores en basemap oficial</strong></td>
                    <td>{comunas['basemap_total']}</td>
                    <td>Valores de referencia</td>
                </tr>
                <tr style="background: #d4edda;">
                    <td><strong>✅ Coincidencias exactas</strong></td>
                    <td><strong>{comunas['correct']}</strong></td>
                    <td>{comunas['match_percentage']:.1f}% de coincidencia</td>
                </tr>
                <tr style="background: {'#fff3cd' if comunas['not_in_basemap'] > 0 else 'transparent'};">
                    <td><strong>⚠️ NO encontrados en basemap</strong></td>
                    <td><strong>{comunas['not_in_basemap']}</strong></td>
                    <td>Valores que no están en el basemap oficial</td>
                </tr>
            </tbody>
        </table>
    '''
    
    # Lista de comunas no encontradas
    if comunas['not_in_basemap'] > 0:
        html += f'''
        <div class="alert alert-warning" style="margin-bottom: 30px;">
            <strong>⚠️ Comunas NO encontradas en basemap:</strong><br>
            <code style="display: block; margin-top: 10px; white-space: pre-wrap;">{', '.join(comunas['not_in_basemap_list'])}</code>
        </div>
        '''
    else:
        html += '''
        <div class="alert alert-success" style="margin-bottom: 30px;">
            <strong>🎉 ¡Perfecto!</strong> Todos los valores de comuna coinciden exactamente con el basemap oficial.
        </div>
        '''
    
    # Tabla de barrios
    html += f'''
        <h3 style="margin-bottom: 15px;">📍 Barrios/Veredas</h3>
        <table style="margin-bottom: 30px;">
            <thead>
                <tr>
                    <th>Métrica</th>
                    <th>Valor</th>
                    <th>Detalles</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Valores únicos en GeoJSON</strong></td>
                    <td>{barrios['total']}</td>
                    <td>Valores distintos encontrados</td>
                </tr>
                <tr>
                    <td><strong>Valores en basemap oficial</strong></td>
                    <td>{barrios['basemap_total']}</td>
                    <td>Valores de referencia</td>
                </tr>
                <tr style="background: #d4edda;">
                    <td><strong>✅ Coincidencias exactas</strong></td>
                    <td><strong>{barrios['correct']}</strong></td>
                    <td>{barrios['match_percentage']:.1f}% de coincidencia</td>
                </tr>
                <tr style="background: {'#fff3cd' if barrios['not_in_basemap'] > 0 else 'transparent'};">
                    <td><strong>⚠️ NO encontrados en basemap</strong></td>
                    <td><strong>{barrios['not_in_basemap']}</strong></td>
                    <td>Valores que no están en el basemap oficial</td>
                </tr>
            </tbody>
        </table>
    '''
    
    # Lista de primeros barrios no encontrados
    if barrios['not_in_basemap'] > 0:
        more_text = f" (mostrando primeros 20 de {barrios['not_in_basemap']})" if barrios['not_in_basemap'] > 20 else ""
        html += f'''
        <div class="alert alert-warning">
            <strong>⚠️ Barrios NO encontrados en basemap{more_text}:</strong><br>
            <code style="display: block; margin-top: 10px; white-space: pre-wrap; max-height: 200px; overflow-y: auto;">{', '.join(barrios['not_in_basemap_list'])}</code>
            <p style="margin-top: 10px; font-size: 0.9em;">
                <strong>Nota:</strong> Estos valores pueden ser variaciones legítimas (acentos, mayúsculas) o barrios nuevos que no están en el basemap.
            </p>
        </div>
        '''
    else:
        html += '''
        <div class="alert alert-success">
            <strong>🎉 ¡Perfecto!</strong> Todos los valores de barrio coinciden exactamente con el basemap oficial.
        </div>
        '''
    
    html += '''
    </div>
    '''
    
    return html


def main():
    """Función principal"""
    print("\n" + "="*80)
    print("🔍 CONTROL DE CALIDAD - UNIDADES DE PROYECTO")
    print("="*80)
    
    # Ruta del GeoJSON
    geojson_path = "app_outputs/unidades_proyecto_transformed.geojson"
    
    if not os.path.exists(geojson_path):
        print(f"❌ Error: No se encontró el archivo {geojson_path}")
        return False
    
    print(f"\n📁 Analizando: {geojson_path}")
    print(f"📊 Tamaño: {os.path.getsize(geojson_path) / 1024:.2f} KB")
    
    # Ejecutar validación
    print("\n🔄 Ejecutando validación de calidad...")
    validation_result = validate_geojson(geojson_path, verbose=True)
    
    # Guardar resultado JSON
    output_dir = Path("app_outputs/quality_reports")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    json_path = output_dir / f"quality_report_{timestamp}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(validation_result, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Reporte JSON guardado: {json_path}")
    
    # Generar reporte HTML
    print("🎨 Generando reporte HTML...")
    html_content = generate_html_report(validation_result, geojson_path)
    
    html_path = output_dir / f"quality_report_{timestamp}.html"
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Reporte HTML guardado: {html_path}")
    
    # Abrir en navegador
    print("\n🌐 Abriendo reporte en el navegador...")
    import webbrowser
    webbrowser.open(html_path.absolute().as_uri())
    
    print("\n" + "="*80)
    print("✅ CONTROL DE CALIDAD COMPLETADO")
    print("="*80)
    print(f"\n📊 Resumen:")
    print(f"   • Puntuación de calidad: {validation_result['statistics']['quality_score']}/100")
    print(f"   • Rating: {validation_result['statistics']['quality_rating']}")
    print(f"   • Total unidades: {validation_result['total_unidades']:,}")
    print(f"   • Total intervenciones: {validation_result['total_intervenciones']:,}")
    print(f"   • Unidades con problemas: {validation_result['unidades_with_issues']:,}")
    print(f"   • Intervenciones con problemas: {validation_result['intervenciones_with_issues']:,}")
    print(f"   • Problemas detectados: {validation_result['total_issues']:,}")
    print(f"\n📁 Archivos generados:")
    print(f"   • JSON: {json_path}")
    print(f"   • HTML: {html_path}")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
