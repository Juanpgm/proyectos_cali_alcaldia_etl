# -*- coding: utf-8 -*-
"""
Script para contar categorías en Firebase después de la actualización del pipeline.
Genera un reporte completo de todas las categorías normalizadas.
"""

import sys
import os
from collections import Counter
from typing import Dict, Any

# Agregar rutas necesarias
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def count_categories_in_firebase(collection_name: str = "unidades_proyecto") -> Dict[str, Any]:
    """
    Cuenta todas las categorías en Firebase y genera un reporte.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        Diccionario con los conteos de todas las categorías
    """
    print(f"{'='*80}")
    print(f"REPORTE DE CATEGORÍAS - FIREBASE")
    print(f"{'='*80}\n")
    
    try:
        db = get_firestore_client()
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return {}
        
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()
        
        # Contadores para cada categoría
        estados = Counter()
        fuentes_financiacion = Counter()
        tipos_intervencion = Counter()
        tipos_equipamiento = Counter()
        clases_up = Counter()
        centros_gestores = Counter()
        comunas_corregimientos = Counter()
        frente_activo = Counter()
        geometry_types = Counter()
        
        total_docs = 0
        docs_con_geometria = 0
        
        print("📥 Obteniendo datos de Firebase...")
        
        for doc in docs:
            total_docs += 1
            data = doc.to_dict()
            
            # Firebase puede almacenar las propiedades directamente o anidadas
            # Primero intentar obtener de 'properties', sino del nivel raíz
            if 'properties' in data and isinstance(data['properties'], dict):
                properties = data['properties']
            else:
                properties = data
            
            # Contar estados
            estado = properties.get('estado', 'Sin estado')
            if estado is None or estado == '':
                estado = 'Sin estado'
            estados[estado] += 1
            
            # Contar fuentes de financiación
            fuente = properties.get('fuente_financiacion', 'Sin fuente')
            if fuente is None or fuente == '':
                fuente = 'Sin fuente'
            fuentes_financiacion[fuente] += 1
            
            # Contar tipos de intervención
            tipo_int = properties.get('tipo_intervencion', 'Sin tipo')
            if tipo_int is None or tipo_int == '':
                tipo_int = 'Sin tipo'
            tipos_intervencion[tipo_int] += 1
            
            # Contar tipos de equipamiento
            tipo_equip = properties.get('tipo_equipamiento', 'Sin tipo')
            if tipo_equip is None or tipo_equip == '':
                tipo_equip = 'Sin tipo'
            tipos_equipamiento[tipo_equip] += 1
            
            # Contar clases UP
            clase = properties.get('clase_up', 'Sin clase')
            if clase is None or clase == '':
                clase = 'Sin clase'
            clases_up[clase] += 1
            
            # Contar centros gestores
            centro = properties.get('nombre_centro_gestor', 'Sin centro')
            if centro is None or centro == '':
                centro = 'Sin centro'
            centros_gestores[centro] += 1
            
            # Contar comunas/corregimientos
            comuna = properties.get('comuna_corregimiento', 'Sin comuna')
            if comuna is None or comuna == '':
                comuna = 'Sin comuna'
            comunas_corregimientos[comuna] += 1
            
            # Contar frente activo
            frente = properties.get('frente_activo', 'Sin dato')
            if frente is None or frente == '':
                frente = 'Sin dato'
            frente_activo[frente] += 1
            
            # Contar tipo de geometría
            geometry = data.get('geometry', {})
            if geometry and geometry.get('type'):
                geometry_types[geometry.get('type')] += 1
                docs_con_geometria += 1
        
        print(f"✅ Total de registros analizados: {total_docs}\n")
        
        # Imprimir resultados
        print(f"{'='*80}")
        print("📊 CONTEO POR CATEGORÍAS")
        print(f"{'='*80}\n")
        
        # Estados
        print(f"{'─'*80}")
        print("🔵 ESTADOS")
        print(f"{'─'*80}")
        for estado, count in estados.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {estado:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(estados.values()):>5}\n")
        
        # Fuentes de Financiación
        print(f"{'─'*80}")
        print("💰 FUENTES DE FINANCIACIÓN")
        print(f"{'─'*80}")
        for fuente, count in fuentes_financiacion.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {fuente:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(fuentes_financiacion.values()):>5}\n")
        
        # Tipos de Intervención
        print(f"{'─'*80}")
        print("🔧 TIPOS DE INTERVENCIÓN")
        print(f"{'─'*80}")
        for tipo, count in tipos_intervencion.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {tipo:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(tipos_intervencion.values()):>5}\n")
        
        # Tipos de Equipamiento (Top 10)
        print(f"{'─'*80}")
        print("🏢 TIPOS DE EQUIPAMIENTO (Top 15)")
        print(f"{'─'*80}")
        for tipo, count in tipos_equipamiento.most_common(15):
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {tipo:.<50} {count:>5} ({percentage:>5.1f}%)")
        if len(tipos_equipamiento) > 15:
            otros_count = sum(count for tipo, count in tipos_equipamiento.most_common()[15:])
            otros_percentage = (otros_count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {'Otros (' + str(len(tipos_equipamiento) - 15) + ' tipos)':.<50} {otros_count:>5} ({otros_percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(tipos_equipamiento.values()):>5}\n")
        
        # Clases UP
        print(f"{'─'*80}")
        print("📋 CLASES UP")
        print(f"{'─'*80}")
        for clase, count in clases_up.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {clase:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(clases_up.values()):>5}\n")
        
        # Frente Activo
        print(f"{'─'*80}")
        print("🚧 FRENTE ACTIVO")
        print(f"{'─'*80}")
        for frente, count in frente_activo.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {frente:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(frente_activo.values()):>5}\n")
        
        # Centros Gestores
        print(f"{'─'*80}")
        print("🏛️  CENTROS GESTORES")
        print(f"{'─'*80}")
        for centro, count in centros_gestores.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {centro:.<50} {count:>5} ({percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(centros_gestores.values()):>5}\n")
        
        # Comunas/Corregimientos (Top 15)
        print(f"{'─'*80}")
        print("📍 COMUNAS/CORREGIMIENTOS (Top 15)")
        print(f"{'─'*80}")
        for comuna, count in comunas_corregimientos.most_common(15):
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {comuna:.<50} {count:>5} ({percentage:>5.1f}%)")
        if len(comunas_corregimientos) > 15:
            otros_count = sum(count for comuna, count in comunas_corregimientos.most_common()[15:])
            otros_percentage = (otros_count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {'Otros (' + str(len(comunas_corregimientos) - 15) + ' ubicaciones)':.<50} {otros_count:>5} ({otros_percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {sum(comunas_corregimientos.values()):>5}\n")
        
        # Geometrías
        print(f"{'─'*80}")
        print("🗺️  TIPOS DE GEOMETRÍA")
        print(f"{'─'*80}")
        for geom_type, count in geometry_types.most_common():
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {geom_type:.<50} {count:>5} ({percentage:>5.1f}%)")
        sin_geometria = total_docs - docs_con_geometria
        sin_geometria_percentage = (sin_geometria / total_docs * 100) if total_docs > 0 else 0
        print(f"  {'Sin geometría':.<50} {sin_geometria:>5} ({sin_geometria_percentage:>5.1f}%)")
        print(f"  {'Total':.>50} {total_docs:>5}\n")
        
        print(f"{'='*80}")
        print("✅ REPORTE COMPLETADO")
        print(f"{'='*80}\n")
        
        return {
            'total_docs': total_docs,
            'estados': dict(estados),
            'fuentes_financiacion': dict(fuentes_financiacion),
            'tipos_intervencion': dict(tipos_intervencion),
            'tipos_equipamiento': dict(tipos_equipamiento),
            'clases_up': dict(clases_up),
            'centros_gestores': dict(centros_gestores),
            'comunas_corregimientos': dict(comunas_corregimientos),
            'frente_activo': dict(frente_activo),
            'geometry_types': dict(geometry_types),
            'docs_con_geometria': docs_con_geometria
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo datos de Firebase: {e}")
        import traceback
        traceback.print_exc()
        return {}


if __name__ == "__main__":
    count_categories_in_firebase()
