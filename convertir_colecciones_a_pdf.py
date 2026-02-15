# -*- coding: utf-8 -*-
"""
Script para convertir archivos Excel de colecciones Firebase a PDF
Mantiene los archivos originales
"""

import pandas as pd
import os
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, PageBreak, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT

def create_pdf_from_excel(excel_path, pdf_path, collection_name):
    """Convertir un archivo Excel a PDF"""
    print(f'\n📄 Convirtiendo: {collection_name}')
    
    try:
        # Leer el archivo Excel
        df = pd.read_excel(excel_path, engine='openpyxl')
        
        print(f'  ✓ Leído: {len(df)} filas, {len(df.columns)} columnas')
        
        # Configurar el documento PDF en orientación horizontal para más columnas
        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=landscape(letter),
            rightMargin=30,
            leftMargin=30,
            topMargin=40,
            bottomMargin=30
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#1f4788'),
            spaceAfter=20,
            alignment=TA_CENTER
        )
        
        info_style = ParagraphStyle(
            'InfoStyle',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.grey,
            spaceAfter=10,
            alignment=TA_CENTER
        )
        
        # Elementos del documento
        elements = []
        
        # Título
        title_text = f"Colección: {collection_name}"
        title = Paragraph(title_text, title_style)
        elements.append(title)
        
        # Información
        info_text = f"Total de registros: {len(df)} | Campos: {len(df.columns)} | Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        info = Paragraph(info_text, info_style)
        elements.append(info)
        elements.append(Spacer(1, 0.2*inch))
        
        # Limitar número de filas para el PDF (para que no sea demasiado grande)
        max_rows = 100
        if len(df) > max_rows:
            print(f'  ⚠️  Limitando a {max_rows} filas para el PDF (de {len(df)} totales)')
            df_display = df.head(max_rows)
            note_text = f"<b>Nota:</b> Mostrando las primeras {max_rows} filas de {len(df)} registros totales."
            note = Paragraph(note_text, info_style)
            elements.append(note)
            elements.append(Spacer(1, 0.1*inch))
        else:
            df_display = df
        
        # Limitar número de columnas para que quepa en la página
        max_cols = 10
        if len(df.columns) > max_cols:
            print(f'  ⚠️  Limitando a {max_cols} columnas para el PDF (de {len(df.columns)} totales)')
            df_display = df_display.iloc[:, :max_cols]
            col_note_text = f"<b>Nota:</b> Mostrando las primeras {max_cols} columnas de {len(df.columns)} campos totales."
            col_note = Paragraph(col_note_text, info_style)
            elements.append(col_note)
            elements.append(Spacer(1, 0.1*inch))
        
        # Preparar datos para la tabla
        # Convertir valores a strings y truncar si son muy largos
        table_data = []
        
        # Headers
        headers = [str(col)[:30] for col in df_display.columns]  # Limitar largo de headers
        table_data.append(headers)
        
        # Filas
        for _, row in df_display.iterrows():
            row_data = []
            for val in row:
                if pd.isna(val):
                    row_data.append('')
                else:
                    # Convertir a string y truncar
                    str_val = str(val)[:50]  # Limitar a 50 caracteres
                    row_data.append(str_val)
            table_data.append(row_data)
        
        # Calcular ancho de columnas dinámicamente
        page_width = landscape(letter)[0] - 60  # Ancho de página menos márgenes
        col_width = page_width / len(df_display.columns)
        
        # Crear tabla
        table = Table(table_data, repeatRows=1, colWidths=[col_width] * len(df_display.columns))
        
        # Estilo de la tabla
        table.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            
            # Cuerpo
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('TOPPADDING', (0, 1), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
            
            # Bordes
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            
            # Alternar colores en filas
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
        ]))
        
        elements.append(table)
        
        # Construir PDF
        doc.build(elements)
        
        print(f'  ✅ PDF creado: {pdf_path}')
        return True
        
    except Exception as e:
        print(f'  ❌ Error convirtiendo {collection_name}: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    print('='*80)
    print('CONVERSIÓN DE COLECCIONES FIREBASE A PDF')
    print('='*80)
    print(f'Fecha: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Verificar carpeta origen
    source_dir = 'colecciones_firebase'
    if not os.path.exists(source_dir):
        print(f'\n❌ Error: No existe la carpeta {source_dir}')
        return
    
    # Crear carpeta para PDFs
    pdf_dir = os.path.join(source_dir, 'pdf')
    os.makedirs(pdf_dir, exist_ok=True)
    print(f'\n📁 Carpeta PDF creada: {pdf_dir}')
    
    # Buscar archivos Excel
    excel_files = [f for f in os.listdir(source_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        print(f'\n⚠️  No se encontraron archivos .xlsx en {source_dir}')
        return
    
    print(f'\n✓ Se encontraron {len(excel_files)} archivos Excel')
    
    # Convertir cada archivo
    print('\n' + '='*80)
    print('INICIANDO CONVERSIÓN')
    print('='*80)
    
    successful = 0
    failed = 0
    
    for i, excel_file in enumerate(excel_files, 1):
        print(f'\n[{i}/{len(excel_files)}] ', end='')
        
        excel_path = os.path.join(source_dir, excel_file)
        collection_name = os.path.splitext(excel_file)[0]
        pdf_path = os.path.join(pdf_dir, f'{collection_name}.pdf')
        
        if create_pdf_from_excel(excel_path, pdf_path, collection_name):
            successful += 1
        else:
            failed += 1
    
    # Resumen final
    print('\n' + '='*80)
    print('CONVERSIÓN COMPLETADA')
    print('='*80)
    
    print(f'\n✅ Archivos convertidos exitosamente: {successful}')
    if failed > 0:
        print(f'❌ Archivos con errores: {failed}')
    
    print(f'\n📂 Ubicación PDFs: {pdf_dir}/')
    print(f'📂 Archivos Excel originales: {source_dir}/ (conservados)')
    
    # Listar PDFs creados
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    if pdf_files:
        print(f'\n📄 Archivos PDF generados ({len(pdf_files)}):')
        for f in sorted(pdf_files):
            print(f'  - {f}')

if __name__ == '__main__':
    main()
