# -*- coding: utf-8 -*-
"""
Script para calcular y mostrar métricas finales desde Firebase
"""

import sys
sys.path.append('pipelines')
sys.path.append('database')

# Importar la función de métricas del pipeline
import importlib.util
spec = importlib.util.spec_from_file_location("pipeline", "pipelines/unidades_proyecto_pipeline.py")
pipeline_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline_module)

# Ejecutar la función de métricas
pipeline_module.calculate_final_metrics('unidades_proyecto')
