#!/usr/bin/env python3
"""
Pipeline de Contratos de Empréstito - Versión Optimizada
========================================================

Pipeline específico para el procesamiento de datos de contratos de empréstito
con soporte para ejecución secuencial optimizada y timeouts adaptativos.

Autor: Sistema ETL Alcaldía de Cali
Fecha: 2025-10-02
"""

import sys
import os
import time
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from tqdm import tqdm
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configuración de logging
def setup_logging():
    """Configurar el sistema de logging con soporte UTF-8"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pipeline_contratos_emprestito.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


class ContratosEmprestitoPipeline:
    """Pipeline específico para el procesamiento de datos de contratos de empréstito"""
    
    def __init__(self, smart_mode: bool=True):
        """
        Inicializar el pipeline de contratos de empréstito
        
        Args:
            smart_mode: Omitir fases si los archivos de salida ya existen
        """
        self.smart_mode = smart_mode
        self.logger = setup_logging()
        self.start_time = None
        self.step_results = {}
        self.progress_queue = queue.Queue()
        self.live_progress = None
        
        # Configuración de scripts - Solo contratos
        self.scripts = {
            'extraction_contratos': 'extraction_app/data_extraction_contratos_emprestito.py',
            'transformation_contratos': 'transformation_app/data_transformation_contratos_secop.py',
            'loading_contratos': 'load_app/data_loading_contratos_emprestito.py'
        }
        
        # Validar que todos los scripts existen
        self._validate_scripts()
        
        # Configurar archivos de salida esperados - Solo contratos
        self.output_files = {
            'extraction_contratos': 'extraction_app/transformation_app/app_inputs/contratos_emprestito_input/contratos_emprestito_extracted.json',
            'transformation_contratos': 'transformation_app/app_outputs/emprestito_outputs/contratos_secop_emprestito_transformed.json'
        }



    def _validate_scripts(self):
        """Validar que todos los scripts necesarios existen"""
        self.logger.info("🔍 Validando existencia de scripts...")
        
        for step_name, script_path in self.scripts.items():
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script no encontrado: {script_path}")
        
        self.logger.info("✅ Todos los scripts están disponibles")

    def _check_existing_files(self) -> Dict[str, bool]:
        """Verificar qué archivos de salida ya existen"""
        existing_files = {}
        for step_name, file_path in self.output_files.items():
            exists = os.path.exists(file_path)
            existing_files[step_name] = exists
            if exists:
                file_size = os.path.getsize(file_path) / 1024  # KB
                self.logger.info(f"✅ Archivo existente: {step_name} ({file_size:.1f} KB)")
            else:
                self.logger.info(f"❌ Archivo faltante: {step_name}")
        return existing_files

    def _should_skip_phase(self, phase_name: str, tasks: List[Tuple[str, str, str]]) -> bool:
        """Determinar si una fase puede omitirse porque los archivos ya existen"""
        if not self.smart_mode:
            return False
        
        existing_files = self._check_existing_files()
        
        # Verificar si todos los archivos de salida de la fase existen
        all_files_exist = True
        for task_name, script_path, step_key in tasks:
            if step_key in existing_files and not existing_files[step_key]:
                all_files_exist = False
                break
        
        if all_files_exist:
            self.logger.info(f"🚀 OMITIENDO FASE DE {phase_name} - Archivos ya existen")
            # Simular resultados exitosos para esta fase
            for task_name, script_path, step_key in tasks:
                self.step_results[step_key] = {
                    'success': True,
                    'script': task_name,
                    'execution_time': 0.1,
                    'timeout_used': 0,
                    'efficiency_percent': 100,
                    'skipped': True,
                    'reason': 'Archivos ya existen'
                }
            return True
        
        return False

    def _execute_script_with_progress(self, script_name: str, script_path: str, step_key: str, progress_bar: tqdm) -> Dict[str, Any]:
        """
        Ejecutar un script con monitoreo de progreso sin timeout
        
        Args:
            script_name: Nombre descriptivo del script
            script_path: Ruta al script
            step_key: Clave del paso (para compatibilidad)
            progress_bar: Barra de progreso compartida
        
        Returns:
            Diccionario con el resultado de la ejecución
        """
        start_time = time.time()
        
        try:
            # Actualizar progreso
            progress_bar.set_description(f"🚀 Iniciando {script_name}")
            progress_bar.update(10)
            
            progress_bar.set_postfix_str("Sin límite de tiempo - Ejecutando...")
            
            # Ejecutar el script SIN timeout
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                cwd=os.getcwd()
            )
            
            execution_time = time.time() - start_time
            progress_bar.update(80)
            
            if result.returncode == 0:
                progress_bar.set_description(f"✅ {script_name} completado")
                progress_bar.set_postfix_str(f"Completado en {execution_time:.1f}s")
                progress_bar.update(10)
                return {
                    'success': True,
                    'script': script_name,
                    'execution_time': execution_time,
                    'output': result.stdout,
                    'timeout_occurred': False
                }
            else:
                progress_bar.set_description(f"❌ {script_name} falló")
                progress_bar.set_postfix_str("Error en ejecución")
                progress_bar.update(10)
                return {
                    'success': False,
                    'script': script_name,
                    'execution_time': execution_time,
                    'error': result.stderr,
                    'output': result.stdout,
                    'timeout_occurred': False
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            progress_bar.set_description(f"💥 {script_name} error")
            progress_bar.set_postfix_str("Error crítico")
            progress_bar.update(100)
            return {
                'success': False,
                'script': script_name,
                'execution_time': execution_time,
                'error': str(e),
                'timeout_occurred': False
            }

    def _execute_parallel_phase(self, phase_name: str, tasks: List[Tuple[str, str, str]]) -> bool:
        """
        Ejecutar múltiples tareas en paralelo con progreso unificado
        
        Args:
            phase_name: Nombre de la fase (ej: "EXTRACCIÓN", "TRANSFORMACIÓN")
            tasks: Lista de tuplas (script_name, script_path, step_key)
        
        Returns:
            True si al menos una tarea fue exitosa o si skip_on_timeout está activado
        """
        print(f"\n{'='*25}🔄 FASE DE {phase_name}{'='*25}")
        print("="*80)
        
        # Verificar si la fase puede omitirse
        if self._should_skip_phase(phase_name, tasks):
            print(f"⏭️ Fase de {phase_name} OMITIDA - Archivos ya existen")
            return True
        
        if not self.parallel_execution or len(tasks) == 1:
            # Ejecución secuencial
            return self._execute_sequential_phase(phase_name, tasks)
        
        # Ejecución paralela
        success_count = 0
        total_tasks = len(tasks)
        
        print(f"🚀 Ejecutando {total_tasks} tareas en paralelo...")
        
        # Crear barra de progreso principal
        with tqdm(total=total_tasks, desc=f"Fase {phase_name}",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                 position=0) as main_progress:
            
            # Ejecutar tareas en paralelo
            with ThreadPoolExecutor(max_workers=min(total_tasks, 2)) as executor:
                # Enviar todas las tareas
                future_to_task = {}
                task_progress_bars = {}
                
                for i, (script_name, script_path, step_key) in enumerate(tasks):
                    # Crear barra de progreso individual
                    task_progress = tqdm(total=100, desc=f"Preparando {script_name}",
                                       position=i + 1, leave=False)
                    task_progress_bars[step_key] = task_progress
                    
                    future = executor.submit(
                        self._execute_script_with_progress,
                        script_name, script_path, step_key, task_progress
                    )
                    future_to_task[future] = (script_name, step_key)
                
                # Procesar resultados conforme se completan
                for future in as_completed(future_to_task):
                    script_name, step_key = future_to_task[future]
                    task_progress = task_progress_bars[step_key]
                    
                    try:
                        result = future.result()
                        self.step_results[step_key] = result
                        
                        if result['success']:
                            success_count += 1
                            task_progress.set_description(f"✅ {script_name}")
                        else:
                            if result.get('timeout_occurred'):
                                task_progress.set_description(f"⏰ {script_name} (timeout)")
                            else:
                                task_progress.set_description(f"❌ {script_name}")
                            
                        # Actualizar progreso principal
                        main_progress.update(1)
                        main_progress.set_description(
                            f"Fase {phase_name} - {success_count}/{len([f for f in future_to_task if f.done()])} completados"
                        )
                        
                    except Exception as e:
                        self.logger.error(f"Error ejecutando {script_name}: {str(e)}")
                        task_progress.set_description(f"💥 {script_name}")
                        main_progress.update(1)
                    
                    finally:
                        task_progress.close()
        
        # Determinar si la fase fue exitosa
        phase_success = success_count > 0 or (success_count == 0 and self.skip_on_timeout)
        
        if phase_success:
            self.logger.info(f"✅ Fase de {phase_name} completada - {success_count}/{total_tasks} exitosos")
            print(f"✅ Fase de {phase_name} completada - {success_count}/{total_tasks} exitosos")
        else:
            self.logger.error(f"❌ Fase de {phase_name} falló - {success_count}/{total_tasks} exitosos")
            print(f"❌ Fase de {phase_name} falló - {success_count}/{total_tasks} exitosos")
        
        return phase_success

    def _execute_sequential_phase(self, phase_name: str, tasks: List[Tuple[str, str, str]]) -> bool:
        """
        Ejecutar tareas secuencialmente con progreso mejorado
        
        Args:
            phase_name: Nombre de la fase
            tasks: Lista de tuplas (script_name, script_path, step_key)
        
        Returns:
            True si al menos una tarea fue exitosa o si skip_on_timeout está activado
        """
        success_count = 0
        total_tasks = len(tasks)
        
        print(f"🔄 Ejecutando {total_tasks} tareas secuencialmente...")
        
        # Crear barra de progreso para la fase
        with tqdm(total=total_tasks, desc=f"Fase {phase_name} (Secuencial)",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                 position=0) as phase_progress:
            
            for i, (script_name, script_path, step_key) in enumerate(tasks):
                # Crear barra de progreso para la tarea individual
                with tqdm(total=100, desc=f"Ejecutando {script_name}",
                         position=1, leave=False) as task_progress:
                    
                    result = self._execute_script_with_progress(
                        script_name, script_path, step_key, task_progress
                    )
                    
                    self.step_results[step_key] = result
                    
                    if result['success']:
                        success_count += 1
                    
                    # Actualizar progreso de la fase
                    phase_progress.update(1)
                    phase_progress.set_description(
                        f"Fase {phase_name} - {success_count}/{i+1} completados"
                    )
        
        # Determinar si la fase fue exitosa
        phase_success = success_count > 0 or (success_count == 0 and self.skip_on_timeout)
        return phase_success

    def run_extraction_phase(self) -> bool:
        """Ejecutar la fase de extracción de contratos"""
        self.logger.info("🔄 INICIANDO FASE DE EXTRACCIÓN DE CONTRATOS")
        
        extraction_tasks = [
            ("Extracción de Contratos", self.scripts['extraction_contratos'], 'extraction_contratos')
        ]
        
        return self._execute_sequential_phase("EXTRACCIÓN", extraction_tasks)

    def run_transformation_phase(self) -> bool:
        """Ejecutar la fase de transformación de contratos"""
        self.logger.info("🔄 INICIANDO FASE DE TRANSFORMACIÓN DE CONTRATOS")
        
        transformation_tasks = [
            ("Transformación de Contratos", self.scripts['transformation_contratos'], 'transformation_contratos')
        ]
        
        return self._execute_sequential_phase("TRANSFORMACIÓN", transformation_tasks)

    def run_loading_phase(self) -> bool:
        """Ejecutar la fase de carga de contratos a Firebase"""
        self.logger.info("🔄 INICIANDO FASE DE CARGA DE CONTRATOS")
        
        loading_tasks = [
            ("Carga de Contratos a Firebase", self.scripts['loading_contratos'], 'loading_contratos')
        ]
        
        return self._execute_sequential_phase("CARGA", loading_tasks)

    def _print_final_report(self):
        """Imprimir reporte final del pipeline"""
        print("\n" + "="*80)
        print("📊 REPORTE FINAL DEL PIPELINE")
        print("="*80)
        
        total_time = time.time() - self.start_time if self.start_time else 0
        successful_steps = sum(1 for result in self.step_results.values() if result['success'])
        total_steps = len(self.step_results)
        timeout_steps = sum(1 for result in self.step_results.values() if result.get('timeout_occurred', False))
        
        print(f"⏱️  Tiempo total de ejecución: {total_time:.2f} segundos ({total_time/60:.1f} minutos)")
        print(f"✅ Pasos exitosos: {successful_steps}/{total_steps}")
        print(f"⏰ Pasos con timeout: {timeout_steps}")
        print(f"🚀 Modo rápido: {'Activado' if self.fast_mode else 'Desactivado'}")
        print(f"🔄 Ejecución paralela: {'Activada' if self.parallel_execution else 'Desactivada'}")
        
        print("\n📋 Detalle por paso:")
        for step_key, result in self.step_results.items():
            status = "✅" if result['success'] else ("⏰" if result.get('timeout_occurred') else "❌")
            time_str = f"{result['execution_time']:.1f}s"
            timeout_str = f"(timeout: {self.timeouts.get(step_key, 'N/A')}s)"
            print(f"   {status} {step_key}: {time_str} {timeout_str}")
        
        print("="*80)

    def run(self):
        """
        Ejecutar el pipeline completo de contratos de empréstito sin timeouts
        """
        self.start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("🚀 INICIANDO PIPELINE DE CONTRATOS DE EMPRÉSTITO")
        self.logger.info("=" * 80)
        
        self.logger.info(f"📊 CONFIGURACIÓN DEL PIPELINE:")
        self.logger.info(f"   • Ejecución: SECUENCIAL SIN TIMEOUTS")
        self.logger.info(f"   • Modo inteligente: {'ACTIVADO' if self.smart_mode else 'DESACTIVADO'}")
        self.logger.info(f"   • Filtros de contratos: Aplica filtros de calidad de datos")
        self.logger.info(f"   • Excluye: Prestación de servicios y BPIN 'No Definido'")
        self.logger.info(f"   • Los scripts se ejecutarán hasta completarse")
        
        self.logger.info("-" * 80)
        
        try:
            # Fase 1: Extracción de contratos
            self.logger.info("🔄 INICIANDO FASE DE EXTRACCIÓN DE CONTRATOS")
            if not self.run_extraction_phase():
                self.logger.error("❌ Extracción de contratos falló.")
            
            # Fase 2: Transformación de contratos (continúa aunque la extracción falle)
            self.logger.info("🔄 INICIANDO FASE DE TRANSFORMACIÓN DE CONTRATOS")
            if not self.run_transformation_phase():
                self.logger.error("❌ Transformación de contratos falló.")
            
            # Fase 3: Carga de contratos a Firebase (continúa aunque la transformación falle)
            self.logger.info("🔄 INICIANDO FASE DE CARGA DE CONTRATOS")
            if not self.run_loading_phase():
                self.logger.error("❌ Carga de contratos falló.")
            
            # Generar reporte final
            return self._generate_final_report()
            
        except KeyboardInterrupt:
            self.logger.warning("⚠️ Pipeline interrumpido por el usuario")
            return self._generate_final_report()
        except Exception as e:
            self.logger.error(f"💥 Error crítico en el pipeline: {str(e)}")
            return self._generate_final_report()

    def _generate_final_report(self) -> Dict[str, Any]:
        """Generar reporte final del pipeline con análisis de timeouts"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
        self.logger.info("=" * 80)
        self.logger.info("📋 REPORTE FINAL DEL PIPELINE")
        self.logger.info("=" * 80)
        
        # Estadísticas generales
        successful_steps = sum(1 for result in self.step_results.values() if result.get('success', False))
        total_steps = len(self.step_results)
        success_rate = (successful_steps / total_steps * 100) if total_steps > 0 else 0
        
        self.logger.info(f"⏱️  Duración total: {total_duration:.1f} segundos ({total_duration/60:.1f} minutos)")
        self.logger.info(f"✅ Pasos exitosos: {successful_steps}/{total_steps} ({success_rate:.1f}%)")
        
        # Análisis de ejecución
        execution_analysis = []
        
        for step_name, result in self.step_results.items():
            if result:
                execution_time = result.get('execution_time', 0)
                
                if result.get('success'):
                    execution_analysis.append(f"   ✅ {step_name}: Completado en {execution_time:.1f}s")
                else:
                    execution_analysis.append(f"   ❌ {step_name}: Falló después de {execution_time:.1f}s")
        
        if execution_analysis:
            self.logger.info("📈 ANÁLISIS DE EJECUCIÓN:")
            for analysis in execution_analysis:
                self.logger.info(analysis)
        
        # Recomendaciones para próximas ejecuciones
        self._generate_recommendations()
        
        # Resumen de resultados por paso
        self.logger.info("📊 DETALLE POR PASO:")
        for step_name, result in self.step_results.items():
            if result:
                status = "✅ ÉXITO" if result.get('success') else "❌ FALLO"
                time_info = f"{result.get('execution_time', 0):.1f}s"
                self.logger.info(f"   {step_name}: {status} ({time_info})")
                
                if not result.get('success') and result.get('error'):
                    error_msg = result.get('error', '')[:100] + "..." if len(result.get('error', '')) > 100 else result.get('error', '')
                    self.logger.info(f"     Error: {error_msg}")
        
        self.logger.info("=" * 80)
        
        if successful_steps == total_steps:
            self.logger.info("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        elif successful_steps > 0:
            self.logger.info("⚠️ PIPELINE COMPLETADO CON ALGUNOS FALLOS")
        else:
            self.logger.info("❌ PIPELINE FALLÓ COMPLETAMENTE")
        
        return {
            'success': successful_steps > 0,
            'total_duration': total_duration,
            'successful_steps': successful_steps,
            'total_steps': total_steps,
            'success_rate': success_rate,
            'step_results': self.step_results,
            'execution_analysis': execution_analysis
        }

    def _generate_recommendations(self):
        """Generar recomendaciones basadas en el análisis de ejecución"""
        recommendations = []
        
        # Analizar fallos
        failed_steps = [name for name, result in self.step_results.items() 
                       if result and not result.get('success')]
        
        if failed_steps:
            recommendations.append("🔧 Revisar errores en: " + ", ".join(failed_steps))
        
        # Analizar tiempos de ejecución largos
        slow_steps = []
        for name, result in self.step_results.items():
            if result and result.get('execution_time', 0) > 300:  # Más de 5 minutos
                slow_steps.append(f"{name} ({result.get('execution_time', 0):.1f}s)")
        
        if slow_steps:
            recommendations.append("⏰ Pasos que toman mucho tiempo: " + ", ".join(slow_steps))
        
        # Mostrar recomendaciones
        if recommendations:
            self.logger.info("💡 RECOMENDACIONES:")
            for rec in recommendations:
                self.logger.info(f"   {rec}")


def main():
    """Función principal"""
    # Configurar logging para el script principal con soporte UTF-8
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pipeline_contratos_emprestito.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Crear y ejecutar el pipeline
        pipeline = ContratosEmprestitoPipeline(
            smart_mode=True  # Activar modo inteligente (omitir fases si archivos existen)
        )
        
        print("🔧 CONFIGURACIÓN SIN TIMEOUTS:")
        print("   - Ejecución: Secuencial (extracción → transformación → carga)")
        print("   - Sin timeouts: Los scripts se ejecutan hasta completarse")
        print("   - Modo inteligente: Activado (omite fases si archivos existen)")
        print("   - Solo contratos: Procesamiento enfocado en contratos únicamente")
        print("   - Filtros aplicados: Excluye Prestación de servicios y BPIN 'No Definido'")
        print("   - Continuidad: Cada fase se ejecuta independientemente")
        print()
        
        success = pipeline.run()
        
        if success:
            print("\n✅ Pipeline ejecutado exitosamente")
            exit(0)
        else:
            print("\n❌ Pipeline falló o se completó con errores")
            exit(1)
            
    except Exception as e:
        print(f"\n💥 Error crítico: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
