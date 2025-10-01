#!/usr/bin/env python3
"""
Pipeline de Contratación de Empréstito - Versión Optimizada
==========================================================

Pipeline principal para el procesamiento de datos de contratación de empréstito
con soporte para ejecución paralela, timeouts adaptativos y feedback mejorado.

Autor: Sistema ETL Alcaldía de Cali
Fecha: 2025-09-30
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
            logging.FileHandler('pipeline_contratacion_emprestito.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


class ContratacionEmprestitoPipeline:
    """Pipeline principal para el procesamiento de datos de contratación de empréstito"""
    
    def __init__(self, fast_mode: bool=False, skip_on_timeout: bool=True, parallel_execution: bool=True, adaptive_timeouts: bool=True):
        """
        Inicializar el pipeline de contratación de empréstito
        
        Args:
            fast_mode: Usar timeouts más cortos para ejecución rápida
            skip_on_timeout: Continuar con el siguiente paso si hay timeout
            parallel_execution: Ejecutar pasos independientes en paralelo cuando sea posible
            adaptive_timeouts: Usar timeouts adaptativos basados en estimaciones realistas
        """
        self.fast_mode = fast_mode
        self.skip_on_timeout = skip_on_timeout
        self.parallel_execution = parallel_execution
        self.adaptive_timeouts = adaptive_timeouts
        self.logger = setup_logging()
        self.start_time = None
        self.step_results = {}
        self.progress_queue = queue.Queue()
        self.live_progress = None
        
        # Configurar timeouts adaptativos basados en análisis de rendimiento real
        self._configure_adaptive_timeouts()
        
        # Configuración de scripts
        self.scripts = {
            'extraction_contratos': 'extraction_app/data_extraction_contratos_emprestito.py',
            'extraction_procesos': 'extraction_app/data_extraction_procesos_emprestito.py',
            'transformation_contratos': 'transformation_app/data_transformation_contratos_secop.py',
            'transformation_procesos': 'transformation_app/data_transformation_procesos_secop.py'
        }
        
        # Validar que todos los scripts existen
        self._validate_scripts()

    def _configure_adaptive_timeouts(self):
        """Configurar timeouts adaptativos optimizados basados en análisis de rendimiento real"""
        if self.adaptive_timeouts:
            # Timeouts optimizados basados en análisis de ejecuciones reales
            base_timeouts = {
                # Extracción: Timeouts más agresivos pero realistas
                'extraction_contratos': {
                    'estimated_time': 30,  # Reducido de 45 a 30
                    'buffer_factor': 2.0,  # Reducido de 3.0 a 2.0
                    'min_timeout': 60,  # Reducido de 120 a 60
                    'max_timeout': 300  # Reducido de 600 a 300
                },
                'extraction_procesos': {
                    'estimated_time': 15,  # Reducido de 20 a 15
                    'buffer_factor': 2.5,  # Reducido de 4.0 a 2.5
                    'min_timeout': 30,  # Reducido de 60 a 30
                    'max_timeout': 120  # Reducido de 300 a 120
                },
                # Transformación: Timeouts más eficientes
                'transformation_contratos': {
                    'estimated_time': 20,  # Reducido de 30 a 20
                    'buffer_factor': 2.5,  # Reducido de 4.0 a 2.5
                    'min_timeout': 60,  # Reducido de 120 a 60
                    'max_timeout': 180  # Reducido de 480 a 180
                },
                'transformation_procesos': {
                    'estimated_time': 15,  # Reducido de 25 a 15
                    'buffer_factor': 2.5,  # Reducido de 4.0 a 2.5
                    'min_timeout': 45,  # Reducido de 100 a 45
                    'max_timeout': 120  # Reducido de 400 a 120
                }
            }
            
            # Calcular timeouts finales
            self.timeouts = {}
            for step, config in base_timeouts.items():
                calculated_timeout = int(config['estimated_time'] * config['buffer_factor'])
                
                # Aplicar límites mínimos y máximos
                final_timeout = max(config['min_timeout'],
                                  min(calculated_timeout, config['max_timeout']))
                
                # Ajustar según modo rápido
                if self.fast_mode:
                    # En modo rápido, usar el 80% del timeout calculado pero respetando mínimos
                    fast_timeout = max(config['min_timeout'] // 2, int(final_timeout * 0.8))
                    self.timeouts[step] = fast_timeout
                else:
                    self.timeouts[step] = final_timeout
        else:
            # Timeouts fijos optimizados
            if self.fast_mode:
                self.timeouts = {
                    'extraction_contratos': 60,  # Reducido de 90 a 60
                    'extraction_procesos': 30,  # Reducido de 60 a 30
                    'transformation_contratos': 60,  # Reducido de 120 a 60
                    'transformation_procesos': 45  # Reducido de 100 a 45
                }
            else:
                self.timeouts = {
                    'extraction_contratos': 180,  # Reducido de 300 a 180
                    'extraction_procesos': 90,  # Reducido de 180 a 90
                    'transformation_contratos': 120,  # Reducido de 300 a 120
                    'transformation_procesos': 90  # Reducido de 240 a 90
                }

    def _validate_scripts(self):
        """Validar que todos los scripts necesarios existen"""
        self.logger.info("🔍 Validando existencia de scripts...")
        
        for step_name, script_path in self.scripts.items():
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script no encontrado: {script_path}")
        
        self.logger.info("✅ Todos los scripts están disponibles")

    def _execute_script_with_progress(self, script_name: str, script_path: str, step_key: str, progress_bar: tqdm) -> Dict[str, Any]:
        """
        Ejecutar un script con monitoreo de progreso en tiempo real y timeout adaptativo
        
        Args:
            script_name: Nombre descriptivo del script
            script_path: Ruta al script
            step_key: Clave para obtener el timeout
            progress_bar: Barra de progreso compartida
        
        Returns:
            Diccionario con el resultado de la ejecución
        """
        timeout = self.timeouts.get(step_key, 300)
        start_time = time.time()
        
        try:
            # Actualizar progreso
            progress_bar.set_description(f"🚀 Iniciando {script_name}")
            progress_bar.update(10)
            
            # Mostrar información de timeout adaptativo
            timeout_info = f"(timeout adaptativo: {timeout}s = {timeout/60:.1f}min)"
            progress_bar.set_postfix_str(timeout_info)
            
            # Ejecutar el script
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=os.getcwd()
            )
            
            execution_time = time.time() - start_time
            progress_bar.update(80)
            
            if result.returncode == 0:
                progress_bar.set_description(f"✅ {script_name} completado")
                efficiency = (execution_time / timeout) * 100
                progress_bar.set_postfix_str(f"Eficiencia: {efficiency:.1f}%")
                progress_bar.update(10)
                return {
                    'success': True,
                    'script': script_name,
                    'execution_time': execution_time,
                    'timeout_used': timeout,
                    'efficiency_percent': efficiency,
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
                    'timeout_used': timeout,
                    'error': result.stderr,
                    'output': result.stdout,
                    'timeout_occurred': False
                }
                
        except subprocess.TimeoutExpired:
            execution_time = time.time() - start_time
            progress_bar.set_description(f"⏰ {script_name} timeout")
            progress_bar.set_postfix_str(f"Excedió {timeout}s")
            progress_bar.update(100)
            
            # Sugerir timeout mejorado para próxima ejecución
            suggested_timeout = int(execution_time * 1.5)
            
            return {
                'success': False,
                'script': script_name,
                'execution_time': execution_time,
                'timeout_used': timeout,
                'suggested_timeout': suggested_timeout,
                'error': f'Script excedió el timeout adaptativo de {timeout} segundos. Sugerencia: usar {suggested_timeout}s próxima vez.',
                'timeout_occurred': True
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
                'timeout_used': timeout,
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
        """Ejecutar la fase de extracción"""
        self.logger.info("🔄 INICIANDO FASE DE EXTRACCIÓN")
        
        extraction_tasks = [
            ("Extracción de Contratos", self.scripts['extraction_contratos'], 'extraction_contratos'),
            ("Extracción de Procesos", self.scripts['extraction_procesos'], 'extraction_procesos')
        ]
        
        return self._execute_parallel_phase("EXTRACCIÓN", extraction_tasks)

    def run_transformation_phase(self) -> bool:
        """Ejecutar la fase de transformación"""
        self.logger.info("🔄 INICIANDO FASE DE TRANSFORMACIÓN")
        
        transformation_tasks = [
            ("Transformación de Contratos", self.scripts['transformation_contratos'], 'transformation_contratos'),
            ("Transformación de Procesos", self.scripts['transformation_procesos'], 'transformation_procesos')
        ]
        
        return self._execute_parallel_phase("TRANSFORMACIÓN", transformation_tasks)

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

    def run(self, fast_mode: Optional[bool]=None):
        """
        Ejecutar el pipeline completo de contratación de empréstito
        
        Args:
            fast_mode: Sobrescribir configuración de modo rápido si se especifica
        """
        # Configurar modo si se especifica
        if fast_mode is not None:
            self.fast_mode = fast_mode
            # Reconfigurar timeouts con el nuevo modo
            self._configure_adaptive_timeouts()
        
        self.start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("🚀 INICIANDO PIPELINE DE CONTRATACIÓN DE EMPRÉSTITO")
        self.logger.info("=" * 80)
        
        # Mostrar configuración detallada
        mode_text = "RÁPIDO" if self.fast_mode else "NORMAL"
        timeout_type = "ADAPTATIVOS" if self.adaptive_timeouts else "FIJOS"
        
        self.logger.info(f"📊 CONFIGURACIÓN DEL PIPELINE:")
        self.logger.info(f"   • Modo de ejecución: {mode_text}")
        self.logger.info(f"   • Timeouts: {timeout_type}")
        self.logger.info(f"   • Ejecución paralela: {'HABILITADA' if self.parallel_execution else 'DESHABILITADA'}")
        self.logger.info(f"   • Continuar en timeout: {'SÍ' if self.skip_on_timeout else 'NO'}")
        
        # Mostrar timeouts configurados
        self.logger.info(f"⏱️  TIMEOUTS CONFIGURADOS ({timeout_type}):")
        for step, timeout in self.timeouts.items():
            minutes = timeout / 60
            self.logger.info(f"   • {step}: {timeout}s ({minutes:.1f} min)")
        
        self.logger.info("-" * 80)
        
        try:
            # Fase 1: Extracción (paralela)
            extraction_tasks = [
                ('Extracción de Contratos', 'extraction_app/data_extraction_contratos_emprestito.py', 'extraction_contratos'),
                ('Extracción de Procesos', 'extraction_app/data_extraction_procesos_emprestito.py', 'extraction_procesos')
            ]
            
            extraction_results = self._execute_parallel_phase("EXTRACCIÓN", extraction_tasks)
            
            # Verificar si alguna extracción falló
            extraction_success = extraction_results
            
            if not extraction_success and not self.skip_on_timeout:
                self.logger.error("❌ Todas las extracciones fallaron. Deteniendo pipeline.")
                return self._generate_final_report()
            
            # Fase 2: Transformación (paralela)
            transformation_tasks = [
                ('Transformación de Contratos', 'transformation_app/data_transformation_contratos_secop.py', 'transformation_contratos'),
                ('Transformación de Procesos', 'transformation_app/data_transformation_procesos_secop.py', 'transformation_procesos')
            ]
            
            transformation_results = self._execute_parallel_phase("TRANSFORMACIÓN", transformation_tasks)
            
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
        
        # Análisis de timeouts y eficiencia
        timeout_analysis = []
        efficiency_analysis = []
        
        for step_name, result in self.step_results.items():
            if result:
                execution_time = result.get('execution_time', 0)
                timeout_used = result.get('timeout_used', 0)
                
                if result.get('timeout_occurred'):
                    suggested = result.get('suggested_timeout', 'N/A')
                    timeout_analysis.append(f"   ⏰ {step_name}: Timeout ({timeout_used}s) - Sugerencia: {suggested}s")
                elif result.get('success'):
                    efficiency = result.get('efficiency_percent', 0)
                    efficiency_analysis.append(f"   ⚡ {step_name}: {efficiency:.1f}% eficiencia ({execution_time:.1f}s de {timeout_used}s)")
        
        if timeout_analysis:
            self.logger.info("🚨 ANÁLISIS DE TIMEOUTS:")
            for analysis in timeout_analysis:
                self.logger.info(analysis)
        
        if efficiency_analysis:
            self.logger.info("📈 ANÁLISIS DE EFICIENCIA:")
            for analysis in efficiency_analysis:
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
            'timeout_analysis': timeout_analysis,
            'efficiency_analysis': efficiency_analysis
        }

    def _generate_recommendations(self):
        """Generar recomendaciones basadas en el análisis de ejecución"""
        recommendations = []
        
        # Analizar timeouts
        timeout_steps = [name for name, result in self.step_results.items() 
                        if result and result.get('timeout_occurred')]
        
        if timeout_steps:
            recommendations.append("🔧 Considerar aumentar timeouts para: " + ", ".join(timeout_steps))
        
        # Analizar eficiencia
        inefficient_steps = []
        for name, result in self.step_results.items():
            if result and result.get('success'):
                efficiency = result.get('efficiency_percent', 100)
                if efficiency < 30:  # Menos del 30% de eficiencia
                    inefficient_steps.append(name)
        
        if inefficient_steps:
            recommendations.append("⚡ Timeouts muy altos para: " + ", ".join(inefficient_steps))
        
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
            logging.FileHandler('pipeline_contratacion_emprestito.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Crear y ejecutar el pipeline
        pipeline = ContratacionEmprestitoPipeline(
            fast_mode=True,  # Usar modo rápido por defecto
            skip_on_timeout=True,  # Continuar aunque haya timeouts
            parallel_execution=True  # Activar ejecución paralela
        )
        
        print("🔧 CONFIGURACIÓN OPTIMIZADA:")
        print("   - Modo rápido: Activado (timeouts optimizados 30-60s)")
        print("   - Ejecución paralela: Activada (extracción y transformación simultáneas)")
        print("   - Continuar en timeout: Activado (no se detiene por timeouts)")
        print("   - Timeouts reducidos: 50-70% más rápidos que versión anterior")
        print("   - Para cambiar: pipeline.run(fast_mode=False)")
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
