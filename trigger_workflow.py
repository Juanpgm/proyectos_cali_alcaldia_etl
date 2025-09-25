#!/usr/bin/env python3
"""
Script para ejecutar el workflow de refresh manual de Unidades de Proyecto desde código
"""
import requests
import json
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional

class GitHubWorkflowTrigger:
    """Clase para disparar workflows de GitHub Actions desde Python."""
    
    def __init__(self, owner: str, repo: str, token: str):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        }
    
    def trigger_workflow(self, workflow_file: str, inputs: Dict[str, Any] = None, ref: str = "main") -> Dict[str, Any]:
        """
        Dispara un workflow de GitHub Actions.
        
        Args:
            workflow_file: Nombre del archivo de workflow (ej: 'unidades_proyecto_manual_refresh.yml')
            inputs: Diccionario con los inputs del workflow
            ref: Branch desde el cual ejecutar (default: 'main')
        """
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_file}/dispatches"
        
        payload = {
            "ref": ref,
            "inputs": inputs or {}
        }
        
        print(f"🚀 Disparando workflow: {workflow_file}")
        print(f"📊 Inputs: {json.dumps(inputs, indent=2)}")
        print(f"🌿 Branch: {ref}")
        
        response = requests.post(url, headers=self.headers, json=payload)
        
        if response.status_code == 204:
            print("✅ Workflow disparado exitosamente")
            return {"status": "success", "message": "Workflow triggered successfully"}
        else:
            error_msg = f"Error {response.status_code}: {response.text}"
            print(f"❌ Error disparando workflow: {error_msg}")
            return {"status": "error", "message": error_msg}
    
    def get_workflow_runs(self, workflow_file: str, limit: int = 10) -> Dict[str, Any]:
        """Obtiene las ejecuciones recientes de un workflow."""
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_file}/runs"
        
        params = {"per_page": limit}
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Error obteniendo runs: {response.status_code} - {response.text}")
            return {"workflow_runs": []}
    
    def wait_for_completion(self, workflow_file: str, timeout_minutes: int = 10) -> Dict[str, Any]:
        """
        Espera a que complete la ejecución más reciente del workflow.
        
        Args:
            workflow_file: Nombre del archivo de workflow
            timeout_minutes: Tiempo máximo de espera en minutos
        """
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        print(f"⏳ Esperando completación del workflow (timeout: {timeout_minutes}m)...")
        
        while time.time() - start_time < timeout_seconds:
            runs = self.get_workflow_runs(workflow_file, limit=1)
            
            if runs.get("workflow_runs"):
                latest_run = runs["workflow_runs"][0]
                status = latest_run["status"]
                conclusion = latest_run.get("conclusion")
                
                print(f"📊 Estado: {status} | Conclusión: {conclusion}")
                
                if status == "completed":
                    result = {
                        "status": "completed",
                        "conclusion": conclusion,
                        "run_id": latest_run["id"],
                        "html_url": latest_run["html_url"],
                        "created_at": latest_run["created_at"],
                        "updated_at": latest_run["updated_at"]
                    }
                    
                    if conclusion == "success":
                        print("✅ Workflow completado exitosamente")
                    else:
                        print(f"❌ Workflow falló: {conclusion}")
                    
                    return result
            
            time.sleep(30)  # Esperar 30 segundos antes de verificar de nuevo
        
        print(f"⏰ Timeout alcanzado ({timeout_minutes}m)")
        return {"status": "timeout", "message": f"Workflow no completó en {timeout_minutes} minutos"}

def test_workflow_trigger():
    """Función de prueba para demostrar el uso del trigger."""
    print("🧪 MODO PRUEBA - Disparando workflow de Unidades de Proyecto")
    print("=" * 80)
    
    # Configuración (en producción, usa variables de entorno)
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    OWNER = "Juanpgm"
    REPO = "proyectos_cali_alcaldia_etl"
    WORKFLOW_FILE = "unidades_proyecto_manual_refresh.yml"
    
    if not GITHUB_TOKEN:
        print("❌ Error: Se requiere GITHUB_TOKEN como variable de entorno")
        print("💡 Obtén un token en: https://github.com/settings/tokens")
        print("💡 Permisos necesarios: repo, workflow")
        return False
    
    # Crear instancia del trigger
    trigger = GitHubWorkflowTrigger(OWNER, REPO, GITHUB_TOKEN)
    
    # Configurar inputs para modo prueba
    test_inputs = {
        "test_mode": "true",
        "debug_mode": "true",
        "force_full_sync": "false"
    }
    
    # Disparar workflow
    result = trigger.trigger_workflow(WORKFLOW_FILE, test_inputs, ref="fresh-start")
    
    if result["status"] == "success":
        print("\n⏳ Esperando completación...")
        completion_result = trigger.wait_for_completion(WORKFLOW_FILE, timeout_minutes=5)
        
        if completion_result["status"] == "completed":
            print(f"\n🔗 Ver resultados: {completion_result['html_url']}")
            return completion_result["conclusion"] == "success"
        else:
            print(f"\n⚠️ No se pudo confirmar completación: {completion_result}")
            return False
    else:
        return False

def production_workflow_trigger():
    """Función para ejecutar workflow en modo producción."""
    print("🚀 MODO PRODUCCIÓN - Disparando workflow de Unidades de Proyecto")
    print("=" * 80)
    
    # Configuración
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    OWNER = "Juanpgm"
    REPO = "proyectos_cali_alcaldia_etl"
    WORKFLOW_FILE = "unidades_proyecto_manual_refresh.yml"
    
    if not GITHUB_TOKEN:
        print("❌ Error: Se requiere GITHUB_TOKEN como variable de entorno")
        return False
    
    # Crear instancia del trigger
    trigger = GitHubWorkflowTrigger(OWNER, REPO, GITHUB_TOKEN)
    
    # Configurar inputs para producción
    production_inputs = {
        "test_mode": "false",
        "debug_mode": "false",
        "force_full_sync": "false"
    }
    
    # Confirmar ejecución
    print("⚠️ ATENCIÓN: Esto ejecutará el pipeline en modo PRODUCCIÓN")
    confirmation = input("¿Continuar? (y/N): ").lower().strip()
    
    if confirmation != 'y':
        print("❌ Operación cancelada")
        return False
    
    # Disparar workflow
    result = trigger.trigger_workflow(WORKFLOW_FILE, production_inputs, ref="fresh-start")
    
    if result["status"] == "success":
        print("\n⏳ Esperando completación...")
        completion_result = trigger.wait_for_completion(WORKFLOW_FILE, timeout_minutes=15)
        
        if completion_result["status"] == "completed":
            print(f"\n🔗 Ver resultados: {completion_result['html_url']}")
            return completion_result["conclusion"] == "success"
        else:
            print(f"\n⚠️ No se pudo confirmar completación: {completion_result}")
            return False
    else:
        return False

def main():
    """Función principal con menú interactivo."""
    print("🔄 GitHub Workflow Trigger - Unidades de Proyecto")
    print("=" * 80)
    print("1. 🧪 Ejecutar en MODO PRUEBA (solo test conexiones)")
    print("2. 🚀 Ejecutar en MODO PRODUCCIÓN (pipeline completo)")
    print("3. 📊 Ver ejecuciones recientes")
    print("4. ❌ Salir")
    print()
    
    choice = input("Selecciona una opción (1-4): ").strip()
    
    if choice == "1":
        success = test_workflow_trigger()
        if success:
            print("\n🎉 Test completado exitosamente")
        else:
            print("\n❌ Test falló")
    
    elif choice == "2":
        success = production_workflow_trigger()
        if success:
            print("\n🎉 Pipeline ejecutado exitosamente")
        else:
            print("\n❌ Pipeline falló")
    
    elif choice == "3":
        GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
        if not GITHUB_TOKEN:
            print("❌ Error: Se requiere GITHUB_TOKEN")
            return
        
        trigger = GitHubWorkflowTrigger("Juanpgm", "proyectos_cali_alcaldia_etl", GITHUB_TOKEN)
        runs = trigger.get_workflow_runs("unidades_proyecto_manual_refresh.yml", limit=5)
        
        print("📊 Ejecuciones recientes:")
        for run in runs.get("workflow_runs", []):
            print(f"  • {run['created_at'][:19]} | {run['status']} | {run.get('conclusion', 'N/A')} | {run['html_url']}")
    
    elif choice == "4":
        print("👋 ¡Hasta luego!")
    
    else:
        print("❌ Opción inválida")

if __name__ == "__main__":
    main()