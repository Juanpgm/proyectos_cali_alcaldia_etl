"""
Bot para extraer y descargar archivos de ejecución presupuestal
de la página web de la Alcaldía de Cali usando programación funcional.
"""

import os
import requests
import time
import re
from pathlib import Path
from functools import partial
from itertools import chain
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction_app/extraction_logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constantes
BASE_URL = "https://www.cali.gov.co/planeacion/publicaciones/115111/programas-proyectos-ejecucion-dapm/"
DOWNLOAD_DIR = Path("transformation_app/app_inputs/ejecucion_presupuestal_input")
VIGENCIAS = ["2024", "2025"]
TIMEOUT = 20


def setup_chrome_driver():
    """Configurar driver de Chrome con configuración más estable."""
    options = Options()
    
    # Opciones más conservadoras para evitar crashes
    stable_options = [
        "--headless=new",  # Usar nueva versión de headless
        "--no-sandbox", 
        "--disable-dev-shm-usage", 
        "--disable-gpu",
        "--disable-extensions", 
        "--disable-plugins", 
        "--disable-images",
        "--disable-javascript",  # Deshabilitar JS para evitar crashes
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor",
        "--window-size=1920,1080",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    list(map(options.add_argument, stable_options))
    
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.notifications": 2
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    try:
        service = Service(ChromeDriverManager().install())
    except Exception:
        service = Service()
    
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)  # Reducir tiempo de espera
    return driver


def wait_and_find(driver, xpath, timeout=TIMEOUT):
    """Esperar y encontrar elemento por XPath."""
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
    except TimeoutException:
        return None


def safe_click(driver, element):
    """Hacer clic seguro en elemento."""
    try:
        driver.execute_script("arguments[0].click();", element)
        time.sleep(2)
        return True
    except Exception as e:
        logger.warning(f"Error haciendo clic: {e}")
        return False


def extract_download_links(driver):
    """Extraer todos los enlaces .xlsx de la estructura HTML."""
    search_patterns = [
        "//a[@title='Descargar']",
        "//a[contains(@href, 'descargar')]",
        "//a[contains(@href, '.xlsx')]",
        "//a[contains(@href, '.xls')]",
        "//a[contains(text(), 'Descargar')]",
        "//a[contains(text(), '.xlsx')]",
        # Buscar en toda la estructura HTML
        "//*[contains(@href, '.xlsx')]",
        "//*[contains(@href, '.xls')]",
        "//*[contains(@href, 'descargar')]"
    ]
    
    # Función para buscar elementos por patrón
    find_by_pattern = partial(driver.find_elements, By.XPATH)
    
    # Buscar elementos usando todos los patrones
    all_elements = list(chain(*map(find_by_pattern, search_patterns)))
    
    # Filtrar elementos únicos por href y que sean archivos válidos
    unique_elements = {}
    for element in all_elements:
        try:
            href = element.get_attribute('href')
            if href and (
                'descargar' in href.lower() or 
                '.xlsx' in href.lower() or 
                '.xls' in href.lower()
            ):
                unique_elements[href] = element
        except:
            continue
    
    logger.info(f"Enlaces únicos de descarga encontrados: {len(unique_elements)}")
    return list(unique_elements.values())


def extract_expedition_date(driver, element):
    """Extraer la fecha de expedición del elemento o su contexto con algoritmo afinado."""
    try:
        # Estrategia 1: Buscar directamente en la página elementos que contengan "Expedición"
        expedition_xpath_patterns = [
            # Buscar elementos que contengan específicamente "Expedición" 
            "//*[contains(text(), 'Expedición') or contains(text(), 'expedición') or contains(text(), 'EXPEDICIÓN')]",
            # Buscar en elementos que tengan la palabra seguida de dos puntos
            "//*[contains(text(), 'Expedición:') or contains(text(), 'expedición:')]",
            # Buscar en spans, divs, p que contengan expedición
            "//span[contains(text(), 'Expedición') or contains(text(), 'expedición')]",
            "//div[contains(text(), 'Expedición') or contains(text(), 'expedición')]", 
            "//p[contains(text(), 'Expedición') or contains(text(), 'expedición')]",
            "//td[contains(text(), 'Expedición') or contains(text(), 'expedición')]",
            "//th[contains(text(), 'Expedición') or contains(text(), 'expedición')]"
        ]
        
        # Buscar elementos que contengan la palabra "Expedición"
        for xpath_pattern in expedition_xpath_patterns:
            try:
                expedition_elements = driver.find_elements(By.XPATH, xpath_pattern)
                for exp_element in expedition_elements:
                    element_text = exp_element.text.strip()
                    logger.info(f"Encontrado elemento con Expedición: '{element_text}'")
                    
                    # Patrones más específicos para extraer fecha después de "Expedición"
                    expedition_patterns = [
                        # Expedición: DD/MM/YYYY o DD-MM-YYYY
                        r'expedición[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                        r'expedicion[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                        # Solo fecha después de la palabra
                        r'expedición.*?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                        r'expedicion.*?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                    ]
                    
                    for pattern in expedition_patterns:
                        match = re.search(pattern, element_text.lower())
                        if match:
                            date_str = match.group(1)
                            date_normalized = date_str.replace('/', '-')
                            logger.info(f"✓ Fecha de expedición extraída: {date_normalized}")
                            return date_normalized
                    
                    # Si no hay fecha en el mismo elemento, buscar en elementos siguientes
                    try:
                        next_sibling = exp_element.find_element(By.XPATH, "./following-sibling::*[1]")
                        sibling_text = next_sibling.text.strip()
                        date_match = re.search(r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})', sibling_text)
                        if date_match:
                            date_str = date_match.group(1)
                            date_normalized = date_str.replace('/', '-')
                            logger.info(f"✓ Fecha de expedición en elemento siguiente: {date_normalized}")
                            return date_normalized
                    except:
                        pass
                        
            except Exception as e:
                logger.debug(f"Error con patrón XPath {xpath_pattern}: {e}")
                continue
        
        # Estrategia 2: Buscar en el contexto del elemento de descarga actual
        try:
            # Buscar en el elemento padre más amplio
            parent_containers = [
                "./ancestor::div[1]",
                "./ancestor::tr[1]", 
                "./ancestor::li[1]",
                "./ancestor::article[1]",
                "./ancestor::section[1]"
            ]
            
            for container_xpath in parent_containers:
                try:
                    parent_element = element.find_element(By.XPATH, container_xpath)
                    parent_text = parent_element.text.lower()
                    
                    # Buscar "expedición" en el texto del contenedor
                    if 'expedición' in parent_text or 'expedicion' in parent_text:
                        logger.info(f"Contenedor con expedición encontrado: {parent_text[:100]}...")
                        
                        expedition_patterns = [
                            r'expedición[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                            r'expedicion[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
                            r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4}).*expedición',
                            r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4}).*expedicion'
                        ]
                        
                        for pattern in expedition_patterns:
                            match = re.search(pattern, parent_text)
                            if match:
                                date_str = match.group(1)
                                date_normalized = date_str.replace('/', '-')
                                logger.info(f"✓ Fecha de expedición en contenedor: {date_normalized}")
                                return date_normalized
                                
                except Exception as e:
                    logger.debug(f"Error buscando en contenedor {container_xpath}: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"Error en estrategia 2: {e}")
        
        # Estrategia 3: Buscar cualquier fecha cerca del elemento de descarga
        try:
            # Buscar fechas en un radio más amplio del elemento
            nearby_xpath = "./ancestor::*[position()<=3]//*[contains(text(), '/') or contains(text(), '-')]"
            nearby_elements = element.find_elements(By.XPATH, nearby_xpath)
            
            for nearby_element in nearby_elements[:5]:  # Limitar a 5 elementos
                text = nearby_element.text.strip()
                date_match = re.search(r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})', text)
                if date_match:
                    date_str = date_match.group(1)
                    date_normalized = date_str.replace('/', '-')
                    logger.info(f"✓ Fecha encontrada en elementos cercanos: {date_normalized}")
                    return date_normalized
                    
        except Exception as e:
            logger.debug(f"Error en estrategia 3: {e}")
        
        # Estrategia 4: Como último recurso, usar la fecha actual
        from datetime import datetime
        current_date = datetime.now().strftime("%d-%m-%Y")
        logger.warning(f"No se encontró fecha de expedición, usando fecha actual: {current_date}")
        return current_date
        
    except Exception as e:
        logger.error(f"Error crítico extrayendo fecha de expedición: {e}")
        from datetime import datetime
        current_date = datetime.now().strftime("%d-%m-%Y")
        return current_date


def determine_vigencia_and_month(element, fallback_vigencia="unknown"):
    """Determinar vigencia y mes de un elemento."""
    try:
        # Mapeo de meses en español
        meses = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        
        # Buscar en texto del elemento y elementos padre
        text_sources = [
            element.get_attribute('href') or "",
            element.get_attribute('title') or "",
            element.text or ""
        ]
        
        # Buscar en elementos padre
        try:
            parent = element.find_element(By.XPATH, "./ancestor::*[contains(., '2024') or contains(., '2025')][1]")
            text_sources.append(parent.text or "")
        except:
            pass
        
        combined_text = " ".join(text_sources).lower()
        
        # Determinar vigencia
        vigencia = fallback_vigencia
        for year in ["2024", "2025", "2026", "2027"]:
            if year in combined_text:
                vigencia = year
                break
        
        # Determinar mes
        mes = "00"  # Default si no se encuentra mes
        for mes_nombre, mes_numero in meses.items():
            if mes_nombre in combined_text:
                mes = mes_numero
                break
        
        # Si no se encuentra mes por nombre, buscar números
        if mes == "00":
            import re
            # Buscar patrones como "mes 01", "mes1", "01", etc.
            month_patterns = re.findall(r'(?:mes\s*)?(\d{1,2})', combined_text)
            for month_num in month_patterns:
                if 1 <= int(month_num) <= 12:
                    mes = f"{int(month_num):02d}"
                    break
        
        return vigencia, mes
        
    except Exception:
        return fallback_vigencia, "00"


def create_filename(element, vigencia, mes, expedition_date, index):
    """Crear nombre de archivo usando directamente la fecha de expedición como nombre base."""
    try:
        # El nombre del archivo será directamente la fecha de expedición
        # Si expedition_date es una fecha válida, la usamos como nombre principal
        if expedition_date and expedition_date != "sin-fecha" and len(expedition_date.split('-')) == 3:
            # Usar directamente la fecha de expedición como nombre del archivo
            base_name = expedition_date
            logger.info(f"Usando fecha de expedición como nombre base: {base_name}")
        else:
            # Fallback si no hay fecha válida
            title = element.get_attribute('title') or element.text.strip() or "archivo"
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
            
            if safe_title and safe_title.lower() != "descargar":
                base_name = safe_title[:20]  # Limitar longitud
            else:
                base_name = f"ejecucion_presupuestal_{index:03d}"
            
            logger.warning(f"Fecha de expedición no válida '{expedition_date}', usando título: {base_name}")
        
        # Formato: directamente el nombre de expedición + vigencia y mes como sufijo
        filename = f"{base_name}_vigencia_{vigencia}_mes_{mes}.xlsx"
        
        # Limpiar caracteres especiales pero mantener guiones de fecha
        filename = "".join(c for c in filename if c.isalnum() or c in ('_', '-', '.')).replace(' ', '_')
        
        # Verificar que termine con .xlsx
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        logger.info(f"Nombre de archivo generado: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error creando nombre de archivo: {e}")
        fallback_name = f"{expedition_date}_vigencia_{vigencia}_mes_{mes}_archivo_{index:03d}.xlsx"
        return fallback_name


def download_file(url, filename):
    """Descargar archivo desde URL con barra de progreso."""
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = DOWNLOAD_DIR / filename
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Hacer request con stream=True para mostrar progreso
        response = requests.get(url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        
        # Obtener tamaño del archivo
        total_size = int(response.headers.get('content-length', 0))
        
        # Crear barra de progreso
        with open(file_path, 'wb') as f:
            if total_size > 0:
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Descargando {filename[:30]}..."
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                # Sin tamaño conocido, mostrar progreso simple
                print(f"Descargando {filename}...", end="", flush=True)
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        print(".", end="", flush=True)
                print(" ✓")
        
        file_size = file_path.stat().st_size
        logger.info(f"✓ Descargado: {filename} ({file_size:,} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error descargando {filename}: {e}")
        return False


def click_vigencia_tabs(driver):
    """Hacer clic solo en pestañas de Vigencia para años 2024 en adelante con método más seguro."""
    # Usar requests para obtener el HTML y analizarlo
    try:
        # Obtener la fuente de la página actual
        page_source = driver.page_source
        
        # Buscar específicamente elementos que contengan "Vigencia" y años 2024+
        vigencia_patterns = [
            "//*[contains(text(), 'Vigencia 2024')]",
            "//*[contains(text(), 'Vigencia 2025')]", 
            "//*[contains(text(), 'Vigencia 2026')]",
            "//*[contains(text(), 'Vigencia 2027')]"
        ]
        
        clicked_tabs = []
        
        for pattern in vigencia_patterns:
            try:
                elements = driver.find_elements(By.XPATH, pattern)
                for element in elements:
                    try:
                        text = element.text.strip()
                        if text and text not in clicked_tabs:
                            logger.info(f"Intentando hacer clic en: {text}")
                            
                            # Método más suave para hacer clic
                            try:
                                # Scroll al elemento primero
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(1)
                                
                                # Verificar si el elemento es clickeable
                                if element.is_enabled() and element.is_displayed():
                                    element.click()
                                    clicked_tabs.append(text)
                                    time.sleep(2)  # Esperar que cargue el contenido
                                    logger.info(f"✓ Clic exitoso en: {text}")
                                else:
                                    logger.warning(f"Elemento no clickeable: {text}")
                                    
                            except Exception as click_error:
                                logger.warning(f"Error haciendo clic en {text}: {click_error}")
                                # Intentar método alternativo
                                try:
                                    driver.execute_script("arguments[0].click();", element)
                                    clicked_tabs.append(text)
                                    time.sleep(2)
                                    logger.info(f"✓ Clic exitoso (método JS) en: {text}")
                                except:
                                    logger.warning(f"Falló ambos métodos de clic en: {text}")
                            
                    except Exception as e:
                        logger.warning(f"Error procesando elemento de vigencia: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error buscando vigencias con patrón {pattern}: {e}")
                continue
        
        logger.info(f"Total de pestañas de vigencia activadas: {len(clicked_tabs)}")
        return clicked_tabs
        
    except Exception as e:
        logger.error(f"Error general en click_vigencia_tabs: {e}")
        return []


def scroll_page(driver):
    """Hacer scroll en la página para cargar contenido dinámico."""
    scroll_scripts = [
        "window.scrollTo(0, document.body.scrollHeight);",
        "window.scrollTo(0, 0);",
        "window.scrollTo(0, document.body.scrollHeight/2);"
    ]
    
    for script in scroll_scripts:
        driver.execute_script(script)
        time.sleep(1)


def process_downloads(driver):
    """Procesar todas las descargas encontradas analizando directamente el HTML de la página."""
    # Hacer scroll para cargar contenido
    print("📄 Cargando contenido de la página...")
    scroll_page(driver)
    
    # Intentar hacer clic en pestañas de vigencia de manera más segura
    print("🔍 Buscando pestañas de vigencia...")
    try:
        clicked_tabs = click_vigencia_tabs(driver)
        logger.info(f"Pestañas de vigencia activadas: {clicked_tabs}")
        
        # Hacer scroll adicional después de activar pestañas
        scroll_page(driver)
        time.sleep(3)  # Esperar que cargue todo el contenido
        
    except Exception as e:
        logger.warning(f"Error con pestañas de vigencia, continuando con análisis directo: {e}")
    
    # Si las pestañas fallan, analizar directamente el HTML completo
    print("🔗 Analizando HTML completo para extraer enlaces...")
    
    # Obtener todo el HTML de la página
    page_source = driver.page_source
    
    # Extraer todos los enlaces .xlsx de la estructura HTML
    download_elements = extract_download_links(driver)
    logger.info(f"Total de enlaces .xlsx encontrados en la página: {len(download_elements)}")
    
    # Si no encuentra elementos con selenium, usar requests para analizar URLs directamente
    if not download_elements:
        print("🔍 Búsqueda alternativa de enlaces en el HTML...")
        try:
            import re
            from urllib.parse import urljoin, urlparse
            
            # Buscar URLs de descarga en el HTML
            xlsx_urls = re.findall(r'href=["\']([^"\']*\.xlsx?[^"\']*)["\']', page_source, re.IGNORECASE)
            download_urls = re.findall(r'href=["\']([^"\']*descargar[^"\']*)["\']', page_source, re.IGNORECASE)
            
            all_urls = list(set(xlsx_urls + download_urls))
            
            # Convertir URLs relativas en absolutas
            base_url = driver.current_url
            absolute_urls = []
            for url in all_urls:
                if url.startswith('http'):
                    absolute_urls.append(url)
                else:
                    absolute_urls.append(urljoin(base_url, url))
            
            logger.info(f"URLs encontradas en HTML: {len(absolute_urls)}")
            
            # Simular elementos para el procesamiento
            class MockElement:
                def __init__(self, url):
                    self.url = url
                def get_attribute(self, attr):
                    return self.url if attr == 'href' else None
                @property
                def text(self):
                    return "Archivo de descarga"
            
            download_elements = [MockElement(url) for url in absolute_urls[:10]]  # Limitar a 10
            
        except Exception as e:
            logger.error(f"Error en búsqueda alternativa: {e}")
    
    if not download_elements:
        print("⚠️  No se encontraron archivos para descargar")
        return 0, 0
    
    print(f"📊 Se encontraron {len(download_elements)} archivos para descargar")
    
    # Procesar cada enlace con barra de progreso general
    successful_downloads = 0
    downloaded_files = set()  # Para evitar duplicados
    
    with tqdm(download_elements, desc="🗂️  Procesando archivos", unit="archivo") as pbar:
        for index, element in enumerate(pbar, 1):
            try:
                url = element.get_attribute('href')
                if not url:
                    continue
                    
                logger.info(f"📥 Procesando {index}/{len(download_elements)}: URL: {url}")
                logger.info(f"   🗓️  Extrayendo fecha de expedición...")
                expedition_date = extract_expedition_date(driver, element)
                logger.info(f"   ✓ Fecha de expedición extraída: '{expedition_date}'")
                
                vigencia, mes = determine_vigencia_and_month(element, f"vigencia_{index}")
                filename = create_filename(element, vigencia, mes, expedition_date, index)
                
                # Actualizar descripción de la barra de progreso
                pbar.set_postfix({
                    'Archivo': filename[:30] + "..." if len(filename) > 30 else filename,
                    'Vigencia': vigencia,
                    'Expedición': expedition_date
                })
                
                logger.info(f"   📁 Nombre final del archivo: {filename}")
                logger.info(f"   📊 Vigencia: {vigencia}, Mes: {mes}")
                
                # Verificar si ya se descargó este archivo
                if filename in downloaded_files:
                    logger.info(f"⏭️  Archivo ya descargado: {filename} - Saltando...")
                    continue
                
                if download_file(url, filename):
                    successful_downloads += 1
                    downloaded_files.add(filename)
                    pbar.set_postfix({
                        'Descargados': f"{successful_downloads}/{len(download_elements)}",
                        'Último': filename[:20] + "..."
                    })
                
                time.sleep(1)  # Pausa entre descargas
                
            except Exception as e:
                logger.warning(f"❌ Error procesando enlace {index}: {e}")
                continue
    
    print(f"\n✅ Proceso completado!")
    print(f"📁 Archivos descargados exitosamente: {successful_downloads}/{len(download_elements)}")
    print(f"📂 Ubicación: {DOWNLOAD_DIR}")
    
    if downloaded_files:
        print("\n📋 Archivos descargados:")
        for i, filename in enumerate(sorted(downloaded_files), 1):
            print(f"   {i:2d}. {filename}")
    
    return successful_downloads, len(download_elements)


def main():
    """Función principal usando programación funcional."""
    logger.info("Iniciando extracción de archivos de ejecución presupuestal")
    
    driver = None
    try:
        # Configurar driver
        driver = setup_chrome_driver()
        logger.info("Driver configurado en modo headless")
        
        # Navegar a la página
        driver.get(BASE_URL)
        logger.info(f"Navegando a: {BASE_URL}")
        
        # Esperar a que cargue la página
        if not wait_and_find(driver, "//*[@id='infoPrincipal']"):
            raise Exception("No se pudo cargar la página principal")
        
        logger.info("Página cargada correctamente")
        
        # Procesar descargas
        successful, total = process_downloads(driver)
        
        logger.info(f"Proceso completado: {successful}/{total} archivos descargados")
        
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        
    finally:
        if driver:
            driver.quit()
            logger.info("Driver cerrado")


if __name__ == "__main__":
    main()
