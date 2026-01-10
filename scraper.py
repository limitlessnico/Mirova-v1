import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random
from urllib.parse import urlparse
import re
import pytz
import shutil

# --- LIBRERIAS DE VISION ---
try:
    import pytesseract
    from PIL import Image
    from io import BytesIO
except ImportError:
    pass # Se maneja en el log si falla

# --- CONFIGURACIÓN ---
VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
CARPETA_PRINCIPAL = "monitoreo_datos"
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp.csv")

def obtener_hora_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def limpiar_todo():
    """ MODO PRUEBAS: Limpieza inicial. """
    print("🧹 LIMPIEZA INICIAL ACTIVADA...")
    if os.path.exists("registro_vrp.csv"): 
        try: os.remove("registro_vrp.csv")
        except: pass
    if os.path.exists("imagenes"): 
        try: shutil.rmtree("imagenes")
        except: pass
    if os.path.exists(CARPETA_PRINCIPAL): 
        try: shutil.rmtree(CARPETA_PRINCIPAL)
        except: pass

def procesar_imagen_ocr(imagen_pil):
    """
    Truco de Magia: Convierte la imagen a Blanco y Negro absoluto
    para que Tesseract pueda leer el texto rojo.
    """
    # 1. Convertir a escala de grises
    gray = imagen_pil.convert('L')
    
    # 2. Aplicar umbral (Thresholding)
    # Todo lo que no sea casi blanco, lo volvemos negro puro.
    # El texto rojo suele ser oscuro en escala de grises.
    umbral = 200 
    blancoynegro = gray.point(lambda x: 0 if x < umbral else 255, '1')
    
    return blancoynegro

def leer_fecha_de_imagen(session, img_url):
    """ Descarga, procesa y lee la fecha de la imagen. """
    try:
        res = session.get(img_url, timeout=10)
        if res.status_code != 200: return None, None
        
        imagen_original = Image.open(BytesIO(res.content))
        
        # --- PRE-PROCESAMIENTO ---
        imagen_procesada = procesar_imagen_ocr(imagen_original)
        
        # --- LECTURA OCR ---
        # --psm 6 asume un bloque de texto uniforme
        texto_leido = pytesseract.image_to_string(imagen_procesada, config='--psm 6')
        
        # Limpieza básica del texto para el log
        texto_debug = texto_leido.replace('\n', ' ').strip()[:50]
        
        # Patrón flexible: 10 Jan 2026 ...
        patron = r"(?i)(\d{1,2}[-\s][a-z]{3}[-\s]\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        
        match = re.search(patron, texto_leido)
        if match:
            fecha_str = match.group(1).replace(" ", "-") # Normalizar
            try:
                fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y-%H:%M:%S")
                return fecha_obj, res.content
            except:
                # Intento secundario si el formato varía
                try:
                    fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
                    return fecha_obj, res.content
                except:
                    pass
        
        return None, res.content # Devolvemos la imagen aunque no leamos fecha
                
    except Exception as e:
        print(f"   ⚠️ Error interno OCR: {e}")
        return None, None

def obtener_etiqueta_sensor(codigo):
    mapa = {"MOD": "MODIS", "VIR": "VIIRS-750m", "VIR375": "VIIRS-375m", "MIR": "MIR-Combined"}
    return mapa.get(codigo, codigo)

def procesar():
    limpiar_todo()
    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

    ahora_cl = obtener_hora_chile()
    fecha_exec = ahora_cl.strftime("%Y-%m-%d")
    hora_exec = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando V8.1 (OCR Mejorado + Fix Descargas): {fecha_exec} {hora_exec}")
    registros_nuevos = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(1)
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # 1. Encontrar URL Imagen Principal
                img_url_final = None
                tags = soup.find_all(['img', 'a'])
                palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src: continue
                    # Filtro más estricto para asegurar que es la imagen principal
                    if any(k in src for k in palabras_clave) and src.lower().endswith(('.png', '.jpg')):
                        if src.startswith('http'): img_url_final = src
                        else: img_url_final = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        break 

                # 2. Intentar OCR
                fecha_detectada = None
                contenido_imagen = None
                origen = "Espera..."
                
                if img_url_final:
                    # Intentamos leer
                    fecha_obj, contenido_descargado = leer_fecha_de_imagen(session, img_url_final)
                    contenido_imagen = contenido_descargado # Guardamos los bytes para no descargar de nuevo
                    
                    if fecha_obj:
                        fecha_detectada = fecha_obj
                        origen = "✅ OCR (Leído)"
                    else:
                        origen = "❌ FALLBACK (No leído)"

                # 3. Definir Datos Finales
                if fecha_detectada:
                    fecha_web = fecha_detectada.strftime("%Y-%m-%d")
                    hora_web = fecha_detectada.strftime("%H:%M:%S")
                    timestamp_str = f"{fecha_web} {hora_web}"
                else:
                    fecha_web = fecha_exec
                    hora_web = f"{hora_exec}_Sys"
                    timestamp_str = f"{fecha_exec} {hora_exec}"

                print(f"   👁️ {nombre_v} {s_label} -> {timestamp_str} [{origen}]")

                # 4. Preparar guardado
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, fecha_web)
                os.makedirs(ruta_carpeta, exist_ok=True)

                # VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break
                
                # 5. GUARDAR IMAGEN (Corrección: Ahora guarda SIEMPRE si hay imagen)
                descargas = 0
                if img_url_final and contenido_imagen:
                    if "✅" in origen: prefijo = hora_web.replace(":", "-") + "_"
                    else: prefijo = hora_exec.replace(":", "-") + "_Sys_"
                    
                    nombre_orig = os.path.basename(urlparse(img_url_final).path)
                    nombre_final = f"{prefijo}{nombre_orig}"
                    ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                    
                    try:
                        with open(ruta_archivo, 'wb') as f: 
                            f.write(contenido_imagen)
                        descargas = 1
                    except Exception as e:
                        print(f"Error guardando imagen: {e}")

                registros_nuevos.append({
                    "Timestamp": timestamp_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Datos_Web": fecha_web,
                    "Hora_Datos_Web": hora_web,
                    "Fecha_Revision": fecha_exec,
                    "Hora_Revision": hora_exec,
                    "Ruta_Fotos": ruta_carpeta if descargas > 0 else "Sin descarga"
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    if registros_nuevos:
        cols = ["Timestamp", "Volcan", "Sensor", "VRP_MW", "Fecha_Datos_Web", "Hora_Datos_Web", "Fecha_Revision", "Hora_Revision", "Ruta_Fotos"]
        df = pd.DataFrame(registros_nuevos)
        df = df.reindex(columns=cols)
        df.to_csv(DB_FILE, index=False)
        print(f"💾 CSV V8.1 Generado: {DB_FILE}")

if __name__ == "__main__":
    procesar()
