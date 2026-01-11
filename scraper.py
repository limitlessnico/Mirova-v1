import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
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
    pass

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
    """ 
    MODO PRUEBAS ACTIVADO:
    Borra todos los datos anteriores al iniciar para no mezclar pruebas.
    """
    print("🧹 LIMPIEZA DE PRUEBAS ACTIVADA: Borrando datos antiguos...")
    if os.path.exists("registro_vrp.csv"): 
        try: os.remove("registro_vrp.csv")
        except: pass
    if os.path.exists(DB_FILE):
        try: os.remove(DB_FILE)
        except: pass
    if os.path.exists("imagenes"): 
        try: shutil.rmtree("imagenes")
        except: pass
    if os.path.exists(CARPETA_PRINCIPAL): 
        try: shutil.rmtree(CARPETA_PRINCIPAL)
        except: pass

def procesar_imagen_ocr(imagen_pil):
    gray = imagen_pil.convert('L')
    umbral = 200 
    blancoynegro = gray.point(lambda x: 0 if x < umbral else 255, '1')
    return blancoynegro

def validar_y_corregir_fecha(fecha_ocr, fecha_sistema):
    """ CORRECCIÓN: Si el OCR lee una fecha futura, la ajusta a HOY. """
    # Si la fecha OCR es mayor a mañana (margen de error)
    if fecha_ocr.date() > (fecha_sistema.date() + timedelta(days=1)):
        print(f"   ⚠️ CORRECCIÓN: Fecha futura ({fecha_ocr.date()}) detectada. Ajustando a hoy.")
        # Mantenemos la HORA leída, pero usamos el AÑO/MES/DÍA real
        fecha_corregida = fecha_ocr.replace(year=fecha_sistema.year, 
                                            month=fecha_sistema.month, 
                                            day=fecha_sistema.day)
        return fecha_corregida
    return fecha_ocr

def leer_fecha_de_imagen(session, img_url, fecha_referencia_cl):
    try:
        res = session.get(img_url, timeout=10)
        if res.status_code != 200: return None, None
        
        imagen_original = Image.open(BytesIO(res.content))
        imagen_procesada = procesar_imagen_ocr(imagen_original)
        
        texto_leido = pytesseract.image_to_string(imagen_procesada, config='--psm 6')
        
        patron = r"(?i)(\d{1,2}[-\s][a-z]{3}[-\s]\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        match = re.search(patron, texto_leido)
        
        if match:
            fecha_str = match.group(1).replace(" ", "-")
            fecha_obj = None
            try:
                fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y-%H:%M:%S")
            except:
                try:
                    fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
                except:
                    pass
            
            if fecha_obj:
                # Validar que no sea futuro
                fecha_obj = validar_y_corregir_fecha(fecha_obj, fecha_referencia_cl)
                return fecha_obj, res.content
        
        return None, res.content
                
    except Exception as e:
        print(f"   ⚠️ Error interno OCR: {e}")
        return None, None

def obtener_etiqueta_sensor(codigo):
    mapa = {"MOD": "MODIS", "VIR": "VIIRS-750m", "VIR375": "VIIRS-375m", "MIR": "MIR-Combined"}
    return mapa.get(codigo, codigo)

def procesar():
    # 1. EJECUTAR LIMPIEZA
    limpiar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL): 
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

    ahora_cl = obtener_hora_chile()
    fecha_exec = ahora_cl.strftime("%Y-%m-%d")
    hora_exec = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando V10.1 (Pruebas Limpias + Corrección Fechas): {fecha_exec} {hora_exec}")
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

                # Buscar URL Imagen
                img_url_final = None
                tags = soup.find_all(['img', 'a'])
                palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src: continue
                    if any(k in src for k in palabras_clave) and src.lower().endswith(('.png', '.jpg')):
                        if src.startswith('http'): img_url_final = src
                        else: img_url_final = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        break 

                # OCR con Validación de Fecha
                fecha_detectada = None
                contenido_imagen = None
                origen = "..."
                
                if img_url_final:
                    fecha_obj, contenido_descargado = leer_fecha_de_imagen(session, img_url_final, ahora_cl)
                    contenido_imagen = contenido_descargado
                    
                    if fecha_obj:
                        fecha_detectada = fecha_obj
                        origen = "✅ OCR"
                    else:
                        origen = "❌ FALLBACK"

                # Definir Datos
                if fecha_detectada:
                    fecha_web = fecha_detectada.strftime("%Y-%m-%d")
                    hora_web = fecha_detectada.strftime("%H:%M:%S")
                    timestamp_str = f"{fecha_web} {hora_web}"
                else:
                    fecha_web = fecha_exec
                    hora_web = f"{hora_exec}_Sys"
                    timestamp_str = f"{fecha_exec} {hora_exec}"

                print(f"   👁️ {nombre_v} {s_label} -> {timestamp_str} [{origen}]")

                # Carpetas
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, fecha_web)
                os.makedirs(ruta_carpeta, exist_ok=True)

                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break
                
                # Guardar Imagen
                ruta_foto_csv = "Sin descarga"
                if img_url_final and contenido_imagen:
                    if "✅" in origen: prefijo = hora_web.replace(":", "-") + "_"
                    else: prefijo = hora_exec.replace(":", "-") + "_Sys_"
                    
                    nombre_orig = os.path.basename(urlparse(img_url_final).path)
                    nombre_final = f"{prefijo}{nombre_orig}"
                    ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                    ruta_foto_csv = ruta_archivo
                    
                    # Como limpiamos todo al principio, guardamos sin miedo
                    with open(ruta_archivo, 'wb') as f: f.write(contenido_imagen)

                registros_nuevos.append({
                    "Timestamp": timestamp_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Datos_Web": fecha_web,
                    "Hora_Datos_Web": hora_web,
                    "Fecha_Revision": fecha_exec,
                    "Hora_Revision": hora_exec,
                    "Ruta_Fotos": ruta_foto_csv
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # Guardar CSV (Modo escritura 'w' porque borramos el viejo)
    if registros_nuevos:
        cols = ["Timestamp", "Volcan", "Sensor", "VRP_MW", "Fecha_Datos_Web", "Hora_Datos_Web", "Fecha_Revision", "Hora_Revision", "Ruta_Fotos"]
        df_nuevo = pd.DataFrame(registros_nuevos)
        df_nuevo = df_nuevo.reindex(columns=cols)
        
        df_nuevo.to_csv(DB_FILE, index=False)
        print(f"💾 Archivo nuevo generado: {DB_FILE}")

if __name__ == "__main__":
    procesar()
