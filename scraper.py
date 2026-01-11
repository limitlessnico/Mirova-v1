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
    """ MODO PRUEBAS: Borra todo para verificar el nuevo formato limpio. """
    print("🧹 LIMPIEZA DE PRUEBAS ACTIVADA...")
    if os.path.exists("registro_vrp.csv"): try: os.remove("registro_vrp.csv"); except: pass
    if os.path.exists(DB_FILE): try: os.remove(DB_FILE); except: pass
    if os.path.exists("imagenes"): try: shutil.rmtree("imagenes"); except: pass
    if os.path.exists(CARPETA_PRINCIPAL): try: shutil.rmtree(CARPETA_PRINCIPAL); except: pass

def procesar_imagen_ocr(imagen_pil):
    gray = imagen_pil.convert('L')
    umbral = 200 
    blancoynegro = gray.point(lambda x: 0 if x < umbral else 255, '1')
    return blancoynegro

def validar_y_corregir_fecha(fecha_ocr, fecha_sistema):
    """ Evita fechas del futuro. """
    if fecha_ocr.date() > (fecha_sistema.date() + timedelta(days=1)):
        print(f"   ⚠️ CORRECCIÓN: Fecha futura ({fecha_ocr.date()}). Ajustando a hoy.")
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
    limpiar_todo() # Limpieza activada

    if not os.path.exists(CARPETA_PRINCIPAL): 
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

    ahora_cl = obtener_hora_chile()
    # Fecha/Hora de ejecución combinada para la columna de revisión
    fecha_revision_full = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    
    # Variables simples para carpetas
    fecha_exec_simple = ahora_cl.strftime("%Y-%m-%d")
    hora_exec_simple = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando V11.0 (Nuevas Columnas): {fecha_revision_full}")
    registros_nuevos = []
    
    # Contador para el ID
    contador_id = 0

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(1)
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # Buscar Imagen
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

                # OCR
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

                # --- LÓGICA DE FECHAS Y UNIX TIMESTAMP ---
                if fecha_detectada:
                    # Usamos la fecha leída por OCR
                    fecha_completa_str = fecha_detectada.strftime("%Y-%m-%d %H:%M:%S")
                    # Convertimos a Unix Timestamp (segundos desde 1970)
                    unix_time = int(fecha_detectada.timestamp())
                    
                    # Variables para carpetas
                    fecha_carpeta = fecha_detectada.strftime("%Y-%m-%d")
                    hora_archivo = fecha_detectada.strftime("%H:%M:%S")
                else:
                    # Usamos la fecha del sistema (Fallback)
                    fecha_completa_str = f"{fecha_exec_simple} {hora_exec_simple}"
                    unix_time = int(ahora_cl.timestamp())
                    
                    fecha_carpeta = fecha_exec_simple
                    hora_archivo = f"{hora_exec_simple}_Sys"

                print(f"   👁️ {nombre_v} {s_label} -> {fecha_completa_str} [{origen}]")

                # Carpetas
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, fecha_carpeta)
                os.makedirs(ruta_carpeta, exist_ok=True)

                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break
                
                # Guardar Imagen
                ruta_foto_csv = "Sin descarga"
                if img_url_final and contenido_imagen:
                    if "✅" in origen: prefijo = hora_archivo.replace(":", "-") + "_"
                    else: prefijo = hora_exec_simple.replace(":", "-") + "_Sys_"
                    
                    nombre_orig = os.path.basename(urlparse(img_url_final).path)
                    nombre_final = f"{prefijo}{nombre_orig}"
                    ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                    ruta_foto_csv = ruta_archivo
                    
                    with open(ruta_archivo, 'wb') as f: f.write(contenido_imagen)

                # --- AGREGAR REGISTRO CON NUEVAS COLUMNAS ---
                registros_nuevos.append({
                    "ID": contador_id,               # 1. Correlativo
                    "Unix_Time": unix_time,          # 2. Formato Unix
                    "Fecha_Completa": fecha_completa_str, # 3. Renombrado
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Revision_Completa": fecha_revision_full, # 4. Fusionado
                    "Ruta_Fotos": ruta_foto_csv
                })
                
                # Aumentamos el contador para la siguiente fila
                contador_id += 1

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # --- GUARDAR CSV FINAL ---
    if registros_nuevos:
        # Definimos el orden exacto de las columnas
        cols = [
            "ID", 
            "Unix_Time", 
            "Fecha_Completa", 
            "Volcan", 
            "Sensor", 
            "VRP_MW", 
            "Fecha_Revision_Completa", 
            "Ruta_Fotos"
        ]
        
        df_nuevo = pd.DataFrame(registros_nuevos)
        df_nuevo = df_nuevo.reindex(columns=cols)
        
        df_nuevo.to_csv(DB_FILE, index=False)
        print(f"💾 CSV V11.0 Generado: {DB_FILE}")

if __name__ == "__main__":
    procesar()
