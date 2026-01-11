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

CARPETA_PRINCIPAL = "monitoreo_satelital"
NOMBRE_CARPETA_IMAGENES = "imagenes_satelitales"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, NOMBRE_CARPETA_IMAGENES)
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
CARPETA_OBSOLETA = "monitoreo_datos"

def obtener_hora_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def limpiar_basura():
    """ Mantenimiento de carpetas """
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA); print("🗑️ Carpeta vieja eliminada.")
        except: pass
    # MODO PRUEBAS: Limpia la carpeta actual para reiniciar el CSV
    if os.path.exists(CARPETA_PRINCIPAL):
        try: shutil.rmtree(CARPETA_PRINCIPAL); print("🧹 Limpieza de pruebas ejecutada.")
        except: pass

def procesar_imagen_ocr(imagen_pil):
    gray = imagen_pil.convert('L')
    umbral = 200 
    blancoynegro = gray.point(lambda x: 0 if x < umbral else 255, '1')
    return blancoynegro

def validar_y_corregir_fecha(fecha_ocr, fecha_sistema):
    if fecha_ocr.date() > (fecha_sistema.date() + timedelta(days=1)):
        fecha_corregida = fecha_ocr.replace(year=fecha_sistema.year, 
                                            month=fecha_sistema.month, 
                                            day=fecha_sistema.day)
        return fecha_corregida
    return fecha_ocr

def leer_fecha_de_imagen_bytes(contenido_imagen, fecha_referencia_cl):
    try:
        imagen_original = Image.open(BytesIO(contenido_imagen))
        imagen_procesada = procesar_imagen_ocr(imagen_original)
        texto_leido = pytesseract.image_to_string(imagen_procesada, config='--psm 6')
        
        patron = r"(?i)(\d{1,2}[-\s][a-z]{3}[-\s]\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        match = re.search(patron, texto_leido)
        
        if match:
            fecha_str = match.group(1).replace(" ", "-")
            fecha_obj = None
            try: fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y-%H:%M:%S")
            except:
                try: fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
                except: pass
            
            if fecha_obj:
                return validar_y_corregir_fecha(fecha_obj, fecha_referencia_cl)
        return None
    except: return None

def obtener_etiqueta_sensor(codigo):
    mapa = {"MOD": "MODIS", "VIR": "VIIRS", "VIR375": "VIIRS375", "MIR": "MIR-Combined"}
    return mapa.get(codigo, codigo)

def buscar_distancia_en_html(soup_text):
    try:
        # Busca "Dist = 5.2 km" o "Dist: 5.2"
        patron = r"Dist\s*[=:]\s*([\d\.]+)"
        match = re.search(patron, soup_text, re.IGNORECASE)
        if match: return float(match.group(1))
    except: pass
    return 0.0

def descargar_y_guardar(session, url, ruta_guardado):
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            with open(ruta_guardado, 'wb') as f: f.write(r.content)
            return True
    except: pass
    return False

def procesar():
    limpiar_basura() # Limpieza activada

    if not os.path.exists(CARPETA_PRINCIPAL): 
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

    ahora_cl = obtener_hora_chile()
    fecha_proceso_str = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    fecha_exec_simple = ahora_cl.strftime("%Y-%m-%d")
    hora_exec_simple = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando Monitor Volcanico VRP (V16.0): {fecha_proceso_str}")
    registros_nuevos = []
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

                # 1. Detectar las 4 imágenes
                urls_imagenes = {"Latest": None, "VRP": None, "LogVRP": None, "Dist": None}
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src: continue
                    if src.startswith('http'): full_url = src
                    else: full_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                    
                    lower_src = src.lower()
                    if not lower_src.endswith(('.png', '.jpg', '.jpeg')): continue

                    if "latest10nti" in lower_src: urls_imagenes["Latest"] = full_url
                    elif "logvrp" in lower_src: urls_imagenes["LogVRP"] = full_url
                    elif "_vrp" in lower_src: urls_imagenes["VRP"] = full_url
                    elif "_dist" in lower_src: urls_imagenes["Dist"] = full_url

                # 2. OCR
                fecha_detectada = None
                contenido_latest = None
                origen = "..."
                
                if urls_imagenes["Latest"]:
                    try:
                        resp_img = session.get(urls_imagenes["Latest"], timeout=10)
                        if resp_img.status_code == 200:
                            contenido_latest = resp_img.content
                            fecha_detectada = leer_fecha_de_imagen_bytes(contenido_latest, ahora_cl)
                    except: pass

                if fecha_detectada:
                    origen = "✅ OCR"
                    fecha_satelite_str = fecha_detectada.strftime("%Y-%m-%d %H:%M:%S")
                    unix_time = int(fecha_detectada.timestamp())
                    fecha_carpeta = fecha_detectada.strftime("%Y-%m-%d")
                    hora_archivo = fecha_detectada.strftime("%H:%M:%S")
                else:
                    origen = "❌ FALLBACK"
                    fecha_satelite_str = f"{fecha_exec_simple} {hora_exec_simple}"
                    unix_time = int(ahora_cl.timestamp())
                    fecha_carpeta = fecha_exec_simple
                    hora_archivo = f"{hora_exec_simple}_Sys"

                # 3. Guardar Imágenes
                ruta_carpeta_volcan = os.path.join(RUTA_IMAGENES_BASE, nombre_v)
                ruta_carpeta_dia = os.path.join(ruta_carpeta_volcan, fecha_carpeta)
                os.makedirs(ruta_carpeta_dia, exist_ok=True)
                
                if "✅" in origen: prefijo = hora_archivo.replace(":", "-") + "_"
                else: prefijo = hora_exec_simple.replace(":", "-") + "_Sys_"
                ruta_foto_principal = "No encontrada"

                for tipo, url in urls_imagenes.items():
                    if url:
                        nombre_orig = os.path.basename(urlparse(url).path)
                        nombre_final = f"{prefijo}{nombre_orig}"
                        ruta_final = os.path.join(ruta_carpeta_dia, nombre_final)
                        
                        if tipo == "Latest" and contenido_latest:
                            with open(ruta_final, 'wb') as f: f.write(contenido_latest)
                            ruta_foto_principal = ruta_final
                        else:
                            descargar_y_guardar(session, url, ruta_final)

                # 4. Datos VRP y Distancia
                vrp_valor = 0.0
                distancia_km = 0.0
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        try:
                            vrp_txt = b.text.split('=')[-1].replace('MW', '').strip()
                            vrp_valor = float(vrp_txt) if vrp_txt != "NaN" else 0.0
                        except: pass
                        break
                
                if vrp_valor > 0:
                    distancia_km = buscar_distancia_en_html(soup.get_text())

                # --- 5. CLASIFICACIÓN DE ALERTA ---
                # Definimos el estado basado en VRP y Distancia
                clasificacion = "NORMAL" # Por defecto
                
                if vrp_valor > 0:
                    if distancia_km <= 5.0:
                        clasificacion = "ALERTA VOLCANICA"
                    else:
                        clasificacion = "FALSO POSITIVO"
                
                # Feedback en consola
                print(f"   👁️ {nombre_v} {s_label} -> VRP:{vrp_valor} | Dist:{distancia_km}km | [{clasificacion}]")

                # Agregar al registro con la nueva columna CLASIFICACION
                registros_nuevos.append({
                    "ID": contador_id,
                    "timestamp": unix_time,
                    "Fecha_Satelite": fecha_satelite_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp_valor,
                    "Distancia_km": distancia_km,
                    "Clasificacion": clasificacion, # NUEVA COLUMNA
                    "Fecha_Proceso": fecha_proceso_str,
                    "Ruta_Fotos": ruta_foto_principal
                })
                contador_id += 1

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # --- GUARDAR CSVS ---
    if registros_nuevos:
        cols = ["ID", "timestamp", "Fecha_Satelite", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Clasificacion", "Fecha_Proceso", "Ruta_Fotos"]
        df_completo = pd.DataFrame(registros_nuevos)
        df_completo = df_completo.reindex(columns=cols)
        
        df_completo.to_csv(DB_FILE, index=False)
        print(f"💾 CSV Maestro generado: {DB_FILE}")
        
        for v in df_completo['Volcan'].unique():
            df_volcan = df_completo[df_completo['Volcan'] == v]
            ruta_csv_volcan = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
            df_volcan.to_csv(ruta_csv_volcan, index=False)
            print(f"   📄 CSV Individual: {ruta_csv_volcan}")

if __name__ == "__main__":
    procesar()
