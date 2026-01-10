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

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"

# --- CONFIGURACIÓN ---
CARPETA_PRINCIPAL = "monitoreo_datos"
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp.csv")

def limpiar_todo():
    """ MODO PRUEBAS: Borra todo para verificar limpia. """
    print("🧹 LIMPIEZA INICIAL ACTIVADA...")
    
    # 1. Borrar archivo CSV antiguo si existe
    if os.path.exists("registro_vrp.csv"):
        try:
            os.remove("registro_vrp.csv")
        except:
            pass
            
    # 2. Borrar carpeta imagenes antigua si existe
    if os.path.exists("imagenes"):
        try:
            shutil.rmtree("imagenes")
        except:
            pass
            
    # 3. Borrar carpeta principal actual
    if os.path.exists(CARPETA_PRINCIPAL):
        try:
            shutil.rmtree(CARPETA_PRINCIPAL)
        except:
            pass

def obtener_datos_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def obtener_info_web(soup):
    """ 
    Busca el texto rojo 'Last Update' en la página.
    """
    try:
        texto_pagina = soup.get_text()
        # Regex para capturar "Last update: 10-jan-2026 19:55:00"
        patron = r"Last Update\s*[:\.]?\s*(\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        
        match = re.search(patron, texto_pagina, re.IGNORECASE)
        if match:
            fecha_str = match.group(1)
            fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H:%M:%S")
    except Exception as e:
        pass
    return None, None

def obtener_etiqueta_sensor(codigo):
    mapa = {"MOD": "MODIS", "VIR": "VIIRS-750m", "VIR375": "VIIRS-375m", "MIR": "MIR-Combined"}
    return mapa.get(codigo, codigo)

def procesar():
    # 1. Limpieza
    limpiar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL):
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

    ahora_cl = obtener_datos_chile()
    fecha_ejecucion = ahora_cl.strftime("%Y-%m-%d")
    hora_ejecucion = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando Test: {fecha_ejecucion} {hora_ejecucion}")
    registros_nuevos = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(random.uniform(1, 2))
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # Captura del dato rojo
                fecha_web, hora_web = obtener_info_web(soup)

                # Definimos el Timestamp
                if fecha_web and hora_web:
                    timestamp_str = f"{fecha_web} {hora_web}"
                    carpeta_fecha = fecha_web
                    hora_web_final = hora_web
                    origen_dato = "WEB"
                else:
                    timestamp_str = f"{fecha_ejecucion} {hora_ejecucion}"
                    carpeta_fecha = fecha_ejecucion
                    hora_web_final = f"{hora_ejecucion}_Sys"
                    origen_dato = "SYS"

                print(f"      ✨ {nombre_v} {s_label} -> {timestamp_str} [{origen_dato}]")

                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, carpeta_fecha)
                os.makedirs(ruta_carpeta, exist_ok=True)

                # VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Descarga Imágenes
                descargas = 0
                tags = soup.find_all(['img', 'a'])
                
                if hora_web:
                    prefijo_hora = hora_web.replace(":", "-") + "_"
                else:
                    prefijo_hora = hora_ejecucion.replace(":", "-") + "_Sys_"

                palabras_clave = [
                    'Latest', 'VRP', 'Dist', 'log', 
                    'Time', 'Map', 'Trend', 'Energy'
                ]
                ext_validas = ['.jpg', '.jpeg', '.png']

                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    if src.startswith('http'): img_url = src
                    else: img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                    
                    nombre_original = os.path.basename(urlparse(img_url).path)
                    
                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):
                        
                        nombre_final = f"{prefijo_hora}{nombre_original}"
                        ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                        
                        try:
                            time.sleep(0.1)
                            img_res = session.get(img_url, timeout=10)
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                with open(ruta_archivo, 'wb') as f: f.write(img_res.content)
                                descargas += 1
                        except: pass

                registros_nuevos.append({
                    "Timestamp": timestamp_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Datos_Web": carpeta_fecha,
                    "Hora_Datos_Web": hora_web_final,
                    "Fecha_Revision": fecha_ejecucion,
                    "Hora_Revision": hora_ejecucion,
                    "Ruta_Fotos": ruta_carpeta if descargas > 0 else "Sin cambios"
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # Guardado
    if registros_nuevos:
        cols = [
            "Timestamp", "Volcan", "Sensor", "VRP_MW", 
            "Fecha_Datos_Web", "Hora_Datos_Web", 
            "Fecha_Revision", "Hora_Revision", "Ruta_Fotos"
        ]
        
        df_nuevo = pd.DataFrame(registros_nuevos)
        df_nuevo = df_nuevo.reindex(columns=cols)
        df_nuevo.to_csv(DB_FILE, index=False)
        print(f"💾 Nuevo CSV generado: {DB_FILE}")

if __name__ == "__main__":
    procesar()
