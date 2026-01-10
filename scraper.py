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
    """ 
    MODO PRUEBAS: Borra todo para verificar que la captura de hora funciona bien. 
    """
    print("🧹 LIMPIEZA INICIAL ACTIVADA...")
    
    # 1. Borrar archivo CSV antiguo
    if os.path.exists("registro_vrp.csv"):
        try:
            os.remove("registro_vrp.csv")
        except:
            pass
            
    # 2. Borrar carpeta imagenes antigua
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
    except: 
        return datetime.now(pytz.utc)

def obtener_info_web(soup):
    """ 
    ESTRATEGIA AGRESIVA: Busca cualquier patrón de fecha DD-Mes-YYYY HH:MM:SS
    en todo el texto de la página, ignorando si dice 'Last Update' o no.
    """
    try:
        texto_pagina = soup.get_text()
        
        # Regex: 1o2 digitos - 3letras - 4digitos - espacio - Hora:Min:Seg
        # Ej: 10-jan-2026 19:55:00
        patron = r"(\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        
        match = re.search(patron, texto_pagina, re.IGNORECASE)
        if match:
            fecha_str = match.group(1)
            # Intentamos convertir el texto encontrado a objeto fecha
            try:
                fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
                return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H:%M:%S")
            except ValueError:
                pass
    except Exception as e:
        print(f"   ⚠️ Error leyendo fecha: {e}")
        pass
    return None, None

def obtener_etiqueta_sensor(codigo):
    mapa = {
        "MOD": "MODIS", 
        "VIR": "VIIRS-750m", 
        "VIR375": "VIIRS-375m", 
        "MIR": "MIR-Combined"
    }
    return mapa.get(codigo, codigo)

def procesar():
    # 1. Limpieza Total
    limpiar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL):
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 
        'Referer': BASE_URL
    })

    ahora_cl = obtener_datos_chile()
    fecha_ejecucion = ahora_cl.strftime("%Y-%m-%d")
    hora_ejecucion = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando Scraper V5.1: {fecha_ejecucion} {hora_ejecucion}")
    registros_nuevos = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(random.uniform(1, 1.5))
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # --- EXTRACCIÓN FECHA (TEXTO ROJO) ---
                fecha_web, hora_web = obtener_info_web(soup)

                # Definimos el Timestamp y origen
                if fecha_web and hora_web:
                    origen = "✅ WEB"
                    timestamp_str = f"{fecha_web} {hora_web}"
                    carpeta_fecha = fecha_web
                    hora_final = hora_web
                else:
                    origen = "❌ FALLBACK (Chile)"
                    timestamp_str = f"{fecha_ejecucion} {hora_ejecucion}"
                    carpeta_fecha = fecha_ejecucion
                    hora_final = f"{hora_ejecucion}_Sys"

                print(f"   🔎 {nombre_v} {s_label}: {timestamp_str} [{origen}]")

                # Crear Carpeta
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, carpeta_fecha)
                os.makedirs(ruta_carpeta, exist_ok=True)

                # VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Imágenes
                descargas = 0
                tags = soup.find_all(['img', 'a'])
                
                # Prefijo para el nombre de archivo
                if hora_web:
                    prefijo = hora_web.replace(":", "-") + "_"
                else:
                    prefijo = hora_ejecucion.replace(":", "-") + "_Sys_"

                # Lista vertical para evitar errores de copiado
                palabras_clave = [
                    'Latest', 'VRP', 'Dist', 'log', 
                    'Time', 'Map', 'Trend', 'Energy'
                ]
                ext_validas = ['.jpg', '.jpeg', '.png']

                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    if src.startswith('http'): 
                        img_url = src
                    else: 
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                    
                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)
                    
                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):
                        
                        nombre_final = f"{prefijo}{nombre_original}"
                        ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                        
                        try:
                            time.sleep(0.1)
                            img_res = session.get(img_url, timeout=10)
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                with open(ruta_archivo, 'wb') as f: 
                                    f.write(img_res.content)
                                descargas += 1
                        except: pass

                # Agregar a la lista para CSV
                registros_nuevos.append({
                    "Timestamp": timestamp_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Datos_Web": carpeta_fecha,
                    "Hora_Datos_Web": hora_final,
                    "Fecha_Revision": fecha_ejecucion,
                    "Hora_Revision": hora_ejecucion,
                    "Ruta_Fotos": ruta_carpeta if descargas > 0 else "Sin cambios"
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # Guardado del CSV Final
    if registros_nuevos:
        cols = [
            "Timestamp", "Volcan", "Sensor", "VRP_MW", 
            "Fecha_Datos_Web", "Hora_Datos_Web", 
            "Fecha_Revision", "Hora_Revision", "Ruta_Fotos"
        ]
        
        df = pd.DataFrame(registros_nuevos)
        df = df.reindex(columns=cols)
        df.to_csv(DB_FILE, index=False)
        print(f"💾 CSV Generado correctamente: {DB_FILE}")

if __name__ == "__main__":
    procesar()
