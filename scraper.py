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
    MODO PRUEBAS: Borra todo lo antiguo antes de empezar.
    """
    print("🧹 INICIANDO LIMPIEZA DE ARCHIVOS ANTIGUOS...")

    # 1. Borrar basura "Legacy" (Archivos sueltos en la raíz)
    if os.path.exists("registro_vrp.csv"):
        try: os.remove("registro_vrp.csv")
        except: pass
    
    if os.path.exists("imagenes"):
        try: shutil.rmtree("imagenes")
        except: pass

    # 2. Borrar carpeta de resultados actual
    if os.path.exists(CARPETA_PRINCIPAL):
        try:
            shutil.rmtree(CARPETA_PRINCIPAL)
            print(f"   ✨ Carpeta '{CARPETA_PRINCIPAL}' reiniciada.")
        except Exception as e:
            print(f"   ⚠️ No se pudo borrar carpeta principal: {e}")

def obtener_datos_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        ahora_chile = datetime.now(tz_chile)
        offset = ahora_chile.strftime('%z') 
        zona_str = f"UTC{offset[:3]}"
        return ahora_chile, zona_str
    except Exception as e:
        print(f"⚠️ Error zona horaria: {e}. Usando UTC-3.")
        return datetime.now(), "UTC-03"

def obtener_fecha_update(soup):
    try:
        texto_pagina = soup.get_text()
        patron = r"Last Update\s*:?\s*(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2})"
        match = re.search(patron, texto_pagina, re.IGNORECASE)
        if match:
            fecha_str = match.group(1)
            fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H-%M-%S")
    except: pass
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
    # --- LIMPIEZA TOTAL ACTIVADA ---
    limpiar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL):
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
        print(f"📁 Nueva carpeta creada: {CARPETA_PRINCIPAL}")

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': BASE_URL
    })

    ahora_chile, zona_actual = obtener_datos_chile()
    fecha_hoy_cl = ahora_chile.strftime("%Y-%m-%d")
    hora_hoy_cl = ahora_chile.strftime("%H:%M:%S")
    
    print(f"🕒 Hora Chile: {hora_hoy_cl} ({zona_actual})")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"⚡ Escaneando: {nombre_v} - {s_label}")
                time.sleep(random.uniform(2, 4))
                
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')

                fecha_dato, hora_dato = obtener_fecha_update(soup)
                
                if fecha_dato and hora_dato:
                    carpeta_fecha = fecha_dato
                    carpeta_hora = hora_dato
                else:
                    carpeta_fecha = fecha_hoy_cl
                    carpeta_hora = ahora_chile.strftime("%H-%M-%S_Sys")

                ruta_final = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, carpeta_fecha, carpeta_hora)

                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        raw_vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        vrp = raw_vrp if raw_vrp else "0"
                        break

                descargas = 0
                tags = soup.find_all(['img', 'a'])
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue

                    if src.startswith('http'): img_url = src
                    else: img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"

                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)
                    
                    palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map', 'Trend', 'Energy']
                    ext_validas = ['.jpg', '.jpeg', '.png']

                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):

                        os.makedirs(ruta_final, exist_ok=True)
                        ruta_archivo = os.path.join(ruta_final, nombre_original)
                        
                        if os.path.exists(ruta_archivo): continue

                        try:
                            time.sleep(0.5)
                            img_res = session.get(img_url, timeout=10)
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                with open(ruta_archivo, 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except: pass

                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Sat": carpeta_fecha,
                    "Hora_Sat": carpeta_hora,
                    "Fecha_Revision": fecha_hoy_cl,
                    "Hora_Revision": hora_hoy_cl,
                    "Zona_Horaria": zona_actual,
                    "Ruta_Fotos": ruta_final if descargas > 0 else "Sin nuevas fotos"
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # --- GUARDADO CSV ---
    if registros_ciclo:
        # Aquí estaba el error: he puesto la lista en una sola línea para evitar cortes
        cols = ["Volcan", "Sensor", "VRP_MW", "Fecha_Sat", "Hora_Sat", "Fecha_Revision", "Hora_Revision", "Zona_Horaria", "Ruta_Fotos"]
        
        df_nuevo = pd.DataFrame(registros_ciclo)
        df_nuevo = df_nuevo.reindex(columns=cols)
        
        df_nuevo.to_csv(DB_FILE, index=False)
        print(f"🆕 CSV creado desde cero: {DB_FILE}")

if __name__ == "__main__":
    procesar()
