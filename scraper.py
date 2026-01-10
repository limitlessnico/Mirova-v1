import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random
from urllib.parse import urlparse
import re  # IMPORTANTE: Nueva librería para leer patrones de texto

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def obtener_fecha_update(soup):
    """
    Usa Expresiones Regulares (Regex) para encontrar la fecha exacta
    sin importar si hay espacios ocultos o cambios en el HTML.
    Busca el patrón: Last Update seguido de una fecha.
    """
    try:
        texto_pagina = soup.get_text()
        
        # Patrón Mágico: Busca "Last Update", dos puntos, y luego la fecha (DD-Mes-YYYY HH:MM:SS)
        # \s* permite que haya o no haya espacios.
        patron = r"Last Update\s*:\s*(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2})"
        
        match = re.search(patron, texto_pagina, re.IGNORECASE)
        
        if match:
            fecha_str = match.group(1) # Capturamos solo la parte de la fecha
            # Ejemplo capturado: "10-Jan-2026 06:42:00"
            
            # Convertimos a objeto fecha
            fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H-%M-%S")
            
    except Exception as e:
        # Si falla, no pasa nada, el código principal usará la hora del sistema
        pass
        
    return None, None

def procesar():
    if not os.path.exists('imagenes'):
        os.makedirs('imagenes', exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': BASE_URL
    })

    # Hora de respaldo (por si falla la lectura de la web)
    ahora_sys = datetime.now()
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS-750m" if modo == "VIR" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"⚡ {nombre_v} ({s_label})...")
                
                # Pausa optimizada (Velocidad Alta)
                time.sleep(random.uniform(2, 4))
                
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: 
                    print(f"   ❌ Error HTTP {res.status_code}")
                    continue
                
                soup = BeautifulSoup(res.text, 'html.parser')

                # --- DETECCIÓN DE FECHA REAL (NUEVO MÉTODO REGEX) ---
                fecha_dato, hora_dato = obtener_fecha_update(soup)

                if fecha_dato and hora_dato:
                    carpeta_fecha = fecha_dato
                    carpeta_hora = hora_dato
                else:
                    # Fallback solo si Regex falla
                    print("   ⚠️ No se detectó fecha en web, usando hora sistema.")
                    carpeta_fecha = ahora_sys.strftime("%Y-%m-%d")
                    carpeta_hora = ahora_sys.strftime("%H-%M-%S_Sys")

                # Ruta Final: imagenes / Volcan / Fecha_Dato / Hora_Dato
                ruta_final = os.path.join("imagenes", nombre_v, carpeta_fecha, carpeta_hora)

                # --- EXTRACCIÓN VRP ---
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # --- DESCARGA DE IMÁGENES ---
                descargas = 0
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue

                    if src.startswith('http'):
                        img_url = src
                    else:
                        clean_src = src.replace('../', '').lstrip('/')
                        img_url = f"{BASE_URL}/{clean_src}"

                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)

                    # Filtros de nombre
                    palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                    ext_validas = ['.jpg', '.jpeg', '.png']

                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):

                        # Crear carpeta solo si vamos a guardar algo
                        os.makedirs(ruta_final, exist_ok=True)
                        ruta_archivo = os.path.join(ruta_final, nombre_original)

                        if os.path.exists(ruta_archivo):
                            continue

                        try:
                            time.sleep(0.5) # Pausa mínima
                            img_res = session.get(img_url, timeout=10)
                            
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                with open(ruta_archivo, 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except: pass

                if descargas > 0:
                    print(f"   ✅ Guardadas {descargas} imágenes en {carpeta_hora}")
                
                registros_ciclo.append({
                    "Volcan": nombre_v, 
                    "Sensor": s_label, 
                    "VRP_MW": vrp,
                    "Fecha_Dato": carpeta_fecha,
                    "Hora_Dato": carpeta_hora,
                    "Check_Sistema": ahora_sys.strftime("%H:%M:%S")
                })

            except Exception as e:
                print(f"   ⚠️ Error: {e}")

    # Guardar CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
