import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random
from urllib.parse import urlparse

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def obtener_fecha_update(soup):
    """
    Busca la cadena 'Last Update' en el HTML y devuelve fecha y hora formateadas.
    Retorna (YYYY-MM-DD, HH-MM-SS) o None si falla.
    """
    try:
        # Buscamos cualquier texto que contenga "Last Update"
        tag = soup.find(string=lambda text: "Last Update" in text if text else False)
        if tag:
            # El formato suele ser: "Last Update:10-Jan-2026 06:42:00"
            texto_limpio = tag.strip().replace("Last Update:", "").strip()
            
            # Convertimos el texto a objeto fecha (ej: 10-Jan-2026 06:42:00)
            fecha_obj = datetime.strptime(texto_limpio, "%d-%b-%Y %H:%M:%S")
            
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H-%M-%S")
    except Exception as e:
        print(f"   ⚠️ No se pudo leer fecha Update: {e}")
    return None, None

def procesar():
    if not os.path.exists('imagenes'):
        os.makedirs('imagenes', exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': BASE_URL
    })

    # Hora de ejecución (Respaldo por si falla la lectura web)
    ahora_ejecucion = datetime.now()
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS-750m" if modo == "VIR" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"📡 Conectando: {nombre_v} - {s_label}...")
                time.sleep(random.uniform(15, 25))
                
                res = session.get(url_sitio, timeout=45)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')

                # --- NUEVA LÓGICA DE CARPETAS ---
                # 1. Intentamos leer el "Last Update" real de la página
                fecha_web, hora_web = obtener_fecha_update(soup)

                if fecha_web and hora_web:
                    # Si lo encontramos, usamos la fecha/hora DEL DATO
                    carpeta_fecha = fecha_web
                    carpeta_hora = hora_web
                    print(f"   🕒 Fecha detectada en web: {carpeta_hora}")
                else:
                    # Si no, usamos la fecha/hora DE EJECUCIÓN (Fallback)
                    carpeta_fecha = ahora_ejecucion.strftime("%Y-%m-%d")
                    carpeta_hora = ahora_ejecucion.strftime("%H-%M-%S_script") # Sufijo para diferenciar
                    print(f"   ⚠️ Usando hora del sistema (no se halló fecha web)")

                ruta_final = os.path.join("imagenes", nombre_v, carpeta_fecha, carpeta_hora)

                # --- EXTRACCIÓN DE DATOS ---
                # 1. Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 2. Descarga de Imágenes (Nombres Originales)
                descargas = 0
                os.makedirs(ruta_final, exist_ok=True)
                
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

                    # Filtro de palabras clave (Latest, VRP, Dist)
                    palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                    
                    if any(k in nombre_original for k in palabras_clave):
                        ext_validas = ['.jpg', '.jpeg', '.png']
                        if not any(nombre_original.lower().endswith(ext) for ext in ext_validas):
                            continue

                        try:
                            # Verificar si ya existe para no descargarlo mil veces si es la misma hora
                            ruta_completa = os.path.join(ruta_final, nombre_original)
                            if os.path.exists(ruta_completa):
                                # Si ya existe el archivo en esa carpeta de hora específica, saltamos
                                continue

                            time.sleep(1)
                            img_res = session.get(img_url, timeout=15)
                            
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                with open(ruta_completa, 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                                print(f"      💾 Guardado: {nombre_original}")
                                
                        except Exception as e_img:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Ok", "VRP_MW": vrp,
                    "Sensor": s_label, "Fotos_Guardadas": descargas,
                    "Fecha_Dato": carpeta_fecha, "Hora_Dato": carpeta_hora # Guardamos la hora real en CSV
                })

            except Exception as e:
                print(f"⚠️ Error: {e}")

    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
