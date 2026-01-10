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
    Busca la cadena 'Last Update' en el HTML.
    Ejemplo esperado en HTML: <div ...>Last Update:10-Jan-2026 12:35:00</div>
    """
    try:
        # Buscamos el texto exacto
        tag = soup.find(string=lambda text: "Last Update" in text if text else False)
        if tag:
            # Limpiamos el string para dejar solo la fecha
            texto_limpio = tag.strip().replace("Last Update:", "").strip()
            # Parseamos: 10-Jan-2026 12:35:00
            fecha_obj = datetime.strptime(texto_limpio, "%d-%b-%Y %H:%M:%S")
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H-%M-%S")
    except Exception as e:
        # Si falla, no rompemos el programa, solo devolvemos None
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
                
                # --- CAMBIO DE VELOCIDAD 1: Pausa reducida ---
                # Antes era 20-35s. Ahora es 2-4s.
                time.sleep(random.uniform(2, 4))
                
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: 
                    print(f"   ❌ Error HTTP {res.status_code}")
                    continue
                
                soup = BeautifulSoup(res.text, 'html.parser')

                # --- DETECCIÓN DE FECHA REAL DEL DATO ---
                fecha_dato, hora_dato = obtener_fecha_update(soup)

                if fecha_dato and hora_dato:
                    carpeta_fecha = fecha_dato
                    carpeta_hora = hora_dato
                else:
                    # Fallback si no encuentra la fecha en la web
                    print("   ⚠️ No se detectó 'Last Update', usando hora sistema.")
                    carpeta_fecha = ahora_sys.strftime("%Y-%m-%d")
                    carpeta_hora = ahora_sys.strftime("%H-%M-%S_Sys")

                # Ruta: imagenes / Volcan / Fecha / Hora
                ruta_final = os.path.join("imagenes", nombre_v, carpeta_fecha, carpeta_hora)

                # --- EXTRACCIÓN VRP ---
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # --- DESCARGA DE IMÁGENES ---
                descargas = 0
                # Solo creamos la carpeta si encontramos imágenes válidas para guardar
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue

                    # Normalizar URL
                    if src.startswith('http'):
                        img_url = src
                    else:
                        clean_src = src.replace('../', '').lstrip('/')
                        img_url = f"{BASE_URL}/{clean_src}"

                    # Obtener nombre original
                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)

                    # Filtros
                    palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                    ext_validas = ['.jpg', '.jpeg', '.png']

                    # ¿Es un archivo válido?
                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):

                        # Preparamos carpeta
                        os.makedirs(ruta_final, exist_ok=True)
                        ruta_archivo = os.path.join(ruta_final, nombre_original)

                        # Evitar re-descargar si ya existe en esa carpeta exacta
                        if os.path.exists(ruta_archivo):
                            continue

                        try:
                            # --- CAMBIO DE VELOCIDAD 2: Pausa mínima ---
                            time.sleep(0.5) 
                            
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
