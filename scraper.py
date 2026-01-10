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

def procesar():
    if not os.path.exists('imagenes'):
        os.makedirs('imagenes', exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': BASE_URL
    })

    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
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
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)

                # 1. Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 2. Descarga Inteligente (Usando nombres originales)
                descargas = 0
                os.makedirs(ruta_final, exist_ok=True)
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue

                    # Limpieza de URL
                    if src.startswith('http'):
                        img_url = src
                    else:
                        clean_src = src.replace('../', '').lstrip('/')
                        img_url = f"{BASE_URL}/{clean_src}"

                    # --- LÓGICA DE NOMBRE ORIGINAL ---
                    # 1. Obtenemos el nombre real del archivo (ej: Lascar_MODIS_Latest10NTI.png)
                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)

                    # 2. FILTRO DE ORO: Solo aceptamos archivos que contengan estas palabras clave.
                    # Esto descarta automáticamente el logo, íconos, botones y basura.
                    # Basado en tus archivos: 'Latest', 'VRP', 'Dist' son los datos reales.
                    palabras_clave = ['Latest', 'VRP', 'Dist', 'log', 'Time', 'Map']
                    
                    if any(k in nombre_original for k in palabras_clave):
                        
                        # Doble chequeo: debe ser imagen
                        ext_validas = ['.jpg', '.jpeg', '.png']
                        if not any(nombre_original.lower().endswith(ext) for ext in ext_validas):
                            continue

                        try:
                            # Descargar
                            time.sleep(1)
                            img_res = session.get(img_url, timeout=15)
                            
                            # Filtro de peso (> 2.5KB) por seguridad
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                
                                # GUARDAMOS CON EL NOMBRE ORIGINAL
                                # Ya no inventamos nombres. Se guarda tal cual viene de Mirova.
                                ruta_completa = os.path.join(ruta_final, nombre_original)
                                
                                with open(ruta_completa, 'wb') as f:
                                    f.write(img_res.content)
                                
                                descargas += 1
                                print(f"      💾 Guardado: {nombre_original}")
                                
                        except Exception as e_img:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Ok", "VRP_MW": vrp,
                    "Sensor": s_label, "Fotos_Guardadas": descargas,
                    "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
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
