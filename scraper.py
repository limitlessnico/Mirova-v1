import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random
import shutil

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def limpiar_imagenes():
    """Borra el historial para empezar una colecta limpia."""
    if os.path.exists('imagenes'):
        shutil.rmtree('imagenes')
    os.makedirs('imagenes', exist_ok=True)

def procesar():
    limpiar_imagenes() # Mantiene tu repositorio limpio
    
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
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"Procesando: {nombre_v} - {s_label}")
                time.sleep(random.uniform(15, 25)) # Pausa de sigilo
                
                res = session.get(url_sitio, timeout=45)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)

                # Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # --- DESCARGA MEJORADA PARA VIIRS ---
                descargas = 0
                # Buscamos imágenes en etiquetas <img> y también en posibles enlaces <a>
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    # Extraer el enlace (src para imágenes, href para enlaces directos)
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    # Filtro para capturar mapas térmicos y tendencias de TODOS los sensores
                    if any(key in src.lower() for key in ['temp_modis', 'temp_viirs', 'map_last', 'trend']):
                        os.makedirs(ruta_final, exist_ok=True)
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        try:
                            time.sleep(3)
                            img_res = session.get(img_url, timeout=20)
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                nombre_f = f"{s_label}_captura_{descargas}.png"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except: continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Actualizado", "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label, "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Finalizado {s_label}: {descargas} fotos.")
            except Exception as e:
                print(f"Error: {e}")

    # Guardado del CSV (Sigue funcionando correctamente)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
