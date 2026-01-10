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
    """Borra todo el contenido de la carpeta imagenes para empezar de cero."""
    if os.path.exists('imagenes'):
        print("Limpiando carpetas de imágenes antiguas...")
        for nombre in os.listdir('imagenes'):
            ruta = os.path.join('imagenes', nombre)
            try:
                if os.path.isdir(ruta):
                    shutil.rmtree(ruta)
                else:
                    os.unlink(ruta)
            except Exception as e:
                print(f"No se pudo borrar {ruta}: {e}")
    else:
        os.makedirs('imagenes', exist_ok=True)

def procesar():
    # 1. Limpieza inicial
    limpiar_imagenes()

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.mirovaweb.it/',
        'Connection': 'keep-alive'
    })

    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Sensores: MODIS, VIIRS y VIIRS-375m
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                # PAUSA DE SIGILO: Entre 20 y 35 segundos para no ser detectados
                print(f"Accediendo a {nombre_v} ({s_label})...")
                time.sleep(random.uniform(20, 35)) 
                
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

                # DESCARGA DE IMÁGENES
                descargas = 0
                for img in soup.find_all('img'):
                    src = img.get('src', '')
                    if any(key in src.lower() for key in ['temp_modis', 'temp_viirs', 'map_last']):
                        os.makedirs(ruta_final, exist_ok=True)
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        try:
                            time.sleep(random.uniform(4, 7)) # Pausa entre fotos
                            img_res = session.get(img_url, timeout=25)
                            if img_res.status_code == 200 and len(img_res.content) > 3000:
                                ext = "png" if "png" in src.lower() else "jpg"
                                nombre_f = f"{s_label}_{descargas}.{ext}"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Actualizado", "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label, "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Éxito en {nombre_v} {s_label}: {descargas} fotos guardadas.")
            except Exception as e:
                print(f"Fallo en {nombre_v}: {e}")

    # Guardado seguro en CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        try:
            if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
                df_base = pd.read_csv(DB_FILE)
                pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
            else:
                df_nuevo.to_csv(DB_FILE, index=False)
        except:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
