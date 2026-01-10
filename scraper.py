import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.mirovaweb.it/'
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
                time.sleep(10) # Pausa para evitar bloqueos del servidor
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta, exist_ok=True)

                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                descargas = 0
                for img in soup.find_all('img'):
                    src = img.get('src', '')
                    if any(key in src.lower() for key in ['temp_modis', 'temp_viirs', 'map_last', 'trend']):
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        try:
                            time.sleep(2)
                            img_res = session.get(img_url, timeout=15)
                            if img_res.status_code == 200 and len(img_res.content) > 2000:
                                tipo = "mapa" if "map" in src else "termico"
                                nombre_f = f"{s_label}_{tipo}_{descargas}.png"
                                with open(os.path.join(ruta, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"OK: {nombre_v} {s_label} - {descargas} fotos")
            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # Guardado seguro: Esto repara el archivo de 1 Byte automáticamente
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        try:
            if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
                df_base = pd.read_csv(DB_FILE)
                df_final = pd.concat([df_base, df_nuevo], ignore_index=True)
                df_final.to_csv(DB_FILE, index=False)
            else:
                df_nuevo.to_csv(DB_FILE, index=False)
        except:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
