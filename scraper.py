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
    # Headers más realistas para evitar que nos salten las imágenes
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Connection': 'close' # Ayuda a que no nos bloqueen por múltiples peticiones
    })

    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Procesamos sensores con pausas largas para "engañar" al servidor
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"Consultando {nombre_v} - {s_label}...")
                time.sleep(15) # PAUSA CRÍTICA: 15 segundos entre sensores
                
                res = session.get(url_sitio, timeout=40)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta, exist_ok=True)

                # Extraer VRP (Valor Térmico)
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Descarga de Imágenes con re-intento
                descargas = 0
                for img in soup.find_all('img'):
                    src = img.get('src', '')
                    if any(key in src.lower() for key in ['temp_modis', 'temp_viirs', 'map_last', 'trend']):
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        try:
                            time.sleep(3) # Pausa entre fotos
                            img_res = session.get(img_url, timeout=20)
                            if img_res.status_code == 200 and len(img_res.content) > 3000:
                                ext = "png" if "png" in src.lower() else "jpg"
                                nombre_f = f"{s_label}_{descargas}.{ext}"
                                with open(os.path.join(ruta, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Actualizado", "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label, "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
            except Exception as e:
                print(f"Error: {e}")

    # Guardado del CSV (Ya verificado que funciona)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
