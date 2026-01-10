import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    # Mantenemos la carpeta existente para que GitHub no la oculte si está vacía temporalmente
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
        # Los 3 sensores que ya registran datos en tu CSV
        for modo in ["MOD", "VIR", "VIR375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS-750m" if modo == "VIR" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"Buscando imágenes: {nombre_v} - {s_label}")
                # Pausa necesaria para evitar que el servidor nos bloquee las fotos pesadas
                time.sleep(random.uniform(20, 35)) 
                
                res = session.get(url_sitio, timeout=45)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)

                # 1. Extraer VRP (Valor de radiación térmica)
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 2. Descarga Total de Imágenes para este sensor
                descargas = 0
                # Buscamos en etiquetas de imagen (img) y enlaces (a)
                for tag in soup.find_all(['img', 'a']):
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    # FILTRO MAESTRO: Captura nombres típicos de MODIS y VIIRS
                    if any(k in src.lower() for k in ['temp', 'map', 'trend', 'vir', 'modis', 'current']):
                        os.makedirs(ruta_final, exist_ok=True)
                        img_url = src if src.startswith('http') else f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        try:
                            time.sleep(3) # Pequeña pausa entre fotos del mismo sensor
                            img_res = session.get(img_url, timeout=25)
                            
                            # Solo guardamos si es una imagen real (mayor a 3KB)
                            if img_res.status_code == 200 and len(img_res.content) > 3000:
                                ext = "png" if "png" in src.lower() else "jpg"
                                # Guardamos con el nombre del sensor para que no haya duda
                                nombre_f = f"{s_label}_archivo_{descargas}.{ext}"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except: continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Actualizado", "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label, "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Finalizado {s_label}: {descargas} fotos guardadas.")

            except Exception as e:
                print(f"Error en {nombre_v} {s_label}: {e}")

    # Guardado del CSV (Suma los nuevos datos al historial)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
