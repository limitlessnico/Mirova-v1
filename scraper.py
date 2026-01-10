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
    # SEGURO DE CARPETA: Solo creamos si no existe. NUNCA borramos.
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
        # Usamos exactamente las rutas VIR y VIR375 que validaste
        for modo in ["MOD", "VIR", "VIR375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS-750m" if modo == "VIR" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"--- Iniciando {nombre_v} | {s_label} ---")
                time.sleep(random.uniform(20, 30)) # Sigilo para no ser bloqueados
                
                res = session.get(url_sitio, timeout=45)
                if res.status_code != 200: 
                    print(f"Error {res.status_code} al acceder a {s_label}")
                    continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Extraer VRP (Dato térmico)
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Crear ruta de guardado
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta_final, exist_ok=True)

                # DESCARGA DE IMÁGENES (Búsqueda agresiva en img y enlaces a)
                descargas = 0
                for tag in soup.find_all(['img', 'a']):
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    # Filtro para atrapar mapas de calor de MODIS y VIIRS por igual
                    if any(k in src.lower() for k in ['temp', 'map_last', 'trend', 'vir', 'modis']):
                        img_url = src if src.startswith('http') else f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        try:
                            time.sleep(4) # Pausa entre fotos
                            img_res = session.get(img_url, timeout=25)
                            # Si la imagen pesa más de 3KB, es real (evita archivos de error)
                            if img_res.status_code == 200 and len(img_res.content) > 3000:
                                ext = "png" if "png" in src.lower() else "jpg"
                                nombre_f = f"{s_label}_foto_{descargas}.{ext}"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                descargas += 1
                        except: continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Actualizado", "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label, "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"OK: {s_label} finalizado con {descargas} fotos.")

            except Exception as e:
                print(f"Error en {nombre_v} {s_label}: {e}")

    # GUARDADO DEL CSV (Suma los nuevos datos al historial existente)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)
        print("CSV actualizado correctamente.")

if __name__ == "__main__":
    procesar()
