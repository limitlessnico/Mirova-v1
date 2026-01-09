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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Procesamos los 3 sensores pero con pausas largas
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_detalles = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(3) # Pausa de 3 segundos para no ser bloqueados
                res = requests.get(url_detalles, headers=headers, timeout=25)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 1. Crear carpeta (forma simple para evitar errores)
                ruta = f"imagenes/{nombre_v}/{fecha_hoy}/{hora_carpeta}"
                if not os.path.exists(ruta):
                    os.makedirs(ruta)

                # 2. Extraer VRP (Dato numérico)
                vrp_valor = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp_valor = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 3. Descarga de imágenes con URL absoluta corregida
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        # Si la ruta es '../images/...' la convertimos a URL completa
                        clean_src = src.replace('../', '').lstrip('/')
                        img_url = f"{BASE_URL}/{clean_src}"
                        
                        try:
                            img_data = requests.get(img_url, headers=headers, timeout=15).content
                            if len(img_data) > 2000: # Solo guardamos si es una imagen real
                                filename = f"{s_label}_foto_{i}.png"
                                with open(os.path.join(ruta, filename), 'wb') as f:
                                    f.write(img_data)
                        except:
                            continue

                # 4. Guardar datos en la lista (Llenado total de columnas)
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp_valor,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Completado: {nombre_v} - {s_label}")

            except Exception as e:
                print(f"Fallo en {nombre_v} {s_label}: {e}")

    # 5. Actualizar el CSV sin perder datos antiguos
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_base = pd.read_csv(DB_FILE)
            # Forzamos que las columnas coincidan para evitar huecos
            df_final = pd.concat([df_base, df_nuevo], ignore_index=True, sort=False)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
