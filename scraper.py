import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Probamos ambos sensores para cada volcán
        for modo in ["MOD", "VIRS"]:
            sensor_name = "MODIS" if modo == "MOD" else "VIIRS"
            url = f"{BASE_URL}volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                res = requests.get(url, headers=headers, timeout=20)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # FORZAR ESTRUCTURA: imagenes / Volcan / Fecha / Hora
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
                os.makedirs(ruta_final, exist_ok=True)

                # Descargar imágenes y renombrarlas con el sensor
                imgs = soup.find_all('img')
                for i, img in enumerate(imgs):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        img_url = src if src.startswith('http') else BASE_URL + src
                        img_data = requests.get(img_url, headers=headers).content
                        with open(os.path.join(ruta_final, f"{sensor_name}_img_{i}.png"), 'wb') as f:
                            f.write(img_data)

                # Guardamos TODOS los datos para evitar celdas vacías
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "VRP_MW": vrp,
                    "Sensor": sensor_name,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S"),
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M")
                })
                print(f"Capturado {nombre_v} - {sensor_name}")

            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # Guardar en CSV asegurando que las columnas coincidan
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Alineamos columnas para evitar que aparezcan en blanco
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
