import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time

VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.mirovaweb.it/'
    }
    
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_minuto = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                res = requests.get(url_sitio, headers=headers, timeout=20)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # 1. CORRECCIÓN DE CARPETAS (Sin el error de modo)
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_minuto)
                os.makedirs(ruta_final, exist_ok=True)

                # 2. DESCARGA DE IMÁGENES
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        img_url = src if src.startswith('http') else f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        try:
                            img_data = requests.get(img_url, headers=headers, timeout=10).content
                            if len(img_data) > 1000:
                                nombre_f = f"{s_label}_img_{i}.png"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_data)
                        except:
                            continue

                # 3. EXTRAER VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 4. LLENADO TOTAL DEL CSV (Aseguramos todas las columnas)
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                time.sleep(1)

            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # 5. GUARDADO PERSISTENTE
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Unimos asegurando que no se pierdan columnas
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
