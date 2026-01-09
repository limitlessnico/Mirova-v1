
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
BASE_URL = "https://www.mirovaweb.it/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_detalles = f"{BASE_URL}NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                res = requests.get(url_detalles, headers=headers, timeout=20)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 1. Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 2. Carpeta con jerarquía (ya funcionando según tus capturas)
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
                os.makedirs(ruta_final, exist_ok=True)
                
                with open(os.path.join(ruta_final, "leeme.txt"), "w") as f:
                    f.write(f"Captura {s_label} realizada a las {ahora.strftime('%H:%M:%S')}")

                # 3. Descarga con reconstrucción de URL absoluta
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last', 'output']):
                        # Limpiamos el src y construimos la URL completa correctamente
                        clean_src = src.lstrip('./').lstrip('/')
                        img_url = f"{BASE_URL}{clean_src}"
                        
                        try:
                            img_res = requests.get(img_url, headers=headers, timeout=15)
                            if img_res.status_code == 200:
                                ext = clean_src.split('.')[-1][:3] # png o jpg
                                nombre_archivo = f"{s_label}_img_{i}.{ext}"
                                with open(os.path.join(ruta_final, nombre_archivo), 'wb') as f:
                                    f.write(img_res.content)
                                print(f"Guardado: {nombre_archivo}")
                            time.sleep(0.3)
                        except:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Sensor": s_label, "VRP_MW": vrp,
                    "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S"),
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M")
                })

            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # Guardado de CSV (asegurando persistencia de datos previos)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            pd.concat([df_antiguo, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
