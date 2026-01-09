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
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIRS", "VIRS375"]:
            if modo == "MOD": s_label = "MODIS"
            elif modo == "VIRS": s_label = "VIIRS"
            else: s_label = "VIIRS-375m"
            
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

                # Carpeta específica
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
                os.makedirs(ruta_final, exist_ok=True)
                
                # Archivo marcador para forzar commit
                with open(os.path.join(ruta_final, "leeme.txt"), "w") as f:
                    f.write(f"Captura {s_label} - {ahora.strftime('%H:%M:%S')}")

                # --- DESCARGA MEJORADA ---
                imgs = soup.find_all('img')
                for i, img in enumerate(imgs):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        # Construcción robusta de la URL
                        img_url = src if src.startswith('http') else f"https://www.mirovaweb.it/{src.lstrip('/')}"
                        
                        try:
                            img_res = requests.get(img_url, headers=headers, timeout=15)
                            if img_res.status_code == 200:
                                nombre_archivo = f"{s_label}_img_{i}.png"
                                with open(os.path.join(ruta_final, nombre_archivo), 'wb') as f:
                                    f.write(img_res.content)
                                time.sleep(0.5) # Pausa técnica para no ser bloqueado
                        except:
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S"),
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M")
                })

            except Exception as e:
                print(f"Error: {e}")

    # Guardado de CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            pd.concat([df_antiguo, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
