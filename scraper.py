import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

# Volvemos a la configuración simple que funcionó al inicio
VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    # Usamos headers básicos, como en la primera versión exitosa
    headers = {'User-Agent': 'Mozilla/5.0'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    nuevos_datos = []

    for vid, nombre_v in VOLCANES.items():
        # IMPORTANTE: Solo consultamos la página MOD (la más estable)
        # para recuperar la funcionalidad de descarga primero.
        url = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            res = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Recreamos la ruta que necesitas
            ruta_v = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
            os.makedirs(ruta_v, exist_ok=True)
            
            # Buscamos las imágenes como lo hacíamos cuando funcionaba
            imgs = soup.find_all('img')
            for i, img in enumerate(imgs):
                src = img.get('src')
                if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                    # Unión de URL simple
                    img_url = src if src.startswith('http') else "https://www.mirovaweb.it/" + src.lstrip('../')
                    
                    img_res = requests.get(img_url, headers=headers)
                    if img_res.status_code == 200:
                        with open(os.path.join(ruta_v, f"img_{i}.png"), 'wb') as f:
                            f.write(img_res.content)

            # Extraer dato para el CSV
            vrp = "0"
            for b in soup.find_all('b'):
                if "VRP =" in b.text:
                    vrp = b.text.split('=')[-1].replace('MW', '').strip()
                    break
            
            nuevos_datos.append({
                "Volcan": nombre_v,
                "VRP_MW": vrp,
                "Fecha": fecha_hoy,
                "Hora": ahora.strftime("%H:%M:%S")
            })
            print(f"Éxito con {nombre_v}")

        except Exception as e:
            print(f"Error en {nombre_v}: {e}")

    # Guardado simple del CSV
    if nuevos_datos:
        df = pd.DataFrame(nuevos_datos)
        if os.path.exists(DB_FILE):
            df_old = pd.read_csv(DB_FILE)
            df = pd.concat([df_old, df], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
