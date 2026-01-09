import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# --- CONFIGURACIÓN ---
VOLCANES = {"355100": "Lascar", "357120": "Villarrica"} # Solo dos para probar rápido
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"
IMG_BASE_FOLDER = "imagenes"

def ejecutar_prueba_forzada():
    if not os.path.exists(IMG_BASE_FOLDER):
        os.makedirs(IMG_BASE_FOLDER)
    
    resultados = []
    
    for vid, nombre in VOLCANES.items():
        url = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Buscamos cualquier imagen para probar el guardado
            img = soup.find('img', src=lambda s: s and "temp" in s)
            if img:
                ruta_volcan = os.path.join(IMG_BASE_FOLDER, nombre)
                if not os.path.exists(ruta_volcan): os.makedirs(ruta_volcan)
                
                img_data = requests.get(BASE_URL + img['src']).content
                with open(os.path.join(ruta_volcan, "test.png"), 'wb') as f:
                    f.write(img_data)
                
                resultados.append({
                    'ID': f"TEST_{vid}",
                    'Volcan': nombre,
                    'Estado': 'Captura exitosa'
                })
        except:
            continue

    if resultados:
        pd.DataFrame(resultados).to_csv(DB_FILE, index=False)
        print("Archivos creados localmente. Listos para el push.")

if __name__ == "__main__":
    ejecutar_prueba_forzada()
