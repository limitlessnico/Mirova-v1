import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time

# Listado de volcanes a monitorear
VOLCANES = {
    "355100": "Lascar", "357120": "Villarrica", "357110": "Llaima",
    "357060": "Nevados_de_Chillan", "357080": "Copahue", "357150": "Puyehue_Cordon_Caulle",
    "358020": "Calbuco", "357040": "Planchon_Peteroa", "358010": "Osorno", "358050": "Hudson"
}

BASE_URL = "https://www.mirovaweb.it/NRT/"
IMG_BASE_FOLDER = "imagenes"

def descargar_imagen(url, ruta_carpeta, nombre_archivo):
    try:
        if not os.path.exists(ruta_carpeta): os.makedirs(ruta_carpeta)
        path_completo = os.path.join(ruta_carpeta, nombre_archivo)
        # Si la imagen ya existe, no la bajamos de nuevo para ahorrar tiempo
        if os.path.exists(path_completo): return False
        
        img_data = requests.get(url, timeout=15).content
        with open(path_completo, 'wb') as f:
            f.write(img_data)
        return True
    except: return False

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0'}
    for vid, nombre_v in VOLCANES.items():
        print(f"Procesando {nombre_v}...")
        # Revisamos el sensor MODIS y VIIRS
        for sensor_pag in ["volcanoDetails_MOD.php", "volcanoDetails_VIR.php"]:
            try:
                url = f"{BASE_URL}{sensor_pag}?volcano_id={vid}"
                res = requests.get(url, headers=headers, timeout=20)
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Buscamos las imágenes térmicas (suelen tener 'temp' o 'last' en el nombre)
                imgs = soup.find_all('img', src=lambda s: s and any(x in s for x in ['temp', 'map', 'last']))
                
                for i, img in enumerate(imgs[:2]): # Bajamos las 2 más recientes
                    img_url = BASE_URL + img['src']
                    ruta = os.path.join(IMG_BASE_FOLDER, nombre_v)
                    descargar_imagen(img_url, ruta, f"thermal_{i}_{sensor_pag[:3]}.png")
                
                time.sleep(1) # Pausa para no ser bloqueados
            except: continue

if __name__ == "__main__":
    procesar()

