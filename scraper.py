import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import shutil
from datetime import datetime

# --- CONFIGURACIÓN ---
VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"
IMG_FOLDER = "imagenes"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    # 1. LIMPIEZA INICIAL (Borra todo lo anterior para asegurar que lo nuevo es real)
    if os.path.exists(IMG_FOLDER):
        shutil.rmtree(IMG_FOLDER)
        print(f"Directorio {IMG_FOLDER} limpiado.")
    os.makedirs(IMG_FOLDER)

    nuevos_datos = []

    for vid, nombre_v in VOLCANES.items():
        print(f"\n>>> TRABAJANDO EN: {nombre_v} (ID: {vid})")
        url_detalles = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            # 2. DESCARGA DE IMÁGENES
            res = requests.get(url_detalles, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Filtramos imágenes que realmente son térmicas o mapas
            imgs = soup.find_all('img', src=lambda s: s and any(x in s.lower() for x in ['temp', 'map', 'last', 'output']))
            
            ruta_v = os.path.join(IMG_FOLDER, nombre_v)
            if not os.path.exists(ruta_v): os.makedirs(ruta_v)
            
            for i, img in enumerate(imgs):
                src = img.get('src')
                img_url = src if src.startswith('http') else BASE_URL + src
                
                img_data = requests.get(img_url, headers=headers, timeout=15).content

