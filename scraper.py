import requests
from bs4 import BeautifulSoup
import os
import pandas as pd

VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
} # Probamos con 3 para asegurar que no falle por tiempo

BASE_URL = "https://www.mirovaweb.it/NRT/"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0'}
    for vid, nombre_v in VOLCANES.items():
        print(f"Intentando con {nombre_v}...")
        url = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            res = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Buscamos TODAS las etiquetas <img> sin filtros
            imgs = soup.find_all('img')
            print(f"Encontradas {len(imgs)} imágenes en {nombre_v}")
            
            ruta = os.path.join("imagenes", nombre_v)
            if not os.path.exists(ruta): os.makedirs(ruta)
            
            for i, img in enumerate(imgs):
                src = img.get('src')
                if not src: continue
                
                # Construir URL completa
                img_url = src if src.startswith('http') else BASE_URL + src
                
                # Descargar
                img_data = requests.get(img_url, headers=headers).content
                ext = src.split('.')[-1][:3] # saca png, jpg, etc
                with open(os.path.join(ruta, f"archivo_{i}.{ext}"), 'wb') as f:
                    f.write(img_data)
                    
        except Exception as e:
            print(f"Error en {nombre_v}: {e}")

if __name__ == "__main__":
    procesar()
