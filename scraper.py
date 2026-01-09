import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time

# Lista de volcanes
VOLCANES = {
    "355100": "Lascar", "357120": "Villarrica", "357110": "Llaima",
    "357060": "Nevados_de_Chillan", "357080": "Copahue", "357150": "Puyehue_Cordon_Caulle",
    "358020": "Calbuco", "357040": "Planchon_Peteroa", "358010": "Osorno", "358050": "Hudson"
}

BASE_URL = "https://www.mirovaweb.it/NRT/"
SENSORES = {"MODIS": "volcanoDetails_MOD.php", "VIIRS": "volcanoDetails_VIR.php"}

def descargar_imagen(url, ruta, nombre):
    try:
        if not os.path.exists(ruta): os.makedirs(ruta)
        img_data = requests.get(url, timeout=15).content
        with open(os.path.join(ruta, nombre), 'wb') as f:
            f.write(img_data)
        return True
    except: return False

def procesar():
    resultados = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for vid, nombre_v in VOLCANES.items():
        print(f"Revisando {nombre_v}...")
        for s_nom, s_pag in SENSORES.items():
            try:
                url = f"{BASE_URL}{s_pag}?volcano_id={vid}"
                res = requests.get(url, headers=headers, timeout=20)
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Buscamos todas las imágenes de mapas y gráficos térmicos
                # Buscamos por carpetas comunes en Mirova: 'temp', 'map', 'last'
                imgs = soup.find_all('img', src=lambda s: s and any(x in s.lower() for x in ['temp', 'map', 'graph', 'last']))
                
                if imgs:
                    ruta = f"imagenes/{nombre_v}/{s_nom}"
                    for i, img in enumerate(imgs[:3]): # Bajamos las 3 más importantes
                        img_url = BASE_URL + img['src']
                        descargar_imagen(img_url, ruta, f"captura_{i}.png")
                    
                    resultados.append({'Volcan': nombre_v, 'Sensor': s_nom, 'Estado': 'Imagen capturada'})
                
                time.sleep(1) # Pausa suave para no saturar
            except: continue

    if resultados:
        pd.DataFrame(resultados).to_csv("registro_vrp.csv", index=False)
        print("¡Proceso completado exitosamente!")

if __name__ == "__main__":
    procesar()
