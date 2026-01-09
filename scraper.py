import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

VOLCANES = {
    "355100": "Lascar", "357120": "Villarrica", "357110": "Llaima",
    "357060": "Nevados_de_Chillan", "357080": "Copahue", "357150": "Puyehue_Cordon_Caulle",
    "358020": "Calbuco", "357040": "Planchon_Peteroa", "358010": "Osorno", "358050": "Hudson"
}

BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"
IMG_BASE_FOLDER = "imagenes"

SENSORES = ["volcanoDetails_MOD.php", "volcanoDetails_VIR.php", "volcanoDetails_VIR375.php", "volcanoDetails_MIR.php"]

def descargar(url, carpeta, nombre):
    try:
        if not os.path.exists(carpeta): os.makedirs(carpeta)
        path = os.path.join(carpeta, nombre)
        if os.path.exists(path): return
        r = requests.get(url, timeout=15)
        with open(path, 'wb') as f: f.write(r.content)
        print(f"OK: {nombre}")
    except: pass

def procesar():
    resultados = []
    for vid, nombre_v in VOLCANES.items():
        print(f"Procesando {nombre_v}...")
        for pag in SENSORES:
            try:
                sensor_nom = pag.split('_')[1].replace('.php', '')
                res = requests.get(f"{BASE_URL}{pag}?volcano_id={vid}", timeout=20)
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Buscamos datos para el CSV
                link_csv = soup.find('a', href=lambda h: h and "get_data.php" in h)
                if link_csv:
                    df = pd.read_csv(BASE_URL + link_csv['href'], sep=None, engine='python')
                    ultima = df.iloc[-1]
                    fecha = str(ultima['DATE']).replace("/", "-")
                    hora = str(ultima['TIME']).replace(":", "-")
                    
                    # Carpeta: imagenes / Volcan / Fecha / Sensor
                    ruta = os.path.join(IMG_BASE_FOLDER, nombre_v, fecha, sensor_nom)
                    
                    # Descargamos TODAS las imágenes de la página
                    imgs = soup.find_all('img', src=lambda s: s and any(x in s for x in ["temp", "map", "graph", "comp"]))
                    for i, img in enumerate(imgs):
                        descargar(BASE_URL + img['src'], ruta, f"img_{hora}_{i}.png")
                    
                    resultados.append({'ID': f"{vid}_{sensor_nom}_{fecha}_{hora}", 'Volcan': nombre_v, 'Fecha': fecha, 'VRP': ultima['VRP(MW)']})
            except: continue
            
    if resultados:
        pd.DataFrame(resultados).to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()

