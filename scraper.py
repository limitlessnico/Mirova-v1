import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURACIÓN DE VOLCANES ---
VOLCANES = {
    "355100": "Lascar",
    "357120": "Villarrica",
    "357110": "Llaima",
    "357060": "Nevados_de_Chillan",
    "357080": "Copahue",
    "357150": "Puyehue_Cordon_Caulle",
    "358020": "Calbuco",
    "357040": "Planchon_Peteroa",
    "358010": "Osorno",
    "358050": "Hudson"
}

BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"
IMG_BASE_FOLDER = "imagenes"

SENSORES = {
    "MODIS": "volcanoDetails_MOD.php",
    "VIIRS750": "volcanoDetails_VIR.php",
    "VIIRS375": "volcanoDetails_VIR375.php",
    "CombinedMIR": "volcanoDetails_MIR.php"
}

def descargar_imagen(url, ruta_carpeta, nombre_archivo):
    try:
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)
        
        path_completo = os.path.join(ruta_carpeta, nombre_archivo)
        if os.path.exists(path_completo): return 
        
        img_data = requests.get(url, timeout=10).content
        with open(path_completo, 'wb') as f:
            f.write(img_data)
        print(f"Guardada: {nombre_archivo} en {ruta_carpeta}")
    except Exception as e:
        print(f"Error descargando imagen: {e}")

def procesar_sensor(volcan_id, nombre_volcan, nombre_sensor, pagina):
    url_completa = f"{BASE_URL}{pagina}?volcano_id={volcan_id}"
    try:
        res = requests.get(url_completa, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        link_data = soup.find('a', href=lambda h: h and "get_data.php" in h)
        if not link_data: return None
        
        df_data = pd.read_csv(BASE_URL + link_data['href'], sep=None, engine='python')
        if df_data.empty: return None
        
        ultima = df_data.iloc[-1]
        fecha_original = str(ultima['DATE']) 
        fecha_carpeta = fecha_original.replace("/", "-") 
        hora_id = str(ultima['TIME']).replace(":", "-")
        
        # --- ESTRUCTURA ACTUALIZADA ---
        # imagenes / Villarrica / 2024-05-24 / MODIS
        ruta_destino = os.path.join(IMG_BASE_FOLDER, nombre_volcan, fecha_carpeta, nombre_sensor)
        
        img_tags = soup.find_all('img', src=lambda s: s and ("temp" in s or "map" in s or "graph" in s or "comp" in s))
        
        for i, img in enumerate(img_tags):
            img_url = BASE_URL + img['src']
            ext = img['src'].split('.')[-1]
            nombre_foto = f"toma_{hora_id}_{i}.{ext}"
            descargar_imagen(img_url, ruta_destino, nombre_foto)
            
        return {
            'ID': f"{volcan_id}_{nombre_sensor}_{fecha_carpeta}_{hora_id}",
            'Volcan': nombre_volcan,
            'Volcan_ID': volcan_id,
            'Sensor': nombre_sensor,
            'Fecha': fecha_original,
            'Hora': ultima['TIME'],
            'VRP_MW': ultima['VRP(MW)']
        }
    except Exception as e:
        print(f"Error en {nombre_sensor} ({nombre_volcan}): {e}")
        return None

def ejecutar_total():
    resultados = []
    for vid, nombre in VOLCANES.items():
        for sensor, pagina in SENSORES.items():
            res = procesar_sensor(vid, nombre, sensor, pagina)
            if res: resultados.append(res)
            
    if resultados:
        df_nuevos = pd.DataFrame(resultados)
        if os.path.exists(DB_FILE):
            df_actual = pd.read_csv(DB_FILE)
            df_final = pd.concat([df_actual, df_nuevos]).drop_duplicates(subset=['ID'])
        else:
            df_final = df_nuevos
        df_final.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    ejecutar_total()
