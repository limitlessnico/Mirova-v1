import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# --- CONFIGURACIÓN DE VOLCANES ---
VOLCANES = {
    "355100": "Lascar", "357120": "Villarrica", "357110": "Llaima",
    "357060": "Nevados_de_Chillan", "357080": "Copahue", "357150": "Puyehue_Cordon_Caulle",
    "358020": "Calbuco", "357040": "Planchon_Peteroa", "358010": "Osorno", "358050": "Hudson"
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
        if not os.path.exists(ruta_carpeta): os.makedirs(ruta_carpeta)
        path_completo = os.path.join(ruta_carpeta, nombre_archivo)
        if os.path.exists(path_completo): return 
        
        img_data = requests.get(url, timeout=15).content
        with open(path_completo, 'wb') as f:
            f.write(img_data)
        print(f"Descargada: {nombre_archivo}")
    except: pass

def procesar_sensor(volcan_id, nombre_volcan, nombre_sensor, pagina):
    url_completa = f"{BASE_URL}{pagina}?volcano_id={volcan_id}"
    try:
        res = requests.get(url_completa, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        link_data = soup.find('a', href=lambda h: h and "get_data.php" in h)
        if not link_data: return None
        
        df_data = pd.read_csv(BASE_URL + link_data['href'], sep=None, engine='python')
        if df_data.empty: return None
        
        ultima = df_data.iloc[-1]
        vrp_val = float(ultima['VRP(MW)'])
        
        # --- CAMBIO PARA PRUEBA: Quitamos el filtro de VRP > 0 temporalmente ---
        # El sistema guardará la última imagen disponible aunque el VRP sea 0
        
        fecha_id = str(ultima['DATE']).replace("/", "-")
        hora_id = str(ultima['TIME']).replace(":", "-")
        ruta_destino = os.path.join(IMG_BASE_FOLDER, nombre_volcan, fecha_id, nombre_sensor)
        
        # Seleccionamos las imágenes reales de los mapas y gráficos
        img_tags = soup.find_all('img', src=lambda s: s and any(x in s for x in ["temp", "map", "graph", "comp"]))
        
        for i, img in enumerate(img_tags):
            img_url = BASE_URL + img['src']
            ext = img['src'].split('.')[-1]
            nombre_foto = f"toma_{hora_id}_{i}.{ext}"
            descargar_imagen(img_url, ruta_destino, nombre_foto)
            
        return {
            'ID': f"{volcan_id}_{nombre_sensor}_{fecha_id}_{hora_id}",
            'Volcan': nombre_volcan, 'Sensor': nombre_sensor,
            'Fecha': ultima['DATE'], 'Hora': ultima['TIME'], 'VRP_MW': vrp_val
        }
    except Exception as e:
        print(f"Error: {e}")
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
            # Limpiamos datos de prueba anteriores
            df_actual = df_actual[df_actual['Volcan'] != 'Villarrica'] if 'Estado' in df_actual.columns else df_actual
            df_final = pd.concat([df_actual, df_nuevos]).drop_duplicates(subset=['ID'])
        else: df_final = df_nuevos
        df_final.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    ejecutar_total()
