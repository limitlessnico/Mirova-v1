import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import pytz
import time

# --- CONFIGURACIÓN ESTRICTA DE LÍMITES ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "limite_km": 3.0}, # Límite estricto 3km
    "355030": {"nombre": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "limite_km": 5.0},
    "357110": {"nombre": "Llaima", "limite_km": 5.0},
    "357070": {"nombre": "Nevados de Chillan", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "limite_km": 4.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "limite_km": 20.0},
    "358041": {"nombre": "Chaiten", "limite_km": 5.0},
    "357040": {"nombre": "Peteroa", "limite_km": 3.0}
}

CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

def log(mensaje):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(f"[{ahora}] {mensaje}\n")
    print(f"[{ahora}] {mensaje}")

def descargar_v95(session, id_v, nombre_v, dt_utc, sensor_tabla, es_alerta):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    # El mapeo que sabemos que Mirova acepta para las imágenes
    sensor_map = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_web = sensor_map.get(sensor_tabla, sensor_tabla)
    
    # Si es alerta real bajamos el set completo (4 fotos), si no solo la Latest
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta else ["Latest"]
    ruta_csv = "No descargada"

    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        filename = f"{h_a}_{nombre_v}_{s_label}_{t}.png"
        path_file = os.path.join(ruta_dia, filename)
        
        # URL de descarga directa
        url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_web}&type={t}"
        try:
            r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=25)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_file, 'wb') as f: f.write(r.content)
                log(f"✅ Descarga: {nombre_v} {t}")
                if t in ["VRP", "Latest"]: ruta_csv = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
        except: continue
    return ruta_csv

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    log("🚀 INICIO V95.0")

    try:
        # 1. Leer CSV para mantener estructura exacta
        df_master = pd.read_csv(DB_MASTER)
        columnas = df_master.columns.tolist()

        # 2. Scraping tabla Mirova
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        nuevos_registros = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp, dist, sensor = float(cols
