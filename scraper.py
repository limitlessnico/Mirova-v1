import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import shutil
from urllib.parse import urlparse
import pytz

# --- CONFIGURACIÓN DE LOS 8 VOLCANES CHILENOS ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355101": {"nombre": "Lastarria", "limite_km": 3.0},
    "355030": {"nombre": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "limite_km": 5.0},
    "357110": {"nombre": "Llaima", "limite_km": 5.0},
    "357070": {"nombre": "Nevados de Chillan", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "limite_km": 4.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "limite_km": 20.0},
    "358030": {"nombre": "Chaiten", "limite_km": 5.0},
    "357040": {"nombre": "Peteroa", "limite_km": 3.0}
}

URL_LATEST = "https://www.mirovaweb.it/NRT/latest.php"
BASE_URL = "https://www.mirovaweb.it"

CARPETA_PRINCIPAL = "monitoreo_satelital"
NOMBRE_CARPETA_IMAGENES = "imagenes_satelitales"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, NOMBRE_CARPETA_IMAGENES)

# BASES DE DATOS
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv") 
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
DB_HD = os.path.join(CARPETA_PRINCIPAL, "registro_hd_msi_oli.csv")

CARPETA_OBSOLETA = "monitoreo_datos"

# COLUMNAS
COLUMNAS_MASTER = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Chile", "Volcan", "Sensor", 
    "VRP_MW", "Distancia_km", "Clasificacion", "Fecha_Proceso", "Ruta_Fotos", "Tipo_Registro"
]
COLUMNAS_REPORTE = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Chile", "Volcan", "Sensor", 
    "VRP_MW", "Distancia_km", "Fecha_Proceso", "Ruta_Fotos", "Tipo_Registro"
]
COLUMNAS_HD = [
    "Fecha_Detectada", "Volcan", "Sensor", "Tipo_Imagen", "Ruta_Foto", "Fecha_Proceso"
]

def obtener_hora_chile_actual():
    try: return datetime.now(pytz.timezone('America/Santiago'))
    except: return datetime.now(pytz.utc)

def convertir_utc_a_chile(dt_obj_utc):
    try:
        dt_utc = dt_obj_utc.replace(tzinfo=pytz.utc)
        return dt_utc.astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    except: return dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")

def modo_nuclear_borrar_todo():
    """ 
    ☢️ MODO NUCLEAR: Borra todo de forma segura.
    """
    print("☢️  MODO NUCLEAR ACTIVADO: Limpiando historial...")
    
    if os.path.exists(CARPETA_OBSOLETA):
        try: 
            shutil.rmtree(CARPETA_OBSOLETA)
        except: 
            pass

    if os.path.exists(CARPETA_PRINCIPAL):
        try: 
            shutil.rmtree(CARPETA_PRINCIPAL)
            print("✅ Historial eliminado.") 
        except: 
            pass

def mapear_url_sensor(nombre_sensor_web):
    s = nombre_sensor_web.upper().strip()
    if "VIIRS375" in s: return "VIR375"
    if "VIIRS" in s and "375" not in s: return "VIR" 
    if "MODIS" in s: return "MOD"
    return "MOD" 

# --- DESCARGA NORMAL (MODIS/VIIRS) ---
def descargar_fotos_vrp(session, id_volcan, nombre_volcan, sensor_web, fecha_utc_dt):
    suffix = mapear_url_sensor(sensor_web)
    url_detalle = f"{BASE_URL}/NRT/volcanoDetails_{suffix}.php?volcano_id={id_volcan}"
    rutas_guardadas = "No descargadas"
    
    try:
        res = session.get(url_detalle, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        mapa_fotos = {"Latest": None, "Dist": None, "VRP": None}
        
        tags = soup.find_all(['img', 'a'])
        for tag in tags:
            src = tag.get('src') or tag.get('href')
            if not src: continue
            if src.startswith('http'): full = src
            else: full = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
            low = src.lower()
            if "latest10nti" in low: mapa_fotos["Latest"] = full
            elif "_dist" in low: mapa_fotos["Dist"] = full
            elif "_vrp" in low: mapa_fotos["VRP"] = full

        fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
        hora_archivo = fecha_utc_dt.strftime("%H-%M-%S")
        ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
        os.makedirs(ruta_dia, exist_ok=True)
        
        archivos_bajados = []
        prefijo = f"{hora_archivo}_{nombre_volcan}_{sensor_web}_"
        
        for tipo, url in mapa_fotos.items():
            if url:
                nombre_archivo = f"{prefijo}{tipo}.png"
                ruta_final = os.path.join(ruta_dia, nombre_archivo)
                if not os.path.exists(ruta_final):
                    r_img = session.get(url, timeout=20)
                    if r_img.status_code == 200:
                        with open(ruta_final, 'wb') as f: f.write(r_img.content)
                        archivos_bajados.append(ruta_final)
