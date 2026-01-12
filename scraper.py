import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import pytz
import time

# --- CONFIGURACIÓN ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "limite_km": 3.0},
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

# Definición de columnas estándar para todos los archivos
COLUMNAS_ESTANDAR = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", 
    "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", 
    "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"
]

def log_debug(mensaje, tipo="INFO"):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    prefijo = {"INFO": "🔵", "EXITO": "✅", "ERROR": "❌", "ADVERTENCIA": "⚠️"}.get(tipo, "⚪")
    linea = f"[{ahora}] {prefijo} {mensaje}"
    print(linea)
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

def descargar_v104(session, nombre_v, dt_utc, sensor_tabla, es_alerta_real):
    """Mantiene la lógica original de descarga de imágenes sin cambios"""
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    s_url = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
    nombre_url = nombre_v.replace(" ", "_").replace("-", "_")
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta_real else ["Latest"]
    ruta_relativa = "No descargada"

    for t in tipos:
        t_url = f"{t}10NTI" if t == "Latest" else t
        url = f"https://www.mirovaweb
