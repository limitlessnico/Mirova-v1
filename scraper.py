import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random
from urllib.parse import urlparse
import re
import pytz
import shutil

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"

# --- CONFIGURACIÓN ---
CARPETA_PRINCIPAL = "monitoreo_datos"
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp.csv")

def limpiar_todo():
    """ 
    MODO PRUEBAS: Borra todo lo antiguo para verificar la nueva estructura limpia.
    """
    print("🧹 LIMPIEZA INICIAL ACTIVADA...")
    # 1. Borrar basura del raíz (Legacy)
    if os.path.exists("registro_vrp.csv"): 
        try: os.remove("registro_vrp.csv")
        except: pass
    if os.path.exists("imagenes"): 
        try: shutil.rmtree("imagenes")
        except: pass
    
    # 2. Borrar carpeta de resultados actual
    if os.path.exists(CARPETA_PRINCIPAL):
        try:
            shutil.rmtree(CARPETA_PRINCIPAL)
            print(f"   ✨ Carpeta '{CARPETA_PRINCIPAL}' eliminada y reiniciada.")
        except Exception as e: print(f"   ⚠️ No se pudo borrar carpeta principal: {e}")

def obtener_datos_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def obtener_info_web(soup):
    """ Retorna la FECHA y HORA que muestra la web """
    try:
        texto_pagina = soup.get_text()
        patron = r"Last Update\s*:?\s*(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2})"
        match = re.search(patron, texto_pagina, re.IGNORECASE)
        if match:
            fecha_str = match.group(1)
            fecha_obj = datetime.strptime(fecha_str, "%d-%b-%Y %H:%M:%S")
            return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H:%M:%S")
    except: pass
    return None, None

def obtener_etiqueta_sensor(codigo):
    mapa = {"MOD": "MODIS", "VIR": "VIIRS-750m", "VIR375": "VIIRS-375m", "MIR": "MIR-Combined"}
    return mapa.get(codigo, codigo)

def procesar():
    # --- PASO 1: LIMPIEZA TOTAL ---
    limpiar_todo()

    # Creamos la carpeta fresca
    if not os.path.exists(CARPETA
