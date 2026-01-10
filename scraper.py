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
    MODO PRUEBAS: Borra todo lo antiguo para verificar la nueva estructura.
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
        except Exception as e: 
            print(f"   ⚠️ No se pudo borrar carpeta principal: {e}")

def obtener_datos_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: 
        return datetime.now(pytz.utc)

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
    mapa = {
        "MOD": "MODIS", 
        "VIR": "VIIRS-750m", 
        "VIR375": "VIIRS-375m", 
        "MIR": "MIR-Combined"
    }
    return mapa.get(codigo, codigo)

def procesar():
    # --- PASO 1: LIMPIEZA TOTAL ---
    limpiar_todo()

    # Creamos la carpeta fresca
    if not os.path.exists(CARPETA_PRINCIPAL):
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 
        'Referer': BASE_URL
    })

    ahora_cl = obtener_datos_chile()
    fecha_ejecucion = ahora_cl.strftime("%Y-%m-%d")
    hora_ejecucion = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando Test: {fecha_ejecucion} {hora_ejecucion}")
    
    registros_nuevos = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(random.uniform(1, 2))
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # 1. Obtener Datos Web
                fecha_web, hora_web = obtener_info_web(soup)

                # Definir valores para procesar
                if fecha_web:
                    carpeta_fecha = fecha_web
                    hora_web_final = hora_web
                else:
                    carpeta_fecha = fecha_ejecucion
                    hora_web_final = "No_Detectado"

                print(f"      ✨ Datos: {nombre_v} {s_label} -> {hora_web_final}")

                # 2. Carpeta del Día (Única por fecha satélite)
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, carpeta_fecha)
                os.makedirs(ruta_carpeta, exist_ok=True)

                # Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 3. Descarga con PREFIJO DE HORA
                descargas = 0
                tags = soup.find_all(['img', 'a'])
                
                # Prefijo: "14-30-00_" para que no se sobrescriban
                if hora_web:
                    prefijo_hora = hora_web.replace(":", "-") + "_"
                else:
                    prefijo_hora = hora_ejecucion.replace(":", "-") + "_Sys_"

                # --- LISTA SEGURA (Vertical para evitar errores de copia) ---
                palabras_clave = [
                    'Latest', 'VRP', 'Dist', 'log', 
                    'Time', 'Map', 'Trend', 'Energy'
                ]
                ext_validas = ['.jpg', '.jpeg', '.png']
