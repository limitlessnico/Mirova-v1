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

# --- CONFIGURACIÓN ---
VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
CARPETA_PRINCIPAL = "monitoreo_datos"
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp.csv")

def obtener_hora_chile():
    """Retorna la fecha y hora actual en Chile Continental (para Fallback)."""
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except:
        return datetime.now(pytz.utc)

def limpiar_todo():
    """ 
    MODO PRUEBAS: Borra todo lo anterior para asegurar una prueba limpia.
    """
    print("🧹 LIMPIEZA INICIAL ACTIVADA...")
    if os.path.exists("registro_vrp.csv"): 
        try: os.remove("registro_vrp.csv")
        except: pass
    if os.path.exists("imagenes"): 
        try: shutil.rmtree("imagenes")
        except: pass
    if os.path.exists(CARPETA_PRINCIPAL): 
        try: shutil.rmtree(CARPETA_PRINCIPAL)
        except: pass

def buscar_fecha_en_raw(html_crudo):
    """ 
    Busca directamente patrones de fecha en el texto crudo.
    Soporta: "10-jan-2026 19:55:00" y variaciones.
    """
    try:
        # Patrón flexible:
        # (?i) -> Ignora mayúsculas/minúsculas (detecta Jan, jan, JAN)
        # \d{1,2} -> Día (1 o 2 dígitos)
        # [-\s] -> Separador (puede ser guión o espacio)
        # [a-z]{3} -> Mes (3 letras)
        # [-\s] -> Separador
        # \d{4} -> Año (4 dígitos)
        # \s+ -> Espacio(s)
        # \d{1,2}:\d{2}:\d{2} -> Hora:Min:Seg
        patron = r"(?i)(\d{1,2}[-\s][a-z]{3}[-\s]\d{4}\s+\d{1,2}:\d{2}:\d{2})"
        
        match = re.search(patron, html_crudo)
        if match:
            fecha_str = match.group(1)
            # Intentamos convertirlo a objeto fecha
            # Normalizamos separadores para que strptime funcione bien
            fecha_str_clean = fecha_str.replace(" ", "-")
            
            try:
                # El %b detecta "jan", "Feb", etc.
                fecha_obj = datetime.strptime(fecha_str_clean, "%d-%b-%Y %H:%M:%S")
                return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H:%M:%S")
            except ValueError:
                # Si falla con guiones, probamos formato con espacios por si acaso
                try:
                    fecha_obj = datetime.strptime(fecha_str, "%d %b %Y %H:%M:%S")
                    return fecha_obj.strftime("%Y-%m-%d"), fecha_obj.strftime("%H:%M:%S")
                except:
                    pass
                
    except Exception as e:
        print(f"   ⚠️ Error buscando fecha: {e}")
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
    # 1. Limpieza inicial
    limpiar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL):
        os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 
        'Referer': BASE_URL
    })

    # Datos de respaldo (Chile)
    ahora_cl = obtener_hora_chile()
    fecha_exec = ahora_cl.strftime("%Y-%m-%d")
    hora_exec = ahora_cl.strftime("%H:%M:%S")
    
    print(f"🕒 Iniciando V7.1 (Búsqueda Directa): {fecha_exec} {hora_exec}")
    registros_nuevos = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIR", "VIR375", "MIR"]:
            s_label = obtener_etiqueta_sensor(modo)
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                time.sleep(random.uniform(1, 1.5))
                res = session.get(url_sitio, timeout=30)
                if res.status_code != 200: continue
                
                # Leemos el código fuente DIRECTO (sin filtrar HTML)
                html_crudo = res.text
                
                # --- BUSCADOR DE FECHA ---
                fecha_web, hora_web = buscar_fecha_en_raw(html_crudo)

                if fecha_web and hora_web:
                    origen = "✅ WEB"
                    timestamp_str = f"{fecha_web} {hora_web}"
                    carpeta_fecha = fecha_web
                    hora_final = hora_web
                else:
                    origen = "❌ FALLBACK (Chile)"
                    timestamp_str = f"{fecha_exec} {hora_exec}"
                    carpeta_fecha = fecha_exec
                    hora_final = f"{hora_exec}_Sys"

                print(f"   🔎 {nombre_v} {s_label} -> {timestamp_str} [{origen}]")

                # Parsear resto de datos (VRP, Imágenes)
                soup = BeautifulSoup(html_crudo, 'html.parser')
                ruta_carpeta = os.path.join(CARPETA_PRINCIPAL, "imagenes", nombre_v, carpeta_fecha)
                os.makedirs(ruta_carpeta, exist_ok=True)

                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                descargas = 0
                tags = soup.find_all(['img', 'a'])
                
                # Definir prefijo de archivo según si encontramos fecha real o no
                if origen == "✅ WEB":
                    prefijo = hora_web.replace(":", "-") + "_"
                else:
                    prefijo = hora_exec.replace(":", "-") + "_Sys_"

                palabras_clave = [
                    'Latest', 'VRP', 'Dist', 'log', 
                    'Time', 'Map', 'Trend', 'Energy'
                ]
                ext_validas = ['.jpg', '.jpeg', '.png']

                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    if src.startswith('http'): 
                        img_url = src
                    else: 
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                    
                    path = urlparse(img_url).path
                    nombre_original = os.path.basename(path)
                    
                    if any(k in nombre_original for k in palabras_clave) and \
                       any(nombre_original.lower().endswith(ext) for ext in ext_validas):
                        
                        nombre_final = f"{prefijo}{nombre_original}"
                        ruta_archivo = os.path.join(ruta_carpeta, nombre_final)
                        
                        try:
                            if not os.path.exists(ruta_archivo):
                                img_res = session.get(img_url, timeout=10)
                                if img_res.status_code == 200 and len(img_res.content) > 2000:
                                    with open(ruta_archivo, 'wb') as f: 
                                        f.write(img_res.content)
                                    descargas += 1
                        except: pass

                registros_nuevos.append({
                    "Timestamp": timestamp_str,
                    "Volcan": nombre_v,
                    "Sensor": s_label,
                    "VRP_MW": vrp,
                    "Fecha_Datos_Web": carpeta_fecha,
                    "Hora_Datos_Web": hora_final,
                    "Fecha_Revision": fecha_exec,
                    "Hora_Revision": hora_exec,
                    "Ruta_Fotos": ruta_carpeta if descargas > 0 else "Sin cambios"
                })

            except Exception as e:
                print(f"⚠️ Error en {nombre_v}: {e}")

    # --- GUARDAR CSV ---
    if registros_nuevos:
        cols = [
            "Timestamp", "Volcan", "Sensor", "VRP_MW", 
            "Fecha_Datos_Web", "Hora_Datos_Web", 
            "Fecha_Revision", "Hora_Revision", "Ruta_Fotos"
        ]
        
        df = pd.DataFrame(registros_nuevos)
        df = df.reindex(columns=cols)
        df.to_csv(DB_FILE, index=False)
        print(f"💾 CSV V7.1 Generado: {DB_FILE}")

if __name__ == "__main__":
    procesar()
