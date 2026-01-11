import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import shutil
from urllib.parse import urlparse
import pytz

# --- CONFIGURACIÓN ---
MIS_VOLCANES = {
    "355100": "Lascar",
    "357120": "Villarrica",
    "357110": "Llaima"
}

URL_LATEST = "https://www.mirovaweb.it/NRT/latest.php"
BASE_URL = "https://www.mirovaweb.it"

CARPETA_PRINCIPAL = "monitoreo_satelital"
NOMBRE_CARPETA_IMAGENES = "imagenes_satelitales"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, NOMBRE_CARPETA_IMAGENES)
DB_FILE = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
CARPETA_OBSOLETA = "monitoreo_datos"

# COLUMNAS OFICIALES
COLUMNAS_OFICIALES = [
    "timestamp", 
    "Fecha_Satelite", 
    "Volcan", 
    "Sensor", 
    "VRP_MW", 
    "Distancia_km",   # Nueva
    "Clasificacion",  # Nueva
    "Fecha_Proceso", 
    "Ruta_Fotos"
]

def obtener_hora_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def modo_nuclear_borrar_todo():
    """
    ⚠️ PELIGRO: Borra todo para reiniciar la base de datos limpia.
    """
    print("☢️  MODO NUCLEAR ACTIVADO: Borrando historial corrupto...")
    
    # Borrar carpeta obsoleta
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA)
        except: pass

    # Borrar carpeta principal (CSVs y Fotos)
    if os.path.exists(CARPETA_PRINCIPAL):
        try: 
            shutil.rmtree(CARPETA_PRINCIPAL)
            print("✅ Carpeta 'monitoreo_satelital' eliminada.")
        except Exception as e: 
            print(f"⚠️ No se pudo borrar carpeta: {e}")

def mapear_url_sensor(nombre_sensor_web):
    s = nombre_sensor_web.upper().strip()
    if "VIIRS375" in s: return "VIR375"
    if "VIIRS" in s and "375" not in s: return "VIR" 
    if "MODIS" in s: return "MOD"
    return "MOD" 

def descargar_fotos(session, id_volcan, nombre_volcan, sensor_web, fecha_utc_dt):
    suffix = mapear_url_sensor(sensor_web)
    url_detalle = f"{BASE_URL}/NRT/volcanoDetails_{suffix}.php?volcano_id={id_volcan}"
    rutas_guardadas = "No descargadas"
    
    try:
        res = session.get(url_detalle, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        mapa_fotos = {"Latest": None, "Dist": None, "VRP": None, "LogVRP": None}
        
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
            elif "logvrp" in low: mapa_fotos["LogVRP"] = full

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
                else:
                    archivos_bajados.append(ruta_final)
        
        if archivos_bajados:
            rutas_guardadas = archivos_bajados[0]

    except Exception as e:
        print(f"⚠️ Error descargando fotos {nombre_volcan}: {e}")

    return rutas_guardadas

def procesar():
    # 1. EJECUTAR LIMPIEZA COMPLETA (Solicitado por usuario)
    modo_nuclear_borrar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    ahora_cl = obtener_hora_chile()
    print(f"🚀 Iniciando V21.1 (Hard Reset + Columnas Nuevas): {ahora_cl}")
    
    print(f"🕵️  Consultando {URL_LATEST} ...")
    try:
        res = session.get(URL_LATEST, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        tabla = soup.find('table', {'id': 'example'})
        if not tabla: tabla = soup.find('table')
        
        registros_nuevos = []
        
        if tabla:
            tbody = tabla.find('tbody')
            filas = tbody.find_all('tr') if tbody else tabla.find_all('tr')[1:]
            print(f"📊 Filas encontradas: {len(filas)}") 
            
            # Como borramos todo, no hay claves previas
            ids_procesados_hoy = set()

            for fila in filas:
                cols = fila.find_all('td')
                if len(cols) < 6: continue 
                
                try:
                    hora_str = cols[0].text.strip()
                    id_volc = cols[1].text.strip()
                    vrp_str = cols[3].text.strip()
                    dist_str = cols[4].text.strip()
                    sensor_str = cols[5].text.strip()
                    
                    if id_volc not in MIS_VOLCANES: continue
                    
                    nombre_limpio = MIS_VOLCANES[id_volc]
                    dt_obj = datetime.strptime(hora_str, "%d-%b-%Y %H:%M:%S")
                    unix_time = int(dt_obj.timestamp())
                    fecha_fmt = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    
                    clave = f"{fecha_fmt}_{nombre_limpio}_{sensor_str}"
                    
                    if clave in ids_procesados_hoy: continue
                    ids_procesados_hoy.add(clave)
                    
                    vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                    dist_val = float(dist_str) if dist_str.replace('.','').isdigit() else 0.0
                    
                    clasificacion = "NORMAL"
                    if vrp_val > 0:
                        if dist_val <= 5.0: clasificacion = "ALERTA VOLCANICA"
                        else: clasificacion = "FALSO POSITIVO"
                    
                    print(f"🔥 RECUPERANDO: {nombre_limpio} | {fecha_fmt} | Dist:{dist_val}km")
                    
                    rutas = descargar_fotos(session, id_volc, nombre_limpio, sensor_str, dt_obj)
                    
                    registros_nuevos.append({
                        "timestamp": unix_time,
                        "Fecha_Satelite": fecha_fmt,
                        "Volcan": nombre_limpio,
                        "Sensor": sensor_str,
                        "VRP_MW": vrp_val,
                        "Distancia_km": dist_val,
                        "Clasificacion": clasificacion,
                        "Fecha_Proceso": ahora_cl.strftime("%Y-%m-%d %H:%M:%S"),
                        "Ruta_Fotos": rutas
                    })

                except Exception as e: continue

            # --- GUARDADO ---
            if registros_nuevos:
                df_new = pd.DataFrame(registros_nuevos)
                # Ordenar por fecha (opcional)
                df_new = df_new.sort_values(by="timestamp")
                # Asegurar columnas
                df_new = df_new.reindex(columns=COLUMNAS_OFICIALES)
                
                df_new.to_csv(DB_FILE, index=False)
                print(f"💾 CSV Maestro generado desde cero ({len(registros_nuevos)} registros).")
                
                # Generar individuales
                print("🔄 Generando CSVs individuales...")
                for v in df_new['Volcan'].unique():
                    df_v = df_new[df_new['Volcan'] == v]
                    r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
                    os.makedirs(os.path.dirname(r), exist_ok=True)
                    df_v.to_csv(r, index=False)
            else:
                print("💤 No se encontraron datos en la tabla (¿Error de conexión?).")

    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    procesar()
