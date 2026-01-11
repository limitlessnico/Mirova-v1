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
# IDs que nos interesan (Deben coincidir con lo que salga en la tabla)
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

def obtener_hora_chile():
    try:
        tz_chile = pytz.timezone('Chile/Continental')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def limpiar_basura():
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA)
        except: pass
    # En V19 NO borramos la carpeta principal para mantener historial

def mapear_url_sensor(nombre_sensor_web):
    """
    Convierte el nombre del sensor en la tabla (ej: 'VIIRS375')
    al sufijo que usa la URL de detalle (ej: 'VIR375').
    """
    s = nombre_sensor_web.upper().strip()
    if "VIIRS375" in s: return "VIR375"
    if "VIIRS" in s and "375" not in s: return "VIR" # Asumimos VIIRS 750
    if "MODIS" in s: return "MOD"
    return "MOD" # Default

def descargar_fotos(session, id_volcan, nombre_volcan, sensor_web, fecha_utc_dt):
    """
    Va a la página de detalle del volcán y baja las 4 fotos correspondientes
    a la fecha detectada.
    """
    suffix = mapear_url_sensor(sensor_web)
    url_detalle = f"{BASE_URL}/NRT/volcanoDetails_{suffix}.php?volcano_id={id_volcan}"
    
    rutas_guardadas = "No descargadas"
    
    try:
        res = session.get(url_detalle, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Mapa de URLs de imágenes
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

        # Crear carpeta por día
        fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
        hora_archivo = fecha_utc_dt.strftime("%H-%M-%S")
        
        ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
        os.makedirs(ruta_dia, exist_ok=True)
        
        # Descargar
        archivos_bajados = []
        prefijo = f"{hora_archivo}_{nombre_volcan}_{sensor_web}_"
        
        for tipo, url in mapa_fotos.items():
            if url:
                nombre_archivo = f"{prefijo}{tipo}.png"
                ruta_final = os.path.join(ruta_dia, nombre_archivo)
                
                # Solo bajamos si no existe (ahorrar ancho de banda)
                if not os.path.exists(ruta_final):
                    r_img = session.get(url, timeout=20)
                    if r_img.status_code == 200:
                        with open(ruta_final, 'wb') as f: f.write(r_img.content)
                        archivos_bajados.append(ruta_final)
                else:
                    archivos_bajados.append(ruta_final) # Ya existía
        
        if archivos_bajados:
            rutas_guardadas = archivos_bajados[0] # Referencia a la primera (Latest)

    except Exception as e:
        print(f"⚠️ Error descargando fotos {nombre_volcan}: {e}")

    return rutas_guardadas

def procesar():
    limpiar_basura()
    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    ahora_cl = obtener_hora_chile()
    print(f"🚀 Iniciando V19.0 (Sin OCR - Modo Tabla Maestra): {ahora_cl}")

    # --- PASO 1: LEER LA TABLA GIGANTE ---
    print(f"🕵️  Consultando {URL_LATEST} ...")
    res = session.get(URL_LATEST, timeout=30)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # Buscamos la tabla (usualmente tiene id="example" en Datatables)
    tabla = soup.find('table', {'id': 'example'})
    if not tabla: tabla = soup.find('table') # Fallback
    
    registros_nuevos = []
    ids_procesados = set() # Para evitar duplicados en esta corrida
    
    if tabla:
        # IMPORTANTE: Saltamos el header (thead) y vamos al body (tbody)
        tbody = tabla.find('tbody')
        filas = tbody.find_all('tr') if tbody else tabla.find_all('tr')[1:]
        
        print(f"📊 Filas encontradas en la tabla: {len(filas)}") 
        # SI ESTE NUMERO ES > 500, ¡TRIUNFAMOS! ES CLIENT-SIDE.
        # SI ES 5, TENDREMOS QUE USAR OTRA ESTRATEGIA.

        # --- LEER HISTORIAL PARA NO DUPLICAR ---
        db_keys = set()
        if os.path.exists(DB_FILE):
            try:
                df_old = pd.read_csv(DB_FILE)
                for _, row in df_old.iterrows():
                    # Clave única: Fecha + Volcan + Sensor
                    k = f"{row['Fecha_Satelite']}_{row['Volcan']}_{row['Sensor']}"
                    db_keys.add(k)
            except: pass

        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue # Filas vacías o erróneas
            
            try:
                # Extraer datos TEXTUALES (Cero OCR)
                # Estructura: Time | ID | Volcano | VRP | Dist | Sensor
                hora_str = cols[0].text.strip()
                id_volc = cols[1].text.strip()
                # nombre_volc = cols[2].text.strip() # No usamos el de la web, usamos el nuestro limpio
                vrp_str = cols[3].text.strip()
                dist_str = cols[4].text.strip()
                sensor_str = cols[5].text.strip()
                
                # FILTRO 1: ¿Es uno de mis volcanes?
                if id_volc not in MIS_VOLCANES: continue
                
                nombre_limpio = MIS_VOLCANES[id_volc]
                
                # Parsear fecha
                # Formato esperado: 11-Jan-2026 00:35:00
                dt_obj = datetime.strptime(hora_str, "%d-%b-%Y %H:%M:%S")
                unix_time = int(dt_obj.timestamp())
                fecha_fmt = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                
                # Crear clave única
                clave = f"{fecha_fmt}_{nombre_limpio}_{sensor_str}"
                
                # FILTRO 2: ¿Ya lo guardé antes?
                if clave in db_keys: 
                    # print(f"  ⏭️ {clave} ya existe.")
                    continue
                
                if clave in ids_procesados: continue # Evitar duplicados en la misma tabla
                ids_procesados.add(clave)
                
                # Parsear números
                vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                dist_val = float(dist_str) if dist_str.replace('.','').isdigit() else 0.0
                
                # CLASIFICACIÓN V19
                clasificacion = "NORMAL"
                if vrp_val > 0:
                    if dist_val <= 5.0: clasificacion = "ALERTA VOLCANICA"
                    else: clasificacion = "FALSO POSITIVO"
                
                print(f"🔥 NUEVO: {nombre_limpio} ({sensor_str}) | {fecha_fmt} | VRP:{vrp_val} | Dist:{dist_val} | {clasificacion}")
                
                # --- PASO 2: BAJAR FOTOS ---
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

            except Exception as e:
                # print(f"Error parseando fila: {e}")
                continue

    # --- GUARDAR EN CSV ---
    if registros_nuevos:
        df_new = pd.DataFrame(registros_nuevos)
        cols = ["timestamp", "Fecha_Satelite", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Clasificacion", "Fecha_Proceso", "Ruta_Fotos"]
        df_new = df_new.reindex(columns=cols)
        
        if os.path.exists(DB_FILE):
            df_new.to_csv(DB_FILE, mode='a', header=False, index=False)
        else:
            df_new.to_csv(DB_FILE, index=False)
            
        print(f"💾 Guardados {len(registros_nuevos)} nuevos eventos.")
        
        # Actualizar CSVs individuales
        df_full = pd.read_csv(DB_FILE)
        for v in df_full['Volcan'].unique():
            df_v = df_full[df_full['Volcan'] == v]
            r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
            os.makedirs(os.path.dirname(r), exist_ok=True)
            df_v.to_csv(r, index=False)
    else:
        print("💤 No hay datos nuevos.")

if __name__ == "__main__":
    procesar()
