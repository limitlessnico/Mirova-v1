import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import shutil
from urllib.parse import urlparse
import pytz

# --- CONFIGURACIÓN DE LOS 10 VOLCANES CHILENOS (Límites Calibrados) ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355101": {"nombre": "Lastarria", "limite_km": 3.0},
    "355030": {"nombre": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "limite_km": 5.0}, # Estándar
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

def obtener_hora_chile_actual():
    try: return datetime.now(pytz.timezone('America/Santiago'))
    except: return datetime.now(pytz.utc)

def convertir_utc_a_chile(dt_obj_utc):
    try:
        dt_utc = dt_obj_utc.replace(tzinfo=pytz.utc)
        return dt_utc.astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    except: return dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")

def modo_nuclear_borrar_todo():
    """ Borra todo para regenerar estructura limpia con los nuevos límites """
    print("☢️  MODO NUCLEAR ACTIVADO: Reevaluando historial con nuevos límites km...")
    
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA)
        except: pass
        
    if os.path.exists(CARPETA_PRINCIPAL):
        try: 
            shutil.rmtree(CARPETA_PRINCIPAL)
            print("✅ Historial eliminado correctamente.") 
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

def check_evidencia_existente(nombre_volcan, fecha_utc_dt):
    fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
    if os.path.exists(ruta_dia) and len(os.listdir(ruta_dia)) > 0: return True 
    return False

def procesar():
    # EJECUTAR LIMPIEZA INICIAL (Para aplicar nuevos filtros a todo el historial)
    modo_nuclear_borrar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    ahora_cl_proceso = obtener_hora_chile_actual()
    
    print(f"🚀 Iniciando V25.4 (Filtros Km Personalizados): {ahora_cl_proceso}")
    print(f"🕵️  Consultando {URL_LATEST} ...")
    
    try:
        res = session.get(URL_LATEST, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        tabla = soup.find('table', {'id': 'example'})
        if not tabla: tabla = soup.find('table')
        
        registros_todos = [] # Para el Master
        registros_positivos = [] # Para el Reporte y los Individuales
        
        if tabla:
            tbody = tabla.find('tbody')
            filas = tbody.find_all('tr') if tbody else tabla.find_all('tr')[1:]
            print(f"📊 Filas encontradas: {len(filas)}") 
            
            ids_procesados_hoy = set()

            for fila in filas:
                cols = fila.find_all('td')
                if len(cols) < 6: continue 
                
                try:
                    hora_str_utc = cols[0].text.strip()
                    id_volc = cols[1].text.strip()
                    vrp_str = cols[3].text.strip()
                    dist_str = cols[4].text.strip()
                    sensor_str = cols[5].text.strip()
                    
                    # 1. FILTRO: ¿Es uno de nuestros 10 volcanes?
                    if id_volc not in VOLCANES_CONFIG: continue
                    
                    config = VOLCANES_CONFIG[id_volc]
                    nombre_v = config["nombre"]
                    limite_km = config["limite_km"]
                    
                    # Fechas
                    dt_obj_utc = datetime.strptime(hora_str_utc, "%d-%b-%Y %H:%M:%S")
                    unix_time = int(dt_obj_utc.timestamp())
                    fecha_fmt_utc = dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")
                    fecha_fmt_chile = convertir_utc_a_chile(dt_obj_utc)

                    clave = f"{fecha_fmt_utc}_{nombre_v}_{sensor_str}"
                    if clave in ids_procesados_hoy: continue
                    ids_procesados_hoy.add(clave)
                    
                    vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                    dist_val = float(dist_str) if dist_str.replace('.','').isdigit() else 0.0
                    
                    # --- LÓGICA DE CLASIFICACIÓN ---
                    descargar_ahora = False
                    tipo_registro = "RUTINA"
                    clasificacion = "NORMAL"
                    es_alerta_real = False

                    if vrp_val > 0:
                        if dist_val <= limite_km:
                            # CASO A: ALERTA REAL
                            clasificacion = "ALERTA VOLCANICA"
                            descargar_ahora = True
                            tipo_registro = "ALERTA"
                            es_alerta_real = True
                            print(f"🔥 ALERTA REAL: {nombre_v} | {dist_val}km (Límite: {limite_km}km)")
                        else:
                            # CASO B: FALSO POSITIVO
                            clasificacion = "FALSO POSITIVO (Fuera de limite)"
                            descargar_ahora = False 
                            tipo_registro = "RUTINA"
                            print(f"⚠️  Descartado por Distancia: {nombre_v} a {dist_val}km")
                    else:
                        # CASO C: EVIDENCIA DIARIA (VRP=0)
                        clasificacion = "NORMAL"
                        if "VIIRS" in sensor_str.upper():
                            if not check_evidencia_existente(nombre_v, dt_obj_utc):
                                descargar_ahora = True
                                tipo_registro = "EVIDENCIA_DIARIA"
                                print(f"📸 Evidencia Calma: {nombre_v}")
                    
                    rutas = "No descargadas"
                    if descargar_ahora:
                        rutas = descargar_fotos(session, id_volc, nombre_v, sensor_str, dt_obj_utc)
                    
                    # DATO PARA EL MASTER
                    dato_master = {
                        "timestamp": unix_time,
                        "Fecha_Satelite_UTC": fecha_fmt_utc,
                        "Fecha_Chile": fecha_fmt_chile,
                        "Volcan": nombre_v,
                        "Sensor": sensor_str,
                        "VRP_MW": vrp_val,
                        "Distancia_km": dist_val,
                        "Clasificacion": clasificacion,
                        "Fecha_Proceso": ahora_cl_proceso.strftime("%Y-%m-%d %H:%M:%S"),
                        "Ruta_Fotos": rutas,
                        "Tipo_Registro": tipo_registro
                    }
                    registros_todos.append(dato_master)

                    # DATO PARA POSITIVOS
                    if es_alerta_real:
                        dato_reporte = dato_master.copy()
                        del dato_reporte["Clasificacion"] 
                        registros_positivos.append(dato_reporte)

                except Exception as e: continue

            # --- GUARDADO ---
            
            # 1. MASTER CSV
            if registros_todos:
                df_master = pd.DataFrame(registros_todos)
                df_master = df_master.sort_values(by="timestamp")
                df_master = df_master.reindex(columns=COLUMNAS_MASTER)
                df_master.to_csv(DB_MASTER, index=False)
                print(f"💾 Master regenerado ({len(registros_todos)} registros).")

            # 2. POSITIVOS CSV
            if registros_positivos:
                df_pos = pd.DataFrame(registros_positivos)
                df_pos = df_pos.sort_values(by="timestamp")
                df_pos = df_pos.reindex(columns=COLUMNAS_REPORTE)
                df_pos.to_csv(DB_POSITIVOS, index=False)
                print(f"🔥 Reporte de Alertas generado ({len(registros_positivos)} eventos).")

                # 3. INDIVIDUALES
                print("🔄 Generando CSVs individuales (Solo Alertas)...")
                for v in df_pos['Volcan'].unique():
                    df_v = df_pos[df_pos['Volcan'] == v]
                    r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
                    os.makedirs(os.path.dirname(r), exist_ok=True)
                    df_v.to_csv(r, index=False)
            else:
                pd.DataFrame(columns=COLUMNAS_REPORTE).to_csv(DB_POSITIVOS, index=False)
                print("💤 No se detectaron Alertas Reales activas.")

    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    procesar()
