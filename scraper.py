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
                else:
                    archivos_bajados.append(ruta_final)
        
        if archivos_bajados:
            rutas_guardadas = archivos_bajados[0]

    except Exception: pass
    return rutas_guardadas

# --- DESCARGA ESPECIAL (MSI/OLI - PATRULLERO) ---
def patrullar_hd(session, id_volcan, nombre_volcan, sensor_hd):
    """
    Entra a la pestaña MSI u OLI y busca la imagen compuesta (Latest 6 Images).
    """
    url_detalle = f"{BASE_URL}/NRT/volcanoDetails_{sensor_hd}.php?volcano_id={id_volcan}"
    
    try:
        res = session.get(url_detalle, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        target_img_url = None
        
        # Buscamos la imagen que contiene "Last6" o "Latest" en su nombre
        tags = soup.find_all('img')
        for tag in tags:
            src = tag.get('src')
            if not src: continue
            low = src.lower()
            # Patrón detectado en tus capturas: "Last6Images"
            if "last6images" in low or "latest" in low:
                if src.startswith('http'): target_img_url = src
                else: target_img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                break
        
        if target_img_url:
            # Guardamos en carpeta especial HD
            ahora = datetime.now()
            # Usamos fecha actual porque la imagen se actualiza en el servidor pero mantiene el nombre a veces
            # O agregamos un hash para diferenciar
            ruta_carpeta = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, "HD_SENSORS", sensor_hd)
            os.makedirs(ruta_carpeta, exist_ok=True)
            
            # Nombre fijo: 'MSI_Composite.png'. Git detectará si los píxeles cambiaron.
            nombre_archivo = f"{sensor_hd}_Latest_Composite.png"
            ruta_final = os.path.join(ruta_carpeta, nombre_archivo)
            
            r_img = session.get(target_img_url, timeout=30)
            if r_img.status_code == 200:
                with open(ruta_final, 'wb') as f: f.write(r_img.content)
                return ruta_final

    except Exception as e:
        print(f"⚠️ Error patrullando {sensor_hd} en {nombre_volcan}: {e}")
    
    return None

def check_evidencia_existente(nombre_volcan, fecha_utc_dt):
    fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
    if os.path.exists(ruta_dia) and len(os.listdir(ruta_dia)) > 0: return True 
    return False

def procesar():
    # 1. MODO NUCLEAR (Activo para limpiar errores previos)
    modo_nuclear_borrar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    ahora_cl_proceso = obtener_hora_chile_actual()
    
    print(f"🚀 Iniciando V27.1 (Fix Sintaxis + Soporte MSI/OLI): {ahora_cl_proceso}")
    
    # ---------------------------------------------------------
    # FASE 1: EL ESPÍA (Latest.php para MODIS/VIIRS)
    # ---------------------------------------------------------
    print(f"🕵️  Fase 1: Espiando Tabla Maestra (VRP)...")
    
    registros_todos = [] 
    registros_positivos = []
    
    try:
        res = session.get(URL_LATEST, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        tabla = soup.find('table', {'id': 'example'})
        if not tabla: tabla = soup.find('table')
        
        if tabla:
            tbody = tabla.find('tbody')
            filas = tbody.find_all('tr') if tbody else tabla.find_all('tr')[1:]
            
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
                    
                    if id_volc not in VOLCANES_CONFIG: continue
                    
                    config = VOLCANES_CONFIG[id_volc]
                    nombre_v = config["nombre"]
                    limite_km = config["limite_km"]
                    
                    dt_obj_utc = datetime.strptime(hora_str_utc, "%d-%b-%Y %H:%M:%S")
                    unix_time = int(dt_obj_utc.timestamp())
                    fecha_fmt_utc = dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")
                    fecha_fmt_chile = convertir_utc_a_chile(dt_obj_utc)

                    clave = f"{fecha_fmt_utc}_{nombre_v}_{sensor_str}"
                    if clave in ids_procesados_hoy: continue
                    ids_procesados_hoy.add(clave)
                    
                    vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                    dist_val = float(dist_str) if dist_str.replace('.','').isdigit() else 0.0
                    
                    # LÓGICA VRP
                    descargar_ahora = False
                    tipo_registro = "RUTINA"
                    clasificacion = "NORMAL"
                    es_alerta_real = False

                    if vrp_val > 0:
                        if dist_val <= limite_km:
                            clasificacion = "ALERTA VOLCANICA"
                            descargar_ahora = True
                            tipo_registro = "ALERTA"
                            es_alerta_real = True
                            print(f"🔥 ALERTA: {nombre_v} ({sensor_str}) | {dist_val}km")
                        else:
                            clasificacion = "FALSO POSITIVO"
                            print(f"⚠️  Ignorado: {nombre_v} a {dist_val}km")
                    else:
                        clasificacion = "NORMAL"
                        # Solo VIIRS375 guarda evidencia diaria si no hay nada más
                        if "VIIRS375" in sensor_str.upper():
                            if not check_evidencia_existente(nombre_v, dt_obj_utc):
                                descargar_ahora = True
                                tipo_registro = "EVIDENCIA_DIARIA"
                                print(f"📸 Evidencia: {nombre_v}")
                    
                    rutas = "No descargadas"
                    if descargar_ahora:
                        rutas = descargar_fotos_vrp(session, id_volc, nombre_v, sensor_str, dt_obj_utc)
                    
                    # DATA MASTER
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

                    if es_alerta_real:
                        dato_pos = dato_master.copy()
                        del dato_pos["Clasificacion"]
                        registros_positivos.append(dato_pos)

                except Exception: continue

    except Exception as e: print(f"Error Fase 1: {e}")

    # ---------------------------------------------------------
    # FASE 2: EL PATRULLERO (Visita MSI y OLI)
    # ---------------------------------------------------------
    print(f"🛰️  Fase 2: Patrullando Sensores HD (MSI/OLI)...")
    registros_hd = []

    for vid, config in VOLCANES_CONFIG.items():
        nombre_v = config["nombre"]
        
        # Patrullar MSI
        ruta_msi = patrullar_hd(session, vid, nombre_v, "MSI")
        if ruta_msi:
            registros_hd.append({
                "Fecha_Detectada": ahora_cl_proceso.strftime("%Y-%m-%d"),
                "Volcan": nombre_v,
                "Sensor": "MSI",
                "Tipo_Imagen": "Last6_Composite",
                "Ruta_Foto": ruta_msi,
                "Fecha_Proceso": ahora_cl_proceso.strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"   -> MSI capturado para {nombre_v}")
            
        # Patrullar OLI
        ruta_oli = patrullar_hd(session, vid, nombre_v, "OLI")
        if ruta_oli:
            registros_hd.append({
                "Fecha_Detectada": ahora_cl_proceso.strftime("%Y-%m-%d"),
                "Volcan": nombre_v,
                "Sensor": "OLI",
                "Tipo_Imagen": "Last6_Composite",
                "Ruta_Foto": ruta_oli,
                "Fecha_Proceso": ahora_cl_proceso.strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"   -> OLI capturado para {nombre_v}")

    # ---------------------------------------------------------
    # GUARDADO FINAL
    # ---------------------------------------------------------
    
    # 1. MASTER
    if registros_todos:
        df_m = pd.DataFrame(registros_todos).reindex(columns=COLUMNAS_MASTER)
        df_m.to_csv(DB_MASTER, index=False)
        print(f"💾 Master regenerado ({len(registros_todos)}).")

    # 2. POSITIVOS
    if registros_positivos:
        df_p = pd.DataFrame(registros_positivos).reindex(columns=COLUMNAS_REPORTE)
        df_p.to_csv(DB_POSITIVOS, index=False)
        print(f"🔥 Alertas regeneradas.")
        # Individuales
        for v in df_p['Volcan'].unique():
            df_v = df_p[df_p['Volcan'] == v]
            r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
            os.makedirs(os.path.dirname(r), exist_ok=True)
            df_v.to_csv(r, index=False)
    else:
        pd.DataFrame(columns=COLUMNAS_REPORTE).to_csv(DB_POSITIVOS, index=False)

    # 3. HD REPORT (Nuevo)
    if registros_hd:
        df_hd = pd.DataFrame(registros_hd).reindex(columns=COLUMNAS_HD)
        df_hd.to_csv(DB_HD, index=False)
        print(f"💎 Reporte HD generado.")

if __name__ == "__main__":
    procesar()
