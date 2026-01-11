import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import shutil
from urllib.parse import urlparse
import pytz

# --- CONFIGURACIÓN DE LOS 10 VOLCANES CHILENOS ---
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

def limpieza_mantenimiento():
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA)
        except: pass

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
                # En V31, si viene una corrección, sobrescribimos la imagen para tener la mas actual
                if os.path.exists(ruta_final):
                    os.remove(ruta_final) # Borramos la vieja para asegurar que baja la nueva corregida
                
                r_img = session.get(url, timeout=20)
                if r_img.status_code == 200:
                    with open(ruta_final, 'wb') as f: f.write(r_img.content)
                    archivos_bajados.append(ruta_final)
        
        if archivos_bajados:
            rutas_guardadas = archivos_bajados[0]

    except Exception: pass
    return rutas_guardadas

# --- DESCARGA ESPECIAL (MSI/OLI - PATRULLERO) ---
def patrullar_hd(session, id_volcan, nombre_volcan, sensor_hd):
    url_detalle = f"{BASE_URL}/NRT/volcanoDetails_{sensor_hd}.php?volcano_id={id_volcan}"
    try:
        res = session.get(url_detalle, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        target_img_url = None
        tags = soup.find_all('img')
        for tag in tags:
            src = tag.get('src')
            if not src: continue
            low = src.lower()
            if "last6images" in low or "latest" in low:
                if src.startswith('http'): target_img_url = src
                else: target_img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                break
        
        if target_img_url:
            ruta_carpeta = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, "HD_SENSORS", sensor_hd)
            os.makedirs(ruta_carpeta, exist_ok=True)
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
    limpieza_mantenimiento()

    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    ahora_cl_proceso = obtener_hora_chile_actual()
    
    print(f"🚀 Iniciando V31.0 (Auditor de Cambios de Distancia): {ahora_cl_proceso}")
    
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
            sensores_vistos_en_ciclo = set() 
            
            # NUEVO V31: Usamos un Diccionario para recordar la distancia anterior
            # Clave: "Fecha_Volcan_Sensor" -> Valor: Distancia (float)
            db_conocimiento = {}
            
            if os.path.exists(DB_MASTER):
                try:
                    df_old = pd.read_csv(DB_MASTER)
                    for _, row in df_old.iterrows():
                        f_sat = str(row.get('Fecha_Satelite_UTC', ''))
                        volc = str(row.get('Volcan', ''))
                        sens = str(row.get('Sensor', ''))
                        dist_antigua = float(row.get('Distancia_km', 0.0))
                        
                        k = f"{f_sat}_{volc}_{sens}"
                        # Guardamos la distancia para comparar cambios
                        # Si hay duplicados en el CSV, esto guardará el último valor (el más reciente)
                        db_conocimiento[k] = dist_antigua
                except: pass

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
                    
                    # Evitar duplicados dentro del MISMO ciclo de 15 min
                    if clave in ids_procesados_hoy: continue
                    ids_procesados_hoy.add(clave)
                    
                    # --- LECTURA DE VALORES ---
                    vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                    if dist_str.replace('.','').isdigit():
                        dist_val = float(dist_str)
                    else:
                        dist_val = 999.9

                    # --- DETECCIÓN DE CAMBIOS (LÓGICA V31) ---
                    es_correccion = False
                    
                    if clave in db_conocimiento:
                        dist_guardada = db_conocimiento[clave]
                        # Si la distancia es IGUAL, es duplicado -> Ignorar
                        if abs(dist_val - dist_guardada) < 0.01: # Tolerancia minúscula
                            continue
                        else:
                            # Si es DIFERENTE, es una corrección -> Procesar
                            es_correccion = True
                            print(f"🔄 CORRECCIÓN DETECTADA en {nombre_v}: {dist_guardada}km -> {dist_val}km")
                    
                    # --- LÓGICA DE ALERTA ---
                    descargar_ahora = False
                    tipo_registro = "RUTINA"
                    if es_correccion: tipo_registro = "CORRECCION_DATA"
                    
                    clasificacion = "NORMAL"
                    es_alerta_real = False
                    
                    clave_sensor_volcan = f"{nombre_v}_{sensor_str}"
                    es_el_dato_mas_nuevo = False
                    if clave_sensor_volcan not in sensores_vistos_en_ciclo:
                        es_el_dato_mas_nuevo = True
                        sensores_vistos_en_ciclo.add(clave_sensor_volcan)

                    if vrp_val > 0:
                        if dist_val <= limite_km:
                            clasificacion = "ALERTA VOLCANICA"
                            if not es_correccion: tipo_registro = "ALERTA" # Si no es correccion, es alerta normal
                            es_alerta_real = True
                            print(f"🔥 ALERTA ACTIVA: {nombre_v} ({sensor_str}) | {dist_val}km")
                            
                            # Descargar si es nuevo O si es una corrección (para actualizar la foto)
                            if es_el_dato_mas_nuevo:
                                descargar_ahora = True
                            else:
                                rutas = "Imagen Sobreescrita en Web"
                        else:
                            clasificacion = "FALSO POSITIVO"
                            print(f"⚠️  Falso Positivo: {nombre_v} a {dist_val}km")
                            # Si antes era alerta y ahora es falso positivo (corrección), igual guardamos el cambio
                    else:
                        clasificacion = "NORMAL"
                        if "VIIRS375" in sensor_str.upper():
                            # Si es corrección, bajamos de nuevo para asegurar
                            should_download = (not check_evidencia_existente(nombre_v, dt_obj_utc)) or es_correccion
                            if should_download and es_el_dato_mas_nuevo:
                                descargar_ahora = True
                                tipo_registro = "EVIDENCIA_DIARIA"
                                if es_correccion: tipo_registro = "CORREC_EVIDENCIA"
                                print(f"📸 Evidencia: {nombre_v}")
                    
                    rutas = "No descargadas"
                    if descargar_ahora:
                        rutas = descargar_fotos_vrp(session, id_volc, nombre_v, sensor_str, dt_obj_utc)
                    elif not es_el_dato_mas_nuevo and es_alerta_real:
                         rutas = "Imagen No Disponible (Sobreescrita)"
                    
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

    # ---------------------------------------------------------
    # GUARDADO INCREMENTAL
    # ---------------------------------------------------------
    
    if registros_todos:
        df_new = pd.DataFrame(registros_todos).reindex(columns=COLUMNAS_MASTER)
        if os.path.exists(DB_MASTER):
            try:
                df_old = pd.read_csv(DB_MASTER)
                df_comb = pd.concat([df_old, df_new], ignore_index=True)
            except: df_comb = df_new
        else: df_comb = df_new
        df_comb.to_csv(DB_MASTER, index=False)
        print(f"💾 Master actualizado (+{len(registros_todos)}).")

    if registros_positivos:
        df_new_pos = pd.DataFrame(registros_positivos).reindex(columns=COLUMNAS_REPORTE)
        if os.path.exists(DB_POSITIVOS):
            try:
                df_old_pos = pd.read_csv(DB_POSITIVOS)
                df_comb_pos = pd.concat([df_old_pos, df_new_pos], ignore_index=True)
            except: df_comb_pos = df_new_pos
        else: df_comb_pos = df_new_pos
        
        df_comb_pos.to_csv(DB_POSITIVOS, index=False)
        print(f"🔥 Reporte Alertas actualizado.")
        
        for v in df_comb_pos['Volcan'].unique():
            df_v = df_comb_pos[df_comb_pos['Volcan'] == v]
            r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
            os.makedirs(os.path.dirname(r), exist_ok=True)
            df_v.to_csv(r, index=False)

    if registros_hd:
        df_new_hd = pd.DataFrame(registros_hd).reindex(columns=COLUMNAS_HD)
        if os.path.exists(DB_HD):
             try:
                df_old_hd = pd.read_csv(DB_HD)
                df_comb_hd = pd.concat([df_old_hd, df_new_hd]).drop_duplicates(subset=['Fecha_Detectada', 'Volcan', 'Sensor'])
             except: df_comb_hd = df_new_hd
        else: df_comb_hd = df_new_hd
        df_comb_hd.to_csv(DB_HD, index=False)
        print(f"💎 Reporte HD actualizado.")

if __name__ == "__main__":
    procesar()
