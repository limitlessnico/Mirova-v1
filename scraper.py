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

# DOS BASES DE DATOS
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv") 
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")

CARPETA_OBSOLETA = "monitoreo_datos"

# COLUMNAS OFICIALES (Con Fecha_Chile incluida)
COLUMNAS_OFICIALES = [
    "timestamp", 
    "Fecha_Satelite_UTC", 
    "Fecha_Chile",        
    "Volcan", 
    "Sensor", 
    "VRP_MW", 
    "Distancia_km",
    "Clasificacion",
    "Fecha_Proceso", 
    "Ruta_Fotos",
    "Tipo_Registro"
]

def obtener_hora_chile_actual():
    try:
        tz_chile = pytz.timezone('America/Santiago')
        return datetime.now(tz_chile)
    except: return datetime.now(pytz.utc)

def convertir_utc_a_chile(dt_obj_utc):
    """ Convierte UTC a Hora Chile (Invierno/Verano automático) """
    try:
        utc_zone = pytz.utc
        dt_utc = dt_obj_utc.replace(tzinfo=utc_zone)
        chile_zone = pytz.timezone('America/Santiago')
        dt_chile = dt_utc.astimezone(chile_zone)
        return dt_chile.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")

def modo_nuclear_borrar_todo():
    """ 
    ☢️ MODO NUCLEAR: Borra todo para iniciar limpio con la nueva estructura.
    """
    print("☢️  BORRANDO HISTORIAL COMPLETO para regeneración limpia...")
    
    # Borrar carpeta obsoleta si existe
    if os.path.exists(CARPETA_OBSOLETA):
        try: shutil.rmtree(CARPETA_OBSOLETA)
        except: pass

    # Borrar la carpeta principal actual
    if os.path.exists(CARPETA_PRINCIPAL):
        try: shutil.rmtree(CARPETA_PRINCIPAL)
        print("✅ Carpeta antigua eliminada con éxito.")
    except: pass

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
    if os.path.exists(ruta_dia):
        if len(os.listdir(ruta_dia)) > 0: return True 
    return False

def procesar():
    # 1. EJECUTAR BORRADO NUCLEAR (Solo para esta versión de limpieza)
    modo_nuclear_borrar_todo()

    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    ahora_cl_proceso = obtener_hora_chile_actual()
    print(f"🚀 Iniciando V24.1 (Reinicio Total + Hora CL): {ahora_cl_proceso}")
    
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
            
            # Como borramos todo, empezamos de cero
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
                    
                    if id_volc not in MIS_VOLCANES: continue
                    
                    nombre_limpio = MIS_VOLCANES[id_volc]
                    
                    # Fechas
                    dt_obj_utc = datetime.strptime(hora_str_utc, "%d-%b-%Y %H:%M:%S")
                    unix_time = int(dt_obj_utc.timestamp())
                    fecha_fmt_utc = dt_obj_utc.strftime("%Y-%m-%d %H:%M:%S")
                    fecha_fmt_chile = convertir_utc_a_chile(dt_obj_utc) # CONVERSIÓN

                    clave = f"{fecha_fmt_utc}_{nombre_limpio}_{sensor_str}"
                    
                    if clave in ids_procesados_hoy: continue
                    ids_procesados_hoy.add(clave)
                    
                    vrp_val = float(vrp_str) if vrp_str.replace('.','').isdigit() else 0.0
                    dist_val = float(dist_str) if dist_str.replace('.','').isdigit() else 0.0
                    
                    # Lógica Alerta vs Evidencia
                    descargar_ahora = False
                    tipo_registro = "RUTINA"
                    clasificacion = "NORMAL"

                    # 1. Alerta
                    if vrp_val > 0:
                        if dist_val <= 5.0: clasificacion = "ALERTA VOLCANICA"
                        else: clasificacion = "FALSO POSITIVO"
                        descargar_ahora = True
                        tipo_registro = "ALERTA"
                        print(f"🔥 RECUPERANDO ALERTA: {nombre_limpio} | {fecha_fmt_chile} (CL)")

                    # 2. Evidencia (Como estamos reiniciando, guardará 1 evidencia por día)
                    else:
                        clasificacion = "NORMAL"
                        if "VIIRS" in sensor_str.upper():
                            if not check_evidencia_existente(nombre_limpio, dt_obj_utc):
                                descargar_ahora = True
                                tipo_registro = "EVIDENCIA_DIARIA"
                                print(f"📸 Recuperando Evidencia: {nombre_limpio} | {fecha_fmt_chile} (CL)")
                    
                    rutas = "No descargadas"
                    if descargar_ahora:
                        rutas = descargar_fotos(session, id_volc, nombre_limpio, sensor_str, dt_obj_utc)
                    
                    registros_nuevos.append({
                        "timestamp": unix_time,
                        "Fecha_Satelite_UTC": fecha_fmt_utc,
                        "Fecha_Chile": fecha_fmt_chile,
                        "Volcan": nombre_limpio,
                        "Sensor": sensor_str,
                        "VRP_MW": vrp_val,
                        "Distancia_km": dist_val,
                        "Clasificacion": clasificacion,
                        "Fecha_Proceso": ahora_cl_proceso.strftime("%Y-%m-%d %H:%M:%S"),
                        "Ruta_Fotos": rutas,
                        "Tipo_Registro": tipo_registro
                    })

                except Exception as e: continue

            # --- GUARDADO ---
            if registros_nuevos:
                df_new = pd.DataFrame(registros_nuevos)
                # Ordenar por fecha para que el CSV quede bonito
                df_new = df_new.sort_values(by="timestamp")
                df_new = df_new.reindex(columns=COLUMNAS_OFICIALES)

                # Guardar Master
                df_new.to_csv(DB_MASTER, index=False)
                print(f"💾 Master regenerado ({len(registros_nuevos)} registros).")

                # Guardar Positivos
                df_positivos = df_new[df_new['VRP_MW'] > 0]
                df_positivos.to_csv(DB_POSITIVOS, index=False)
                print(f"🔥 Reporte Positivos regenerado.")

                # Guardar Individuales (Solo Positivos)
                print("🔄 Regenerando carpetas individuales...")
                for v in df_new['Volcan'].unique():
                    df_v = df_new[(df_new['Volcan'] == v) & (df_new['VRP_MW'] > 0)]
                    r = os.path.join(RUTA_IMAGENES_BASE, v, f"registro_{v}.csv")
                    os.makedirs(os.path.dirname(r), exist_ok=True)
                    df_v.to_csv(r, index=False)
            else:
                print("💤 No se encontraron datos en la tabla.")

    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    procesar()
