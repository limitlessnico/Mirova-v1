import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import shutil
import pytz

# --- CONFIGURACIÓN DE VOLCANES ---
VOLCANES_CONFIG = {
    "355030": {"nombre": "Isluga", "limite_km": 5.0},
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "limite_km": 3.0},
    "357040": {"nombre": "Peteroa", "limite_km": 3.0},
    "357070": {"nombre": "Nevados de Chillan", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "limite_km": 4.0},
    "357110": {"nombre": "Llaima", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "limite_km": 5.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "limite_km": 20.0},
    "358041": {"nombre": "Chaiten", "limite_km": 5.0}
}

URL_LATEST = "https://www.mirovaweb.it/NRT/latest.php"
BASE_URL = "https://www.mirovaweb.it"
CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")

# ARCHIVOS MAESTROS
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv") 
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

def obtener_hora_chile_actual():
    return datetime.now(pytz.timezone('America/Santiago'))

def convertir_utc_a_chile(dt_obj_utc):
    dt_utc = dt_obj_utc.replace(tzinfo=pytz.utc)
    return dt_utc.astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")

def log_bitacora(mensaje):
    ahora = obtener_hora_chile_actual().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ahora}] {mensaje}\n"
    print(linea.strip())
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea)

def obtener_nivel_mirova(vrp, es_alerta):
    try:
        v = float(vrp)
        if v <= 0: return "NORMAL"
        if not es_alerta: return "FALSO POSITIVO"
        if v < 1: return "Muy Bajo"
        if v < 10: return "Bajo"
        if v < 100: return "Moderado"
        return "Alto"
    except: return "SIN DATOS"

def descargar_set_completo(session, id_volcan, nombre_volcan, fecha_utc_dt):
    sensores = ["MODIS", "VIIRS375", "VIIRS"] 
    tipos = ["logVRP", "VRP", "Latest", "Dist"]
    fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
    hora_archivo = fecha_utc_dt.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
    os.makedirs(ruta_dia, exist_ok=True)
    for sensor_web in sensores:
        for tipo in tipos:
            url_img = f"{BASE_URL}/NRT/get_latest_image.php?volcano_id={id_volcan}&sensor={sensor_web}&type={tipo}"
            s_label = "VIIRS750" if sensor_web == "VIIRS" else sensor_web
            nombre_archivo = f"{hora_archivo}_{nombre_volcan}_{s_label}_{tipo}.png"
            ruta_final = os.path.join(ruta_dia, nombre_archivo)
            if not os.path.exists(ruta_final):
                try:
                    r = session.get(url_img, timeout=20)
                    if r.status_code == 200 and len(r.content) > 5000:
                        with open(ruta_final, 'wb') as f: f.write(r.content)
                except: continue

def procesar():
    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL)
    session = requests.Session()
    ahora_cl = obtener_hora_chile_actual()
    log_bitacora(f"🚀 INICIO CICLO V36.5 (REPARACIÓN RUTAS): {ahora_cl}")

    try:
        # 1. LIMPIEZA DE ARCHIVOS FUERA DE LUGAR
        for archivo in os.listdir(CARPETA_PRINCIPAL):
            if archivo.startswith("registro_") and archivo.endswith(".csv"):
                ruta_full = os.path.join(CARPETA_PRINCIPAL, archivo)
                if ruta_full not in [DB_MASTER, DB_POSITIVOS]:
                    try: os.remove(ruta_full)
                    except: pass

        # 2. SCRAPING
        res = session.get(URL_LATEST, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        tabla = soup.find('table', {'id': 'example'}) or soup.find('table')
        if not tabla: return
        filas = tabla.find('tbody').find_all('tr')
        nuevos_datos = []

        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            config = VOLCANES_CONFIG[id_v]
            nombre_v = config["nombre"]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp_val = float(cols[3].text.strip()); dist_val = float(cols[4].text.strip())
            sensor_str = cols[5].text.strip()
            es_alerta_distancia = (vrp_val > 0 and dist_val <= config["limite_km"])

            # CONSTRUCCIÓN DE RUTA FOTO
            f_carp = dt_utc.strftime("%Y-%m-%d")
            h_arch = dt_utc.strftime("%H-%M-%S")
            s_lab = "VIIRS750" if "375" not in sensor_str else "VIIRS375"
            ruta_foto = f"imagenes_satelitales/{nombre_v}/{f_carp}/{h_arch}_{nombre_v}_{s_lab}_VRP.png"

            dato = {
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Chile": convertir_utc_a_chile(dt_utc),
                "Volcan": nombre_v, "Sensor": sensor_str, "VRP_MW": vrp_val,
                "Distancia_km": dist_val, "Clasificacion Mirova": obtener_nivel_mirova(vrp_val, es_alerta_distancia),
                "Ruta Foto": ruta_foto,
                "Fecha_Proceso": ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
            }
            nuevos_datos.append(dato)
            if es_alerta_distancia or (vrp_val == 0 and "375" in sensor_str):
                descargar_set_completo(session, id_v, nombre_v, dt_utc)

        # 3. ACTUALIZACIÓN MASTER Y CONSOLIDADO
        if nuevos_datos:
            df_new = pd.DataFrame(nuevos_datos)
            if os.path.exists(DB_MASTER):
                df_old = pd.read_csv(DB_MASTER)
                if 'Clasificacion' in df_old.columns:
                    df_old = df_old.rename(columns={'Clasificacion': 'Clasificacion Mirova'})
                df_master = pd.concat([df_old, df_new]).drop_duplicates(subset=["Fecha_Satelite_UTC", "Volcan", "Sensor"])
            else:
                df_master = df_new

            # Reparar "Ruta Foto" en histórico si faltara
            def reparar_ruta(row):
                if pd.isna(row['Ruta Foto']) or row['Ruta Foto'] == "":
                    dt = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                    f_c = dt.strftime("%Y-%m-%d")
                    h_a = dt.strftime("%H-%M-%S")
                    s_l = "VIIRS750" if "375" not in str(row['Sensor']) else "VIIRS375"
                    return f"imagenes_satelitales/{row['Volcan']}/{f_c}/{h_a}_{row['Volcan']}_{s_l}_VRP.png"
                return row['Ruta Foto']

            df_master['Ruta Foto'] = df_master.apply(reparar_ruta, axis=1)
            df_master.to_csv(DB_MASTER, index=False)
            
            # Guardar Consolidado Positivos
            niveles = ["Muy Bajo", "Bajo", "Moderado", "Alto"]
            df_pos = df_master[df_master['Clasificacion Mirova'].isin(niveles)]
            df_pos.to_csv(DB_POSITIVOS, index=False)

            # 4. RE-FORMATEO DE INDIVIDUALES (SOLO POSITIVOS)
            for v_id, cfg in VOLCANES_CONFIG.items():
                v_nombre = cfg["nombre"]
                ruta_carpeta = os.path.join(RUTA_IMAGENES_BASE, v_nombre)
                if os.path.exists(ruta_carpeta):
                    ruta_csv = os.path.join(ruta_carpeta, f"registro_{v_nombre.replace(' ', '_')}.csv")
                    df_v_limpio = df_pos[df_pos['Volcan'] == v_nombre].copy()
                    df_v_limpio.to_csv(ruta_csv, index=False)
            
            log_bitacora(f"💾 Sincronización completa. Rutas reparadas e individuales limpios.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
