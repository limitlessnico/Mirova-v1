import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
import numpy as np

# --- CONFIGURACIÓN MAESTRA ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "id_mirova": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "id_mirova": "Lastarria", "limite_km": 3.0},
    "355030": {"nombre": "Isluga", "id_mirova": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "id_mirova": "Villarrica", "limite_km": 5.0},
    "357110": {"nombre": "Llaima", "id_mirova": "Llaima", "limite_km": 5.0},
    "357070": {"nombre": "Nevados de Chillan", "id_mirova": "ChillanNevadosde", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "id_mirova": "Copahue", "limite_km": 4.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "id_mirova": "PuyehueCordonCaulle", "limite_km": 20.0},
    "358041": {"nombre": "Chaiten", "id_mirova": "Chaiten", "limite_km": 5.0},
    "357040": {"nombre": "Peteroa", "id_mirova": "PlanchonPeteroa", "limite_km": 3.0}
}

CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

COLUMNAS_ESTANDAR = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", 
    "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", 
    "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"
]

def log_debug(mensaje, tipo="INFO"):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    prefijo = {"INFO": "🔵", "EXITO": "✅", "ERROR": "❌", "ADVERTENCIA": "⚠️"}.get(tipo, "⚪")
    print(f"{prefijo} {mensaje}")
    try:
        with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
            f.write(f"[{ahora}] {mensaje}\n")
    except: pass

def obtener_clasificacion_mirova(vrp_mw, es_alerta):
    if not es_alerta or vrp_mw <= 0: return "NULO"
    v = vrp_mw * 1000000 # Watts
    if v < 1e6: return "Muy Bajo"
    if 1e6 <= v < 1e7: return "Bajo"
    if 1e7 <= v < 1e8: return "Moderado"
    if 1e8 <= v < 1e9: return "Alto"
    return "Muy Alto"

def descargar_v104(session, volcan_id, dt_utc, sensor_tabla, es_alerta_real):
    conf = VOLCANES_CONFIG[volcan_id]
    nombre_v = conf["nombre"]
    id_mirova = conf["id_mirova"]
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)
    
    s_url = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta_real else ["Latest"]
    ruta_relativa = "No descargada"
    
    for t in tipos:
        t_url = f"{t}10NTI" if t == "Latest" else t
        url = f"https://www.mirovaweb.it/OUTPUTweb/MIROVA/{s_url}/VOLCANOES/{id_mirova}/{id_mirova}_{s_url}_{t_url}.png"
        filename = f"{h_a}_{nombre_v}_{s_url}_{t}.png"
        path_f = os.path.join(ruta_dia, filename)
        try:
            r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=25)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_f, 'wb') as f: f.write(r.content)
                if t in ["VRP", "Latest"]: ruta_relativa = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
            time.sleep(0.3)
        except: continue
    return ruta_relativa

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    log_debug("INICIO SCRAPER: ESCANEO TOTAL DE TABLA (500+ REGISTROS)", "INFO")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame(columns=COLUMNAS_ESTANDAR)
        df_master['Fecha_Satelite_UTC_dt'] = pd.to_datetime(df_master['Fecha_Satelite_UTC'])

        # --- MEJORA CRÍTICA: FORZAR TABLA COMPLETA ---
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'}
        url_total = "https://www.mirovaweb.it/NRT/latest.php?length=-1"
        res = session.get(url_total, headers=headers, timeout=30)
        
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        log_debug(f"Páginas recibidas. Analizando {len(filas)} filas encontradas.", "INFO")
        
        nuevos_datos = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())
            vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
            volcan_nombre = conf["nombre"]

            es_alerta_real = (vrp > 0 and dist <= conf["limite_km"])
            
            # Tipo de registro con Falsos Positivos
            if vrp > 0:
                tipo = "ALERTA_TERMICA" if es_alerta_real else "FALSO_POSITIVO"
            else:
                tipo = "EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA"

            # Lógica de descubrimiento y preservación
            mask = (df_master['timestamp'] == ts) & (df_master['Volcan'] == volcan_nombre) & (df_master['Sensor'] == sensor)
            registro_previo = df_master[mask]

            if not registro_previo.empty:
                f_descubrimiento = registro_previo.iloc[0]['Fecha_Proceso_GitHub']
                ruta_foto = registro_previo.iloc[0]['Ruta Foto']
                editado = registro_previo.iloc[0].get('Editado', "NO")
            else:
                f_descubrimiento = ahora_cl
                ruta_foto = "No descargada"
                editado = "NO"
                # Intentar descarga si es nuevo y reciente
                if (int(time.time()) - ts) < 86400 and es_alerta_real:
                    ruta_foto = descargar_v104(session, id_v, dt_utc, sensor, True)

            nuevos_datos.append({
                "timestamp": ts, 
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": volcan_nombre, "Sensor": sensor, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": tipo, "Clasificacion Mirova": obtener_clasificacion_mirova(vrp, es_alerta_real),
                "Ruta Foto": ruta_foto, "Fecha_Proceso_GitHub": f_descubrimiento,
                "Ultima_Actualizacion": ahora_cl, "Editado": editado
            })

        # --- GUARDADO Y SINCRONIZACIÓN ---
        df_final = pd.concat([df_master.drop(columns=['Fecha_Satelite_UTC_dt'], errors='ignore'), pd.DataFrame(nuevos_datos)]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')
        df_final = df_final[COLUMNAS_ESTANDAR].sort_values('timestamp', ascending=False)
        
        df_final.to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro']=="ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)

        for id_v, config in VOLCANES_CONFIG.items():
            nom_v = config["nombre"]
            archivo_v = os.path.join(CARPETA_PRINCIPAL, f"registro_{nom_v.replace(' ', '_')}.csv")
            df_v = df_final[(df_final['Volcan'] == nom_v) & (df_final['Tipo_Registro'] == "ALERTA_TERMICA")]
            df_v.to_csv(archivo_v, index=False)

        log_debug("Proceso completado. Escaneo profundo finalizado.", "EXITO")

    except Exception as e:
        log_debug(f"ERROR: {str(e)}", "ERROR")

if __name__ == "__main__":
    procesar()
