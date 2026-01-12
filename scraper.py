import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
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

CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

def obtener_hora_chile():
    return datetime.now(pytz.timezone('America/Santiago'))

def log_bitacora(mensaje):
    ahora = obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ahora}] {mensaje}")
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(f"[{ahora}] {mensaje}\n")

def obtener_nivel_mirova(vrp, dist, volcan_nombre):
    v = float(vrp)
    limit = next((v["limite_km"] for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan_nombre), 5.0)
    es_alerta = (v > 0 and float(dist) <= limit)
    if v <= 0: return "NULO"
    if not es_alerta: return "FALSO POSITIVO"
    if v < 1: return "Muy Bajo"
    if v < 10: return "Bajo"
    if v < 100: return "Moderado"
    return "Alto"

def descargar_imagenes_quirurgica(session, id_v, nombre_v, dt_utc, sensor_tabla, modo="COMPLETO"):
    """
    modo "COMPLETO": Descarga 4 productos solo para el sensor detectado (Alertas).
    modo "MINIMO": Descarga solo 1 producto (Latest) para VIIRS375 (Evidencias).
    """
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    if modo == "COMPLETO":
        # Usamos estrictamente el sensor que Mirova reporta en su tabla
        sensores_objetivo = [sensor_tabla] 
        productos = ["logVRP", "VRP", "Latest", "Dist"]
    else: 
        # Evidencia diaria optimizada a 1 sola imagen de contexto
        sensores_objetivo = ["VIIRS375"]
        productos = ["Latest"]

    for s in sensores_objetivo:
        for p in productos:
            url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s}&type={p}"
            # Estandarización de nombre para VIIRS 750m
            s_label = "VIIRS750" if s == "VIIRS" else s
            nombre_img = f"{h_a}_{nombre_v}_{s_label}_{p}.png"
            path_img = os.path.join(ruta_dia, nombre_img)
            
            if not os.path.exists(path_img):
                try:
                    r = session.get(url, timeout=20)
                    if r.status_code == 200 and len(r.content) > 5000:
                        with open(path_img, 'wb') as f: f.write(r.content)
                except: continue

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = obtener_hora_chile()
    fecha_proceso_actual = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    
    log_bitacora(f"🚀 INICIO CICLO V57.0 (DESCARGA QUIRÚRGICA): {ahora_cl}")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()
        
        # 1. Scraping de la tabla principal
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        nuevos_datos = []

        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp = float(cols[3].text.strip())
            dist = float(cols[4].text.strip())
            sensor_original = cols[5].text.strip()

            nuevos_datos.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": conf["nombre"], "Sensor": sensor_original, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": "PENDIENTE", "Clasificacion Mirova": "PENDIENTE",
                "Ruta Foto": "PENDIENTE", "Fecha_Proceso_GitHub": fecha_proceso_actual
            })

        if nuevos_datos or not df_master.empty:
            if nuevos_datos:
                df_new = pd.DataFrame(nuevos_datos)
                df_master = pd.concat([df_master, df_new]).drop_duplicates(subset=["Fecha_Satelite_UTC", "Volcan", "Sensor"], keep='last')
            
            # --- SANEAMIENTO Y LÓGICA DE NEGOCIO ---
            df_master['date_only'] = df_master['Fecha_Satelite_UTC'].str.split(' ').str[0]
            df_master = df_master.sort_values(['Volcan', 'timestamp'])

            def saneamiento_func(group):
                volcan = group.name[0]
                limit = VOLCANES_CONFIG[next(k for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan)]["limite_km"]
                
                group['Clasificacion Mirova'] = group.apply(lambda r: obtener_nivel_mirova(r['VRP_MW'], r['Distancia_km'], volcan), axis=1)
                is_alerta = (group['VRP_MW'] > 0) & (group['Distancia_km'] <= limit)
                group.loc[is_alerta, 'Tipo_Registro'] = 'ALERTA_TERMICA'
                
                if is_alerta.any():
                    group.loc[~is_alerta, 'Tipo_Registro'] = 'RUTINA'
                    group.loc[~is_alerta, 'Ruta Foto'] = 'No descargada'
                else:
                    first_idx = group.index[0]
                    group.loc[first_idx, 'Tipo_Registro'] = 'EVIDENCIA_DIARIA'
                    if len(group) > 1:
                        group.loc[group.index[1:], 'Tipo_Registro'] = 'RUTINA'
                        group.loc[group.index[1:], 'Ruta Foto'] = 'No descargada'

                ts_actual = int(time.time())
                for idx, row in group.iterrows():
                    # Definir ruta de la foto principal para el CSV
                    if row['Tipo_Registro'] in ['ALERTA_TERMICA', 'EVIDENCIA_DIARIA']:
                        if row['Ruta Foto'] in ["No descargada", "PENDIENTE", "Pendiente"]:
                            dt = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                            s_original = str(row['Sensor'])
                            s_label = "VIIRS750" if s_original == "VIIRS" else s_original
                            # En evidencia diaria, forzamos la ruta al VIIRS375 Latest
                            if row['Tipo_Registro'] == 'EVIDENCIA_DIARIA':
                                group.at[idx, 'Ruta Foto'] = f"imagenes_satelitales/{volcan}/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H-%M-%S')}_{volcan}_VIIRS375_Latest.png"
                            else:
                                group.at[idx, 'Ruta Foto'] = f"imagenes_satelitales/{volcan}/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H-%M-%S')}_{volcan}_{s_label}_VRP.png"
                    
                    # Auditoría de Fecha Proceso (Margen 12h)
                    if (ts_actual - int(row['timestamp'])) > 43200:
                        if row['Fecha_Proceso_GitHub'] == fecha_proceso_actual:
                            group.at[idx, 'Fecha_Proceso_GitHub'] = ""
                return group

            df_master = df_master.groupby(['Volcan', 'date_only'], group_keys=False).apply(saneamiento_func)

            # --- DESCARGA DIRIGIDA SEGÚN SENSOR ---
            for idx, row in df_master[df_master['Fecha_Proceso_GitHub'] == fecha_proceso_actual].iterrows():
                id_v = next(k for k, v in VOLCANES_CONFIG.items() if v["nombre"] == row['Volcan'])
                dt_obj = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                
                if row['Tipo_Registro'] == 'ALERTA_TERMICA':
                    descargar_imagenes_quirurgica(session, id_v, row['Volcan'], dt_obj, row['Sensor'], modo="COMPLETO")
                elif row['Tipo_Registro'] == 'EVIDENCIA_DIARIA':
                    descargar_imagenes_quirurgica(session, id_v, row['Volcan'], dt_obj, row['Sensor'], modo="MINIMO")

            # Guardado Master
            cols = ["timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", "Fecha_Proceso_GitHub"]
            df_master[cols].sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
            
            # Tablas Derivadas
            df_pos = df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].drop(columns=['Tipo_Registro', 'date_only'], errors='ignore')
            df_pos.to_csv(DB_POSITIVOS, index=False)
            for v_nom in df_master['Volcan'].unique():
                csv_v = os.path.join(RUTA_IMAGENES_BASE, v_nom, f"registro_{v_nom.replace(' ', '_')}.csv")
                df_pos[df_pos['Volcan'] == v_nom].to_csv(csv_v, index=False)

            log_bitacora(f"💾 Ciclo V57.0 completado. Descargas filtradas por sensor.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
