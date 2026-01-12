import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
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
    if v <= 0: return "NULO"
    if v > 0 and float(dist) > limit: return "FALSO POSITIVO"
    return "Muy Bajo" if v < 1 else "Bajo" if v < 10 else "Moderado" if v < 100 else "Alto"

def descargar_imagenes_quirurgica(session, id_v, nombre_v, dt_utc, sensor_tabla, modo="COMPLETO"):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    if modo == "COMPLETO":
        tipos = ["logVRP", "VRP", "Latest", "Dist"]
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        for t in tipos:
            url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={sensor_tabla}&type={t}"
            path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_{s_label}_{t}.png")
            if not os.path.exists(path_img):
                try:
                    r = session.get(url, timeout=30)
                    if r.status_code == 200 and len(r.content) > 5000:
                        with open(path_img, 'wb') as f: f.write(r.content)
                        log_bitacora(f"✅ Descargada: {nombre_v} {t}")
                except Exception as e: log_bitacora(f"⚠️ Error en {t}: {e}")
    else:
        if sensor_tabla != "VIIRS375": return 
        url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor=VIIRS375&type=Latest"
        path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_VIIRS375_Latest.png")
        if not os.path.exists(path_img):
            try:
                r = session.get(url, timeout=30)
                if r.status_code == 200 and len(r.content) > 5000:
                    with open(path_img, 'wb') as f: f.write(r.content)
            except: pass

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    # Identificarse como un navegador real para evitar bloqueos
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    
    ahora_cl = obtener_hora_chile()
    fecha_proceso_actual = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    
    log_bitacora(f"🚀 INICIO CICLO V64.0 (FORZADO TOTAL): {ahora_cl}")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()

        # 1. SCRAPING
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf, sensor = VOLCANES_CONFIG[id_v], cols[5].text.strip()
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())
            vrp_n, dist_n = float(cols[3].text.strip()), float(cols[4].text.strip())

            mask = (df_master['timestamp'] == ts) & (df_master['Volcan'] == conf["nombre"]) & (df_master['Sensor'] == sensor) if not df_master.empty else pd.Series([False])

            if not df_master.empty and mask.any():
                idx = df_master.index[mask][0]
                if float(df_master.at[idx, 'VRP_MW']) != vrp_n:
                    df_master.at[idx, 'VRP_MW'], df_master.at[idx, 'Editado'] = vrp_n, "SI"
                df_master.at[idx, 'Ultima_Actualizacion'] = fecha_proceso_actual
            else:
                nueva = {
                    "timestamp": ts, "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                    "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                    "Volcan": conf["nombre"], "Sensor": sensor, "VRP_MW": vrp_n, "Distancia_km": dist_n,
                    "Tipo_Registro": "PENDIENTE", "Clasificacion Mirova": "PENDIENTE", "Ruta Foto": "PENDIENTE",
                    "Fecha_Proceso_GitHub": fecha_proceso_actual, "Ultima_Actualizacion": fecha_proceso_actual, "Editado": "NO"
                }
                df_master = pd.concat([df_master, pd.DataFrame([nueva])], ignore_index=True)

        # 2. LÓGICA DE NEGOCIO Y SANEAMIENTO
        if not df_master.empty:
            df_master['date_only'] = df_master['Fecha_Satelite_UTC'].str.split(' ').str[0]
            
            def saneamiento_v64(group):
                volcan, fecha_str = group.name[0], group.name[1]
                limit = next(v["limite_km"] for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan)
                group = group.sort_values('timestamp')
                
                is_alerta = (group['VRP_MW'] > 0) & (group['Distancia_km'] <= limit)
                group.loc[is_alerta, 'Tipo_Registro'] = 'ALERTA_TERMICA'
                
                if is_alerta.any(): group.loc[~is_alerta, 'Tipo_Registro'] = 'RUTINA'
                else:
                    ayer = (datetime.strptime(fecha_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                    hubo_ayer = ((df_master['Volcan'] == volcan) & (df_master['date_only'] == ayer) & (df_master['Tipo_Registro'] == 'ALERTA_TERMICA')).any()
                    if hubo_ayer: group['Tipo_Registro'] = 'RUTINA'
                    else:
                        v375 = (group['Sensor'] == 'VIIRS375')
                        if v375.any():
                            idx = group[v375].index[0]
                            group.at[idx, 'Tipo_Registro'] = 'EVIDENCIA_DIARIA'
                            group.loc[group.index != idx, 'Tipo_Registro'] = 'RUTINA'
                        else: group['Tipo_Registro'] = 'RUTINA'

                for idx, row in group.iterrows():
                    group.at[idx, 'Clasificacion Mirova'] = obtener_nivel_mirova(row['VRP_MW'], row['Distancia_km'], volcan)
                    if group.at[idx, 'Tipo_Registro'] == 'RUTINA': group.at[idx, 'Ruta Foto'] = "No descargada"
                    else:
                        dt = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                        s_lab = "VIIRS750" if str(row['Sensor']) == "VIIRS" else str(row['Sensor'])
                        tipo_img = "VIIRS375_Latest" if row['Tipo_Registro'] == 'EVIDENCIA_DIARIA' else f"{s_lab}_VRP"
                        group.at[idx, 'Ruta Foto'] = f"imagenes_satelitales/{volcan}/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H-%M-%S')}_{volcan}_{tipo_img}.png"
                return group

            df_master = df_master.groupby(['Volcan', 'date_only'], group_keys=False).apply(saneamiento_v64)
            
            # 3. DESCARGAS FORZADAS (Busca cualquier alerta de hoy que no tenga archivo)
            for idx, row in df_master.iterrows():
                # Solo procesar alertas/evidencias de las últimas 24 horas para no re-descargar todo el historial
                if (int(datetime.now().timestamp()) - row['timestamp']) < 86400:
                    if row['Tipo_Registro'] in ['ALERTA_TERMICA', 'EVIDENCIA_DIARIA']:
                        id_v = next(k for k, v in VOLCANES_CONFIG.items() if v["nombre"] == row['Volcan'])
                        dt_obj = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                        descargar_imagenes_quirurgica(session, id_v, row['Volcan'], dt_obj, row['Sensor'], "COMPLETO" if row['Tipo_Registro'] == 'ALERTA_TERMICA' else "MINIMO")

            # 4. GUARDADO
            cols = ["timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"]
            df_master[cols].sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
            
            df_pos = df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].drop(columns=['Tipo_Registro', 'date_only'], errors='ignore')
            df_pos.to_csv(DB_POSITIVOS, index=False)
            for v_nom in df_master['Volcan'].unique():
                csv_v = os.path.join(RUTA_IMAGENES_BASE, v_nom, f"registro_{v_nom.replace(' ', '_')}.csv")
                df_pos[df_pos['Volcan'] == v_nom].to_csv(csv_v, index=False)
            log_bitacora("💾 Ciclo V64.0 finalizado con éxito.")
    except Exception as e: log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
