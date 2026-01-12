import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN ---
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

def obtener_hora_chile():
    return datetime.now(pytz.timezone('America/Santiago'))

def log_bitacora(mensaje):
    ahora = obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ahora}] {mensaje}")
    with open(os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt"), "a", encoding="utf-8") as f:
        f.write(f"[{ahora}] {mensaje}\n")

def descargar_desde_link_sensor(session, id_v, nombre_v, dt_utc, sensor_tabla, modo="COMPLETO"):
    """
    Entra directamente a la sección del sensor específico para capturar las imágenes.
    """
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    # Mapeo de sensor tabla -> parámetro de URL Mirova
    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_real = mapeo.get(sensor_tabla, sensor_tabla)
    
    tipos = ["logVRP", "VRP", "Latest", "Dist"] if modo == "COMPLETO" else ["Latest"]
    
    # Navegación referer para simular presencia en la página de detalles
    header_extra = {'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}'}

    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_{s_label}_{t}.png")
        
        if not os.path.exists(path_img):
            url_img = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_real}&type={t}"
            try:
                r = session.get(url_img, headers=header_extra, timeout=25)
                if r.status_code == 200 and len(r.content) > 5000:
                    with open(path_img, 'wb') as f:
                        f.write(r.content)
                    log_bitacora(f"✅ CAPTURA DIRECTA ({s_real}): {nombre_v} {t}")
            except: continue

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'})
    
    ahora_cl = obtener_hora_chile()
    log_bitacora(f"🚀 INICIO CICLO V77.0 (LINK DIRECTO): {ahora_cl}")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()

        # 1. ACTUALIZAR CSV DESDE LATEST.PHP
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())
            vrp_n, dist_n, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()

            mask = (df_master['timestamp'] == ts) & (df_master['Volcan'] == conf["nombre"]) & (df_master['Sensor'] == sensor) if not df_master.empty else pd.Series([False])
            
            if not df_master.empty and mask.any():
                idx = df_master.index[mask][0]
                df_master.at[idx, 'Ultima_Actualizacion'] = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
            else:
                nueva = {
                    "timestamp": ts, "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                    "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                    "Volcan": conf["nombre"], "Sensor": sensor, "VRP_MW": vrp_n, "Distancia_km": dist_n,
                    "Tipo_Registro": "PENDIENTE", "Clasificacion Mirova": "PENDIENTE", "Ruta Foto": "PENDIENTE",
                    "Fecha_Proceso_GitHub": ahora_cl.strftime("%Y-%m-%d %H:%M:%S"), "Ultima_Actualizacion": ahora_cl.strftime("%Y-%m-%d %H:%M:%S"), "Editado": "NO"
                }
                df_master = pd.concat([df_master, pd.DataFrame([nueva])], ignore_index=True)

        if not df_master.empty:
            # Lógica de clasificación
            for idx, row in df_master.iterrows():
                volcan = row['Volcan']
                limit = next(v["limite_km"] for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan)
                if row['VRP_MW'] > 0 and row['Distancia_km'] <= limit:
                    df_master.at[idx, 'Tipo_Registro'] = 'ALERTA_TERMICA'
                elif row['Sensor'] == 'VIIRS375' and (row['Tipo_Registro'] == 'PENDIENTE' or pd.isna(row['Tipo_Registro'])):
                    df_master.at[idx, 'Tipo_Registro'] = 'EVIDENCIA_DIARIA'
                elif row['Tipo_Registro'] == 'PENDIENTE':
                    df_master.at[idx, 'Tipo_Registro'] = 'RUTINA'

            # 2. DESCARGAS DESDE LINKS DE SENSORES (Últimas 24h)
            ahora_ts = int(datetime.now().timestamp())
            for idx, row in df_master.iterrows():
                if (ahora_ts - row['timestamp']) < 86400:
                    if row['Tipo_Registro'] in ['ALERTA_TERMICA', 'EVIDENCIA_DIARIA']:
                        id_v = next(k for k, v in VOLCANES_CONFIG.items() if v["nombre"] == row['Volcan'])
                        dt_obj = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                        descargar_desde_link_sensor(session, id_v, row['Volcan'], dt_obj, row['Sensor'], "COMPLETO" if row['Tipo_Registro'] == 'ALERTA_TERMICA' else "MINIMO")

            df_master.sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
            log_bitacora("💾 CICLO V77.0 FINALIZADO.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
