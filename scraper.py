import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN DE RUTAS ---
CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

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

def obtener_hora_chile():
    return datetime.now(pytz.timezone('America/Santiago'))

def log_bitacora(mensaje):
    ahora = obtener_hora_chile().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ahora}] {mensaje}")
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(f"[{ahora}] {mensaje}\n")

def descargar_imagenes_quirurgica(session, id_v, nombre_v, dt_utc, sensor_tabla, modo="COMPLETO"):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    tipos = ["logVRP", "VRP", "Latest", "Dist"] if modo == "COMPLETO" else ["Latest"]
    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_real = mapeo.get(sensor_tabla, sensor_tabla)
    
    header_extra = {'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}'}

    exito = False
    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_{s_label}_{t}.png")
        
        # SIEMPRE INTENTAR si el archivo no está físicamente en el servidor de ejecución
        try:
            r = session.get(f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_real}&type={t}", headers=header_extra, timeout=25)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_img, 'wb') as f:
                    f.write(r.content)
                log_bitacora(f"✅ CAPTURADA: {nombre_v} {t}")
                exito = True
        except: continue
    return exito

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'})
    
    ahora_cl = obtener_hora_chile()
    log_bitacora(f"🚀 INICIO CICLO V80.0 (FUERZA BRUTA): {ahora_cl}")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()

        # 1. ACTUALIZAR DESDE MIROVA
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v, sensor = cols[1].text.strip(), cols[5].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())
            vrp_n, dist_n = float(cols[3].text.strip()), float(cols[4].text.strip())
            conf = VOLCANES_CONFIG[id_v]

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
            # 2. DESCARGAS FORZADAS (REPARACIÓN DE LASCAR)
            ahora_ts = int(datetime.now().timestamp())
            for idx, row in df_master.iterrows():
                # Forzar cualquier alerta o evidencia de HOY
                if (ahora_ts - row['timestamp']) < 86400:
                    volcan = row['Volcan']
                    limit = next(v["limite_km"] for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan)
                    
                    es_alerta = row['VRP_MW'] > 0 and row['Distancia_km'] <= limit
                    es_evidencia = row['Sensor'] == 'VIIRS375'

                    if es_alerta or es_evidencia:
                        id_v = next(k for k, v in VOLCANES_CONFIG.items() if v["nombre"] == volcan)
                        dt_obj = datetime.strptime(row['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S")
                        
                        # AQUÍ ESTÁ EL CAMBIO: No preguntamos si la ruta está llena en el CSV.
                        # Intentamos descargar sí o sí si el registro es de hoy.
                        descargado = descargar_imagenes_quirurgica(session, id_v, volcan, dt_obj, row['Sensor'], "COMPLETO" if es_alerta else "MINIMO")
                        
                        if descargado:
                            df_master.at[idx, 'Tipo_Registro'] = 'ALERTA_TERMICA' if es_alerta else 'EVIDENCIA_DIARIA'
                            s_lab = "VIIRS750" if row['Sensor'] == "VIIRS" else row['Sensor']
                            img_t = "VIIRS375_Latest" if not es_alerta else f"{s_lab}_VRP"
                            df_master.at[idx, 'Ruta Foto'] = f"imagenes_satelitales/{volcan}/{dt_obj.strftime('%Y-%m-%d')}/{dt_obj.strftime('%H-%M-%S')}_{volcan}_{img_t}.png"

            # 3. GUARDADO
            df_master.sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
            df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
            log_bitacora("💾 CICLO V80.0 FINALIZADO.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
