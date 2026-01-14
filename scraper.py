import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time

# --- CONFIGURACIÓN CON IDs INTERNOS DE MIROVA ---
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
                if t in ["VRP", "Latest"]: 
                    ruta_relativa = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
            time.sleep(0.3)
        except: continue
    return ruta_relativa

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    log_debug("INICIO V108.0: FIX URLs PUYEHUE/PETEROA + CALMA 2DA PASADA", "INFO")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame(columns=COLUMNAS_ESTANDAR)
        df_master['Fecha_Satelite_UTC_dt'] = pd.to_datetime(df_master['Fecha_Satelite_UTC'])

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
            ts = int(dt_utc.timestamp())
            vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
            volcan_nombre = conf["nombre"]

            es_alerta_real = (vrp > 0 and dist <= conf["limite_km"])
            tipo = "ALERTA_TERMICA" if es_alerta_real else ("EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA")

            mask = (df_master['timestamp'] == ts) & (df_master['Volcan'] == volcan_nombre) & (df_master['Sensor'] == sensor)
            if not df_master[mask].empty:
                ruta_foto = df_master[mask].iloc[0]['Ruta Foto']
            else:
                ruta_foto = "No descargada"
                hora_utc = dt_utc.hour
                
                if (int(time.time()) - ts) < 86400:
                    if es_alerta_real:
                        # Prioridad: Alerta con ID Mirova correcto
                        ruta_foto = descargar_v104(session, id_v, dt_utc, sensor, True)
                    elif sensor == "VIIRS375" and hora_utc >= 17:
                        # Calma inteligente: Segunda pasada
                        f_ayer = (dt_utc.date() - timedelta(days=1))
                        t_ayer = not df_master[(df_master['Volcan']==volcan_nombre) & (df_master['Fecha_Satelite_UTC_dt'].dt.date == f_ayer) & (df_master['Tipo_Registro']=="ALERTA_TERMICA")].empty
                        t_hoy = not df_master[(df_master['Volcan']==volcan_nombre) & (df_master['Fecha_Satelite_UTC_dt'].dt.date == dt_utc.date()) & (df_master['Tipo_Registro']=="ALERTA_TERMICA")].empty
                        
                        if not t_ayer and not t_hoy:
                            # ¿Es la segunda captura de la tarde?
                            prev_tarde = len(df_master[(df_master['Volcan']==volcan_nombre) & (df_master['Sensor']=="VIIRS375") & (df_master['Fecha_Satelite_UTC_dt'].dt.date == dt_utc.date()) & (df_master['Fecha_Satelite_UTC_dt'].dt.hour >= 17)])
                            if prev_tarde >= 1:
                                ruta_foto = descargar_v104(session, id_v, dt_utc, sensor, False)

            nuevos_datos.append({
                "timestamp": ts, "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": volcan_nombre, "Sensor": sensor, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": tipo, "Clasificacion Mirova": "Bajo" if es_alerta_real else "NULO",
                "Ruta Foto": ruta_foto, "Fecha_Proceso_GitHub": ahora_cl, "Ultima_Actualizacion": ahora_cl, "Editado": "NO"
            })

        df_final = pd.concat([df_master.drop(columns=['Fecha_Satelite_UTC_dt']), pd.DataFrame(nuevos_datos)]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')
        df_final[COLUMNAS_ESTANDAR].sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro']=="ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
        log_debug("Sincronización exitosa con IDs de Mirova corregidos.", "EXITO")

    except Exception as e:
        log_debug(f"ERROR: {str(e)}", "ERROR")

if __name__ == "__main__":
    procesar()
