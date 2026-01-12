import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import pytz
import time

# --- CONFIGURACIÓN DE VOLCANES (Límites Estrictos OVDAS) ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "limite_km": 3.0}, # Límite estricto 3km
    "355030": {"nombre": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "limite_km": 5.0},
    "357110": {"nombre": "Llaima", "limite_km": 5.0},
    "357070": {"nombre": "Nevados de Chillan", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "limite_km": 4.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "limite_km": 20.0},
    "358041": {"nombre": "Chaiten", "limite_km": 5.0},
    "357040": {"nombre": "Peteroa", "limite_km": 3.0}
}

CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

def log_debug(mensaje, tipo="INFO"):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    prefijo = {"INFO": "🔵", "EXITO": "✅", "ERROR": "❌", "ADVERTENCIA": "⚠️"}.get(tipo, "⚪")
    linea = f"[{ahora}] {prefijo} {mensaje}"
    print(linea)
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

def descargar_v99(session, id_v, nombre_v, dt_utc, sensor_tabla, es_alerta_real):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    # Mapeo exacto basado en tus links proporcionados
    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_web = mapeo.get(sensor_tabla, sensor_tabla)
    
    # Si es alerta real (bajo el límite km) bajamos set de 4, si no solo Latest
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta_real else ["Latest"]
    ruta_relativa = "No descargada"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
        'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_web}.php?volcano_id={id_v}'
    }

    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        filename = f"{h_a}_{nombre_v}_{s_label}_{t}.png"
        path_f = os.path.join(ruta_dia, filename)
        url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_web}&type={t}"
        
        try:
            r = session.get(url, headers=headers, timeout=25)
            if r.status_code == 200:
                if len(r.content) > 5000:
                    with open(path_f, 'wb') as f: f.write(r.content)
                    log_debug(f"Imagen guardada: {filename}", "EXITO")
                    if t in ["VRP", "Latest"]: ruta_relativa = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
                else:
                    log_debug(f"Imagen rechazada (Tamaño insuficiente: {len(r.content)} bytes) en {url}", "ADVERTENCIA")
            else:
                log_debug(f"Error HTTP {r.status_code} al solicitar {url}", "ERROR")
            time.sleep(1.2)
        except Exception as e:
            log_debug(f"Fallo de conexión en {t}: {str(e)}", "ERROR")
            
    return ruta_relativa

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    log_debug("INICIO CICLO V99.0 (MODO DEBUG ACTIVADO)", "INFO")

    try:
        if os.path.exists(DB_MASTER):
            df_master = pd.read_csv(DB_MASTER)
            columnas = df_master.columns.tolist()
        else:
            log_debug("No se encontró DB_MASTER, creando nueva estructura.", "ADVERTENCIA")
            return

        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        nuevos_registros = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()

            # --- LÓGICA DE CLASIFICACIÓN (LASTARRIA 3.0 KM) ---
            es_alerta_real = (vrp > 0 and dist <= conf["limite_km"])
            
            if vrp == 0:
                clasif, tipo = "NULO", ("EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA")
            else:
                if es_alerta_real:
                    clasif, tipo = "Bajo", "ALERTA_TERMICA"
                else:
                    clasif, tipo = "FALSO POSITIVO", "RUTINA"
                    log_debug(f"Detectado FALSO POSITIVO en {conf['nombre']}: Distancia {dist}km > Límite {conf['limite_km']}km", "INFO")

            # Descarga (Solo eventos de las últimas 24h)
            ruta_foto = "No descargada"
            if (int(time.time()) - int(dt_utc.timestamp())) < 86400:
                if es_alerta_real or sensor == "VIIRS375":
                    log_debug(f"Iniciando descarga para {conf['nombre']} ({sensor})...", "INFO")
                    ruta_foto = descargar_v99(session, id_v, conf["nombre"], dt_utc, sensor, es_alerta_real)

            row = {col: "" for col in columnas}
            row.update({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": conf["nombre"], "Sensor": sensor, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": tipo, "Clasificacion Mirova": clasif, "Ruta Foto": ruta_foto,
                "Ultima_Actualizacion": ahora_cl, "Editado": "NO"
            })
            nuevos_registros.append(row)

        df_nuevos = pd.DataFrame(nuevos_registros)
        df_final = pd.concat([df_master, df_nuevos]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')
        df_final = df_final[columnas].sort_values('timestamp', ascending=False)
        
        df_final.to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
        log_debug("Ciclo finalizado y bases de datos sincronizadas.", "EXITO")

    except Exception as e:
        log_debug(f"ERROR GENERAL EN PROCESAR: {str(e)}", "ERROR")

if __name__ == "__main__":
    procesar()
