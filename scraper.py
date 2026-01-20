import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
import numpy as np

# =========================
# CONFIGURACIÓN GENERAL
# =========================

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
    "357040": {"nombre": "PlanchonPeteroa", "id_mirova": "PlanchonPeteroa", "limite_km": 3.0}
}

CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")

DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")
ARCHIVO_BITACORA = os.path.join(CARPETA_PRINCIPAL, "bitacora_robot.txt")

COLUMNAS_ESTANDAR = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan",
    "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro",
    "Clasificacion Mirova", "Ruta Foto",
    "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"
]

# =========================
# FUNCIONES AUXILIARES
# =========================

def log_debug(mensaje, tipo="INFO"):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    prefijo = {"INFO": "🔵", "EXITO": "✅", "ERROR": "❌", "ADVERTENCIA": "⚠️"}.get(tipo, "⚪")
    print(f"{prefijo} {mensaje}")
    try:
        with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
            f.write(f"[{ahora}] {mensaje}\n")
    except:
        pass

def obtener_clasificacion_mirova(vrp_mw, es_alerta):
    if not es_alerta or vrp_mw <= 0:
        return "NULO"
    v = vrp_mw * 1e6
    if v < 1e6: return "Muy Bajo"
    if v < 1e7: return "Bajo"
    if v < 1e8: return "Moderado"
    if v < 1e9: return "Alto"
    return "Muy Alto"

# =========================
# DESCARGA DE IMÁGENES
# =========================

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
                with open(path_f, 'wb') as f:
                    f.write(r.content)
                if t in ["VRP", "Latest"]:
                    ruta_relativa = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
            time.sleep(0.3)
        except:
            continue

    return ruta_relativa

# =========================
# PROCESO PRINCIPAL
# =========================

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)

    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")

    log_debug("INICIO SCRAPER", "INFO")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame(columns=COLUMNAS_ESTANDAR)
        df_master['Fecha_Satelite_UTC_dt'] = pd.to_datetime(df_master['Fecha_Satelite_UTC'], errors="coerce")

        headers = {'User-Agent': 'Mozilla/5.0'}
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", headers=headers, timeout=30)

        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')

        log_debug(f"Filas leídas desde latest.php: {len(filas)}", "INFO")

        nuevos_datos = []

        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6:
                continue

            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG:
                continue

            conf = VOLCANES_CONFIG[id_v]
            volcan_nombre = conf["nombre"]

            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())

            vrp = float(cols[3].text.strip())
            dist = float(cols[4].text.strip())
            sensor = cols[5].text.strip()

            es_dentro_rango = dist <= conf["limite_km"]
            es_alerta_real = False

            if vrp > 0:
                if es_dentro_rango:
                    tipo = "ALERTA_TERMICA"
                    es_alerta_real = True
                else:
                    tipo = "FALSO_POSITIVO"
            else:
                tipo = "EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA"

            clasificacion = obtener_clasificacion_mirova(vrp, es_alerta_real)

            mask = (
                (df_master['timestamp'] == ts) &
                (df_master['Volcan'] == volcan_nombre) &
                (df_master['Sensor'] == sensor)
            )

            previo = df_master[mask]

            if not previo.empty:
                f_desc = previo.iloc[0]['Fecha_Proceso_GitHub']
                ruta_foto = previo.iloc[0]['Ruta Foto']
                editado = previo.iloc[0].get('Editado', "NO")
            else:
                f_desc = ahora_cl
                ruta_foto = "No descargada"
                editado = "NO"

                if (int(time.time()) - ts) < 86400:
                    if es_alerta_real:
                        ruta_foto = descargar_v104(session, id_v, dt_utc, sensor, True)

            nuevos_datos.append({
                "timestamp": ts,
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(
                    pytz.timezone('America/Santiago')
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": volcan_nombre,
                "Sensor": sensor,
                "VRP_MW": vrp,
                "Distancia_km": dist,
                "Tipo_Registro": tipo,
                "Clasificacion Mirova": clasificacion,
                "Ruta Foto": ruta_foto,
                "Fecha_Proceso_GitHub": f_desc,
                "Ultima_Actualizacion": ahora_cl,
                "Editado": editado
            })

        if nuevos_datos:
            df_nuevos = pd.DataFrame(nuevos_datos)
            df_final = pd.concat(
                [df_master.drop(columns=['Fecha_Satelite_UTC_dt'], errors="ignore"), df_nuevos]
            ).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')

            df_final = df_final[COLUMNAS_ESTANDAR].sort_values('timestamp', ascending=False)

            df_final.to_csv(DB_MASTER, index=False)
            df_final[df_final['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)

        log_debug("Proceso completado correctamente.", "EXITO")

    except Exception as e:
        log_debug(f"ERROR: {e}", "ERROR")

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    procesar()
