import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import pytz
import time

# --- CONFIGURACIÓN ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "limite_km": 3.0},
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

# Definición de columnas estándar para auditoría
COLUMNAS_ESTANDAR = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", 
    "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", 
    "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"
]

def log_debug(mensaje, tipo="INFO"):
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    prefijo = {"INFO": "🔵", "EXITO": "✅", "ERROR": "❌", "ADVERTENCIA": "⚠️"}.get(tipo, "⚪")
    linea = f"[{ahora}] {prefijo} {mensaje}"
    print(linea)
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

def descargar_v104(session, nombre_v, dt_utc, sensor_tabla, es_alerta_real):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    s_url = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
    nombre_url = nombre_v.replace(" ", "_").replace("-", "_")
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta_real else ["Latest"]
    ruta_relativa = "No descargada"

    for t in tipos:
        t_url = f"{t}10NTI" if t == "Latest" else t
        # URL completa en una sola línea para evitar SyntaxError EOL
        url = f"https://www.mirovaweb.it/OUTPUTweb/MIROVA/{s_url}/VOLCANOES/{nombre_url}/{nombre_url}_{s_url}_{t_url}.png"
        
        filename = f"{h_a}_{nombre_v}_{s_url}_{t}.png"
        path_f = os.path.join(ruta_dia, filename)
        
        try:
            r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_f, 'wb') as f: f.write(r.content)
                log_debug(f"Descargado: {filename}", "EXITO")
                if t in ["VRP", "Latest"]: ruta_relativa = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
            time.sleep(0.5)
        except: continue
            
    return ruta_relativa

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    log_debug("INICIO CICLO V105.1 (CORRECCIÓN SINTAXIS Y AUDITORÍA)", "INFO")

    try:
        # Cargar base de datos maestra o crearla si no existe
        if os.path.exists(DB_MASTER):
            df_master = pd.read_csv(DB_MASTER)
        else:
            df_master = pd.DataFrame(columns=COLUMNAS_ESTANDAR)

        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        nuevos_datos_mirova = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            conf = VOLCANES_CONFIG[id_v]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            ts = int(dt_utc.timestamp())
            vrp = float(cols[3].text.strip())
            dist = float(cols[4].text.strip())
            sensor = cols[5].text.strip()
            volcan_nombre = conf["nombre"]

            # Buscar si el registro ya existe en el CSV consolidado
            mask = (df_master['timestamp'] == ts) & (df_master['Volcan'] == volcan_nombre) & (df_master['Sensor'] == sensor)
            registro_previo = df_master[mask]

            es_alerta_real = (vrp > 0 and dist <= conf["limite_km"])
            clasif = "Bajo" if es_alerta_real else ("FALSO POSITIVO" if vrp > 0 else "NULO")
            tipo = "ALERTA_TERMICA" if es_alerta_real else ("EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA")

            if not registro_previo.empty:
                # Recuperar datos originales para mantener trazabilidad
                f_proceso = registro_previo.iloc[0]['Fecha_Proceso_GitHub']
                u_actualiz = ahora_cl
                ruta_foto = registro_previo.iloc[0]['Ruta Foto']
                
                # Detectar cambios numéricos
                old_vrp = float(registro_previo.iloc[0]['VRP_MW'])
                old_dist = float(registro_previo.iloc[0]['Distancia_km'])
                
                if (vrp != old_vrp) or (dist != old_dist):
                    editado = "SI"
                    log_debug(f"Cambio en {volcan_nombre}: VRP {old_vrp}->{vrp}", "ADVERTENCIA")
                else:
                    editado = registro_previo.iloc[0]['Editado'] if pd.notna(registro_previo.iloc[0]['Editado']) else "NO"
            else:
                f_proceso = ahora_cl
                u_actualiz = ahora_cl
                editado = "NO"
                
                ruta_foto = "No descargada"
                if (int(time.time()) - ts) < 86400:
                    if es_alerta_real or sensor == "VIIRS375":
                        ruta_foto = descargar_v104(session, volcan_nombre, dt_utc, sensor, es_alerta_real)

            nuevos_datos_mirova.append({
                "timestamp": ts,
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": volcan_nombre, "Sensor": sensor, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": tipo, "Clasificacion Mirova": clasif, "Ruta Foto": ruta_foto,
                "Fecha_Proceso_GitHub": f_proceso,
                "Ultima_Actualizacion": u_actualiz,
                "Editado": editado
            })

        # Consolidar manteniendo siempre la lectura más reciente de Mirova
        df_nuevos = pd.DataFrame(nuevos_datos_mirova)
        df_final = pd.concat([df_master, df_nuevos]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')
        
        # Ordenar columnas y exportar
        df_final = df_final[COLUMNAS_ESTANDAR].sort_values('timestamp', ascending=False)
        df_final.to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
        
        # Exportar archivos individuales por volcán
        for id_v, config in VOLCANES_CONFIG.items():
            nombre_v = config["nombre"]
            archivo_v = os.path.join(CARPETA_PRINCIPAL, f"registro_{nombre_v.replace(' ', '_')}.csv")
            df_v = df_final[df_final['Volcan'] == nombre_v]
            df_v.to_csv(archivo_v, index=False)

        log_debug("Sincronización Exitosa.", "EXITO")

    except Exception as e:
        log_debug(f"ERROR: {str(e)}", "ERROR")

if __name__ == "__main__":
    procesar()
