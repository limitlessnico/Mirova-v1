import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import pytz

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

def obtener_hora_chile_actual():
    return datetime.now(pytz.timezone('America/Santiago'))

def log_bitacora(mensaje):
    ahora = obtener_hora_chile_actual().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ahora}] {mensaje}\n"
    print(linea.strip())
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea)

# --- NUEVA ESTRUCTURA DE DESCARGA (Inspirada en V34) ---
def descargar_set_completo(session, id_volcan, nombre_volcan, fecha_utc_dt):
    # Probamos los 3 sensores posibles para asegurar captura
    sensores_web = ["VIR375", "VIR", "MOD"] 
    tipos = ["VRP", "logVRP", "Latest", "Dist"]
    
    fecha_carpeta = fecha_utc_dt.strftime("%Y-%m-%d")
    hora_archivo = fecha_utc_dt.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, fecha_carpeta)
    os.makedirs(ruta_dia, exist_ok=True)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}

    for s_try in sensores_web:
        for tipo in tipos:
            # Mapeo de etiqueta para el nombre del archivo
            s_label = "VIIRS750" if s_try == "VIR" else ("VIIRS375" if s_try == "VIR375" else "MODIS")
            nombre_archivo = f"{hora_archivo}_{nombre_volcan}_{s_label}_{tipo}.png"
            ruta_final = os.path.join(ruta_dia, nombre_archivo)
            
            if not os.path.exists(ruta_final):
                url_img = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_volcan}&sensor={s_try}&type={tipo}"
                try:
                    r = session.get(url_img, headers=headers, timeout=20)
                    if r.status_code == 200 and len(r.content) > 5000:
                        with open(ruta_final, 'wb') as f:
                            f.write(r.content)
                        log_bitacora(f"📸 Foto capturada: {nombre_volcan} {tipo} ({s_label})")
                except: continue

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = obtener_hora_chile_actual()
    log_bitacora(f"🚀 INICIO CICLO V88.0: {ahora_cl}")

    try:
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        nuevos_datos = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp_val, dist_val, sensor_str = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
            conf = VOLCANES_CONFIG[id_v]

            # Lógica de Alerta
            tipo_reg = "RUTINA"
            if vrp_val > 0 and dist_val <= conf["limite_km"]:
                tipo_reg = "ALERTA_TERMICA"
            elif sensor_str == "VIIRS375":
                tipo_reg = "EVIDENCIA_DIARIA"

            nuevos_datos.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": conf["nombre"],
                "Sensor": sensor_str,
                "VRP_MW": vrp_val,
                "Distancia_km": dist_val,
                "Tipo_Registro": tipo_reg,
                "Ultima_Actualizacion": ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
            })

            # DISPARO DE DESCARGA (Solo para eventos de hoy)
            if (int(time.time()) - int(dt_utc.timestamp())) < 86400:
                if tipo_reg != "RUTINA":
                    descargar_set_completo(session, id_v, conf["nombre"], dt_utc)

        # GUARDADO SEGURO
        if nuevos_datos:
            df_new = pd.DataFrame(nuevos_datos)
            if os.path.exists(DB_MASTER):
                df_old = pd.read_csv(DB_MASTER)
                df_master = pd.concat([df_old, df_new]).drop_duplicates(subset=["timestamp", "Volcan", "Sensor"], keep='last')
            else:
                df_master = df_new
            
            df_master.sort_values('timestamp', ascending=False).to_csv(DB_MASTER, index=False)
            df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
            log_bitacora(f"💾 CSVs sincronizados correctamente.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

    log_bitacora("✅ CICLO FINALIZADO.")

if __name__ == "__main__":
    procesar()
