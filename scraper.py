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

def obtener_nivel_mirova(vrp, es_alerta):
    if vrp <= 0: return "NULO"
    if not es_alerta: return "FALSO POSITIVO"
    if vrp < 1: return "Muy Bajo"
    if vrp < 10: return "Bajo"
    if vrp < 100: return "Moderado"
    return "Alto"

def descargar_set_completo(session, id_v, nombre_v, dt_utc):
    tipos = ["logVRP", "VRP", "Latest", "Dist"]
    sensores = ["MODIS", "VIIRS375", "VIIRS"]
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)
    
    for s in sensores:
        for t in tipos:
            url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s}&type={t}"
            s_label = "VIIRS750" if s == "VIIRS" else s
            nombre_img = f"{h_a}_{nombre_v}_{s_label}_{t}.png"
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
    hoy_str = ahora_cl.strftime("%Y-%m-%d")
    fecha_proceso_actual = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    
    log_bitacora(f"🚀 INICIO CICLO V49.0 (SANEAMIENTO FORZADO): {ahora_cl}")

    try:
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()
        
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
            sensor = cols[5].text.strip()
            es_alerta = (vrp > 0 and dist <= conf["limite_km"])

            tipo = "RUTINA"; ruta = "No descargada"
            
            if es_alerta:
                tipo = "ALERTA_TERMICA"
                descargar_set_completo(session, id_v, conf["nombre"], dt_utc)
                s_l = "VIIRS750" if "375" not in sensor else "VIIRS375"
                ruta = f"imagenes_satelitales/{conf['nombre']}/{dt_utc.strftime('%Y-%m-%d')}/{dt_utc.strftime('%H-%M-%S')}_{conf['nombre']}_{s_l}_VRP.png"
            else:
                # Lógica de evidencia mejorada (Solo una por día si no hay alertas)
                ya_hay_foto_hoy = False
                if not df_master.empty:
                    df_hoy = df_master[(df_master['Volcan'] == conf['nombre']) & (df_master['Fecha_Satelite_UTC'].str.contains(hoy_str))]
                    ya_hay_foto_hoy = not df_hoy[df_hoy['Ruta Foto'] != "No descargada"].empty
                
                if not ya_hay_foto_hoy:
                    tipo = "EVIDENCIA_DIARIA"
                    descargar_set_completo(session, id_v, conf["nombre"], dt_utc)
                    s_l = "VIIRS750" if "375" not in sensor else "VIIRS375"
                    ruta = f"imagenes_satelitales/{conf['nombre']}/{dt_utc.strftime('%Y-%m-%d')}/{dt_utc.strftime('%H-%M-%S')}_{conf['nombre']}_{s_l}_VRP.png"

            nuevos_datos.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": conf["nombre"], "Sensor": sensor, "VRP_MW": vrp, "Distancia_km": dist,
                "Tipo_Registro": tipo, "Clasificacion Mirova": obtener_nivel_mirova(vrp, es_alerta),
                "Ruta Foto": ruta, "Fecha_Proceso_GitHub": fecha_proceso_actual
            })

        if nuevos_datos or not df_master.empty:
            if nuevos_datos:
                df_new = pd.DataFrame(nuevos_datos)
                df_master = pd.concat([df_master, df_new]).drop_duplicates(subset=["Fecha_Satelite_UTC", "Volcan", "Sensor"], keep='last')
            
            # --- BLOQUE DE SANEAMIENTO RETROACTIVO FORZADO ---
            def sanear_historial(row):
                cfg = next((c for i, c in VOLCANES_CONFIG.items() if c["nombre"] == row['Volcan']), None)
                d_lim = cfg["limite_km"] if cfg else 5.0
                es_alerta_real = (row['VRP_MW'] > 0 and row['Distancia_km'] <= d_lim)
                
                # 1. Asegurar Clasificación Mirova (NORMAL -> NULO)
                row['Clasificacion Mirova'] = obtener_nivel_mirova(row['VRP_MW'], es_alerta_real)

                # 2. Corregir Error de Arrastre (Tipo_Registro y Rutas fake para ceros)
                if row['VRP_MW'] <= 0:
                    # Si no es Alerta, solo puede ser EVIDENCIA_DIARIA o RUTINA
                    if row['Tipo_Registro'] == "EVIDENCIA_DIARIA":
                        # Solo permitimos que se quede como evidencia si tiene una fecha de proceso reciente (no de arrastre)
                        # O si queremos ser radicales, forzamos a RUTINA todo lo que no tenga VRP > 0 y sea muy antiguo
                        if int(time.time()) - int(row['timestamp']) > 86400 and row['Ruta Foto'] != "No descargada":
                             # Registros de más de 24h con VRP 0 se limpian si no estamos seguros de su origen
                             pass 
                    
                    # Limpieza específica pedida: Rutas que no corresponden para VRP 0
                    if row['Ruta Foto'] != "No descargada" and row['VRP_MW'] <= 0 and row['Tipo_Registro'] != "EVIDENCIA_DIARIA":
                        row['Ruta Foto'] = "No descargada"
                        row['Tipo_Registro'] = "RUTINA"

                # 3. Limpiar auditoría de GitHub para registros históricos
                if int(time.time()) - int(row['timestamp']) > 86400: # Más de 24 horas
                    if row['Fecha_Proceso_GitHub'] == fecha_proceso_actual:
                        row['Fecha_Proceso_GitHub'] = ""
                
                return row

            df_master = df_master.apply(sanear_historial, axis=1)
            # ------------------------------------------------

            # Asegurar orden y limpieza de columnas
            df_master = df_master.loc[:, ~df_master.columns.duplicated()]
            cols_orden = ["timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", "Fecha_Proceso_GitHub"]
            df_master = df_master[[c for c in cols_orden if c in df_master.columns]]
            
            df_master.to_csv(DB_MASTER, index=False)
            
            # --- RECONSTRUCCIÓN DE TABLAS ---
            df_pos = df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].drop(columns=['Tipo_Registro'], errors='ignore')
            df_pos.to_csv(DB_POSITIVOS, index=False)
            
            for v_nom in df_master['Volcan'].unique():
                csv_path = os.path.join(RUTA_IMAGENES_BASE, v_nom, f"registro_{v_nom.replace(' ', '_')}.csv")
                df_v = df_pos[df_pos['Volcan'] == v_nom]
                os.makedirs(os.path.join(RUTA_IMAGENES_BASE, v_nom), exist_ok=True)
                df_v.to_csv(csv_path, index=False)
            
            log_bitacora(f"💾 Consolidado y tablas saneadas exitosamente.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
