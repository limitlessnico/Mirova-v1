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

def obtener_hora_chile_actual():
    return datetime.now(pytz.timezone('America/Santiago'))

def convertir_utc_a_chile(dt_obj_utc):
    dt_utc = dt_obj_utc.replace(tzinfo=pytz.utc)
    return dt_utc.astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")

def log_bitacora(mensaje):
    ahora = obtener_hora_chile_actual().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ahora}] {mensaje}\n"
    print(linea.strip())
    with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
        f.write(linea)

def obtener_nivel_mirova(vrp, es_alerta):
    try:
        v = float(vrp)
        if v <= 0: return "NULO"
        if not es_alerta: return "FALSO POSITIVO"
        if v < 1: return "Muy Bajo"
        if v < 10: return "Bajo"
        if v < 100: return "Moderado"
        return "Alto"
    except: return "SIN DATOS"

def descargar_set_completo(session, id_volcan, nombre_volcan, dt_utc):
    sensores = ["MODIS", "VIIRS375", "VIIRS"] 
    tipos = ["logVRP", "VRP", "Latest", "Dist"]
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_volcan, f_c)
    os.makedirs(ruta_dia, exist_ok=True)
    for s_web in sensores:
        for t in tipos:
            url_img = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_volcan}&sensor={s_web}&type={t}"
            s_l = "VIIRS750" if s_web == "VIIRS" else s_web
            n_a = f"{h_a}_{nombre_volcan}_{s_l}_{t}.png"
            r_f = os.path.join(ruta_dia, n_a)
            if not os.path.exists(r_f):
                try:
                    r = session.get(url_img, timeout=20)
                    if r.status_code == 200 and len(r.content) > 5000:
                        with open(r_f, 'wb') as f: f.write(r.content)
                except: continue

def procesar():
    if not os.path.exists(CARPETA_PRINCIPAL): os.makedirs(CARPETA_PRINCIPAL)
    session = requests.Session()
    ahora_cl = obtener_hora_chile_actual()
    hoy_str = ahora_cl.strftime("%Y-%m-%d")
    fecha_proceso_str = ahora_cl.strftime("%Y-%m-%d %H:%M:%S")
    
    log_bitacora(f"🚀 INICIO CICLO V44.0 (ACTUALIZACIÓN NULO IN-SITU): {ahora_cl}")

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
            
            config = VOLCANES_CONFIG[id_v]
            nombre_v = config["nombre"]
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp_val = float(cols[3].text.strip()); dist_val = float(cols[4].text.strip())
            sensor_str = cols[5].text.strip()
            es_alerta = (vrp_val > 0 and dist_val <= config["limite_km"])
            
            tipo_reg = "RUTINA"; ruta_foto = "No descargada"
            
            if es_alerta:
                tipo_reg = "ALERTA_TERMICA"
                descargar_set_completo(session, id_v, nombre_v, dt_utc)
                s_l = "VIIRS750" if "375" not in sensor_str else "VIIRS375"
                ruta_foto = f"imagenes_satelitales/{nombre_v}/{dt_utc.strftime('%Y-%m-%d')}/{dt_utc.strftime('%H-%M-%S')}_{nombre_v}_{s_l}_VRP.png"
            else:
                ya_hay_foto = False
                if not df_master.empty:
                    ya_hay_foto = not df_master[(df_master['Volcan'] == nombre_v) & 
                                               (df_master['Fecha_Satelite_UTC'].str.contains(hoy_str)) &
                                               (df_master['Ruta Foto'] != "No descargada")].empty
                if not ya_hay_foto:
                    tipo_reg = "EVIDENCIA_DIARIA"
                    descargar_set_completo(session, id_v, nombre_v, dt_utc)
                    s_l = "VIIRS750" if "375" not in sensor_str else "VIIRS375"
                    ruta_foto = f"imagenes_satelitales/{nombre_v}/{dt_utc.strftime('%Y-%m-%d')}/{dt_utc.strftime('%H-%M-%S')}_{nombre_v}_{s_l}_VRP.png"

            nuevos_datos.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": convertir_utc_a_chile(dt_utc),
                "Volcan": nombre_v, "Sensor": sensor_str, "VRP_MW": vrp_val,
                "Distancia_km": dist_val, "Tipo_Registro": tipo_reg,
                "Clasificacion Mirova": obtener_nivel_mirova(vrp_val, es_alerta),
                "Ruta Foto": ruta_foto,
                "Fecha_Proceso_GitHub": fecha_proceso_str
            })

        if nuevos_datos:
            df_new = pd.DataFrame(nuevos_datos)
            df_master = pd.concat([df_master, df_new]).drop_duplicates(subset=["Fecha_Satelite_UTC", "Volcan", "Sensor"], keep='last')
            
            # --- ACTUALIZACIÓN DE VALORES EN COLUMNAS EXISTENTES ---
            def actualizar_existentes(row):
                cfg = next((c for i, c in VOLCANES_CONFIG.items() if c["nombre"] == row['Volcan']), None)
                d_lim = cfg["limite_km"] if cfg else 5.0
                es_al = (row['VRP_MW'] > 0 and row['Distancia_km'] <= d_lim)
                
                # Sobrescribir valor en la columna actual
                row['Clasificacion Mirova'] = obtener_nivel_mirova(row['VRP_MW'], es_al)
                return row

            df_master = df_master.apply(actualizar_existentes, axis=1)
            
            # Asegurar que no hay duplicados de columnas (por si acaso hubiera basura previa)
            df_master = df_master.loc[:, ~df_master.columns.duplicated()]
            
            # Guardar Consolidado
            df_master.to_csv(DB_MASTER, index=False)
            
            # --- RECONSTRUCCIÓN DE TABLAS LIMPIAS ---
            df_pos = df_master[df_master['Tipo_Registro'] == "ALERTA_TERMICA"].copy()
            df_out = df_pos.drop(columns=['Tipo_Registro'])
            df_out.to_csv(DB_POSITIVOS, index=False)
            
            for v_id, cfg in VOLCANES_CONFIG.items():
                v_nombre = cfg["nombre"]
                ruta_v = os.path.join(RUTA_IMAGENES_BASE, v_nombre)
                os.makedirs(ruta_v, exist_ok=True)
                df_v = df_out[df_out['Volcan'] == v_nombre]
                df_v.to_csv(os.path.join(ruta_v, f"registro_{v_nombre.replace(' ', '_')}.csv"), index=False)
            
            log_bitacora(f"💾 Consolidado actualizado con etiquetas 'NULO'.")

    except Exception as e:
        log_bitacora(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
