import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import pytz
import time

# --- CONFIGURACIÓN ---
CARPETA_PRINCIPAL = "monitoreo_satelital"
RUTA_IMAGENES_BASE = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
DB_MASTER = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_POSITIVOS = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_positivos.csv")

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

def obtener_hora_cl(dt_utc):
    return dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")

def descargar_imagenes_v89(session, id_v, nombre_v, dt_utc, sensor_tabla):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    # Sensores reales para Mirova
    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_real = mapeo.get(sensor_tabla, sensor_tabla)
    
    # Si es alerta bajamos 4, si es rutina bajamos 1
    es_alerta = (id_v in VOLCANES_CONFIG) # Simplificado para asegurar descarga
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if es_alerta else ["Latest"]
    
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}'}

    ruta_principal = "No descargada"
    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        filename = f"{h_a}_{nombre_v}_{s_label}_{t}.png"
        path_img = os.path.join(ruta_dia, filename)
        
        if not os.path.exists(path_img):
            url = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_real}&type={t}"
            try:
                r = session.get(url, headers=headers, timeout=20)
                if r.status_code == 200 and len(r.content) > 5000:
                    with open(path_img, 'wb') as f:
                        f.write(r.content)
                    if t == "VRP" or t == "Latest": ruta_principal = f"imagenes_satelitales/{nombre_v}/{f_c}/{filename}"
            except: continue
    return ruta_principal

def procesar():
    session = requests.Session()
    ahora_cl_str = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ahora_cl_str}] 🚀 INICIO V89.0 (RESTAURACIÓN ESTRUCTURA)")

    try:
        # 1. SCRAPING
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        nuevos_list = []
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
            conf = VOLCANES_CONFIG[id_v]

            # DESCARGA INMEDIATA
            ruta = "No descargada"
            if (vrp > 0 and dist <= conf["limite_km"]) or sensor == "VIIRS375":
                ruta = descargar_imagenes_v89(session, id_v, conf["nombre"], dt_utc, sensor)

            nuevos_list.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Fecha_Captura_Chile": obtener_hora_cl(dt_utc),
                "Volcan": conf["nombre"],
                "Sensor": sensor,
                "VRP_MW": vrp,
                "Distancia_km": dist,
                "Tipo_Registro": "ALERTA_TERMICA" if (vrp > 0 and dist <= conf["limite_km"]) else ("EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA"),
                "Clasificacion Mirova": "Bajo" if vrp > 0 else "NULO",
                "Ruta Foto": ruta,
                "Fecha_Proceso_GitHub": ahora_cl_str,
                "Ultima_Actualizacion": ahora_cl_str,
                "Editado": "NO"
            })

        # 2. GESTIÓN DE CSV (RECUPERANDO COLUMNAS)
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()
        df_nuevos = pd.DataFrame(nuevos_list)

        # Unir respetando la estructura
        df_final = pd.concat([df_master, df_nuevos]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor'], keep='last')
        
        # Asegurar que todas las columnas existan
        columnas_requeridas = ["timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile", "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro", "Clasificacion Mirova", "Ruta Foto", "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado"]
        for col in columnas_requeridas:
            if col not in df_final.columns: df_final[col] = ""

        # Ordenar y Guardar
        df_final = df_final[columnas_requeridas].sort_values('timestamp', ascending=False)
        df_final.to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
        
        print(f"[{ahora_cl_str}] ✅ ESTRUCTURA RESTAURADA Y SINCRONIZADA.")

    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {e}")

if __name__ == "__main__":
    procesar()
