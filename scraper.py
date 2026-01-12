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

def descargar_directo(session, id_v, nombre_v, dt_utc, sensor_tabla, modo="COMPLETO"):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    # Mapeo simple: VIIRS375 -> VIR375, VIIRS -> VIR
    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_real = mapeo.get(sensor_tabla, sensor_tabla)
    
    tipos = ["VRP", "logVRP", "Latest", "Dist"] if modo == "COMPLETO" else ["Latest"]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
        'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}'
    }

    count = 0
    for t in tipos:
        # El nombre del archivo lleva la hora de adquisición (h_a)
        path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_{sensor_tabla}_{t}.png")
        
        url_final = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_real}&type={t}"
        
        try:
            r = session.get(url_final, headers=headers, timeout=30)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_img, 'wb') as f:
                    f.write(r.content)
                print(f"[{obtener_hora_chile()}] ✅ Descargado: {nombre_v} {t}")
                count += 1
            time.sleep(1) # Pequeña pausa entre fotos
        except: continue
    return count > 0

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = obtener_hora_chile()
    print(f"[{ahora_cl}] 🚀 INICIO V84.0 (DESCARGA DIRECTA)")

    try:
        # 1. Obtener datos de la tabla
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
            vrp = float(cols[3].text.strip())
            dist = float(cols[4].text.strip())
            sensor = cols[5].text.strip()
            conf = VOLCANES_CONFIG[id_v]

            # REGLA SIMPLE: Si VRP > 0 y está en el límite -> ALERTA. Si es VIIRS375 -> EVIDENCIA.
            tipo = "RUTINA"
            if vrp > 0 and dist <= conf["limite_km"]:
                tipo = "ALERTA_TERMICA"
            elif sensor == "VIIRS375":
                tipo = "EVIDENCIA_DIARIA"

            nuevos_datos.append({
                "timestamp": int(dt_utc.timestamp()),
                "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "Volcan": conf["nombre"],
                "Sensor": sensor,
                "VRP_MW": vrp,
                "Distancia_km": dist,
                "Tipo_Registro": tipo,
                "id_v": id_v
            })

        # 2. Cargar Master y comparar para descargar solo lo nuevo o de hoy
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()
        
        for dato in nuevos_datos:
            # Solo procesamos si no existe el timestamp O si es de hoy y queremos asegurar descarga
            ya_existe = not df_master.empty and ((df_master['timestamp'] == dato['timestamp']) & (df_master['Volcan'] == dato['Volcan'])).any()
            
            # Si es Alerta o Evidencia de hoy (últimas 12h), intentamos descargar
            es_reciente = (int(time.time()) - dato['timestamp']) < 43200
            
            if (not ya_existe or es_reciente) and dato['Tipo_Registro'] != "RUTINA":
                print(f"[{obtener_hora_chile()}] 🛠 Procesando {dato['Volcan']} ({dato['Tipo_Registro']})")
                descargar_directo(session, dato['id_v'], dato['Volcan'], datetime.strptime(dato['Fecha_Satelite_UTC'], "%Y-%m-%d %H:%M:%S"), dato['Sensor'], "COMPLETO" if dato['Tipo_Registro'] == "ALERTA_TERMICA" else "MINIMO")

        # 3. Guardar CSV (Simplemente actualizamos el Master con los nuevos datos)
        df_nuevos = pd.DataFrame(nuevos_datos).drop(columns=['id_v'])
        df_final = pd.concat([df_master, df_nuevos]).drop_duplicates(subset=['timestamp', 'Volcan', 'Sensor']).sort_values('timestamp', ascending=False)
        df_final.to_csv(DB_MASTER, index=False)
        df_final[df_final['Tipo_Registro'] == "ALERTA_TERMICA"].to_csv(DB_POSITIVOS, index=False)
        
        print(f"[{obtener_hora_chile()}] 💾 CICLO V84.0 FINALIZADO.")

    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
