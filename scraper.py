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

def descargar_v86(session, id_v, nombre_v, dt_utc, sensor_tabla):
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    ruta_dia = os.path.join(RUTA_IMAGENES_BASE, nombre_v, f_c)
    os.makedirs(ruta_dia, exist_ok=True)

    mapeo = {"VIIRS375": "VIR375", "VIIRS": "VIR", "MODIS": "MOD"}
    s_real = mapeo.get(sensor_tabla, sensor_tabla)
    
    # Intentaremos los 4 tipos de imagen para Alertas
    tipos = ["VRP", "logVRP", "Latest", "Dist"]
    
    # Simulamos un navegador real al 100%
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
        'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'Referer': f'https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}'
    })

    for t in tipos:
        s_label = "VIIRS750" if sensor_tabla == "VIIRS" else sensor_tabla
        path_img = os.path.join(ruta_dia, f"{h_a}_{nombre_v}_{s_label}_{t}.png")
        
        url_img = f"https://www.mirovaweb.it/NRT/get_latest_image.php?volcano_id={id_v}&sensor={s_real}&type={t}"
        
        try:
            # Primero visitamos la página de detalles para activar la sesión/cookie de Mirova
            session.get(f"https://www.mirovaweb.it/NRT/volcanoDetails_{s_real}.php?volcano_id={id_v}", timeout=20)
            
            # Ahora pedimos la imagen
            r = session.get(url_img, timeout=30)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_img, 'wb') as f:
                    f.write(r.content)
                print(f"[{obtener_hora_chile()}] ✅ DESCARGA EXITOSA: {nombre_v} {t}")
            else:
                print(f"[{obtener_hora_chile()}] ⚠️ Mirova rechazó {nombre_v} {t} (Status {r.status_code})")
            time.sleep(2)
        except: continue

def procesar():
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    session = requests.Session()
    ahora_cl = obtener_hora_chile()
    print(f"[{ahora_cl}] 🚀 INICIO V86.0 (AUTENTICACIÓN COOKIE)")

    try:
        # 1. SCRAPING TABLA
        res = session.get("https://www.mirovaweb.it/NRT/latest.php", timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.find('tbody').find_all('tr')
        
        df_master = pd.read_csv(DB_MASTER) if os.path.exists(DB_MASTER) else pd.DataFrame()
        
        for fila in filas:
            cols = fila.find_all('td')
            if len(cols) < 6: continue
            id_v = cols[1].text.strip()
            if id_v not in VOLCANES_CONFIG: continue
            
            dt_utc = datetime.strptime(cols[0].text.strip(), "%d-%b-%Y %H:%M:%S")
            vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
            conf = VOLCANES_CONFIG[id_v]

            # Si es alerta o evidencia de HOY
            if (int(time.time()) - int(dt_utc.timestamp())) < 43200:
                if (vrp > 0 and dist <= conf["limite_km"]) or sensor == "VIIRS375":
                    print(f"[{obtener_hora_chile()}] 🎯 Atacando {conf['nombre']}...")
                    descargar_v86(session, id_v, conf["nombre"], dt_utc, sensor)

        # Actualización simple del CSV
        # (Aquí puedes mantener tu lógica de guardado anterior)
        print(f"[{obtener_hora_chile()}] 💾 CICLO V86.0 FINALIZADO.")

    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    procesar()
