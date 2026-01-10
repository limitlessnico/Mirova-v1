import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time

VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.mirovaweb.it/'
    }
    
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Procesamos cada sensor de forma independiente
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                # Pausa estratégica para evitar el bloqueo del servidor
                time.sleep(7) 
                res = requests.get(url_sitio, headers=headers, timeout=30)
                if res.status_code != 200:
                    print(f"Servidor ocupado para {nombre_v} - {s_label}")
                    continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 1. Preparar Carpeta
                ruta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta, exist_ok=True)

                # 2. Descarga de imágenes con verificación de flujo
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        try:
                            img_res = requests.get(img_url, headers=headers, timeout=20, stream=True)
                            if img_res.status_code == 200:
                                contenido = img_res.content
                                if len(contenido) > 2000: # Solo guardamos si es una imagen real
                                    ext = "png" if "png" in src.lower() else "jpg"
                                    with open(os.path.join(ruta, f"{s_label}_img_{i}.{ext}"), 'wb') as f:
                                        f.write(contenido)
                        except:
                            continue

                # 3. Extraer VRP con limpieza estricta
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 4. Llenado Garantizado de Columnas (Previene N/A indeseados)
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Procesado: {nombre_v} - {s_label}")

            except Exception as e:
                print(f"Error crítico en {nombre_v} {s_label}: {e}")

    # 5. Consolidación de Base de Datos
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Forzamos la alineación de columnas para evitar huecos en blanco
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
