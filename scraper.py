import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

# Lista de volcanes
VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0'}
    ahora = datetime.now()
    # Definimos las variables de tiempo
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    datos_para_csv = []

    for vid, nombre_v in VOLCANES.items():
        print(f"--- Iniciando captura: {nombre_v} ---")
        url_detalles = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            res = requests.get(url_detalles, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 1. DETECCIÓN DE SENSOR Y VRP
            sensor = "MODIS"
            if "VIIRS" in soup.get_text().upper():
                sensor = "VIIRS"
            
            vrp_valor = "0"
            for b in soup.find_all('b'):
                if "VRP =" in b.text:
                    vrp_valor = b.text.split('=')[1].strip()
                    break

            # 2. ESTRUCTURA DE CARPETAS: imagenes / Volcan / Fecha / Hora
            # Esto asegura que cada pasada del día quede guardada por separado
            ruta_dia = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
            os.makedirs(ruta_dia, exist_ok=True)

            # 3. DESCARGA DE IMÁGENES
            imgs = soup.find_all('img')
            for i, img in enumerate(imgs):
                src = img.get('src')
                if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                    img_url = src if src.startswith('http') else BASE_URL + src
                    img_data = requests.get(img_url, headers=headers).content
                    
                    nombre_archivo = f"{sensor}_img_{i}.png"
                    with open(os.path.join(ruta_dia, nombre_archivo), 'wb') as f:
                        f.write(img_data)
            
            print(f"   [OK] Guardado en: {ruta_dia}")

            datos_para_csv.append({
                "Volcan": nombre_v,
                "Sensor": sensor,
                "VRP_MW": vrp_valor,
                "Fecha": fecha_hoy,
                "Hora": ahora.strftime("%H:%M:%S")
            })

        except Exception as e:
            print(f"   [!] Error en {nombre_v}: {e}")

    # 4. ACTUALIZACIÓN DEL CSV
    if datos_para_csv:
        df_nuevo = pd.DataFrame(datos_para_csv)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
