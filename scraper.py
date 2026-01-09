import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    nuevos_registros = []

    for vid, nombre_v in VOLCANES.items():
        print(f"--- Monitoreando: {nombre_v} ---")
        url = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            res = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 1. Identificar Sensor (MODIS o VIIRS) desde el texto
            sensor = "MODIS"
            if "VIIRS" in soup.get_text().upper():
                sensor = "VIIRS"
            
            # 2. Extraer VRP con limpieza total
            vrp = "0"
            for b in soup.find_all('b'):
                if "VRP =" in b.text:
                    vrp = b.text.split('=')[-1].replace('MW', '').strip()
                    break

            # 3. Crear carpetas: imagenes / Volcan / Fecha / Hora
            ruta_carpeta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
            os.makedirs(ruta_carpeta, exist_ok=True)

            # 4. Descargar imágenes a esa carpeta específica
            imgs = soup.find_all('img')
            for i, img in enumerate(imgs):
                src = img.get('src')
                if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                    img_url = src if src.startswith('http') else BASE_URL + src
                    img_data = requests.get(img_url, headers=headers).content
                    with open(os.path.join(ruta_carpeta, f"{sensor}_img_{i}.png"), 'wb') as f:
                        f.write(img_data)

            # 5. Guardar datos para el CSV sin dejar huecos
            nuevos_registros.append({
                "Volcan": nombre_v,
                "VRP_MW": vrp,
                "Sensor": sensor,
                "Fecha": fecha_hoy,
                "Hora": ahora.strftime("%H:%M:%S"),
                "Estado": "Actualizado"
            })
            print(f"   [OK] {sensor} detectado. VRP: {vrp}")

        except Exception as e:
            print(f"   [!] Error en {nombre_v}: {e}")

    # 6. Guardar CSV (Asegurando que todas las columnas se llenen)
    if nuevos_registros:
        df_nuevo = pd.DataFrame(nuevos_registros)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Combinamos y rellenamos posibles NaN con el valor actual
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True).fillna("N/A")
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)
        print("Base de datos actualizada correctamente.")

if __name__ == "__main__":
    procesar()
