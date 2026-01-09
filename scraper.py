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
    # Headers para evitar bloqueos
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.mirovaweb.it/'
    }
    
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Procesamos un sensor a la vez para no saturar
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                res = requests.get(url_sitio, headers=headers, timeout=20)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')

                # 1. Crear la ruta de carpetas (Día/Hora)
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
                os.makedirs(ruta_final, os.mode=0o777, exist_ok=True)

                # 2. Descarga de imágenes (Lógica original recuperada)
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        # Limpiamos la ruta para que sea una URL válida
                        img_url = src if src.startswith('http') else f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        img_data = requests.get(img_url, headers=headers).content
                        if len(img_data) > 500: # Evita guardar errores de "No encontrado"
                            nombre_archivo = f"{s_label}_archivo_{i}.png"
                            with open(os.path.join(ruta_final, nombre_archivo), 'wb') as f:
                                f.write(img_data)

                # 3. Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 4. Registro completo (Recuperamos las columnas perdidas)
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "VRP_MW": vrp,
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S"),
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M")
                })
                time.sleep(1) # Pausa breve entre sensores

            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # 5. Guardar CSV manteniendo todas las columnas
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Aseguramos que el CSV mantenga la estructura completa
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True)
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
