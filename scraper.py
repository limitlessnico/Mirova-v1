
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
# La base debe ser exacta para las imágenes
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_actual = ahora.strftime("%H-%M")
    
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Consultamos los 3 sensores disponibles
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                res = requests.get(url_sitio, headers=headers, timeout=20)
                if res.status_code != 200: continue
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Obtener VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Crear ruta (Esto ya funciona según tus fotos)
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_actual)
                os.makedirs(ruta_final, exist_ok=True)
                
                # El archivo leeme confirma que el bot llegó hasta aquí
                with open(os.path.join(ruta_final, f"log_{s_label}.txt"), "w") as f:
                    f.write(f"Intento de captura {s_label} a las {ahora.strftime('%H:%M:%S')}")

                # --- NUEVA LÓGICA DE DESCARGA INFALIBLE ---
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last', 'output']):
                        # Limpieza extrema de la URL
                        # Si la URL es '../images/foto.png', la convertimos a 'https://www.mirovaweb.it/images/foto.png'
                        path_limpio = src.replace('../', '').lstrip('/')
                        img_url = f"{BASE_URL}/{path_limpio}"
                        
                        try:
                            img_res = requests.get(img_url, headers=headers, stream=True, timeout=15)
                            if img_res.status_code == 200:
                                ext = "png" if "png" in src.lower() else "jpg"
                                nombre_f = f"{s_label}_img_{i}.{ext}"
                                with open(os.path.join(ruta_final, nombre_f), 'wb') as f:
                                    f.write(img_res.content)
                                print(f"Éxito: {nombre_f}")
                        except:
                            continue
                
                registros_ciclo.append({
                    "Volcan": nombre_v, "Sensor": s_label, "VRP_MW": vrp,
                    "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })
                time.sleep(1) # Pausa para evitar bloqueos

            except Exception as e:
                print(f"Error en {nombre_v}: {e}")

    # Guardado del CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            pd.concat([df_antiguo, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
