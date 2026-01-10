import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    # Headers más humanos para evitar bloqueos
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.mirovaweb.it/'
    }
    
    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                # PAUSA CRÍTICA: Evita que el servidor nos bloquee los sensores VIIRS
                time.sleep(5) 
                res = requests.get(url_sitio, headers=headers, timeout=30)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 1. Carpeta con ruta absoluta para GitHub
                ruta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta, exist_ok=True)

                # 2. Descarga de imágenes mejorada
                imagenes_encontradas = 0
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        # Limpieza total de URL
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        try:
                            img_res = requests.get(img_url, headers=headers, timeout=20)
                            if img_res.status_code == 200 and len(img_res.content) > 1500:
                                ext = "png" if "png" in src.lower() else "jpg"
                                nombre_archivo = f"{s_label}_img_{i}.{ext}"
                                with open(os.path.join(ruta, nombre_archivo), 'wb') as f:
                                    f.write(img_res.content)
                                imagenes_encontradas += 1
                        except:
                            continue

                # 3. Extraer VRP con seguridad
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 4. CSV: Registro con TODAS las columnas llenas
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"OK: {nombre_v} - {s_label} ({imagenes_encontradas} imgs)")

            except Exception as e:
                print(f"Error en {nombre_v} {s_label}: {e}")

    # 5. Guardado del CSV (Previene celdas vacías)
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            # Forzamos que las columnas se alineen perfectamente
            df_final = pd.concat([df_antiguo, df_nuevo], ignore_index=True).fillna("N/A")
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
