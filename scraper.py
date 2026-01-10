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
    # Creamos una sesión para mantener cookies y parecer un humano
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Referer': 'https://www.mirovaweb.it/'
    })

    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    # 1. Visitar la página principal primero para obtener cookies iniciales
    try:
        session.get(BASE_URL, timeout=20)
        time.sleep(2)
    except:
        pass

    for vid, nombre_v in VOLCANES.items():
        # IMPORTANTE: Procesamos los sensores uno por uno con pausas largas
        for modo in ["MOD", "VIRS", "VIRS375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS" if modo == "VIRS" else "VIIRS-375m")
            url_detalles = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"Intentando: {nombre_v} - {s_label}...")
                time.sleep(10) # Pausa larga para no ser detectado
                
                res = session.get(url_detalles, timeout=30)
                if res.status_code != 200: continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)
                os.makedirs(ruta, exist_ok=True)

                # Extraer VRP
                vrp_valor = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp_valor = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # Descarga de imágenes mejorada
                imgs_descargadas = 0
                for i, img in enumerate(soup.find_all('img')):
                    src = img.get('src')
                    if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                        img_url = f"{BASE_URL}/{src.replace('../', '').lstrip('/')}"
                        
                        # Intentar la descarga de la imagen con la sesión activa
                        try:
                            time.sleep(2) # Pausa entre fotos
                            img_data = session.get(img_url, timeout=15).content
                            if len(img_data) > 3000: # Evitar archivos de error pequeños
                                ext = "png" if "png" in src.lower() else "jpg"
                                with open(os.path.join(ruta, f"{s_label}_{i}.{ext}"), 'wb') as f:
                                    f.write(img_data)
                                imgs_descargadas += 1
                        except:
                            continue

                # Guardar datos (asegurando todas las columnas)
                registros_ciclo.append({
                    "Volcan": nombre_v,
                    "Estado": "Actualizado",
                    "VRP_MW": vrp_valor,
                    "Ultima_Actualizacion": ahora.strftime("%Y-%m-%d %H:%M"),
                    "Sensor": s_label,
                    "Fecha": fecha_hoy,
                    "Hora": ahora.strftime("%H:%M:%S")
                })
                print(f"Éxito: {nombre_v} {s_label} ({imgs_descargadas} fotos)")

            except Exception as e:
                print(f"Error en {nombre_v} {s_label}: {e}")

    # Actualizar CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE):
            df_base = pd.read_csv(DB_FILE)
            # Aseguramos que el CSV mantenga su estructura sin N/As raros
            df_final = pd.concat([df_base, df_nuevo], ignore_index=True).drop_duplicates()
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)

if __name__ == "__main__":
    procesar()
