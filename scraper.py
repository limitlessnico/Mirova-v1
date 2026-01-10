import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import time
import random

VOLCANES = {"355100": "Lascar", "357120": "Villarrica", "357110": "Llaima"}
BASE_URL = "https://www.mirovaweb.it"
DB_FILE = "registro_vrp.csv"

def procesar():
    if not os.path.exists('imagenes'):
        os.makedirs('imagenes', exist_ok=True)

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': BASE_URL
    })

    ahora = datetime.now()
    fecha_hoy = ahora.strftime("%Y-%m-%d")
    hora_carpeta = ahora.strftime("%H-%M")
    registros_ciclo = []

    for vid, nombre_v in VOLCANES.items():
        # Iteramos los 3 sensores
        for modo in ["MOD", "VIR", "VIR375"]:
            s_label = "MODIS" if modo == "MOD" else ("VIIRS-750m" if modo == "VIR" else "VIIRS-375m")
            url_sitio = f"{BASE_URL}/NRT/volcanoDetails_{modo}.php?volcano_id={vid}"
            
            try:
                print(f"📡 Conectando: {nombre_v} - {s_label}...")
                time.sleep(random.uniform(15, 25)) # Pausa de seguridad
                
                res = session.get(url_sitio, timeout=45)
                if res.status_code != 200: 
                    print(f"❌ Error HTTP {res.status_code} en {s_label}")
                    continue
                
                soup = BeautifulSoup(res.text, 'html.parser')
                ruta_final = os.path.join("imagenes", nombre_v, fecha_hoy, hora_carpeta)

                # 1. Extraer VRP
                vrp = "0"
                for b in soup.find_all('b'):
                    if "VRP =" in b.text:
                        vrp = b.text.split('=')[-1].replace('MW', '').strip()
                        break

                # 2. Descarga de Imágenes (Lógica "Red de Arrastre")
                descargas = 0
                os.makedirs(ruta_final, exist_ok=True)

                # Buscamos TODAS las etiquetas de imagen y enlaces
                tags = soup.find_all(['img', 'a'])
                
                for tag in tags:
                    src = tag.get('src') or tag.get('href')
                    if not src or not isinstance(src, str): continue
                    
                    # --- MODIFICACIÓN CRÍTICA: FILTRO RELAJADO ---
                    # Ya no filtramos por palabras clave estrictas como 'vir' o 'modis'.
                    # Ahora aceptamos cualquier archivo que parezca una imagen por su extensión.
                    ext_validas = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']
                    if any(ext in src.lower() for ext in ext_validas):
                        
                        # Construir URL absoluta
                        if src.startswith('http'):
                            img_url = src
                        else:
                            # Limpieza de rutas relativas (../)
                            clean_src = src.replace('../', '').lstrip('/')
                            img_url = f"{BASE_URL}/{clean_src}"
                        
                        try:
                            # Pausa micro para no saturar
                            time.sleep(1) 
                            img_res = session.get(img_url, timeout=15)
                            
                            # --- FILTRO DE TAMAÑO (El verdadero guardián) ---
                            # Si pesa más de 2500 bytes (2.5KB), asumimos que es un mapa/gráfico real.
                            # Esto descarta íconos pequeños, flechas, líneas, etc.
                            if img_res.status_code == 200 and len(img_res.content) > 2500:
                                
                                # Determinar extensión real
                                ext = "png" if ".png" in src.lower() else "jpg"
                                
                                # Guardar archivo
                                nombre_f = f"{s_label}_img_{descargas}.{ext}"
                                ruta_completa = os.path.join(ruta_final, nombre_f)
                                
                                with open(ruta_completa, 'wb') as f:
                                    f.write(img_res.content)
                                
                                print(f"   ✅ Guardada: {nombre_f} ({len(img_res.content)//1024} KB)")
                                descargas += 1
                                
                        except Exception as e_img:
                            # Ignoramos errores puntuales de imágenes para seguir con la siguiente
                            continue

                registros_ciclo.append({
                    "Volcan": nombre_v, "Estado": "Ok", "VRP_MW": vrp,
                    "Sensor": s_label, "Fotos_Guardadas": descargas,
                    "Fecha": fecha_hoy, "Hora": ahora.strftime("%H:%M:%S")
                })

            except Exception as e:
                print(f"⚠️ Error General en {nombre_v}: {e}")

    # Guardado del CSV
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
            df_base = pd.read_csv(DB_FILE)
            pd.concat([df_base, df_nuevo], ignore_index=True).to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)
        print("\n📄 CSV Actualizado.")

if __name__ == "__main__":
    procesar()
