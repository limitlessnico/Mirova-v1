import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

# Configuración de volcanes
VOLCANES = {
    "355100": "Lascar", 
    "357120": "Villarrica", 
    "357110": "Llaima"
}
BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"

def procesar():
    headers = {'User-Agent': 'Mozilla/5.0'}
    datos_para_csv = []

    # Asegurar que el directorio base de imágenes existe
    if not os.path.exists("imagenes"):
        os.makedirs("imagenes")

    for vid, nombre_v in VOLCANES.items():
        print(f"--- Procesando {nombre_v} ---")
        url_detalles = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        
        try:
            # DESCARGA DE IMÁGENES
            res = requests.get(url_detalles, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            imgs = soup.find_all('img')
            
            ruta_v = os.path.join("imagenes", nombre_v)
            if not os.path.exists(ruta_v):
                os.makedirs(ruta_v)
            
            # Guardamos las imágenes térmicas principales
            for i, img in enumerate(imgs):
                src = img.get('src')
                if src and any(x in src.lower() for x in ['temp', 'map', 'last']):
                    img_url = src if src.startswith('http') else BASE_URL + src
                    img_data = requests.get(img_url, headers=headers).content
                    with open(os.path.join(ruta_v, f"captura_{i}.png"), 'wb') as f:
                        f.write(img_data)

            # EXTRACCIÓN DE DATOS PARA EL CSV
            # Buscamos el valor de VRP en el texto de la página (método alternativo más seguro)
            vrp_valor = "0"
            for b in soup.find_all('b'):
                if "VRP =" in b.text:
                    vrp_valor = b.text.replace("VRP =", "").strip()
                    break
            
            datos_para_csv.append({
                "Volcan": nombre_v,
                "VRP_MW": vrp_valor,
                "Ultima_Actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M")
            })

        except Exception as e:
            print(f"Error en {nombre_v}: {e}")

    # GUARDAR EL ARCHIVO CSV
    if datos_para_csv:
        df_nuevo = pd.DataFrame(datos_para_csv)
        # Si el archivo ya existe, lo cargamos y agregamos los nuevos datos
        if os.path.exists(DB_FILE):
            df_antiguo = pd.read_csv(DB_FILE)
            df_final = pd.concat([df_antiguo, df_nuevo]).drop_duplicates(subset=['Volcan', 'Ultima_Actualizacion'])
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)
        print("CSV actualizado correctamente.")

if __name__ == "__main__":
    procesar()


