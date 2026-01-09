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
    headers = {'User-Agent': 'Mozilla/5.0'}
    nuevos_datos = []

    for vid, nombre_v in VOLCANES.items():
        print(f"Procesando {nombre_v}...")
        url_detalles = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={vid}"
        url_csv_datos = f"{BASE_URL}get_data.php?volcano_id={vid}"
        
        try:
            # 1. PARTE DE IMÁGENES (Tu código funcional)
            res = requests.get(url_detalles, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            imgs = soup.find_all('img')
            
            ruta = os.path.join("imagenes", nombre_v)
            if not os.path.exists(ruta): os.makedirs(ruta)
            
            for i, img in enumerate(imgs):
                src = img.get('src')
                if not src or not any(x in src.lower() for x in ['temp', 'map', 'last']): continue
                img_url = src if src.startswith('http') else BASE_URL + src
                img_data = requests.get(img_url, headers=headers).content
                ext = src.split('.')[-1][:3]
                with open(os.path.join(ruta, f"archivo_{i}.{ext}"), 'wb') as f:
                    f.write(img_data)

            # 2. PARTE DE BASE DE DATOS (Lo nuevo)
            # Intentamos leer los valores de calor (VRP)
            df_mirova = pd.read_csv(url_csv_datos, sep=None, engine='python')
            if not df_mirova.empty:
                ultima = df_mirova.iloc[-1] # La fila más reciente
                nuevos_datos.append({
                    "Fecha_UT": ultima['DATE'],
                    "Hora_UT": ultima['TIME'],
                    "Volcan": nombre_v,
                    "VRP_MW": ultima['VRP(MW)'],
                    "Captura_Local": datetime.now().strftime("%Y-%m-%d %H:%M")
                })
                print(f"Dato VRP encontrado para {nombre_v}: {ultima['VRP(MW)']} MW")

        except Exception as e:
            print(f"Error en {nombre_v}: {e}")

    # 3. GUARDAR O ACTUALIZAR EL CSV
    if nuevos_datos:
        df_nuevo = pd.DataFrame(nuevos_datos)
        if os.path.exists(DB_FILE):
            df_existente = pd.read_csv(DB_FILE)
            # Combinamos y eliminamos duplicados para no repetir la misma fecha
            df_final = pd.concat([df_existente, df_nuevo]).drop_duplicates(subset=['Volcan', 'Fecha_UT', 'Hora_UT'])
            df_final.to_csv(DB_FILE, index=False)
        else:
            df_nuevo.to_csv(DB_FILE, index=False)
        print("Base de datos 'registro_vrp.csv' actualizada.")

if __name__ == "__main__":
    procesar()

