import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# --- CONFIGURACIÓN DE VOLCANES CHILENOS ---
LISTA_VOLCANES = [
    "355100", # Lascar
    "357120", # Villarrica
    "357110", # Llaima
    "357060", # Nevados de Chillán
    "357080", # Copahue
    "357150", # Puyehue-Cordón Caulle
    "358020", # Calbuco
    "357040", # Planchón-Peteroa
    "358010", # Osorno
    "358050"  # Hudson
]

BASE_URL = "https://www.mirovaweb.it/NRT/"
DB_FILE = "registro_vrp.csv"
IMG_FOLDER = "imagenes"

def procesar_volcan(volcan_id):
    url_pagina = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={volcan_id}"
    print(f"Revisando Volcán ID: {volcan_id}...")
    
    try:
        res = requests.get(url_pagina, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Buscar el enlace a los datos CSV
        link_data = soup.find('a', href=lambda h: h and "get_data.php" in h)
        if not link_data:
            return None

        data_url = BASE_URL + link_data['href']
        df_mirova = pd.read_csv(data_url, sep=None, engine='python')
        
        if df_mirova.empty:
            return None
            
        ultima = df_mirova.iloc[-1]
        fecha_txt = str(ultima['DATE'])
        hora_txt = str(ultima['TIME'])
        vrp_val = float(ultima['VRP(MW)'])
        
        # ID único combinando Volcán + Fecha + Hora
        id_deteccion = f"{volcan_id}_{fecha_txt}_{hora_txt}".replace("/", "-").replace(" ", "")
        
        # Verificar si ya lo tenemos registrado
        if os.path.exists(DB_FILE):
            db_local = pd.read_csv(DB_FILE)
            if id_deteccion in db_local['ID'].astype(str).values:
                return None

        # Solo guardamos si hay actividad térmica (VRP > 0)
        if vrp_val > 0:
            img_tag = soup.find('img', {'id': 'big_graph'})
            img_name = "No disponible"
            if img_tag:
                img_url = BASE_URL + img_tag['src']
                img_name = f"VRP_{id_deteccion}.png"
                img_data = requests.get(img_url).content
                with open(os.path.join(IMG_FOLDER, img_name), 'wb') as f:
                    f.write(img_data)
            
            return {
                'ID': id_deteccion,
                'Volcan_ID': volcan_id,
                'Fecha': fecha_txt,
                'Hora': hora_txt,
                'VRP_MW': vrp_val,
                'Imagen': img_name
            }
    except Exception as e:
        print(f"Error procesando {volcan_id}: {e}")
    return None

def ejecutar_total():
    if not os.path.exists(IMG_FOLDER): os.makedirs(IMG_FOLDER)
    
    nuevos_datos = []
    for vid in LISTA_VOLCANES:
        resultado = procesar_volcan(vid)
        if resultado:
            nuevos_datos.append(resultado)
    
    if nuevos_datos:
        df_nuevos = pd.DataFrame(nuevos_datos)
        if os.path.exists(DB_FILE):
            df_final = pd.concat([pd.read_csv(DB_FILE), df_nuevos], ignore_index=True)
        else:
            df_final = df_nuevos
        df_final.to_csv(DB_FILE, index=False)
        print(f"Éxito: Se registraron {len(nuevos_datos)} nuevas alertas térmicas.")
    else:
        print("Sin novedades en los volcanes seleccionados.")

if __name__ == "__main__":
    ejecutar_total()
