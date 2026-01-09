import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

# Configuración del Volcán (Etna - 355100)
VOLCANO_ID = "355100"
BASE_URL = "https://www.mirovaweb.it/NRT/"
URL_PAGINA = f"{BASE_URL}volcanoDetails_MOD.php?volcano_id={VOLCANO_ID}"
DB_FILE = "registro_vrp.csv"
IMG_FOLDER = "imagenes"

def ejecutar_scraping():
    print(f"Iniciando revisión: {datetime.now()}")
    
    # 1. Obtener la página principal para buscar el archivo de datos
    res = requests.get(URL_PAGINA)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # Buscamos el link que contiene 'get_data.php'
    link_data = soup.find('a', href=lambda h: h and "get_data.php" in h)
    
    if not link_data:
        print("No se encontró el enlace de datos.")
        return

    data_url = BASE_URL + link_data['href']
    
    # 2. Descargar y leer el CSV de Mirova
    # Usamos sep=None para que pandas detecte si es coma o tabulación
    try:
        df_mirova = pd.read_csv(data_url, sep=None, engine='python')
        ultima_deteccion = df_mirova.iloc[-1]
        
        fecha_txt = str(ultima_deteccion['DATE'])
        hora_txt = str(ultima_deteccion['TIME'])
        vrp_val = float(ultima_deteccion['VRP(MW)'])
        id_deteccion = f"{fecha_txt}_{hora_txt}".replace("/", "-")
    except Exception as e:
        print(f"Error procesando datos: {e}")
        return

    # 3. Verificar si ya lo tenemos registrado
    if os.path.exists(DB_FILE):
        db_local = pd.read_csv(DB_FILE)
        if id_deteccion in db_local['ID'].values:
            print(f"Detección {id_deteccion} ya registrada. Nada que hacer.")
            return

    # 4. Si el VRP es mayor a 0, guardamos imagen y dato
    if vrp_val > 0:
        print(f"¡VRP detectado! Valor: {vrp_val} MW")
        
        # Intentar buscar la imagen del gráfico principal
        img_tag = soup.find('img', {'id': 'big_graph'})
        if img_tag:
            img_url = BASE_URL + img_tag['src']
            img_name = f"VRP_{id_deteccion}.png"
            img_path = os.path.join(IMG_FOLDER, img_name)
            
            # Bajar imagen
            img_data = requests.get(img_url).content
            with open(img_path, 'wb') as f:
                f.write(img_data)
        
        # Guardar en base de datos CSV
        nuevo_registro = pd.DataFrame([{
            'ID': id_deteccion,
            'Fecha': fecha_txt,
            'Hora': hora_txt,
            'VRP_MW': vrp_val,
            'Imagen': img_name if img_tag else "No disponible"
        }])
        
        if os.path.exists(DB_FILE):
            db_final = pd.concat([pd.read_csv(DB_FILE), nuevo_registro], ignore_index=True)
        else:
            db_final = nuevo_registro
            
        db_final.to_csv(DB_FILE, index=False)
        print("Datos guardados exitosamente.")

if __name__ == "__main__":
    if not os.path.exists(IMG_FOLDER): os.makedirs(IMG_FOLDER)
    ejecutar_scraping()