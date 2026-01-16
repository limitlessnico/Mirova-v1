import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time

# --- CONFIGURACIÓN MAESTRA ---
VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "id_mirova": "Lascar", "limite_km": 5.0},
    "355120": {"nombre": "Lastarria", "id_mirova": "Lastarria", "limite_km": 3.0},
    "355030": {"nombre": "Isluga", "id_mirova": "Isluga", "limite_km": 5.0},
    "357120": {"nombre": "Villarrica", "id_mirova": "Villarrica", "limite_km": 5.0},
    "357110": {"nombre": "Llaima", "id_mirova": "Llaima", "limite_km": 5.0},
    "357070": {"nombre": "Nevados de Chillan", "id_mirova": "ChillanNevadosde", "limite_km": 5.0},
    "357090": {"nombre": "Copahue", "id_mirova": "Copahue", "limite_km": 4.0},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "id_mirova": "PuyehueCordonCaulle", "limite_km": 20.0},
    "358041": {"nombre": "Chaiten", "id_mirova": "Chaiten", "limite_km": 5.0},
    "357040": {"nombre": "Peteroa", "id_mirova": "PlanchonPeteroa", "limite_km": 3.0}
}

# --- LÓGICA DE CLASIFICACIÓN MIROVA (Corregida) ---
def obtener_clasificacion_mirova(vrp_mw, es_alerta):
    if not es_alerta or vrp_mw <= 0:
        return "NULO"
    
    # Escala estándar de MIROVA en Watts (VRP_MW * 1e6)
    v = vrp_mw * 1000000
    if v < 1e6: return "Muy Bajo"
    if 1e6 <= v < 1e7: return "Bajo"
    if 1e7 <= v < 1e8: return "Moderado"
    if 1e8 <= v < 1e9: return "Alto"
    return "Muy Alto"

# ... (Funciones log_debug y descargar_v104 se mantienen iguales) ...

def procesar():
    # ... (Inicio de carga de archivos igual) ...
    
    nuevos_datos = []
    for fila in filas:
        # ... (Extracción de cols y datos básicos igual) ...
        vrp, dist, sensor = float(cols[3].text.strip()), float(cols[4].text.strip()), cols[5].text.strip()
        
        # --- LÓGICA DE TIPO DE REGISTRO (Corregida para Falsos Positivos) ---
        es_dentro_rango = dist <= conf["limite_km"]
        
        if vrp > 0:
            if es_dentro_rango:
                tipo = "ALERTA_TERMICA"
                es_alerta_real = True
            else:
                tipo = "FALSO_POSITIVO" # Restaurada la etiqueta
                es_alerta_real = False
        else:
            tipo = "EVIDENCIA_DIARIA" if sensor == "VIIRS375" else "RUTINA"
            es_alerta_real = False

        # --- CLASIFICACIÓN MIROVA (Corregida) ---
        clasificacion = obtener_clasificacion_mirova(vrp, es_alerta_real)

        # ... (Resto de la lógica de descubrimiento y descarga igual) ...

        nuevos_datos.append({
            "timestamp": ts, 
            "Fecha_Satelite_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "Fecha_Captura_Chile": dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S"),
            "Volcan": volcan_nombre, 
            "Sensor": sensor, 
            "VRP_MW": vrp, 
            "Distancia_km": dist,
            "Tipo_Registro": tipo, # Ahora incluye FALSO_POSITIVO
            "Clasificacion Mirova": clasificacion, # Ahora dinámica
            "Ruta Foto": ruta_foto, 
            "Fecha_Proceso_GitHub": f_descubrimiento,
            "Ultima_Actualizacion": ahora_cl,
            "Editado": editado
        })

    # ... (Guardado de CSVs igual) ...
