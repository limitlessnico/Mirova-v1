"""
SCRAPER_OCR.PY
Scraper de imágenes MIROVA usando OCR
Recupera eventos perdidos por latest.php
"""

import requests
import os
import pandas as pd
from datetime import datetime
import pytz
import time
from ocr_utils import (
    extraer_eventos_latest10nti,
    analizar_puntos_distancia,
    clasificar_confianza
)

# =========================
# CONFIGURACIÓN
# =========================

VOLCANES_CONFIG = {
    "355100": {"nombre": "Lascar", "id_mirova": "Lascar"},
    "355120": {"nombre": "Lastarria", "id_mirova": "Lastarria"},
    "355030": {"nombre": "Isluga", "id_mirova": "Isluga"},
    "357120": {"nombre": "Villarrica", "id_mirova": "Villarrica"},
    "357110": {"nombre": "Llaima", "id_mirova": "Llaima"},
    "357070": {"nombre": "Nevados de Chillan", "id_mirova": "ChillanNevadosde"},
    "357090": {"nombre": "Copahue", "id_mirova": "Copahue"},
    "357150": {"nombre": "Puyehue-Cordon Caulle", "id_mirova": "PuyehueCordonCaulle"},
    "358041": {"nombre": "Chaiten", "id_mirova": "Chaiten"},
    "357040": {"nombre": "PlanchonPeteroa", "id_mirova": "PlanchonPeteroa"}
}

SENSORES = ["VIIRS375", "VIIRS", "MODIS"]

CARPETA_PRINCIPAL = "monitoreo_satelital"
CARPETA_TEMP = os.path.join(CARPETA_PRINCIPAL, "ocr_temp")
CARPETA_IMAGENES = os.path.join(CARPETA_PRINCIPAL, "imagenes_satelitales")
CARPETA_LOGS = os.path.join(CARPETA_PRINCIPAL, "ocr_logs")

DB_OCR = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_ocr.csv")
DB_CONSOLIDADO = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")

COLUMNAS_OCR = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile",
    "Volcan", "Sensor", "VRP_MW", "Tipo_Registro",
    "Color_Punto_Dist", "Confianza_Validacion", "Requiere_Verificacion",
    "Metodo_Validacion", "Nota_Validacion",
    "Ruta_Foto", "Fecha_Proceso_GitHub", "Version_OCR"
]

# =========================
# FUNCIONES
# =========================

def descargar_imagen_temp(session, url, ruta_destino):
    """Descarga imagen temporal"""
    try:
        r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=25)
        if r.status_code == 200 and len(r.content) > 5000:
            with open(ruta_destino, 'wb') as f:
                f.write(r.content)
            return True
    except:
        pass
    return False


def descargar_imagenes_permanentes(session, volcan_id, sensor, evento, es_verificar):
    """Descarga y guarda imágenes permanentes"""
    conf = VOLCANES_CONFIG[volcan_id]
    nombre_v = conf["nombre"]
    # Normalizar nombre para rutas (sin espacios ni guiones)
    nombre_v_normalizado = nombre_v.replace(' ', '_').replace('-', '_')
    id_mirova = conf["id_mirova"]
    
    dt_utc = evento['datetime']
    f_c = dt_utc.strftime("%Y-%m-%d")
    h_a = dt_utc.strftime("%H-%M-%S")
    
    ruta_dia = os.path.join(CARPETA_IMAGENES, nombre_v_normalizado, f_c)
    os.makedirs(ruta_dia, exist_ok=True)
    
    s_url = "VIIRS750" if sensor == "VIIRS" else sensor
    sufijo = "_VERIFICAR" if es_verificar else ""
    
    tipos = ["VRP", "logVRP", "Latest10NTI", "Dist"]
    ruta_relativa = "No descargada"
    
    for t in tipos:
        t_url = f"{t}10NTI" if t == "Latest10NTI" else t
        url = f"https://www.mirovaweb.it/OUTPUTweb/MIROVA/{s_url}/VOLCANOES/{id_mirova}/{id_mirova}_{s_url}_{t_url}.png"
        
        filename = f"{h_a}_{nombre_v_normalizado}_{s_url}_{t}{sufijo}.png"
        path_f = os.path.join(ruta_dia, filename)
        
        # No descargar si ya existe
        if os.path.exists(path_f):
            if t == "VRP":
                ruta_relativa = f"imagenes_satelitales/{nombre_v_normalizado}/{f_c}/{filename}"
            continue
        
        try:
            r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=25)
            if r.status_code == 200 and len(r.content) > 5000:
                with open(path_f, 'wb') as f:
                    f.write(r.content)
                if t == "VRP":
                    ruta_relativa = f"imagenes_satelitales/{nombre_v_normalizado}/{f_c}/{filename}"
            time.sleep(0.3)
        except:
            continue
    
    return ruta_relativa


def procesar_volcan_sensor(session, volcan_id, sensor, df_ocr, df_consolidado):
    """Procesa un volcán-sensor específico"""
    conf = VOLCANES_CONFIG[volcan_id]
    nombre_v = conf["nombre"]
    id_mirova = conf["id_mirova"]
    
    print(f"\n🔍 Procesando: {nombre_v} - {sensor}")
    
    # URLs de imágenes
    s_url = "VIIRS750" if sensor == "VIIRS" else sensor
    url_latest = f"https://www.mirovaweb.it/OUTPUTweb/MIROVA/{s_url}/VOLCANOES/{id_mirova}/{id_mirova}_{s_url}_Latest10NTI.png"
    url_dist = f"https://www.mirovaweb.it/OUTPUTweb/MIROVA/{s_url}/VOLCANOES/{id_mirova}/{id_mirova}_{s_url}_Dist.png"
    
    # Descargar temporales
    temp_latest = os.path.join(CARPETA_TEMP, f"{nombre_v}_{sensor}_Latest10NTI.png")
    temp_dist = os.path.join(CARPETA_TEMP, f"{nombre_v}_{sensor}_Dist.png")
    
    if not descargar_imagen_temp(session, url_latest, temp_latest):
        print(f"  ⚠️ No se pudo descargar Latest10NTI")
        return []
    
    if not descargar_imagen_temp(session, url_dist, temp_dist):
        print(f"  ⚠️ No se pudo descargar Dist.png")
        # Continuar sin validación de distancia
    
    # OCR de Latest10NTI
    eventos = extraer_eventos_latest10nti(temp_latest)
    
    if not eventos:
        print(f"  ℹ️ No se detectaron eventos")
        return []
    
    # Análisis RGB de Dist.png
    if os.path.exists(temp_dist):
        eventos = analizar_puntos_distancia(temp_dist, eventos)
    
    # Procesar cada evento
    eventos_nuevos = []
    ahora_cl = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    
    for evento in eventos:
        ts = evento['timestamp']
        vrp_mw = evento['vrp_mw']
        
        # Verificar si ya existe en OCR
        existe_ocr = df_ocr[
            (df_ocr['timestamp'] == ts) &
            (df_ocr['Volcan'] == nombre_v) &
            (df_ocr['Sensor'] == sensor)
        ]
        
        if not existe_ocr.empty:
            print(f"  ⏭️ SKIP: {ts} ya en OCR")
            continue
        
        # Verificar si ya existe en consolidado
        existe_consolidado = df_consolidado[
            (df_consolidado['timestamp'] == ts) &
            (df_consolidado['Volcan'] == nombre_v) &
            (df_consolidado['Sensor'] == sensor)
        ]
        
        if not existe_consolidado.empty:
            print(f"  ⏭️ SKIP: {ts} ya en latest.php")
            continue
        
        # Clasificar confianza
        clasificacion = clasificar_confianza(evento)
        
        if not clasificacion['guardar']:
            print(f"  ❌ SKIP: {ts} - {clasificacion['nota']}")
            continue
        
        # Descargar imágenes si es válido
        es_verificar = clasificacion['requiere_verificacion']
        ruta_foto = descargar_imagenes_permanentes(
            session, volcan_id, sensor, evento, es_verificar
        )
        
        # Agregar evento
        dt_utc = evento['datetime']
        eventos_nuevos.append({
            'timestamp': ts,
            'Fecha_Satelite_UTC': dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            'Fecha_Captura_Chile': dt_utc.replace(tzinfo=pytz.utc).astimezone(
                pytz.timezone('America/Santiago')
            ).strftime("%Y-%m-%d %H:%M:%S"),
            'Volcan': nombre_v,
            'Sensor': sensor,
            'VRP_MW': vrp_mw,
            'Tipo_Registro': 'ALERTA_TERMICA_OCR',
            'Color_Punto_Dist': evento.get('color_punto', 'sin_punto'),
            'Confianza_Validacion': clasificacion['confianza'],
            'Requiere_Verificacion': clasificacion['requiere_verificacion'],
            'Metodo_Validacion': evento.get('metodo', 'desconocido'),
            'Nota_Validacion': clasificacion['nota'],
            'Ruta_Foto': ruta_foto,
            'Fecha_Proceso_GitHub': ahora_cl,
            'Version_OCR': '1.0'
        })
        
        print(f"  ✅ NUEVO: {ts} - VRP={vrp_mw:.2f} MW - {clasificacion['confianza']}")
    
    return eventos_nuevos


def procesar():
    """Proceso principal"""
    os.makedirs(CARPETA_PRINCIPAL, exist_ok=True)
    os.makedirs(CARPETA_TEMP, exist_ok=True)
    os.makedirs(CARPETA_LOGS, exist_ok=True)
    
    print("="*80)
    print("🔬 SCRAPER OCR - INICIO")
    print("="*80)
    
    session = requests.Session()
    
    # Cargar CSVs existentes
    df_ocr = pd.read_csv(DB_OCR) if os.path.exists(DB_OCR) else pd.DataFrame(columns=COLUMNAS_OCR)
    df_consolidado = pd.read_csv(DB_CONSOLIDADO) if os.path.exists(DB_CONSOLIDADO) else pd.DataFrame()
    
    todos_eventos_nuevos = []
    
    # Procesar cada volcán × sensor
    for volcan_id in VOLCANES_CONFIG.keys():
        for sensor in SENSORES:
            try:
                eventos_nuevos = procesar_volcan_sensor(
                    session, volcan_id, sensor, df_ocr, df_consolidado
                )
                todos_eventos_nuevos.extend(eventos_nuevos)
            except Exception as e:
                print(f"❌ Error en {VOLCANES_CONFIG[volcan_id]['nombre']} {sensor}: {e}")
                continue
    
    # Guardar eventos nuevos
    if todos_eventos_nuevos:
        df_nuevos = pd.DataFrame(todos_eventos_nuevos)
        df_ocr_final = pd.concat([df_ocr, df_nuevos], ignore_index=True)
        df_ocr_final = df_ocr_final[COLUMNAS_OCR].sort_values('timestamp', ascending=False)
        df_ocr_final.to_csv(DB_OCR, index=False)
        
        print(f"\n✅ Se agregaron {len(todos_eventos_nuevos)} eventos nuevos")
    else:
        print("\nℹ️ No hay eventos nuevos para agregar")
    
    # Limpiar temporales
    import shutil
    if os.path.exists(CARPETA_TEMP):
        shutil.rmtree(CARPETA_TEMP)
        os.makedirs(CARPETA_TEMP)
    
    print("\n✅ Proceso completado")
    print("="*80)


if __name__ == "__main__":
    procesar()
