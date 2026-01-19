import os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import glob
from collections import defaultdict

# ============================================================================
# ANÁLISIS BLACK BOX v3 - SOLO VOLCANES CHILENOS
# ============================================================================

CARPETA_BLACKBOX = "monitoreo_satelital/blackbox_latest"
DB_MASTER = "monitoreo_satelital/registro_vrp_consolidado.csv"
REPORTE_FINAL = "monitoreo_satelital/analisis_blackbox_20260120.csv"
REPORTE_PERDIDOS = "monitoreo_satelital/eventos_perdidos_confirmados.csv"

# ⭐ CRÍTICO: SOLO ESTOS 10 VOLCANES CHILENOS
VOLCANES_CHILENOS = {
    "Isluga", "Lascar", "Lastarria", "PlanchonPeteroa", 
    "Nevados de Chillan", "Copahue", "Llaima", 
    "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"
}

# Mapeo de posibles variaciones de nombres en MIROVA
MAPEO_NOMBRES = {
    "Peteroa": "PlanchonPeteroa",
    "Planchon-Peteroa": "PlanchonPeteroa",
    "ChillanNevadosde": "Nevados de Chillan",
    "Nevados de Chillán": "Nevados de Chillan",
    "PuyehueCordonCaulle": "Puyehue-Cordon Caulle",
    "Puyehue": "Puyehue-Cordon Caulle"
}

def log_info(mensaje):
    """Logger con timestamp"""
    ahora = datetime.now(pytz.timezone('America/Santiago'))
    print(f"[{ahora.strftime('%H:%M:%S')}] {mensaje}")

def normalizar_nombre_volcan(nombre):
    """
    Normaliza nombres de volcanes para comparación
    Retorna None si no es un volcán chileno
    """
    # Aplicar mapeo si existe
    nombre_norm = MAPEO_NOMBRES.get(nombre, nombre)
    
    # Verificar si es volcán chileno
    if nombre_norm in VOLCANES_CHILENOS:
        return nombre_norm
    
    return None

def cargar_snapshots_blackbox():
    """
    Carga SOLO eventos de volcanes chilenos de Black Box
    """
    log_info("📂 Cargando snapshots Black Box...")
    
    archivos_html = sorted(glob.glob(os.path.join(CARPETA_BLACKBOX, "*/*.html")))
    
    if not archivos_html:
        log_info("❌ No se encontraron archivos HTML en Black Box")
        return None
    
    log_info(f"   Encontrados: {len(archivos_html)} archivos HTML")
    
    # Diccionario: event_key -> {info del evento}
    eventos_chilenos = {}
    snapshots_procesados = 0
    eventos_descartados = 0
    errores = 0
    
    for archivo_html in archivos_html:
        # Extraer timestamp del nombre
        nombre = os.path.basename(archivo_html)
        try:
            partes = nombre.replace('latest_', '').replace('.html', '').split('_')
            fecha_snap = datetime.strptime(f"{partes[0]}_{partes[1]}", "%Y%m%d_%H%M%S")
            fecha_snap = fecha_snap.replace(tzinfo=pytz.utc)
        except:
            continue
        
        # Parsear HTML
        try:
            with open(archivo_html, 'r', encoding='utf-8') as f:
                html = f.read()
            
            soup = BeautifulSoup(html, 'html.parser')
            tbody = soup.find('tbody')
            
            if not tbody:
                errores += 1
                continue
            
            filas = tbody.find_all('tr')
            
            for fila in filas:
                cols = fila.find_all('td')
                if len(cols) < 6:
                    continue
                
                try:
                    fecha_evento_str = cols[0].text.strip()
                    volcan_id = cols[1].text.strip()
                    volcan_nombre_raw = cols[2].text.strip()
                    vrp_mw = float(cols[3].text.strip())
                    distancia_km = float(cols[4].text.strip())
                    sensor = cols[5].text.strip()
                    
                    # ⭐ FILTRO CRÍTICO: Solo volcanes chilenos
                    volcan_nombre = normalizar_nombre_volcan(volcan_nombre_raw)
                    
                    if volcan_nombre is None:
                        eventos_descartados += 1
                        continue  # Saltar volcanes no chilenos
                    
                    # Parsear fecha del evento
                    dt_evento = datetime.strptime(fecha_evento_str, "%d-%b-%Y %H:%M:%S")
                    dt_evento = dt_evento.replace(tzinfo=pytz.utc)
                    ts_evento = int(dt_evento.timestamp())
                    
                    # Key única del evento
                    event_key = (ts_evento, volcan_nombre, sensor)
                    
                    # Registrar evento
                    if event_key not in eventos_chilenos:
                        eventos_chilenos[event_key] = {
                            'timestamp': ts_evento,
                            'fecha_evento_utc': dt_evento,
                            'volcan': volcan_nombre,
                            'volcan_id': volcan_id,
                            'sensor': sensor,
                            'vrp_mw': vrp_mw,
                            'distancia_km': distancia_km,
                            'primera_aparicion': fecha_snap,
                            'ultima_aparicion': fecha_snap,
                            'veces_visto': 1,
                            'snapshots': [fecha_snap]
                        }
                    else:
                        # Actualizar registro existente
                        eventos_chilenos[event_key]['ultima_aparicion'] = fecha_snap
                        eventos_chilenos[event_key]['veces_visto'] += 1
                        eventos_chilenos[event_key]['snapshots'].append(fecha_snap)
                
                except Exception as e:
                    continue
            
            snapshots_procesados += 1
            
            # Progreso cada 500 snapshots
            if snapshots_procesados % 500 == 0:
                log_info(f"   Procesados: {snapshots_procesados} / {len(archivos_html)}")
        
        except Exception as e:
            errores += 1
            continue
    
    log_info(f"✅ Snapshots procesados: {snapshots_procesados}")
    log_info(f"   Eventos volcanes chilenos: {len(eventos_chilenos)}")
    log_info(f"   Eventos descartados (internacionales): {eventos_descartados}")
    log_info(f"   Errores de parsing: {errores}")
    
    return eventos_chilenos

def cargar_eventos_capturados():
    """
    Carga eventos que SÍ fueron capturados por el scraper
    """
    log_info("📋 Cargando eventos capturados por scraper...")
    
    if not os.path.exists(DB_MASTER):
        log_info("⚠️ No existe registro_vrp_consolidado.csv")
        return pd.DataFrame()
    
    df = pd.read_csv(DB_MASTER)
    df['Fecha_Satelite_UTC'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
    
    log_info(f"   Total registros en DB: {len(df)}")
    
    return df

def comparar_blackbox_vs_capturados(eventos_blackbox, df_capturados):
    """
    Compara eventos vistos en Black Box vs eventos capturados
    SOLO volcanes chilenos
    """
    log_info("🔍 Comparando Black Box vs Registros Capturados...")
    
    eventos_perdidos = []
    eventos_capturados_ok = []
    
    for event_key, info in eventos_blackbox.items():
        ts_evento = event_key[0]
        volcan = event_key[1]
        sensor = event_key[2]
        
        # Buscar en registros capturados
        if not df_capturados.empty:
            match = df_capturados[
                (df_capturados['timestamp'] == ts_evento) &
                (df_capturados['Volcan'] == volcan) &
                (df_capturados['Sensor'] == sensor)
            ]
        else:
            match = pd.DataFrame()
        
        # Calcular tiempo de vida en buffer
        tiempo_en_buffer = (info['ultima_aparicion'] - info['primera_aparicion']).total_seconds() / 60
        
        if len(match) == 0:
            # EVENTO PERDIDO
            eventos_perdidos.append({
                'timestamp': ts_evento,
                'Fecha_Evento_UTC': info['fecha_evento_utc'].strftime("%Y-%m-%d %H:%M:%S"),
                'Volcan': volcan,
                'Volcan_ID': info['volcan_id'],
                'Sensor': sensor,
                'VRP_MW': info['vrp_mw'],
                'Distancia_km': info['distancia_km'],
                'Primera_Aparicion_Snapshot': info['primera_aparicion'].strftime("%Y-%m-%d %H:%M:%S"),
                'Ultima_Aparicion_Snapshot': info['ultima_aparicion'].strftime("%Y-%m-%d %H:%M:%S"),
                'Tiempo_En_Buffer_Minutos': round(tiempo_en_buffer, 2),
                'Veces_Visto_BlackBox': info['veces_visto'],
                'Estado': 'PERDIDO',
                'Razon_Probable': clasificar_razon_perdida(tiempo_en_buffer, info['veces_visto'])
            })
        else:
            # Evento capturado correctamente
            eventos_capturados_ok.append({
                'timestamp': ts_evento,
                'Volcan': volcan,
                'Sensor': sensor,
                'VRP_MW': info['vrp_mw'],
                'Tiempo_En_Buffer_Minutos': round(tiempo_en_buffer, 2),
                'Veces_Visto_BlackBox': info['veces_visto'],
                'Estado': 'CAPTURADO'
            })
    
    log_info(f"✅ Análisis completado:")
    log_info(f"   Eventos capturados: {len(eventos_capturados_ok)}")
    log_info(f"   Eventos perdidos: {len(eventos_perdidos)}")
    
    return eventos_perdidos, eventos_capturados_ok

def clasificar_razon_perdida(tiempo_buffer, veces_visto):
    """
    Clasifica la razón probable de pérdida de datos
    """
    if tiempo_buffer < 1.5:
        return "EXPULSION_RAPIDA (<1.5min en buffer)"
    elif tiempo_buffer < 3.0:
        return "EXPULSION_NORMAL (1.5-3min en buffer)"
    elif veces_visto < 2:
        return "APARICION_UNICA (1 snapshot solo)"
    else:
        return "ERROR_SCRAPER (estuvo suficiente tiempo)"

def generar_estadisticas(eventos_perdidos, eventos_capturados):
    """
    Genera estadísticas detalladas del análisis
    """
    log_info("\n" + "="*80)
    log_info("📊 ESTADÍSTICAS FINALES - SOLO VOLCANES CHILENOS")
    log_info("="*80)
    
    total_eventos = len(eventos_perdidos) + len(eventos_capturados)
    tasa_captura = (len(eventos_capturados) / total_eventos * 100) if total_eventos > 0 else 0
    
    print(f"\n{'MÉTRICA':<40} {'VALOR':>15}")
    print("-" * 56)
    print(f"{'Total eventos detectados:':<40} {total_eventos:>15}")
    print(f"{'Eventos capturados:':<40} {len(eventos_capturados):>15}")
    print(f"{'Eventos perdidos:':<40} {len(eventos_perdidos):>15}")
    print(f"{'Tasa de captura:':<40} {tasa_captura:>14.2f}%")
    
    if eventos_perdidos:
        df_perdidos = pd.DataFrame(eventos_perdidos)
        
        print("\n" + "="*56)
        print("ANÁLISIS DE EVENTOS PERDIDOS")
        print("="*56)
        
        # Por razón
        print(f"\n{'RAZÓN DE PÉRDIDA':<40} {'CANTIDAD':>15}")
        print("-" * 56)
        for razon, count in df_perdidos['Razon_Probable'].value_counts().items():
            print(f"{razon:<40} {count:>15}")
        
        # Por volcán
        print(f"\n{'VOLCÁN':<40} {'PÉRDIDAS':>15}")
        print("-" * 56)
        for volcan, count in df_perdidos['Volcan'].value_counts().items():
            print(f"{volcan:<40} {count:>15}")
        
        # Por sensor
        print(f"\n{'SENSOR':<40} {'PÉRDIDAS':>15}")
        print("-" * 56)
        for sensor, count in df_perdidos['Sensor'].value_counts().items():
            print(f"{sensor:<40} {count:>15}")
        
        # Estadísticas de tiempo en buffer
        print(f"\n{'ESTADÍSTICA TIEMPO EN BUFFER':<40} {'MINUTOS':>15}")
        print("-" * 56)
        print(f"{'Promedio:':<40} {df_perdidos['Tiempo_En_Buffer_Minutos'].mean():>14.2f}")
        print(f"{'Mediana:':<40} {df_perdidos['Tiempo_En_Buffer_Minutos'].median():>14.2f}")
        print(f"{'Mínimo:':<40} {df_perdidos['Tiempo_En_Buffer_Minutos'].min():>14.2f}")
        print(f"{'Máximo:':<40} {df_perdidos['Tiempo_En_Buffer_Minutos'].max():>14.2f}")
    else:
        print("\n✅ NO SE PERDIERON EVENTOS DE VOLCANES CHILENOS")
    
    print("\n" + "="*80)

def ejecutar_analisis_completo():
    """
    Función principal que ejecuta todo el análisis
    """
    print("\n" + "="*80)
    print("🔬 ANÁLISIS BLACK BOX v3 - SOLO VOLCANES CHILENOS")
    print("="*80)
    print(f"Hora inicio: {datetime.now(pytz.timezone('America/Santiago')).strftime('%Y-%m-%d %H:%M:%S CLT')}")
    print("="*80 + "\n")
    
    # 1. Cargar eventos de Black Box (solo chilenos)
    eventos_blackbox = cargar_snapshots_blackbox()
    
    if not eventos_blackbox:
        log_info("❌ No se pudieron cargar eventos de Black Box")
        return
    
    # 2. Cargar eventos capturados
    df_capturados = cargar_eventos_capturados()
    
    # 3. Comparar
    eventos_perdidos, eventos_capturados = comparar_blackbox_vs_capturados(
        eventos_blackbox, df_capturados
    )
    
    # 4. Guardar resultados
    if eventos_perdidos:
        df_perdidos = pd.DataFrame(eventos_perdidos)
        df_perdidos.to_csv(REPORTE_PERDIDOS, index=False)
        log_info(f"\n💾 Eventos perdidos guardados en: {REPORTE_PERDIDOS}")
    else:
        # Crear CSV vacío con headers
        pd.DataFrame(columns=[
            'timestamp', 'Fecha_Evento_UTC', 'Volcan', 'Volcan_ID', 'Sensor',
            'VRP_MW', 'Distancia_km', 'Primera_Aparicion_Snapshot',
            'Ultima_Aparicion_Snapshot', 'Tiempo_En_Buffer_Minutos',
            'Veces_Visto_BlackBox', 'Estado', 'Razon_Probable'
        ]).to_csv(REPORTE_PERDIDOS, index=False)
        log_info(f"\n💾 CSV vacío creado (sin pérdidas): {REPORTE_PERDIDOS}")
    
    # 5. Generar estadísticas
    generar_estadisticas(eventos_perdidos, eventos_capturados)
    
    # 6. Conclusión
    print("\n" + "="*80)
    print("🎯 CONCLUSIÓN")
    print("="*80)
    
    total = len(eventos_perdidos) + len(eventos_capturados)
    if total > 0:
        tasa = (len(eventos_capturados) / total * 100)
        
        if tasa >= 99:
            print("✅ EXCELENTE: Tasa de captura ≥99%")
            print("   El scraper cada 1 minuto está funcionando perfectamente.")
        elif tasa >= 95:
            print("✅ BUENO: Tasa de captura ≥95%")
            print("   El scraper funciona bien, pérdidas mínimas aceptables.")
        elif tasa >= 90:
            print("⚠️ ACEPTABLE: Tasa de captura 90-95%")
            print("   Hay pérdidas, pero no críticas. Revisar razones.")
        else:
            print("❌ PROBLEMAS: Tasa de captura <90%")
            print("   Se pierden demasiados datos. Acción requerida.")
        
        # Analizar si las pérdidas son por expulsión rápida
        if eventos_perdidos:
            df_p = pd.DataFrame(eventos_perdidos)
            expulsion_rapida = len(df_p[df_p['Razon_Probable'].str.contains('EXPULSION')])
            
            if expulsion_rapida > len(eventos_perdidos) * 0.7:
                print("\n⚠️ DIAGNÓSTICO:")
                print(f"   {expulsion_rapida}/{len(eventos_perdidos)} pérdidas son por EXPULSIÓN del buffer")
                print("   CAUSA: Alta actividad global en MIROVA")
                print("   SOLUCIÓN: Implementar scraper híbrido (tabla + imágenes)")
            else:
                print("\n⚠️ DIAGNÓSTICO:")
                print("   La mayoría de pérdidas NO son por expulsión rápida")
                print("   CAUSA PROBABLE: Error en scraper o GitHub Actions")
                print("   SOLUCIÓN: Revisar logs de GitHub Actions")
    else:
        print("⚠️ No se detectaron eventos en Black Box")
        print("   Verificar que Black Box esté funcionando correctamente")
    
    print("="*80)
    print(f"\nHora fin: {datetime.now(pytz.timezone('America/Santiago')).strftime('%Y-%m-%d %H:%M:%S CLT')}")
    print("="*80 + "\n")

if __name__ == "__main__":
    ejecutar_analisis_completo()
