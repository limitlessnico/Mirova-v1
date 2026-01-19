import os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import glob
from collections import defaultdict

# ============================================================================
# ANÁLISIS BLACK BOX DEFINITIVO - 20 ENERO 2026 12:00 CLT
# ============================================================================

CARPETA_BLACKBOX = "monitoreo_satelital/blackbox_latest"
DB_MASTER = "monitoreo_satelital/registro_vrp_consolidado.csv"
REPORTE_FINAL = "monitoreo_satelital/analisis_blackbox_20260120.csv"
REPORTE_PERDIDOS = "monitoreo_satelital/eventos_perdidos_confirmados.csv"

VOLCANES_MONITOREADOS = [
    "Isluga", "Lascar", "Lastarria", "Peteroa", 
    "Nevados de Chillan", "Copahue", "Llaima", 
    "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"
]

def log_info(mensaje):
    """Logger con timestamp"""
    ahora = datetime.now(pytz.timezone('America/Santiago'))
    print(f"[{ahora.strftime('%H:%M:%S')}] {mensaje}")

def cargar_snapshots_blackbox():
    """
    Carga todos los archivos HTML de Black Box
    Extrae eventos únicos de cada snapshot
    """
    log_info("📂 Cargando snapshots Black Box...")
    
    archivos_html = sorted(glob.glob(os.path.join(CARPETA_BLACKBOX, "*/*.html")))
    
    if not archivos_html:
        log_info("❌ No se encontraron archivos HTML en Black Box")
        return None
    
    log_info(f"   Encontrados: {len(archivos_html)} archivos HTML")
    
    # Diccionario: event_key -> {info del evento + lista de snapshots donde aparece}
    eventos_en_blackbox = {}
    snapshots_procesados = 0
    errores = 0
    
    for archivo_html in archivos_html:
        # Extraer timestamp del nombre: latest_20260116_191635.html
        nombre = os.path.basename(archivo_html)
        try:
            partes = nombre.replace('latest_', '').replace('.html', '').split('_')
            fecha_snap = datetime.strptime(f"{partes[0]}_{partes[1]}", "%Y%m%d_%H%M%S")
            fecha_snap = fecha_snap.replace(tzinfo=pytz.utc)
        except:
            log_info(f"⚠️ No se pudo parsear timestamp de: {nombre}")
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
                    volcan_nombre = cols[2].text.strip()
                    vrp_mw = float(cols[3].text.strip())
                    distancia_km = float(cols[4].text.strip())
                    sensor = cols[5].text.strip()
                    
                    # Parsear fecha del evento
                    dt_evento = datetime.strptime(fecha_evento_str, "%d-%b-%Y %H:%M:%S")
                    dt_evento = dt_evento.replace(tzinfo=pytz.utc)
                    ts_evento = int(dt_evento.timestamp())
                    
                    # Key única del evento
                    event_key = (ts_evento, volcan_nombre, sensor)
                    
                    # Registrar evento
                    if event_key not in eventos_en_blackbox:
                        eventos_en_blackbox[event_key] = {
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
                        eventos_en_blackbox[event_key]['ultima_aparicion'] = fecha_snap
                        eventos_en_blackbox[event_key]['veces_visto'] += 1
                        eventos_en_blackbox[event_key]['snapshots'].append(fecha_snap)
                
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
    log_info(f"   Errores de parsing: {errores}")
    log_info(f"   Eventos únicos encontrados: {len(eventos_en_blackbox)}")
    
    return eventos_en_blackbox

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
    Identifica eventos PERDIDOS confirmados
    """
    log_info("🔍 Comparando Black Box vs Registros Capturados...")
    
    eventos_perdidos = []
    eventos_capturados_ok = []
    
    # FILTRAR SOLO VOLCANES CHILENOS QUE MONITOREAMOS
    for event_key, info in eventos_blackbox.items():
        # IMPORTANTE: Solo analizar volcanes que estamos monitoreando
        if info['volcan'] not in VOLCANES_MONITOREADOS:
            continue  # Saltar volcanes que no son de Chile
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
    log_info("📊 ESTADÍSTICAS FINALES BLACK BOX")
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
    
    print("\n" + "="*80)

def ejecutar_analisis_completo():
    """
    Función principal que ejecuta todo el análisis
    """
    print("\n" + "="*80)
    print("🔬 ANÁLISIS BLACK BOX DEFINITIVO - 20 ENERO 2026")
    print("="*80)
    print(f"Hora inicio: {datetime.now(pytz.timezone('America/Santiago')).strftime('%Y-%m-%d %H:%M:%S CLT')}")
    print("="*80 + "\n")
    
    # 1. Cargar eventos de Black Box
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
    
    if eventos_capturados:
        df_capturados_ok = pd.DataFrame(eventos_capturados)
        # Opcional: guardar también los capturados para análisis
    
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
            print("✅ EXCELENTE: Tasa de captura >99%")
            print("   El scraper cada 1 minuto está funcionando perfectamente.")
        elif tasa >= 95:
            print("✅ BUENO: Tasa de captura >95%")
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
    
    print("="*80)
    print(f"\nHora fin: {datetime.now(pytz.timezone('America/Santiago')).strftime('%Y-%m-%d %H:%M:%S CLT')}")
    print("="*80 + "\n")

if __name__ == "__main__":
    ejecutar_analisis_completo()
