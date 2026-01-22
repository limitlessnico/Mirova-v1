"""
OCR_UTILS.PY - VERSIÓN 3.0
Nueva lógica de clasificación OCR
"""

import cv2
import numpy as np
import pytesseract
from PIL import Image
import re
from datetime import datetime
import pandas as pd


# ===== CONFIGURACIÓN ROI =====
# ROI para análisis temporal: SOLO ÚLTIMO DÍA (máxima precisión)
# Definido por usuario el 21-Ene-2026
# Filosofía: Precisión > Cobertura

ROI_CONFIG = {
    'x_start_pct': 0.8424,  # 84.24% del ancho (último día únicamente)
    'x_end_pct': 0.8635,    # 86.35%
    'y_start_pct': 0.1817,  # 18.17% altura
    'y_end_pct': 0.4933     # 49.33%
}

# JUSTIFICACIÓN:
# - Máxima precisión temporal (sin mezcla de días)
# - Desde 21-Ene-2026 en adelante: alta confiabilidad
# - Eventos antiguos quedarán "sin_punto" (esperado)
# - Ideal para monitoreo en tiempo real


def extraer_eventos_latest10nti(ruta_imagen):
    """
    Extrae fechas y VRP de Latest10NTI.png usando OCR
    
    VERSIÓN ROBUSTA: Múltiples estrategias para manejar OCR inconsistente
    """
    try:
        img = Image.open(ruta_imagen)
        texto = pytesseract.image_to_string(img, config='--oem 3 --psm 6')
        
        print(f"   [DEBUG] Texto OCR completo ({len(texto)} chars)")
        
        # ==== PASO 1: Extraer fechas (filtrar "Last Update") ====
        patron_fecha = r'(\d{2})-([A-Za-z]{3})-(\d{2}\d{2})\s+(\d{2}):(\d{2}):(\d{2})'
        
        matches_fecha = []
        for match in re.finditer(patron_fecha, texto):
            pos = match.start()
            contexto = texto[max(0, pos-20):pos].lower()
            
            if "update:" in contexto:
                print(f"   [DEBUG] Filtrado: {match.group()} pos={pos} (Last Update)")
                continue
            
            matches_fecha.append(match.groups())
        
        print(f"   [DEBUG] Fechas válidas (sin título): {len(matches_fecha)}")
        n_fechas = len(matches_fecha)
        
        # ==== PASO 2: MÚLTIPLES ESTRATEGIAS para extraer VRP ====
        
        # Estrategia 1: Patrón VRP completo
        patron_vrp = r'VRP\s*[=:]?\s*(\d*\.?\d+|NaN)\s*MW'
        matches_vrp_1 = re.findall(patron_vrp, texto, re.IGNORECASE)
        print(f"   [DEBUG] Estrategia 1 (VRP\s*=\s*X MW): {len(matches_vrp_1)} valores")
        
        # Estrategia 2: TODOS los números antes de MW
        patron_mw_todos = r'(\d+\.?\d*|NaN)\s*MW'
        matches_vrp_2 = re.findall(patron_mw_todos, texto, re.IGNORECASE)
        print(f"   [DEBUG] Estrategia 2 (X MW): {len(matches_vrp_2)} valores")
        
        # Estrategia 3: Solo números válidos antes de MW
        patron_num = r'(\d+\.?\d*)\s*MW'
        matches_num = re.findall(patron_num, texto, re.IGNORECASE)
        matches_vrp_3 = []
        for num in matches_num:
            try:
                val = float(num)
                if 0.01 <= val <= 100:
                    matches_vrp_3.append(num)
            except:
                pass
        print(f"   [DEBUG] Estrategia 3 (números válidos): {len(matches_vrp_3)} valores")
        
        # ==== PASO 3: ELEGIR MEJOR ESTRATEGIA ====
        
        # Prioridad: La que da exactamente n_fechas valores
        if len(matches_vrp_1) == n_fechas:
            matches_vrp = matches_vrp_1
            print(f"   ✅ Usando Estrategia 1")
        
        elif len(matches_vrp_2) == n_fechas:
            matches_vrp = matches_vrp_2
            print(f"   ✅ Usando Estrategia 2")
        
        elif len(matches_vrp_3) == n_fechas:
            matches_vrp = matches_vrp_3
            print(f"   ✅ Usando Estrategia 3")
        
        else:
            # FALLBACK: Usar la más cercana y completar con NaN
            estrategias = [
                (len(matches_vrp_1), matches_vrp_1, "Estrategia 1"),
                (len(matches_vrp_2), matches_vrp_2, "Estrategia 2"),
                (len(matches_vrp_3), matches_vrp_3, "Estrategia 3")
            ]
            
            # Ordenar por cercanía a n_fechas
            estrategias.sort(key=lambda x: abs(x[0] - n_fechas))
            
            mejor_len, mejor_matches, mejor_nombre = estrategias[0]
            
            if mejor_len < n_fechas:
                # Completar con NaN
                matches_vrp = mejor_matches + ['NaN'] * (n_fechas - mejor_len)
                print(f"   ⚠️ {mejor_nombre} dio {mejor_len}, completando con NaN")
            else:
                # Truncar
                matches_vrp = mejor_matches[:n_fechas]
                print(f"   ⚠️ {mejor_nombre} dio {mejor_len}, truncando a {n_fechas}")
        
        print(f"   [DEBUG] VRP finales: {len(matches_vrp)}")
        
        # ==== PASO 4: MAPEAR EVENTOS ====
        eventos = []
        for i in range(min(len(matches_fecha), len(matches_vrp))):
            try:
                dia, mes, anio, hora, minuto, segundo = matches_fecha[i]
                
                meses = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }
                
                dt = datetime(
                    int(anio), meses[mes], int(dia),
                    int(hora), int(minuto), int(segundo)
                )
                
                vrp_str = matches_vrp[i]
                vrp_mw = 0.0 if vrp_str.lower() == 'nan' else float(vrp_str)
                
                eventos.append({
                    'timestamp': int(dt.timestamp()),
                    'datetime': dt,
                    'vrp_mw': vrp_mw
                })
                
                print(f"   [DEBUG] Evento {i+1}: {dt.strftime('%d-%b-%Y %H:%M:%S')} VRP={vrp_str} MW")
            
            except Exception as e:
                print(f"   [WARN] Error parseando evento {i+1}: {e}")
                continue
        
        print(f"   ✅ OCR extraído: {len(eventos)} eventos")
        return eventos
    
    except Exception as e:
        print(f"   ❌ Error en OCR: {e}")
        return []


def analizar_puntos_distancia_v3(ruta_imagen, eventos):
    """
    Analiza Dist.png con ROI - VERSIÓN 4 (densidad + estrella verde)
    
    NUEVO V4:
    - Filtra píxeles VERDES (estrella = última detección)
    - Usa ratio rojos/negros para distinguir real vs falso
    - Clasificación más precisa con estrella presente
    """
    try:
        img = cv2.imread(ruta_imagen)
        if img is None:
            print(f"   ❌ No se pudo cargar Dist.png")
            for evento in eventos:
                evento['color_punto'] = 'sin_punto'
                evento['metodo'] = 'sin_imagen'
            return eventos
        
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        height, width = img_rgb.shape[:2]
        
        # Extraer ROI
        roi_x_start = int(width * ROI_CONFIG['x_start_pct'])
        roi_x_end = int(width * ROI_CONFIG['x_end_pct'])
        roi_y_start = int(height * ROI_CONFIG['y_start_pct'])
        roi_y_end = int(height * ROI_CONFIG['y_end_pct'])
        
        roi = img_rgb[roi_y_start:roi_y_end, roi_x_start:roi_x_end]
        
        print(f"   🔍 ROI: {roi.shape} (últimos días)")
        
        # ===== PASO 1: Detectar estrella verde =====
        # Estrella verde = última detección del día
        # Tiene borde negro que puede confundir
        mask_verde = (roi[:, :, 1] > 150) & \
                     ((roi[:, :, 1] - roi[:, :, 0]) > 50) & \
                     ((roi[:, :, 1] - roi[:, :, 2]) > 50)
        num_verdes = np.sum(mask_verde)
        tiene_estrella = num_verdes >= 50
        
        # ===== PASO 2: Detectar rojos (EXCLUIR verdes) =====
        mask_rojo = (roi[:, :, 0] > 150) & \
                    ((roi[:, :, 0] - roi[:, :, 1]) > 50) & \
                    ((roi[:, :, 0] - roi[:, :, 2]) > 50) & \
                    ~mask_verde  # NO contar píxeles de la estrella
        num_rojos = np.sum(mask_rojo)
        
        # ===== PASO 3: Detectar negros (EXCLUIR verdes) =====
        mask_negro = (roi[:, :, 0] < 100) & \
                     (roi[:, :, 1] < 100) & \
                     (roi[:, :, 2] < 100) & \
                     ~mask_verde  # NO contar borde de estrella
        num_negros = np.sum(mask_negro)
        
        print(f"   🟢 Estrella verde: {num_verdes} px ({'SÍ' if tiene_estrella else 'NO'})")
        print(f"   🔴 Píxeles rojos: {num_rojos}")
        print(f"   ⚫ Píxeles negros: {num_negros}")
        
        # ===== PASO 4: Clasificar según densidad y ratio =====
        UMBRAL_PIXELES = 10
        
        tiene_rojos = num_rojos >= UMBRAL_PIXELES
        tiene_negros = num_negros >= UMBRAL_PIXELES
        
        # Si hay estrella, usar RATIO para distinguir real/falso
        if tiene_estrella and (num_rojos > 0 or num_negros > 0):
            ratio = num_rojos / max(num_negros, 1)
            print(f"   📊 Ratio R/N: {ratio:.2f}")
            
            if ratio > 2.0:
                # Rojo DOMINANTE → Evento REAL
                color_final = 'rojo'
                metodo_final = 'rojo_dominante_con_estrella'
                print(f"   ✅ Rojo dominante (ratio>2.0) → REAL")
            elif ratio < 0.5:
                # Negro DOMINANTE → FALSO POSITIVO
                color_final = 'negro'
                metodo_final = 'negro_dominante_con_estrella'
                print(f"   ❌ Negro dominante (ratio<0.5) → FALSO")
            else:
                # INTERMEDIO → Revisar
                color_final = 'mezcla'
                metodo_final = 'mezcla_con_estrella'
                print(f"   ⚠️ Ratio intermedio → MEZCLA")
        else:
            # Sin estrella: lógica normal de densidad
            if not tiene_rojos and not tiene_negros:
                color_final = 'sin_punto'
                metodo_final = 'sin_pixeles_roi'
            elif tiene_rojos and not tiene_negros:
                color_final = 'rojo'
                metodo_final = 'solo_rojos_densidad'
            elif tiene_negros and not tiene_rojos:
                color_final = 'negro'
                metodo_final = 'solo_negros_densidad'
            else:
                color_final = 'mezcla'
                metodo_final = 'mezcla_densidad'
        
        print(f"   🎯 Clasificación: {color_final}")
        
        # Aplicar a todos los eventos
        for evento in eventos:
            evento['color_punto'] = color_final
            evento['metodo'] = metodo_final
        
        return eventos
    
    except Exception as e:
        print(f"   ❌ Error analizando Dist.png: {e}")
        for evento in eventos:
            evento['color_punto'] = 'sin_punto'
            evento['metodo'] = 'error_analisis'
        return eventos


def detectar_puntos_color(img_rgb, color):
    """
    OBSOLETO V3.1: Ya no se usa, reemplazado por densidad de píxeles
    
    Detección antigua por contornos y circularidad
    Mantenida por compatibilidad pero no se llama
    """
    puntos = []
    
    if color == 'rojo':
        # Rojo = R dominante sobre G y B
        mask_r_alto = img_rgb[:, :, 0] > 150
        mask_r_dominante = (img_rgb[:, :, 0] - img_rgb[:, :, 1]) > 50
        mask_r_dominante &= (img_rgb[:, :, 0] - img_rgb[:, :, 2]) > 50
        mask = (mask_r_alto & mask_r_dominante).astype(np.uint8) * 255
        
    elif color == 'negro':
        # Negro/gris oscuro
        mask = cv2.inRange(img_rgb,
                          np.array([0, 0, 0]),
                          np.array([100, 100, 100]))
    else:
        return puntos
    
    contornos, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    for cnt in contornos:
        area = cv2.contourArea(cnt)
        
        if area >= 3:
            M = cv2.moments(cnt)
            if M['m00'] != 0:
                cx = int(M['m10'] / M['m00'])
                cy = int(M['m01'] / M['m00'])
                
                perimeter = cv2.arcLength(cnt, True)
                if perimeter > 0:
                    circularidad = 4 * np.pi * area / (perimeter ** 2)
                    
                    if circularidad > 0.2:
                        puntos.append({
                            'x': cx,
                            'y': cy,
                            'area': area,
                            'circularidad': circularidad
                        })
    
    return puntos


def clasificar_confianza_v3(evento):
    """
    Clasifica confianza según DENSIDAD DE PÍXELES - VERSIÓN 4
    
    CAMBIOS V4:
    - sin_punto → FALSO_POSITIVO (alta), NO publicar
    - Agregar campo 'guardar_imagenes'
    - Solo guardar imágenes si rojo o mezcla
    """
    
    # REGLA 1: VRP inválido
    if np.isnan(evento['vrp_mw']) or evento['vrp_mw'] <= 0:
        return {
            'tipo_registro': None,
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'VRP inválido o cero',
            'guardar': False,
            'publicar': False,
            'guardar_imagenes': False
        }
    
    color = evento.get('color_punto', 'sin_punto')
    metodo = evento.get('metodo', 'desconocido')
    vrp = evento['vrp_mw']
    
    # REGLA 2: Solo rojos → ALERTA ALTA
    if color == 'rojo':
        return {
            'tipo_registro': 'ALERTA_TERMICA_OCR',
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Píxeles rojos dominantes en ROI - Evento real',
            'guardar': True,
            'publicar': True,
            'guardar_imagenes': True  # ← GUARDAR imágenes
        }
    
    # REGLA 3: Solo negros → FALSO POSITIVO ALTA
    if color == 'negro':
        return {
            'tipo_registro': 'FALSO_POSITIVO_OCR',
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Píxeles negros dominantes - Fuera de límite distancia',
            'guardar': True,
            'publicar': False,
            'guardar_imagenes': False  # ← NO guardar imágenes
        }
    
    # REGLA 4: Mezcla → ALERTA MEDIA
    if color == 'mezcla':
        return {
            'tipo_registro': 'ALERTA_TERMICA_OCR',
            'confianza': 'media',
            'requiere_verificacion': True,
            'nota': f'Mezcla rojos/negros - Evento en zona límite (VRP={vrp:.2f} MW)',
            'guardar': True,
            'publicar': True,
            'guardar_imagenes': True  # ← GUARDAR imágenes
        }
    
    # REGLA 5: Sin píxeles → FALSO POSITIVO ALTA (CAMBIADO!)
    if color == 'sin_punto':
        return {
            'tipo_registro': 'FALSO_POSITIVO_OCR',
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Sin píxeles en ROI - Evento fuera de ventana temporal',
            'guardar': True,  # ← Guardar en CSV (auditoría)
            'publicar': False,  # ← NO publicar
            'guardar_imagenes': False  # ← NO guardar imágenes
        }
    
    # Por defecto: NO guardar
    return {
        'tipo_registro': None,
        'confianza': 'invalido',
        'requiere_verificacion': True,
        'nota': 'Sin clasificación clara',
        'guardar': False,
        'publicar': False,
        'guardar_imagenes': False
    }


def verificar_evento_no_existe(evento, volcan, sensor, df_consolidado, df_ocr):
    """
    Verifica que el evento NO exista en consolidado ni en OCR
    Criterio: timestamp + volcan + sensor
    """
    ts = evento['timestamp']
    
    # Verificar en consolidado
    if not df_consolidado.empty:
        existe_consolidado = df_consolidado[
            (df_consolidado['timestamp'] == ts) &
            (df_consolidado['Volcan'] == volcan) &
            (df_consolidado['Sensor'] == sensor)
        ]
        
        if not existe_consolidado.empty:
            print(f"      SKIP: Ya existe en consolidado.csv")
            return False
    
    # Verificar en OCR
    if not df_ocr.empty:
        existe_ocr = df_ocr[
            (df_ocr['timestamp'] == ts) &
            (df_ocr['Volcan'] == volcan) &
            (df_ocr['Sensor'] == sensor)
        ]
        
        if not existe_ocr.empty:
            print(f"      SKIP: Ya existe en ocr.csv")
            return False
    
    return True


# ===== ALIAS PARA COMPATIBILIDAD =====
# Mantener nombres anteriores para no romper scraper_ocr.py

def analizar_puntos_distancia(ruta_imagen, eventos, ventana_dias=2):
    """Alias para compatibilidad con scraper_ocr.py"""
    return analizar_puntos_distancia_v3(ruta_imagen, eventos)


def clasificar_confianza(evento):
    """Alias para compatibilidad con scraper_ocr.py"""
    return clasificar_confianza_v3(evento)
