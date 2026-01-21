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
    Analiza Dist.png con ROI y clasifica según NUEVA LÓGICA V3
    
    Clasificación:
    - Solo rojos → ALERTA_TERMICA_OCR (alta)
    - Solo negros → FALSO_POSITIVO_OCR (alta)
    - Mezcla → ALERTA_TERMICA_OCR (media)
    - Sin puntos → ALERTA_TERMICA_OCR (baja)
    """
    try:
        img = cv2.imread(ruta_imagen)
        if img is None:
            print(f"   ❌ No se pudo cargar Dist.png")
            # Sin imagen → Todos son "sin_punto"
            for evento in eventos:
                evento['color_punto'] = 'sin_punto'
                evento['metodo'] = 'sin_imagen'
            return eventos
        
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        height, width = img_rgb.shape[:2]
        
        # Extraer ROI según configuración
        roi_x_start = int(width * ROI_CONFIG['x_start_pct'])
        roi_x_end = int(width * ROI_CONFIG['x_end_pct'])
        roi_y_start = int(height * ROI_CONFIG['y_start_pct'])
        roi_y_end = int(height * ROI_CONFIG['y_end_pct'])
        
        roi = img_rgb[roi_y_start:roi_y_end, roi_x_start:roi_x_end]
        
        print(f"   🔍 ROI: {roi.shape} (últimos días)")
        
        # Detectar puntos en ROI
        puntos_rojos = detectar_puntos_color(roi, 'rojo')
        puntos_negros = detectar_puntos_color(roi, 'negro')
        
        print(f"   🔴 Rojos en ROI: {len(puntos_rojos)}")
        print(f"   ⚫ Negros en ROI: {len(puntos_negros)}")
        
        # ===== NUEVA LÓGICA V3 =====
        total_puntos = len(puntos_rojos) + len(puntos_negros)
        
        for evento in eventos:
            if total_puntos == 0:
                # Sin puntos en ROI
                evento['color_punto'] = 'sin_punto'
                evento['metodo'] = 'sin_puntos_roi'
                
            elif len(puntos_rojos) > 0 and len(puntos_negros) == 0:
                # Solo rojos
                evento['color_punto'] = 'rojo'
                evento['metodo'] = 'solo_rojos_roi'
                
            elif len(puntos_negros) > 0 and len(puntos_rojos) == 0:
                # Solo negros
                evento['color_punto'] = 'negro'
                evento['metodo'] = 'solo_negros_roi'
                
            else:
                # Mezcla rojos + negros
                evento['color_punto'] = 'mezcla'
                evento['metodo'] = 'mezcla_roi'
        
        return eventos
    
    except Exception as e:
        print(f"   ❌ Error analizando Dist.png: {e}")
        # En caso de error, marcar como sin_punto
        for evento in eventos:
            evento['color_punto'] = 'sin_punto'
            evento['metodo'] = 'error_analisis'
        return eventos


def detectar_puntos_color(img_rgb, color):
    """
    Detecta círculos de un color en la imagen
    
    CORREGIDO v3: Rojo debe ser R DOMINANTE sobre G y B
    """
    puntos = []
    
    if color == 'rojo':
        # CORREGIDO: Rojo = R dominante sobre G y B
        # R > 150 Y (R - G) > 50 Y (R - B) > 50
        # Esto asegura que R es significativamente mayor que G y B
        
        # Paso 1: Máscara básica de R alto
        mask_r_alto = img_rgb[:, :, 0] > 150  # R > 150
        
        # Paso 2: R debe ser dominante
        mask_r_dominante = (img_rgb[:, :, 0] - img_rgb[:, :, 1]) > 50  # R - G > 50
        mask_r_dominante &= (img_rgb[:, :, 0] - img_rgb[:, :, 2]) > 50  # R - B > 50
        
        # Combinar máscaras
        mask = (mask_r_alto & mask_r_dominante).astype(np.uint8) * 255
        
    elif color == 'negro':
        # Negro/gris oscuro: Todo < 100
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
                
                # Filtro de circularidad
                perimeter = cv2.arcLength(cnt, True)
                if perimeter > 0:
                    circularidad = 4 * np.pi * area / (perimeter ** 2)
                    
                    # AJUSTADO: Umbral 0.2 (era 0.4)
                    # Los símbolos de MIROVA son irregulares (círculo+línea)
                    # Circularidad típica: 0.26-0.64
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
    Clasifica confianza según NUEVA LÓGICA V3
    
    Reglas:
    - VRP <= 0 → NO guardar
    - Solo rojos → ALERTA alta (guardar, publicar)
    - Solo negros → FALSO_POSITIVO alta (guardar, NO publicar)
    - Mezcla → ALERTA media (guardar, publicar)
    - Sin puntos + VRP > 0.5 → ALERTA baja (guardar, publicar)
    - Sin puntos + VRP <= 0.5 → ALERTA baja (guardar, NO publicar)
    """
    
    # REGLA 1: VRP inválido
    if np.isnan(evento['vrp_mw']) or evento['vrp_mw'] <= 0:
        return {
            'tipo_registro': None,
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'VRP inválido o cero',
            'guardar': False,
            'publicar': False
        }
    
    color = evento.get('color_punto', 'sin_punto')
    metodo = evento.get('metodo', 'desconocido')
    vrp = evento['vrp_mw']
    
    # REGLA 2: Solo rojos en ROI → ALERTA ALTA
    if color == 'rojo' and 'solo_rojos' in metodo:
        return {
            'tipo_registro': 'ALERTA_TERMICA_OCR',
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Solo puntos rojos en ROI - Validado',
            'guardar': True,
            'publicar': True
        }
    
    # REGLA 3: Solo negros en ROI → FALSO POSITIVO ALTA
    if color == 'negro' and 'solo_negros' in metodo:
        return {
            'tipo_registro': 'FALSO_POSITIVO_OCR',
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Solo puntos negros en ROI - Distancia > límite',
            'guardar': True,
            'publicar': False
        }
    
    # REGLA 4: Mezcla de colores → ALERTA MEDIA
    if color == 'mezcla':
        return {
            'tipo_registro': 'ALERTA_TERMICA_OCR',
            'confianza': 'media',
            'requiere_verificacion': True,
            'nota': 'Mezcla rojos/negros en ROI - Evento en zona límite',
            'guardar': True,
            'publicar': True
        }
    
    # REGLA 5: Sin puntos en ROI
    # CAMBIO: Publicar TODO sin filtro VRP
    if color == 'sin_punto':
        return {
            'tipo_registro': 'ALERTA_TERMICA_OCR',
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': f'Sin puntos en ROI, VRP={vrp:.2f} MW',
            'guardar': True,
            'publicar': True  # ← Publicar siempre
        }
    
    # Por defecto: NO guardar
    return {
        'tipo_registro': None,
        'confianza': 'invalido',
        'requiere_verificacion': True,
        'nota': 'Sin validación suficiente',
        'guardar': False,
        'publicar': False
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
