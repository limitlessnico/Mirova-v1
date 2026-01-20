"""
OCR_UTILS.PY - VERSIÓN MEJORADA
Maneja texto OCR con saltos de línea entre VRP y valores
"""

import pytesseract
from PIL import Image
import cv2
import numpy as np
from datetime import datetime
import re

def extraer_eventos_latest10nti(ruta_imagen):
    """
    Extrae timestamps y VRP de Latest10NTI.png usando OCR
    
    MEJORADO: Maneja saltos de línea entre VRP y valores
    
    Returns:
        list: [{timestamp, datetime, vrp_mw, posicion}, ...]
    """
    eventos = []
    
    try:
        img = Image.open(ruta_imagen)
        
        # Configuración OCR
        custom_config = r'--oem 3 --psm 6'
        texto = pytesseract.image_to_string(img, config=custom_config)
        
        print(f"   [DEBUG] Texto OCR completo ({len(texto)} chars):")
        print(f"   {texto[:500]}...")
        
        # Patrón para timestamps
        patron_fecha = r'(\d{2})-([A-Za-z]{3})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})'
        
        # NUEVO: Buscar números que podrían ser VRP
        # Acepta: "12 MW", ".12 MW", "0.12 MW", "NaN MW"
        patron_numero_mw = r'(\d*\.?\d+|NaN)\s*MW'
        
        lineas = texto.split('\n')
        
        # Paso 1: Encontrar todas las fechas
        fechas_encontradas = []
        for i, linea in enumerate(lineas):
            match_fecha = re.search(patron_fecha, linea)
            if match_fecha:
                fechas_encontradas.append((i, match_fecha))
        
        print(f"   [DEBUG] Fechas encontradas: {len(fechas_encontradas)}")
        
        # Paso 2: Para cada fecha, buscar VRP en líneas cercanas
        meses = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
            'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
            'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        
        vrp_count = 0
        for idx_linea, match_fecha in fechas_encontradas:
            try:
                dia = int(match_fecha.group(1))
                mes = match_fecha.group(2)
                anio = int(match_fecha.group(3))
                hora = int(match_fecha.group(4))
                minuto = int(match_fecha.group(5))
                segundo = int(match_fecha.group(6))
                
                mes_num = meses.get(mes, 1)
                dt = datetime(anio, mes_num, dia, hora, minuto, segundo)
                timestamp = int(dt.timestamp())
                
                # Buscar VRP en líneas cercanas (hasta 5 líneas después)
                vrp_mw = None
                for j in range(idx_linea, min(len(lineas), idx_linea + 6)):
                    linea_busqueda = lineas[j]
                    
                    # Buscar "VRP" en la línea
                    if 'VRP' in linea_busqueda.upper():
                        # Buscar número + MW en esta línea o las siguientes
                        for k in range(j, min(len(lineas), j + 3)):
                            match_num = re.search(patron_numero_mw, lineas[k])
                            if match_num:
                                vrp_str = match_num.group(1)
                                if vrp_str.upper() == 'NAN':
                                    vrp_mw = np.nan
                                else:
                                    try:
                                        # Manejar números que empiezan con punto
                                        if vrp_str.startswith('.'):
                                            vrp_str = '0' + vrp_str
                                        vrp_mw = float(vrp_str)
                                    except:
                                        vrp_mw = np.nan
                                break
                        if vrp_mw is not None:
                            break
                
                # Solo agregar si encontramos VRP
                if vrp_mw is not None:
                    eventos.append({
                        'timestamp': timestamp,
                        'datetime': dt,
                        'vrp_mw': vrp_mw,
                        'posicion': len(eventos)
                    })
                    vrp_count += 1
                    
                    vrp_display = f"{vrp_mw:.2f}" if not np.isnan(vrp_mw) else "NaN"
                    print(f"   [DEBUG] Evento {vrp_count}: {dt.strftime('%d-%b-%Y %H:%M:%S')} VRP={vrp_display} MW")
            
            except Exception as e:
                print(f"   ⚠️ Error parseando fecha: {e}")
                continue
        
        print(f"✅ OCR extraído: {len(eventos)} eventos de Latest10NTI.png")
        return eventos
    
    except Exception as e:
        print(f"❌ Error en OCR: {e}")
        import traceback
        traceback.print_exc()
        return []


def analizar_puntos_distancia(ruta_imagen, eventos, ventana_dias=2):
    """Analiza colores RGB de puntos en Dist.png"""
    try:
        img = cv2.imread(ruta_imagen)
        if img is None:
            print("❌ No se pudo cargar Dist.png")
            return eventos
        
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        puntos = detectar_puntos_grafico(img_rgb)
        
        print(f"🔍 Detectados {len(puntos)} puntos en Dist.png")
        
        for evento in eventos:
            evento['color_punto'] = 'sin_punto'
            evento['puntos_cercanos'] = []
            
            if puntos:
                colores = [p['color'] for p in puntos]
                if all(c == 'rojo' for c in colores):
                    evento['color_punto'] = 'rojo'
                    evento['metodo'] = 'validacion_grupal_todos_rojos'
                elif all(c == 'negro' for c in colores):
                    evento['color_punto'] = 'negro'
                    evento['metodo'] = 'todos_negros'
                else:
                    evento['color_punto'] = 'ambiguo'
                    evento['metodo'] = 'mezcla_colores'
        
        return eventos
    
    except Exception as e:
        print(f"❌ Error analizando Dist.png: {e}")
        return eventos


def detectar_puntos_grafico(img_rgb):
    """Detecta puntos rojos y negros en gráfico"""
    puntos = []
    
    mask_rojo = cv2.inRange(img_rgb, np.array([200, 0, 0]), np.array([255, 50, 50]))
    mask_negro = cv2.inRange(img_rgb, np.array([0, 0, 0]), np.array([50, 50, 50]))
    
    for mask, color in [(mask_rojo, 'rojo'), (mask_negro, 'negro')]:
        contornos, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for cnt in contornos:
            if cv2.contourArea(cnt) > 5:
                M = cv2.moments(cnt)
                if M['m00'] != 0:
                    cx = int(M['m10'] / M['m00'])
                    cy = int(M['m01'] / M['m00'])
                    puntos.append({'x': cx, 'y': cy, 'color': color})
    
    return puntos


def clasificar_confianza(evento):
    """Clasifica nivel de confianza de un evento OCR"""
    if np.isnan(evento['vrp_mw']) or evento['vrp_mw'] <= 0:
        return {
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'VRP inválido o cero',
            'guardar': False
        }
    
    color = evento.get('color_punto', 'sin_punto')
    metodo = evento.get('metodo', 'desconocido')
    
    if color == 'sin_punto':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Sin punto de validación en Dist.png',
            'guardar': True
        }
    
    if color == 'negro' or metodo == 'todos_negros':
        return {
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'Punto negro - Fuera de rango de alerta',
            'guardar': False
        }
    
    if color == 'rojo' and metodo == 'match_unico':
        return {
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Match único - 1 evento, 1 punto rojo',
            'guardar': True
        }
    
    if metodo == 'validacion_grupal_todos_rojos':
        return {
            'confianza': 'media',
            'requiere_verificacion': True,
            'nota': 'Validación grupal - todos los puntos rojos',
            'guardar': True
        }
    
    if color == 'ambiguo' or metodo == 'mezcla_colores':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Match ambiguo - mezcla de puntos rojos y negros',
            'guardar': True
        }
    
    return {
        'confianza': 'media',
        'requiere_verificacion': True,
        'nota': 'Validación estándar',
        'guardar': True
    }
