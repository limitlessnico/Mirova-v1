"""
OCR_UTILS.PY - VERSIÓN CORREGIDA
Utilidades para OCR y análisis RGB de gráficos MIROVA
"""

import pytesseract
from PIL import Image
import cv2
import numpy as np
from datetime import datetime
import re

# =========================
# CONFIGURACIÓN OCR
# =========================

def extraer_eventos_latest10nti(ruta_imagen):
    """
    Extrae timestamps y VRP de Latest10NTI.png usando OCR
    
    CORREGIDO: Ahora lee TODOS los eventos, no solo uno
    
    Returns:
        list: [{timestamp, vrp_mw, posicion}, ...]
    """
    eventos = []
    
    try:
        img = Image.open(ruta_imagen)
        
        # Configuración OCR - MEJORADA
        # psm 11: Sparse text (mejor para texto disperso)
        custom_config = r'--oem 3 --psm 11'
        texto = pytesseract.image_to_string(img, config=custom_config)
        
        print(f"   [DEBUG] Texto OCR completo ({len(texto)} chars):")
        print(f"   {texto[:500]}...")  # Primeros 500 caracteres para debug
        
        # Patrón para timestamps: DD-Mon-YYYY HH:MM:SS
        # CORREGIDO: findall en lugar de search para obtener TODOS
        patron_fecha = r'(\d{2})-([A-Za-z]{3})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})'
        
        # Buscar TODAS las fechas en el texto completo
        matches_fecha = re.findall(patron_fecha, texto)
        
        print(f"   [DEBUG] Fechas encontradas: {len(matches_fecha)}")
        
        if not matches_fecha:
            print(f"   ⚠️ No se encontraron fechas en formato DD-Mon-YYYY HH:MM:SS")
            return []
        
        # Patrón para VRP: buscar "VRP =X.XX MW" o "VRP =NaN MW"
        # CORREGIDO: Más flexible con espacios
        patron_vrp_completo = r'VRP\s*[=:]\s*([\d.]+|NaN)\s*MW'
        
        # Buscar TODOS los VRP en el texto
        matches_vrp = re.findall(patron_vrp_completo, texto, re.IGNORECASE)
        
        print(f"   [DEBUG] VRP encontrados: {len(matches_vrp)}")
        
        # Mapeo de mes abreviado a número
        meses = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
            'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
            'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        
        # Procesar cada fecha encontrada
        for idx, match_fecha in enumerate(matches_fecha):
            try:
                dia = int(match_fecha[0])
                mes = match_fecha[1]
                anio = int(match_fecha[2])
                hora = int(match_fecha[3])
                minuto = int(match_fecha[4])
                segundo = int(match_fecha[5])
                
                mes_num = meses.get(mes, 1)
                
                dt = datetime(anio, mes_num, dia, hora, minuto, segundo)
                timestamp = int(dt.timestamp())
                
                # Asignar VRP correspondiente (si existe)
                vrp_mw = None
                if idx < len(matches_vrp):
                    vrp_str = matches_vrp[idx]
                    if vrp_str.upper() == 'NAN':
                        vrp_mw = np.nan
                    else:
                        try:
                            vrp_mw = float(vrp_str)
                        except:
                            vrp_mw = np.nan
                
                # Solo agregar si encontramos VRP
                if vrp_mw is not None:
                    eventos.append({
                        'timestamp': timestamp,
                        'datetime': dt,
                        'vrp_mw': vrp_mw,
                        'posicion': idx
                    })
                    
                    vrp_display = f"{vrp_mw:.2f}" if not np.isnan(vrp_mw) else "NaN"
                    print(f"   [DEBUG] Evento {idx+1}: {dt.strftime('%d-%b-%Y %H:%M:%S')} VRP={vrp_display} MW")
            
            except Exception as e:
                print(f"   ⚠️ Error parseando fecha {idx+1}: {e}")
                continue
        
        print(f"✅ OCR extraído: {len(eventos)} eventos de Latest10NTI.png")
        return eventos
    
    except Exception as e:
        print(f"❌ Error en OCR: {e}")
        import traceback
        traceback.print_exc()
        return []


def analizar_puntos_distancia(ruta_imagen, eventos, ventana_dias=2):
    """
    Analiza colores RGB de puntos en Dist.png
    
    Args:
        ruta_imagen: Path a Dist.png
        eventos: Lista de eventos de Latest10NTI
        ventana_dias: Días de ventana para buscar puntos
    
    Returns:
        list: Eventos con color asignado
    """
    try:
        # Cargar imagen
        img = cv2.imread(ruta_imagen)
        if img is None:
            print("❌ No se pudo cargar Dist.png")
            return eventos
        
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        height, width = img_rgb.shape[:2]
        
        # Detectar puntos rojos y negros
        puntos = detectar_puntos_grafico(img_rgb)
        
        print(f"🔍 Detectados {len(puntos)} puntos en Dist.png")
        
        # Hacer match temporal
        for evento in eventos:
            # Calcular posición X esperada en gráfico
            # (esto requiere calibración del eje X del gráfico)
            # Por ahora, buscar punto más cercano temporalmente
            
            evento['color_punto'] = 'sin_punto'
            evento['puntos_cercanos'] = []
            
            # Buscar puntos en ventana temporal
            for punto in puntos:
                # Aquí iría lógica de match temporal
                # Por ahora, asignar por proximidad
                pass
            
            # Estrategia simple: si hay puntos, asignar color dominante
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
    """
    Detecta puntos rojos y negros en gráfico
    
    Returns:
        list: [{x, y, color}, ...]
    """
    puntos = []
    height, width = img_rgb.shape[:2]
    
    # Umbral para rojo
    # RGB aprox (255, 0, 0) con tolerancia
    mask_rojo = cv2.inRange(
        img_rgb,
        np.array([200, 0, 0]),    # Mínimo
        np.array([255, 50, 50])   # Máximo
    )
    
    # Umbral para negro
    # RGB aprox (0, 0, 0) con tolerancia
    mask_negro = cv2.inRange(
        img_rgb,
        np.array([0, 0, 0]),
        np.array([50, 50, 50])
    )
    
    # Encontrar contornos rojos
    contornos_rojo, _ = cv2.findContours(
        mask_rojo, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    
    for cnt in contornos_rojo:
        if cv2.contourArea(cnt) > 5:  # Área mínima
            M = cv2.moments(cnt)
            if M['m00'] != 0:
                cx = int(M['m10'] / M['m00'])
                cy = int(M['m01'] / M['m00'])
                puntos.append({'x': cx, 'y': cy, 'color': 'rojo'})
    
    # Encontrar contornos negros
    contornos_negro, _ = cv2.findContours(
        mask_negro, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    
    for cnt in contornos_negro:
        if cv2.contourArea(cnt) > 5:
            M = cv2.moments(cnt)
            if M['m00'] != 0:
                cx = int(M['m10'] / M['m00'])
                cy = int(M['m01'] / M['m00'])
                puntos.append({'x': cx, 'y': cy, 'color': 'negro'})
    
    return puntos


def clasificar_confianza(evento):
    """
    Clasifica nivel de confianza de un evento OCR
    
    Returns:
        dict: {confianza, requiere_verificacion, nota, guardar}
    """
    # VRP inválido
    if np.isnan(evento['vrp_mw']) or evento['vrp_mw'] <= 0:
        return {
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'VRP inválido o cero',
            'guardar': False
        }
    
    color = evento.get('color_punto', 'sin_punto')
    metodo = evento.get('metodo', 'desconocido')
    
    # Sin punto en Dist.png
    if color == 'sin_punto':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Sin punto de validación en Dist.png',
            'guardar': True
        }
    
    # Punto negro (fuera de rango)
    if color == 'negro' or metodo == 'todos_negros':
        return {
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'Punto negro - Fuera de rango de alerta',
            'guardar': False
        }
    
    # Punto rojo único
    if color == 'rojo' and metodo == 'match_unico':
        return {
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Match único - 1 evento, 1 punto rojo',
            'guardar': True
        }
    
    # Validación grupal todos rojos
    if metodo == 'validacion_grupal_todos_rojos':
        return {
            'confianza': 'media',
            'requiere_verificacion': True,
            'nota': 'Validación grupal - todos los puntos rojos',
            'guardar': True
        }
    
    # Mezcla de colores
    if color == 'ambiguo' or metodo == 'mezcla_colores':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Match ambiguo - mezcla de puntos rojos y negros',
            'guardar': True
        }
    
    # Por defecto
    return {
        'confianza': 'media',
        'requiere_verificacion': True,
        'nota': 'Validación estándar',
        'guardar': True
    }
