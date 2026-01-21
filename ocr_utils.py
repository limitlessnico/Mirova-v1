"""
OCR_UTILS.PY - ANÁLISIS DIST.PNG MEJORADO
Valida eventos usando región temporal del gráfico
"""

import cv2
import numpy as np
from datetime import datetime, timedelta

def analizar_puntos_distancia_MEJORADO(ruta_imagen, eventos, ventana_dias=7):
    """
    Analiza Dist.png considerando POSICIÓN TEMPORAL Y ESPACIAL
    
    Args:
        ruta_imagen: Path a Dist.png
        eventos: Lista de eventos de Latest10NTI
        ventana_dias: Días de ventana para buscar puntos (default: 7)
    
    Returns:
        list: Eventos con color asignado según región temporal
    """
    try:
        img = cv2.imread(ruta_imagen)
        if img is None:
            print("❌ No se pudo cargar Dist.png")
            return eventos
        
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        height, width = img_rgb.shape[:2]
        
        # ===== REGIÓN DE INTERÉS (ROI) =====
        # Gráfico "Last Month" está en la parte superior
        # Coordenadas aproximadas (ajustar según imagen real):
        # - X: 70% al 100% del ancho (últimos ~7 días)
        # - Y: 10% al 90% de la altura (excluir bordes)
        
        roi_x_start = int(width * 0.70)  # 70% → derecha (últimos días)
        roi_x_end = int(width * 0.98)    # 98% (dejar margen)
        roi_y_start = int(height * 0.10) # 10% (excluir título)
        roi_y_end = int(height * 0.48)   # 48% (solo "Last Month", no "Last Year")
        
        # Recortar región de interés
        roi = img_rgb[roi_y_start:roi_y_end, roi_x_start:roi_x_end]
        
        print(f"🔍 ROI extraída: {roi.shape} (últimos {ventana_dias} días aprox)")
        
        # Detectar puntos rojos y negros EN LA ROI
        puntos_rojos = detectar_puntos_color(roi, 'rojo')
        puntos_negros = detectar_puntos_color(roi, 'negro')
        
        print(f"   Puntos rojos en ROI: {len(puntos_rojos)}")
        print(f"   Puntos negros en ROI: {len(puntos_negros)}")
        
        # ===== MAPEAR EVENTOS A PUNTOS =====
        # Estrategia: Si hay puntos en ROI, validar eventos
        
        for evento in eventos:
            # Por defecto: sin punto
            evento['color_punto'] = 'sin_punto'
            evento['metodo'] = 'sin_validacion'
            
            # Si hay puntos en ROI, hacer análisis
            if len(puntos_rojos) > 0 or len(puntos_negros) > 0:
                total_puntos = len(puntos_rojos) + len(puntos_negros)
                
                # CASO 1: Solo puntos rojos en ROI
                if len(puntos_rojos) > 0 and len(puntos_negros) == 0:
                    evento['color_punto'] = 'rojo'
                    evento['metodo'] = 'validacion_roi_rojos'
                    print(f"   ✅ Evento validado: Solo puntos rojos en ROI")
                
                # CASO 2: Solo puntos negros en ROI
                elif len(puntos_negros) > 0 and len(puntos_rojos) == 0:
                    evento['color_punto'] = 'negro'
                    evento['metodo'] = 'validacion_roi_negros'
                    print(f"   ❌ Evento inválido: Solo puntos negros en ROI")
                
                # CASO 3: Mezcla de rojos y negros
                else:
                    # Calcular proporción
                    prop_rojos = len(puntos_rojos) / total_puntos
                    
                    if prop_rojos >= 0.7:  # 70% o más rojos
                        evento['color_punto'] = 'rojo'
                        evento['metodo'] = 'validacion_roi_mayoria_rojos'
                        print(f"   ✅ Evento validado: {prop_rojos*100:.0f}% rojos en ROI")
                    elif prop_rojos <= 0.3:  # 30% o menos rojos
                        evento['color_punto'] = 'negro'
                        evento['metodo'] = 'validacion_roi_mayoria_negros'
                        print(f"   ❌ Evento inválido: {prop_rojos*100:.0f}% rojos en ROI")
                    else:
                        evento['color_punto'] = 'ambiguo'
                        evento['metodo'] = 'validacion_roi_mezcla'
                        print(f"   ⚠️ Evento ambiguo: {prop_rojos*100:.0f}% rojos en ROI")
        
        return eventos
    
    except Exception as e:
        print(f"❌ Error analizando Dist.png: {e}")
        import traceback
        traceback.print_exc()
        return eventos


def detectar_puntos_color(img_rgb, color):
    """
    Detecta puntos de un color específico en la imagen
    
    Args:
        img_rgb: Imagen en formato RGB
        color: 'rojo' o 'negro'
    
    Returns:
        list: Lista de puntos detectados [{x, y}, ...]
    """
    puntos = []
    
    if color == 'rojo':
        # Umbral para rojo (círculos rojos en el gráfico)
        # RGB: (255, 0, 0) con tolerancia
        mask = cv2.inRange(
            img_rgb,
            np.array([200, 0, 0]),    # Mínimo R=200
            np.array([255, 50, 50])   # Máximo G=50, B=50
        )
    elif color == 'negro':
        # Umbral para negro (círculos negros en el gráfico)
        # RGB: (0, 0, 0) con tolerancia
        mask = cv2.inRange(
            img_rgb,
            np.array([0, 0, 0]),
            np.array([50, 50, 50])
        )
    else:
        return puntos
    
    # Encontrar contornos
    contornos, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    for cnt in contornos:
        # Filtrar por área mínima (puntos muy pequeños son ruido)
        area = cv2.contourArea(cnt)
        if area > 5:  # Área mínima en píxeles
            # Calcular centro del contorno
            M = cv2.moments(cnt)
            if M['m00'] != 0:
                cx = int(M['m10'] / M['m00'])
                cy = int(M['m01'] / M['m00'])
                puntos.append({'x': cx, 'y': cy, 'area': area})
    
    return puntos


def clasificar_confianza_MEJORADO(evento):
    """
    Clasifica nivel de confianza usando análisis ROI
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
    
    # Sin punto en ROI
    if color == 'sin_punto':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Sin puntos en región temporal (últimos 7 días)',
            'guardar': False
        }
    
    # Punto negro en ROI (fuera de rango)
    if color == 'negro':
        return {
            'confianza': 'invalido',
            'requiere_verificacion': False,
            'nota': 'Punto negro en ROI - Distancia > límite',
            'guardar': False
        }
    
    # Punto rojo en ROI - ALTA CONFIANZA
    if color == 'rojo' and 'roi_rojos' in metodo:
        return {
            'confianza': 'alta',
            'requiere_verificacion': False,
            'nota': 'Punto rojo en ROI temporal - Validado',
            'guardar': True
        }
    
    # Mayoría rojos en ROI - MEDIA CONFIANZA
    if color == 'rojo' and 'mayoria_rojos' in metodo:
        return {
            'confianza': 'media',
            'requiere_verificacion': True,
            'nota': 'Mayoría rojos en ROI - Requiere verificación',
            'guardar': True
        }
    
    # Ambiguo
    if color == 'ambiguo':
        return {
            'confianza': 'baja',
            'requiere_verificacion': True,
            'nota': 'Mezcla rojos/negros en ROI - No concluyente',
            'guardar': False
        }
    
    # Por defecto: NO guardar
    return {
        'confianza': 'baja',
        'requiere_verificacion': True,
        'nota': 'Sin validación suficiente',
        'guardar': False
    }


# ===== CALIBRACIÓN DE ROI =====
# Para ajustar la región de interés según el volcán/sensor

def calibrar_roi_dist(ruta_imagen, mostrar_visual=False):
    """
    Herramienta para calibrar ROI de Dist.png
    
    Uso:
        calibrar_roi_dist('Lastarria_VIIRS375_Dist.png', mostrar_visual=True)
    """
    import matplotlib.pyplot as plt
    
    img = cv2.imread(ruta_imagen)
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    height, width = img_rgb.shape[:2]
    
    # ROI propuesta
    roi_x_start = int(width * 0.70)
    roi_x_end = int(width * 0.98)
    roi_y_start = int(height * 0.10)
    roi_y_end = int(height * 0.48)
    
    print(f"Dimensiones imagen: {width}x{height}")
    print(f"ROI propuesta:")
    print(f"  X: {roi_x_start} → {roi_x_end} ({(roi_x_end-roi_x_start)/width*100:.1f}% del ancho)")
    print(f"  Y: {roi_y_start} → {roi_y_end} ({(roi_y_end-roi_y_start)/height*100:.1f}% de la altura)")
    
    if mostrar_visual:
        # Dibujar ROI sobre la imagen
        img_con_roi = img_rgb.copy()
        cv2.rectangle(img_con_roi, 
                      (roi_x_start, roi_y_start), 
                      (roi_x_end, roi_y_end), 
                      (0, 255, 0), 3)  # Verde
        
        plt.figure(figsize=(12, 6))
        plt.imshow(img_con_roi)
        plt.title('ROI propuesta para análisis temporal')
        plt.axvline(roi_x_start, color='green', linestyle='--', alpha=0.5)
        plt.axvline(roi_x_end, color='green', linestyle='--', alpha=0.5)
        plt.axhline(roi_y_start, color='green', linestyle='--', alpha=0.5)
        plt.axhline(roi_y_end, color='green', linestyle='--', alpha=0.5)
        plt.show()
    
    return {
        'x_start': roi_x_start,
        'x_end': roi_x_end,
        'y_start': roi_y_start,
        'y_end': roi_y_end
    }


if __name__ == "__main__":
    # Test con imagen de Lastarria
    calibrar_roi_dist('Lastarria_VIIRS375_Dist.png', mostrar_visual=True)
