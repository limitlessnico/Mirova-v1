"""
DEFINIR ROI - Ejecutable en GitHub Actions
Descarga imagen Dist.png y analiza para encontrar coordenadas óptimas
"""

import cv2
import numpy as np
import os
import json
from datetime import datetime

# Volcán y sensor de prueba (puedes cambiar)
VOLCAN_TEST = "Lastarria"
SENSOR_TEST = "VIIRS375"

# URL base MIROVA
BASE_URL = "https://www.mirovaweb.it/OUTPUTweb/MIROVA"

# Mapeo de volcanes a IDs MIROVA
VOLCANES_MIROVA = {
    "Lascar": "Lascar",
    "Lastarria": "Lastarria",
    "Copahue": "Copahue",
    "Nevados de Chillan": "ChillanNevadosde",
    "Villarrica": "Villarrica",
    "Puyehue-Cordon Caulle": "PuyehueCordonCaulle"
}


def descargar_imagen_dist(volcan, sensor):
    """Descarga imagen Dist.png de MIROVA"""
    import requests
    
    id_mirova = VOLCANES_MIROVA.get(volcan, volcan)
    sensor_url = "VIIRS750" if sensor == "VIIRS" else sensor
    
    url = f"{BASE_URL}/{sensor_url}/VOLCANOES/{id_mirova}/{id_mirova}_{sensor_url}_Dist.png"
    
    print(f"📥 Descargando imagen...")
    print(f"   URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            filename = f"{volcan}_{sensor}_Dist.png"
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ Imagen descargada: {filename}")
            return filename
        else:
            print(f"❌ Error HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error descargando: {e}")
        return None


def analizar_roi_automatico(imagen_path):
    """
    Analiza la imagen y propone ROI automático
    Basado en estructura típica de gráfico Dist.png
    """
    img = cv2.imread(imagen_path)
    if img is None:
        raise ValueError(f"No se pudo cargar: {imagen_path}")
    
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    height, width = img_rgb.shape[:2]
    
    print(f"\n📊 Análisis de imagen:")
    print(f"   Dimensiones: {width}x{height} px")
    
    # ROI propuesta basada en estructura típica MIROVA
    # "Last Month" está en parte superior
    # Últimos ~7 días están a la derecha
    
    # X: 70% a 98% del ancho (últimos días)
    roi_x_start = int(width * 0.70)
    roi_x_end = int(width * 0.98)
    
    # Y: 10% a 48% de altura (solo "Last Month", no "Last Year")
    roi_y_start = int(height * 0.10)
    roi_y_end = int(height * 0.48)
    
    # Extraer ROI
    roi = img_rgb[roi_y_start:roi_y_end, roi_x_start:roi_x_end]
    
    # Detectar puntos rojos
    mask_rojo = cv2.inRange(roi, np.array([200, 0, 0]), np.array([255, 50, 50]))
    contornos_r, _ = cv2.findContours(mask_rojo, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    puntos_rojos = [c for c in contornos_r if cv2.contourArea(c) > 5]
    
    # Detectar puntos negros
    mask_negro = cv2.inRange(roi, np.array([0, 0, 0]), np.array([50, 50, 50]))
    contornos_n, _ = cv2.findContours(mask_negro, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    puntos_negros = [c for c in contornos_n if cv2.contourArea(c) > 5]
    
    print(f"\n🔍 Análisis de ROI propuesta:")
    print(f"   X: {roi_x_start} → {roi_x_end} px ({(roi_x_end-roi_x_start)/width*100:.1f}% ancho)")
    print(f"   Y: {roi_y_start} → {roi_y_end} px ({(roi_y_end-roi_y_start)/height*100:.1f}% altura)")
    print(f"   Puntos rojos detectados: {len(puntos_rojos)}")
    print(f"   Puntos negros detectados: {len(puntos_negros)}")
    
    # Generar visualización
    img_con_roi = img_rgb.copy()
    cv2.rectangle(img_con_roi, (roi_x_start, roi_y_start), (roi_x_end, roi_y_end), (0, 255, 0), 3)
    
    # Marcar puntos detectados en ROI
    roi_con_puntos = roi.copy()
    for cnt in puntos_rojos:
        M = cv2.moments(cnt)
        if M['m00'] != 0:
            cx = int(M['m10'] / M['m00'])
            cy = int(M['m01'] / M['m00'])
            # Convertir a coordenadas globales
            cx_global = roi_x_start + cx
            cy_global = roi_y_start + cy
            cv2.circle(img_con_roi, (cx_global, cy_global), 8, (255, 0, 255), -1)
            cv2.circle(roi_con_puntos, (cx, cy), 8, (255, 0, 255), -1)
    
    for cnt in puntos_negros:
        M = cv2.moments(cnt)
        if M['m00'] != 0:
            cx = int(M['m10'] / M['m00'])
            cy = int(M['m01'] / M['m00'])
            cx_global = roi_x_start + cx
            cy_global = roi_y_start + cy
            cv2.circle(img_con_roi, (cx_global, cy_global), 8, (0, 255, 255), -1)
            cv2.circle(roi_con_puntos, (cx, cy), 8, (0, 255, 255), -1)
    
    # Guardar visualizaciones
    from PIL import Image
    Image.fromarray(img_con_roi).save('ROI_MARCADA_COMPLETA.png')
    Image.fromarray(roi_con_puntos).save('ROI_AMPLIADA_CON_PUNTOS.png')
    
    print(f"\n💾 Visualizaciones guardadas:")
    print(f"   - ROI_MARCADA_COMPLETA.png")
    print(f"   - ROI_AMPLIADA_CON_PUNTOS.png")
    
    # Calcular porcentajes
    x_min_pct = roi_x_start / width
    x_max_pct = roi_x_end / width
    y_min_pct = roi_y_start / height
    y_max_pct = roi_y_end / height
    
    # Generar datos JSON
    roi_data = {
        'timestamp': datetime.now().isoformat(),
        'volcan_test': VOLCAN_TEST,
        'sensor_test': SENSOR_TEST,
        'dimensiones_imagen': {'width': width, 'height': height},
        'roi_absoluto': {
            'x_start': roi_x_start,
            'x_end': roi_x_end,
            'y_start': roi_y_start,
            'y_end': roi_y_end
        },
        'roi_relativo': {
            'x_start_pct': round(x_min_pct, 4),
            'x_end_pct': round(x_max_pct, 4),
            'y_start_pct': round(y_min_pct, 4),
            'y_end_pct': round(y_max_pct, 4)
        },
        'puntos_detectados': {
            'rojos': len(puntos_rojos),
            'negros': len(puntos_negros)
        }
    }
    
    # Guardar JSON
    with open('roi_coordenadas.json', 'w') as f:
        json.dump(roi_data, f, indent=2)
    
    print(f"\n💾 Coordenadas guardadas: roi_coordenadas.json")
    
    # Generar código Python
    codigo = f"""
# ===== CÓDIGO PARA ocr_utils.py =====
# Generado automáticamente: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Volcán test: {VOLCAN_TEST} - {SENSOR_TEST}

# ROI para análisis temporal (últimos ~7 días)
roi_x_start = int(width * {x_min_pct:.4f})  # {x_min_pct*100:.1f}% del ancho
roi_x_end = int(width * {x_max_pct:.4f})      # {x_max_pct*100:.1f}%
roi_y_start = int(height * {y_min_pct:.4f}) # {y_min_pct*100:.1f}% de altura
roi_y_end = int(height * {y_max_pct:.4f})     # {y_max_pct*100:.1f}%

# Extraer ROI
roi = img_rgb[roi_y_start:roi_y_end, roi_x_start:roi_x_end]

# VALIDACIÓN:
# - Puntos rojos detectados: {len(puntos_rojos)}
# - Puntos negros detectados: {len(puntos_negros)}
"""
    
    # Guardar código
    with open('roi_codigo.py', 'w') as f:
        f.write(codigo)
    
    print(f"\n💻 Código Python generado: roi_codigo.py")
    print("="*70)
    print(codigo)
    print("="*70)
    
    return roi_data


def main():
    print("="*70)
    print("🎯 DEFINIR ROI - GitHub Actions")
    print("="*70)
    print(f"\nVolcán test: {VOLCAN_TEST}")
    print(f"Sensor test: {SENSOR_TEST}")
    
    # Paso 1: Descargar imagen
    imagen_path = descargar_imagen_dist(VOLCAN_TEST, SENSOR_TEST)
    
    if imagen_path and os.path.exists(imagen_path):
        # Paso 2: Analizar y generar ROI
        roi_data = analizar_roi_automatico(imagen_path)
        
        print(f"\n✅ PROCESO COMPLETADO")
        print(f"\n📋 ARCHIVOS GENERADOS:")
        print(f"   1. roi_coordenadas.json - Datos JSON para parsing")
        print(f"   2. roi_codigo.py - Código Python listo para copiar")
        print(f"   3. ROI_MARCADA_COMPLETA.png - Visualización completa")
        print(f"   4. ROI_AMPLIADA_CON_PUNTOS.png - ROI ampliada")
        
        print(f"\n🎯 SIGUIENTE PASO:")
        print(f"   1. Descarga 'roi_coordenadas.json' desde artefactos")
        print(f"   2. Descarga imágenes para verificar visualmente")
        print(f"   3. Si ROI es correcta, copia código a ocr_utils.py")
        print(f"   4. Si necesitas ajustar, modifica porcentajes en JSON")
        
        return 0
    else:
        print(f"\n❌ No se pudo descargar imagen")
        print(f"   Verifica que el volcán existe en MIROVA")
        return 1


if __name__ == "__main__":
    exit(main())
