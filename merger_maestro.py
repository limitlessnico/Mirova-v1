"""
MERGER_MAESTRO.PY
Combina registro_vrp_consolidado.csv + registro_vrp_ocr.csv
Genera registro_vrp_maestro.csv
"""

import pandas as pd
import os

# =========================
# CONFIGURACIÓN
# =========================

CARPETA_PRINCIPAL = "monitoreo_satelital"
DB_CONSOLIDADO = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_consolidado.csv")
DB_OCR = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_ocr.csv")
DB_MAESTRO = os.path.join(CARPETA_PRINCIPAL, "registro_vrp_maestro.csv")

# Columnas del maestro (todas las de consolidado + extras)
COLUMNAS_MAESTRO = [
    "timestamp", "Fecha_Satelite_UTC", "Fecha_Captura_Chile",
    "Volcan", "Sensor", "VRP_MW", "Distancia_km", "Tipo_Registro",
    "Clasificacion Mirova", "Ruta Foto",
    "Fecha_Proceso_GitHub", "Ultima_Actualizacion", "Editado",
    # Nuevas columnas
    "Origen_Dato", "Confianza_Validacion", "Requiere_Verificacion",
    "Nota_Validacion"
]

def merge():
    """Genera CSV maestro"""
    
    print("="*80)
    print("🔄 MERGER - Generando CSV Maestro")
    print("="*80)
    
    # Cargar CSVs
    df_consolidado = pd.read_csv(DB_CONSOLIDADO) if os.path.exists(DB_CONSOLIDADO) else pd.DataFrame()
    df_ocr = pd.read_csv(DB_OCR) if os.path.exists(DB_OCR) else pd.DataFrame()
    
    if df_consolidado.empty and df_ocr.empty:
        print("❌ No hay datos para procesar")
        return
    
    # Preparar consolidado (agregar columnas nuevas)
    if not df_consolidado.empty:
        df_consolidado['Origen_Dato'] = 'latest.php'
        df_consolidado['Confianza_Validacion'] = 'N/A'
        df_consolidado['Requiere_Verificacion'] = False
        df_consolidado['Nota_Validacion'] = 'Capturado por latest.php'
    
    # Preparar OCR (agregar columnas faltantes SOLO si no existen)
    if not df_ocr.empty:
        df_ocr['Origen_Dato'] = 'OCR'
        # Solo agregar si no existen
        if 'Distancia_km' not in df_ocr.columns:
            df_ocr['Distancia_km'] = 0.0
        if 'Clasificacion Mirova' not in df_ocr.columns:
            df_ocr['Clasificacion Mirova'] = 'N/A'
        if 'Ultima_Actualizacion' not in df_ocr.columns:
            df_ocr['Ultima_Actualizacion'] = df_ocr['Fecha_Proceso_GitHub']
        if 'Editado' not in df_ocr.columns:
            df_ocr['Editado'] = 'NO'
    
    # Combinar
    df_maestro = pd.concat([df_consolidado, df_ocr], ignore_index=True)
    
    # Detectar duplicados (mismo timestamp + volcán + sensor)
    df_maestro['duplicado'] = df_maestro.duplicated(
        subset=['timestamp', 'Volcan', 'Sensor'], 
        keep='first'
    )
    
    # Marcar eventos que están en ambos
    for idx, row in df_maestro[df_maestro['duplicado']].iterrows():
        mask = (
            (df_maestro['timestamp'] == row['timestamp']) &
            (df_maestro['Volcan'] == row['Volcan']) &
            (df_maestro['Sensor'] == row['Sensor']) &
            (~df_maestro['duplicado'])
        )
        df_maestro.loc[mask, 'Origen_Dato'] = 'ambos'
    
    # Eliminar duplicados (mantener el de latest.php)
    df_maestro = df_maestro[~df_maestro['duplicado']].copy()
    df_maestro.drop(columns=['duplicado'], inplace=True)
    
    # Ordenar por timestamp DESC
    df_maestro = df_maestro.sort_values('timestamp', ascending=False)
    
    # Seleccionar columnas finales
    columnas_disponibles = [c for c in COLUMNAS_MAESTRO if c in df_maestro.columns]
    df_maestro = df_maestro[columnas_disponibles]
    
    # Guardar
    df_maestro.to_csv(DB_MAESTRO, index=False)
    
    print(f"✅ CSV Maestro generado:")
    print(f"   Total eventos: {len(df_maestro)}")
    print(f"   De latest.php: {len(df_maestro[df_maestro['Origen_Dato'] == 'latest.php'])}")
    print(f"   De OCR: {len(df_maestro[df_maestro['Origen_Dato'] == 'OCR'])}")
    print(f"   En ambos: {len(df_maestro[df_maestro['Origen_Dato'] == 'ambos'])}")
    print("="*80)


if __name__ == "__main__":
    merge()
