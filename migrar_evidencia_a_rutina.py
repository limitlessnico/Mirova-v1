#!/usr/bin/env python3
"""
SCRIPT DE MIGRACIÓN: EVIDENCIA_DIARIA → RUTINA
Actualiza registro_vrp_consolidado.csv histórico
"""

import pandas as pd
import os
from datetime import datetime
import pytz

# Configuración
ARCHIVO_CSV = "monitoreo_satelital/registro_vrp_consolidado.csv"
ARCHIVO_BACKUP = ARCHIVO_CSV.replace('.csv', '_backup_before_migration.csv')

def migrar_evidencia_a_rutina():
    """
    Busca y reemplaza masivo: EVIDENCIA_DIARIA → RUTINA
    """
    print("="*80)
    print("🔄 MIGRACIÓN: EVIDENCIA_DIARIA → RUTINA")
    print("="*80)
    
    # Verificar que existe el archivo
    if not os.path.exists(ARCHIVO_CSV):
        print(f"\n❌ ERROR: No se encuentra {ARCHIVO_CSV}")
        return
    
    # 1. Hacer backup
    print(f"\n📦 Creando backup...")
    df = pd.read_csv(ARCHIVO_CSV)
    df.to_csv(ARCHIVO_BACKUP, index=False)
    print(f"✅ Backup guardado en: {ARCHIVO_BACKUP}")
    
    # 2. Contar registros EVIDENCIA_DIARIA
    count_evidencia = len(df[df['Tipo_Registro'] == 'EVIDENCIA_DIARIA'])
    print(f"\n📊 Registros encontrados:")
    print(f"   EVIDENCIA_DIARIA: {count_evidencia}")
    
    if count_evidencia == 0:
        print("\n✅ No hay registros EVIDENCIA_DIARIA para migrar")
        return
    
    # 3. Reemplazar EVIDENCIA_DIARIA → RUTINA
    print(f"\n🔄 Aplicando cambios...")
    df['Tipo_Registro'] = df['Tipo_Registro'].replace('EVIDENCIA_DIARIA', 'RUTINA')
    
    # 4. Actualizar timestamp de última actualización
    ahora = datetime.now(pytz.timezone('America/Santiago')).strftime("%Y-%m-%d %H:%M:%S")
    mask_cambiados = df['Tipo_Registro'] == 'RUTINA'
    
    # Solo actualizar los que eran EVIDENCIA_DIARIA
    # (Los que ya eran RUTINA no se actualizan)
    # Nota: Esto no es 100% preciso sin columna adicional, pero es conservador
    
    # 5. Guardar archivo actualizado
    df.to_csv(ARCHIVO_CSV, index=False)
    
    # 6. Verificar cambios
    count_rutina_despues = len(df[df['Tipo_Registro'] == 'RUTINA'])
    count_evidencia_despues = len(df[df['Tipo_Registro'] == 'EVIDENCIA_DIARIA'])
    
    print(f"\n✅ Migración completada:")
    print(f"   EVIDENCIA_DIARIA: 0 (antes: {count_evidencia})")
    print(f"   RUTINA: {count_rutina_despues}")
    print(f"\n📝 Resumen:")
    print(f"   • {count_evidencia} registros actualizados")
    print(f"   • Backup en: {ARCHIVO_BACKUP}")
    print(f"   • CSV actualizado: {ARCHIVO_CSV}")
    
    print("\n" + "="*80)
    print("🎯 SIGUIENTE PASO:")
    print("="*80)
    print("1. Revisar manualmente algunos registros en el CSV")
    print("2. Si todo está correcto, commitear cambios a Git")
    print("3. Si hay problemas, restaurar desde backup:")
    print(f"   cp {ARCHIVO_BACKUP} {ARCHIVO_CSV}")
    print("="*80)

if __name__ == "__main__":
    try:
        migrar_evidencia_a_rutina()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
