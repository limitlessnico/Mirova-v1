import os
import pandas as pd

def auditar_sistema():
    csv_path = "monitoreo_satelital/registro_vrp_consolidado.csv"
    if not os.path.exists(csv_path):
        print("❌ Error: No se encuentra el archivo consolidado.")
        return

    df = pd.read_csv(csv_path)
    df['Fecha_Satelite_UTC'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
    df['Solo_Fecha'] = df['Fecha_Satelite_UTC'].dt.date
    
    print(f"=== REPORTE DE AUDITORÍA DE IMÁGENES Y LÓGICA DE CALMA ===\n")

    # 1. Auditoría de archivos físicos (¿Están donde el CSV dice?)
    registros_con_foto = df[df['Ruta Foto'] != 'No descargada']
    fisicos_ok = 0
    faltantes = []

    for _, row in registros_con_foto.iterrows():
        if os.path.exists(row['Ruta Foto']):
            fisicos_ok += 1
        else:
            faltantes.append(f"{row['Volcan']} [{row['Fecha_Satelite_UTC']}]: {row['Ruta Foto']}")

    print(f"📁 INTEGRIDAD FÍSICA:")
    print(f"   - Total en CSV: {len(registros_con_foto)}")
    print(f"   - Confirmados en carpeta: {fisicos_ok}")
    print(f"   - Archivos faltantes (Fantasmas): {len(faltantes)}")
    for f in faltantes[:5]: print(f"     ⚠️ Faltante: {f}")

    # 2. Auditoría de Lógica de Calma (¿Se bajó foto de evidencia tras un día sin alertas?)
    print(f"\n🔍 LÓGICA DE CALMA (EVIDENCIA DIARIA):")
    for volcan in df['Volcan'].unique():
        df_v = df[df['Volcan'] == volcan].sort_values('Fecha_Satelite_UTC')
        fechas = sorted(df_v['Solo_Fecha'].unique())
        
        for i in range(1, len(fechas)):
            ayer, hoy = fechas[i-1], fechas[i]
            alertas_ayer = df_v[(df_v['Solo_Fecha'] == ayer) & (df_v['Tipo_Registro'] == 'ALERTA_TERMICA')]
            
            if len(alertas_ayer) == 0: # Ayer fue calma
                evidencia_hoy = df_v[(df_v['Solo_Fecha'] == hoy) & (df_v['Ruta Foto'] != 'No descargada')]
                if evidencia_hoy.empty:
                    print(f"   ⚠️ {volcan}: Día {ayer} sin alertas, pero el {hoy} no registró foto de evidencia.")

if __name__ == "__main__":
    auditar_sistema()
