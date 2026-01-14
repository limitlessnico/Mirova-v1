import os
import pandas as pd

def auditar_estricto():
    csv_path = "monitoreo_satelital/registro_vrp_consolidado.csv"
    df = pd.read_csv(csv_path)
    df['Solo_Fecha'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.date
    
    print("🔍 AUDITORÍA DE REGLAS LÓGICAS\n")

    for volcan in df['Volcan'].unique():
        df_v = df[df['Volcan'] == volcan].sort_values('timestamp')
        fechas = sorted(df_v['Solo_Fecha'].unique())

        for fecha in fechas:
            dia_actual = df_v[df_v['Solo_Fecha'] == fecha]
            
            # REGLA 1: ¿Hubo alerta y evidencia el mismo día? (Redundancia)
            tiene_alerta = not dia_actual[dia_actual['Tipo_Registro'] == 'ALERTA_TERMICA'].empty
            tiene_evidencia = not dia_actual[dia_actual['Tipo_Registro'] == 'EVIDENCIA_DIARIA'].empty
            
            if tiene_alerta and tiene_evidencia:
                print(f"⚠️ REDUNDANCIA en {volcan} ({fecha}): Bajó evidencia diaria teniendo una alerta activa. (Gasto de espacio)")

            # REGLA 2: El caso Puyehue/Peteroa (Alerta sin fotos)
            alertas_sin_foto = dia_actual[(dia_actual['Tipo_Registro'] == 'ALERTA_TERMICA') & (dia_actual['Ruta Foto'] == 'No descargada')]
            for _, row in alertas_sin_foto.iterrows():
                print(f"❌ FALLO DE CAPTURA en {volcan} ({fecha}): Hay alerta de {row['VRP_MW']} MW pero la foto dice 'No descargada'.")

if __name__ == "__main__":
    auditar_estricto()
