import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def preparar_datos():
    if not os.path.exists(ARCHIVO_POSITIVOS):
        return None
    
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        if df.empty: return None
        
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        
        # FILTRO ESTRICTO: Solo valores mayores a 0 para evitar caídas falsas en el gráfico
        df_listo = df[df['VRP_MW'] > 0].copy()
        return df_listo
    except Exception as e:
        print(f"Error: {e}")
        return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_punto):
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_filtrado = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    plt.figure(figsize=(10, 5))
    
    if not df_filtrado.empty:
        df_filtrado = df_filtrado.sort_values('Fecha_Obj')
        
        # CAMBIO CLAVE: Usamos scatter para NO unir los puntos con líneas
        plt.scatter(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
                    color=color_punto, s=50, edgecolors='white', linewidth=0.5, label='Detecciones Reales')
        
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
        plt.gcf().autofmt_xdate()
    else:
        plt.text(0.5, 0.5, 'Sin actividad térmica validada', 
                 ha='center', va='center', transform=plt.gca().transAxes, color='gray')

    plt.title(f"Actividad Radiativa: {nombre_volcan} (Últimos {dias} Días)", fontsize=12, fontweight='bold')
    plt.ylabel("Energía Radiada (MW)")
    plt.grid(True, linestyle='--', alpha=0.2)
    
    ruta_carpeta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta_carpeta, exist_ok=True)
    plt.savefig(os.path.join(ruta_carpeta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight', dpi=100)
    plt.close()

def procesar_visualizacion():
    df = preparar_datos()
    if df is None: return

    for volcan in df['Volcan'].unique():
        df_v = df[df['Volcan'] == volcan]
        generar_grafico_volcan(df_v, volcan, 30, "Mensual", "#FF5733")
        generar_grafico_volcan(df_v, volcan, 365, "Anual", "#33C1FF")

if __name__ == "__main__":
    procesar_visualizacion()
