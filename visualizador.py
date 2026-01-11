import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_DATOS = "monitoreo_satelital/registro_vrp_consolidado.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def preparar_datos():
    if not os.path.exists(ARCHIVO_DATOS):
        return None
    
    df = pd.read_csv(ARCHIVO_DATOS)
    df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
    df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce').fillna(0)
    
    # --- FILTRO CRÍTICO ---
    # Solo graficamos registros que sean Alertas Reales (dentro del radio) y con energía > 0
    df_limpio = df[(df['Clasificacion'] == 'ALERTA VOLCANICA') & (df['VRP_MW'] > 0)].copy()
    
    return df_limpio

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_linea):
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_filtrado = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    # Si no hay alertas reales en este periodo, creamos un gráfico vacío con un mensaje
    plt.figure(figsize=(10, 5))
    
    if not df_filtrado.empty:
        df_filtrado = df_filtrado.sort_values('Fecha_Obj')
        plt.plot(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
                 marker='o', linestyle='-', color=color_linea, markersize=6, label='Alertas Validadas (MW)')
        plt.fill_between(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], color=color_linea, alpha=0.2)
    else:
        plt.text(0.5, 0.5, 'Sin actividad térmica detectada en este periodo', 
                 ha='center', va='center', fontsize=12, color='gray')

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    plt.gcf().autofmt_xdate()
    plt.title(f"Historial Térmico Real: {nombre_volcan}", fontsize=14, fontweight='bold')
    plt.ylabel("Energía Radiada (MW)")
    plt.grid(True, linestyle='--', alpha=0.3)
    
    ruta_carpeta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta_carpeta, exist_ok=True)
    plt.savefig(os.path.join(ruta_carpeta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight')
    plt.close()

def procesar_visualizacion():
    df = preparar_datos()
    if df is None: return
    
    # Lista de volcanes detectados con alertas
    for volcan in df['Volcan'].unique():
        df_v = df[df['Volcan'] == volcan]
        generar_grafico_volcan(df_v, volcan, 30, "Mensual", "#FF2200") # Rojo intenso para alertas
        generar_grafico_volcan(df_v, volcan, 365, "Anual", "#007ACC")

if __name__ == "__main__":
    procesar_visualizacion()
