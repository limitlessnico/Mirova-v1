import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE RUTAS ---
# Cambiamos la fuente al archivo que ya está filtrado por el scraper
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def preparar_datos():
    """Carga la base de datos de alertas reales"""
    if not os.path.exists(ARCHIVO_POSITIVOS):
        print(f"ℹ️ No se encontró el archivo de positivos: {ARCHIVO_POSITIVOS}")
        return None
    
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        if df.empty:
            return None
            
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce').fillna(0)
        
        # Aunque el archivo es de positivos, aseguramos que no haya ruidos menores a 0.1 MW
        df_listo = df[df['VRP_MW'] > 0].copy()
        return df_listo
    except Exception as e:
        print(f"❌ Error al leer la base de datos: {e}")
        return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_linea):
    """Genera el gráfico para el dashboard"""
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_filtrado = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    plt.figure(figsize=(10, 5))
    
    if not df_filtrado.empty:
        df_filtrado = df_filtrado.sort_values('Fecha_Obj')
        plt.plot(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
                 marker='o', linestyle='-', color=color_linea, linewidth=2, label='Energía Real (MW)')
        plt.fill_between(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], color=color_linea, alpha=0.15)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
        plt.gcf().autofmt_xdate()
    else:
        # Si no hay datos, el gráfico dirá que está en calma
        plt.text(0.5, 0.5, 'Sin actividad térmica reciente', 
                 ha='center', va='center', fontsize=12, color='gray', transform=plt.gca().transAxes)

    plt.title(f"Actividad Térmica: {nombre_volcan} ({sufijo_archivo})", fontsize=14, fontweight='bold')
    plt.ylabel("Potencia (MW)")
    plt.grid(True, linestyle='--', alpha=0.3)
    
    ruta_carpeta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta_carpeta, exist_ok=True)
    plt.savefig(os.path.join(ruta_carpeta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight')
    plt.close()

def procesar_visualizacion():
    print("🎨 Iniciando visualización desde base de datos de positivos...")
    df = preparar_datos()
    
    if df is None:
        print("ℹ️ Nada que graficar hoy.")
        return

    volcanes_con_datos = df['Volcan'].unique()
    for volcan in volcanes_con_datos:
        df_v = df[df['Volcan'] == volcan]
        generar_grafico_volcan(df_v, volcan, 30, "Mensual", "#FF4500")
        generar_grafico_volcan(df_v, volcan, 365, "Anual", "#1E90FF")

if __name__ == "__main__":
    procesar_visualizacion()
