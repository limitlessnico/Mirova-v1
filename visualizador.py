import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN ---
ARCHIVO_DATOS = "monitoreo_satelital/registro_vrp_consolidado.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def preparar_datos():
    """Carga y limpia el CSV para poder graficarlo"""
    if not os.path.exists(ARCHIVO_DATOS):
        print("⚠️ No hay base de datos para graficar.")
        return None
    
    df = pd.read_csv(ARCHIVO_DATOS)
    
    # Convertir fecha UTC a objeto datetime
    df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
    
    # Asegurar que VRP sea numérico
    df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce').fillna(0)
    
    return df

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_linea):
    """Genera un gráfico de línea para un volcán específico"""
    
    # Filtrar por fecha (últimos X días)
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_filtrado = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    if df_filtrado.empty:
        return # No hay datos recientes
    
    # Ordenar por fecha
    df_filtrado = df_filtrado.sort_values('Fecha_Obj')
    
    # --- CREACIÓN DEL GRÁFICO ---
    plt.figure(figsize=(10, 5))
    
    # Graficar datos
    plt.plot(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
             marker='o', linestyle='-', color=color_linea, markersize=4, alpha=0.7, label='Energía (MW)')
    
    # Formato del Eje X
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()
    
    # Títulos
    periodo_txt = "Últimos 30 Días" if dias == 30 else "Último Año"
    plt.title(f"Actividad Radiativa: {nombre_volcan} ({periodo_txt})", fontsize=14, fontweight='bold')
    plt.ylabel("Energía Radiada (MW)")
    plt.xlabel("Fecha")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    
    # Guardar Imagen
    ruta_carpeta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta_carpeta, exist_ok=True)
    
    nombre_archivo = f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"
    ruta_final = os.path.join(ruta_carpeta, nombre_archivo)
    
    plt.savefig(ruta_final, bbox_inches='tight')
    plt.close()
    print(f"   📊 Gráfico generado: {nombre_volcan} ({sufijo_archivo})")

def procesar_visualizacion():
    print("🎨 Iniciando Generador de Gráficos...")
    df = preparar_datos()
    if df is None: return

    os.makedirs(CARPETA_GRAFICOS, exist_ok=True)
    lista_volcanes = df['Volcan'].unique()

    for volcan in lista_volcanes:
        df_v = df[df['Volcan'] == volcan]
        # Gráfico Mensual (Rojo)
        generar_grafico_volcan(df_v, volcan, 30, "Mensual", "#FF5733")
        # Gráfico Anual (Azul)
        generar_grafico_volcan(df_v, volcan, 365, "Anual", "#007ACC")

    print("✅ Visualización completada.")

if __name__ == "__main__":
    procesar_visualizacion()
