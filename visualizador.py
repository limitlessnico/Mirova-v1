import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE RUTAS ---
ARCHIVO_DATOS = "monitoreo_satelital/registro_vrp_consolidado.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def preparar_datos():
    """Carga la base de datos y aplica el filtro de calidad científica"""
    if not os.path.exists(ARCHIVO_DATOS):
        print(f"⚠️ Archivo no encontrado: {ARCHIVO_DATOS}")
        return None
    
    try:
        df = pd.read_csv(ARCHIVO_DATOS)
        # Convertir fechas a objetos datetime para el eje X
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        # Asegurar que VRP sea un número (limpia textos o vacíos)
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce').fillna(0)
        
        # --- FILTRO CRÍTICO (Solo alertas reales dentro del cráter) ---
        # 1. Clasificacion debe ser 'ALERTA VOLCANICA'
        # 2. VRP_MW debe ser mayor a 0
        df_limpio = df[(df['Clasificacion'] == 'ALERTA VOLCANICA') & (df['VRP_MW'] > 0)].copy()
        
        return df_limpio
    except Exception as e:
        print(f"❌ Error procesando CSV: {e}")
        return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_linea):
    """Dibuja el gráfico de tendencia para un volcán específico"""
    
    # Filtrar por el periodo solicitado (Mensual o Anual)
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_filtrado = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    plt.figure(figsize=(10, 5))
    
    if not df_filtrado.empty:
        # Ordenar cronológicamente antes de graficar
        df_filtrado = df_filtrado.sort_values('Fecha_Obj')
        
        # Graficar línea y puntos
        plt.plot(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
                 marker='o', linestyle='-', color=color_linea, 
                 linewidth=2, markersize=6, label='Energía Radiada (MW)')
        
        # Sombreado bajo la curva
        plt.fill_between(df_filtrado['Fecha_Obj'], df_filtrado['VRP_MW'], 
                         color=color_linea, alpha=0.15)
        
        # Formato de fechas en el eje X
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
        plt.gcf().autofmt_xdate()
    else:
        # Si el volcán está en calma o solo hubo falsos positivos, mostrar mensaje
        plt.text(0.5, 0.5, 'Sin actividad térmica detectada en este periodo', 
                 ha='center', va='center', fontsize=12, color='gray', transform=plt.gca().transAxes)

    # Estética del gráfico
    plt.title(f"Tendencia Térmica: {nombre_volcan} ({sufijo_archivo})", fontsize=14, fontweight='bold')
    plt.ylabel("Potencia Radiativa (MW)")
    plt.grid(True, linestyle='--', alpha=0.3)
    
    # Crear carpeta si no existe
    ruta_carpeta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta_carpeta, exist_ok=True)
    
    # Guardado del archivo
    nombre_final = f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"
    plt.savefig(os.path.join(ruta_carpeta, nombre_final), bbox_inches='tight', dpi=100)
    plt.close()
    print(f"   📊 Gráfico generado: {nombre_volcan} ({sufijo_archivo})")

def procesar_visualizacion():
    print("🎨 Generando visualizaciones filtradas...")
    df = preparar_datos()
    
    if df is None or df.empty:
        print("ℹ️ No hay alertas reales para graficar en este momento.")
        return

    # Obtener lista de volcanes que tienen al menos una alerta real
    volcanes_activos = df['Volcan'].unique()

    for volcan in volcanes_activos:
        df_v = df[df['Volcan'] == volcan]
        
        # Generar versión Mensual (30 días)
        generar_grafico_volcan(df_v, volcan, 30, "Mensual", "#FF4500") # Naranja-Rojo
        
        # Generar versión Anual (365 días)
        generar_grafico_volcan(df_v, volcan, 365, "Anual", "#1E90FF") # Azul intenso

    print("✅ Proceso de visualización finalizado.")

if __name__ == "__main__":
    procesar_visualizacion()
