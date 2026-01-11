import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import shutil
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE RUTAS ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def limpiar_graficos_antiguos():
    """Borra la carpeta de gráficos para asegurar que solo se muestren alertas vigentes"""
    if os.path.exists(CARPETA_GRAFICOS):
        shutil.rmtree(CARPETA_GRAFICOS)
    os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

def preparar_datos():
    """Carga los datos y filtra solo alertas reales positivas"""
    if not os.path.exists(ARCHIVO_POSITIVOS): 
        return None
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        if df.empty: 
            return None
        
        # Conversión de fechas y limpieza de valores numéricos
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        
        # Filtro estricto: solo valores mayores a 0 para el dashboard
        return df[df['VRP_MW'] > 0].copy()
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_punto):
    """Genera un gráfico de puntos dispersos con eje de tiempo corregido"""
    ahora = datetime.now()
    fecha_limite = ahora - timedelta(days=dias)
    
    # Filtrar por el rango de tiempo (30 o 365 días)
    df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    # Si no hay alertas en este periodo, no generamos el archivo (el Dashboard lo ocultará)
    if df_f.empty: 
        return 

    plt.figure(figsize=(10, 5))
    df_f = df_f.sort_values('Fecha_Obj')
    
    # Gráfico de puntos (Scatter) para datos discretos satelitales
    plt.scatter(df_f['Fecha_Obj'], df_f['VRP_MW'], 
                color=color_punto, s=70, edgecolors='black', alpha=0.8, zorder=3)
    
    # --- CORRECCIÓN DEL EJE DE FECHAS (X) ---
    ax = plt.gca()
    
    # Forzamos el rango del eje X para que coincida exactamente con el periodo solicitado
    ax.set_xlim([fecha_limite, ahora])
    
    # Formateador: Día-Mes Hora:Minuto (crucial para pasadas múltiples en un día)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m %H:%M'))
    
    # Localizador automático: decide el espacio óptimo entre etiquetas
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    # Estética del gráfico
    plt.title(f"Actividad Térmica Real: {nombre_volcan} (Últimos {dias} días)", fontsize=13, fontweight='bold')
    plt.ylabel("Potencia Radiada (MW)")
    plt.grid(True, linestyle='--', alpha=0.3, zorder=0)
    
    # Rotación automática de etiquetas de fecha para legibilidad
    plt.gcf().autofmt_xdate()
    
    # Guardado organizado por carpetas de volcán
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    
    nombre_img = f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"
    plt.savefig(os.path.join(ruta, nombre_img), bbox_inches='tight', dpi=100)
    plt.close()
    print(f"📊 Gráfico actualizado: {nombre_volcan} ({sufijo_archivo})")

def procesar_visualizacion():
    print("🎨 Iniciando generación de visualizaciones...")
    
    # 1. Limpiamos gráficos viejos (evita mostrar volcanes que ya no tienen alertas)
    limpiar_graficos_antiguos()
    
    # 2. Obtenemos datos limpios
    df = preparar_datos()
    if df is None:
        print("ℹ️ No hay alertas positivas en el registro para graficar.")
        return

    # 3. Iteramos por cada volcán que tiene datos reales
    volcanes_activos = df['Volcan'].unique()
    for v in volcanes_activos:
        df_v = df[df['Volcan'] == v]
        
        # Generar versión Mensual (Naranja) y Anual (Azul)
        generar_grafico_volcan(df_v, v, 30, "Mensual", "#FF4500")
        generar_grafico_volcan(df_v, v, 365, "Anual", "#1E90FF")

    print("✅ Visualización completada con éxito.")

if __name__ == "__main__":
    procesar_visualizacion()
