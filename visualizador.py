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
    """Asegura que solo se grafiquen volcanes con alertas vigentes"""
    if os.path.exists(CARPETA_GRAFICOS):
        shutil.rmtree(CARPETA_GRAFICOS)
    os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

def preparar_datos():
    """Carga y filtra solo detecciones positivas reales"""
    if not os.path.exists(ARCHIVO_POSITIVOS): 
        return None
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        if df.empty: return None
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        return df[df['VRP_MW'] > 0].copy()
    except: return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_tema):
    """Genera un gráfico tipo 'Lollipop' (puntos con líneas guía)"""
    ahora = datetime.now()
    fecha_limite = ahora - timedelta(days=dias)
    
    df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    if df_f.empty: return 

    plt.figure(figsize=(10, 5))
    df_f = df_f.sort_values('Fecha_Obj')
    
    # --- EFECTO ESTÉTICO: LÍNEAS GUÍA VERTICALES ---
    # Esto une el punto con el eje X para facilitar la lectura de la fecha
    plt.vlines(df_f['Fecha_Obj'], 0, df_f['VRP_MW'], 
               color=color_tema, alpha=0.25, linestyle='-', linewidth=1, zorder=1)
    
    # --- PUNTOS DE ACTIVIDAD ---
    plt.scatter(df_f['Fecha_Obj'], df_f['VRP_MW'], 
                color=color_tema, s=85, edgecolors='white', linewidth=1, alpha=0.9, zorder=3)
    
    # --- CONFIGURACIÓN DEL EJE X (SOLO FECHAS) ---
    ax = plt.gca()
    ax.set_xlim([fecha_limite, ahora])
    
    # Formato solicitado: Día-Mes-Año
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    # Estética del gráfico
    plt.title(f"Actividad Térmica: {nombre_volcan} (Últimos {dias} días)", fontsize=13, fontweight='bold')
    plt.ylabel("Potencia Radiada (MW)")
    plt.grid(True, linestyle=':', alpha=0.3, zorder=0)
    plt.gcf().autofmt_xdate() # Rota las fechas para legibilidad
    
    # Guardado por volcán
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    plt.savefig(os.path.join(ruta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight', dpi=100)
    plt.close()

def procesar_visualizacion():
    print("🎨 Generando visualizaciones finales...")
    limpiar_graficos_antiguos()
    df = preparar_datos()
    
    if df is None:
        print("ℹ️ Sin datos para graficar.")
        return

    for v in df['Volcan'].unique():
        df_v = df[df['Volcan'] == v]
        # Mensual: Naranja Intenso | Anual: Azul Brillante
        generar_grafico_volcan(df_v, v, 30, "Mensual", "#FF4500")
        generar_grafico_volcan(df_v, v, 365, "Anual", "#00BFFF")

    print("✅ Dashboard actualizado con éxito.")

if __name__ == "__main__":
    procesar_visualizacion()
