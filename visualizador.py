import pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import shutil
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"

def limpiar_graficos_antiguos():
    """Borra la carpeta de gráficos para asegurar que solo se muestren alertas vigentes"""
    if os.path.exists(CARPETA_GRAFICOS):
        shutil.rmtree(CARPETA_GRAFICOS)
    os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

def preparar_datos():
    if not os.path.exists(ARCHIVO_POSITIVOS): return None
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        if df.empty: return None
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        # Aseguramos que solo procese valores estrictamente positivos
        return df[df['VRP_MW'] > 0].copy()
    except: return None

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_punto):
    fecha_limite = datetime.now() - timedelta(days=dias)
    df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
    
    if df_f.empty: return # Si no hay datos en el periodo, no genera el archivo

    plt.figure(figsize=(10, 5))
    df_f = df_f.sort_values('Fecha_Obj')
    
    # CAMBIO: Usamos 'scatter' (puntos sueltos) en lugar de 'plot' (líneas)
    plt.scatter(df_f['Fecha_Obj'], df_f['VRP_MW'], 
                color=color_punto, s=60, edgecolors='black', alpha=0.8, label='Alertas (MW)')
    
    plt.title(f"Actividad Térmica Real: {nombre_volcan}", fontsize=12, fontweight='bold')
    plt.ylabel("Potencia (MW)")
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    plt.gcf().autofmt_xdate()
    
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    plt.savefig(os.path.join(ruta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight')
    plt.close()

def procesar():
    limpiar_graficos_antiguos() # Paso vital para eliminar el "fantasma" de Copahue
    df = preparar_datos()
    if df is None: return

    for v in df['Volcan'].unique():
        df_v = df[df['Volcan'] == v]
        generar_grafico_volcan(df_v, v, 30, "Mensual", "#FF4500")
        generar_grafico_volcan(df_v, v, 365, "Anual", "#1E90FF")

if __name__ == "__main__":
    procesar()
