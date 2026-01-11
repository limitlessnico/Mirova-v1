import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import json
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"

# Mapeo de iconos por sensor
MAPA_MARCADORES = {
    "MODIS": "^",      # Triángulo
    "VIIRS375": "s",   # Cuadrado
    "VIIRS750": "o",   # Círculo
    "VIIRS": "o"       # Genérico
}

def actualizar_estado_sistema(exito=True):
    estado = {
        "ultima_actualizacion": datetime.now().strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 OPERATIVO" if exito else "🔴 ERROR",
        "color": "#28a745" if exito else "#dc3545"
    }
    with open(ARCHIVO_STATUS, "w") as f:
        json.dump(estado, f)

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_tema):
    ahora = datetime.now()
    fecha_limite = ahora - timedelta(days=dias)
    df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy() if df_volcan is not None else pd.DataFrame()
    
    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    ax.set_xlim([fecha_limite, ahora])

    if not df_f.empty:
        df_f = df_f.sort_values('Fecha_Obj')
        
        # 1. ZONAS DE INTENSIDAD (MIROVA Thresholds)
        ax.axhspan(0, 1, color='green', alpha=0.05, label='Muy Bajo')
        ax.axhspan(1, 10, color='yellow', alpha=0.05, label='Bajo')
        ax.axhspan(10, 100, color='orange', alpha=0.05, label='Moderado')
        ax.axhspan(100, 1000, color='red', alpha=0.05, label='Alto')

        # 2. DIFERENCIACIÓN POR SENSOR
        for sensor, grupo in df_f.groupby('Sensor'):
            m = MAPA_MARCADORES.get(sensor, "o")
            plt.vlines(grupo['Fecha_Obj'], 0, grupo['VRP_MW'], color=color_tema, alpha=0.2, linewidth=1)
            plt.scatter(grupo['Fecha_Obj'], grupo['VRP_MW'], color=color_tema, marker=m, 
                        s=90, edgecolors='white', linewidth=1, alpha=0.9, zorder=3, label=f"Sensor {sensor}")

        # 3. ANOTACIÓN VALOR MÁXIMO
        max_idx = df_f['VRP_MW'].idxmax()
        plt.annotate(f"Máx: {df_f.loc[max_idx, 'VRP_MW']} MW", 
                     xy=(df_f.loc[max_idx, 'Fecha_Obj'], df_f.loc[max_idx, 'VRP_MW']),
                     xytext=(10, 10), textcoords='offset points', fontsize=9, fontweight='bold',
                     bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=color_tema, alpha=0.8))
    else:
        # 4. MENSAJE DE "FALSA CALMA"
        plt.text(0.5, 0.5, 'SIN ANOMALÍAS TÉRMICAS DETECTADAS\n(Últimos 30 días)', 
                 ha='center', va='center', transform=ax.transAxes, fontsize=12, color='gray', fontweight='bold')

    # Estética final
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.title(f"Monitoreo: {nombre_volcan} - {sufijo_archivo}", fontsize=13, fontweight='bold')
    plt.ylabel("Potencia Radiada (MW)")
    plt.grid(True, linestyle=':', alpha=0.3)
    plt.gcf().autofmt_xdate()
    
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    plt.savefig(os.path.join(ruta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight', dpi=100)
    plt.close()

def procesar():
    try:
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        
        # Lista de volcanes completa para evitar que las tarjetas desaparezcan
        VOLCANES = ["Lascar", "Villarrica", "Llaima", "Copahue", "Nevados de Chillan", "Peteroa", "Lastarria", "Isluga", "Puyehue-Cordon Caulle", "Chaiten"]
        
        for v in VOLCANES:
            df_v = df[df['Volcan'] == v] if v in df['Volcan'].values else None
            generar_grafico_volcan(df_v, v, 30, "Mensual", "#FF4500")
            generar_grafico_volcan(df_v, v, 365, "Anual", "#00BFFF")
        
        actualizar_estado_sistema(True)
    except Exception as e:
        print(f"Error: {e}")
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
