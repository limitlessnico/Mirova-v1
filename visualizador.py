import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import json
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE RUTAS ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"

# Diferenciación técnica de sensores
MAPA_MARCADORES = {
    "MODIS": "^",      # Triángulo
    "VIIRS375": "s",   # Cuadrado
    "VIIRS750": "o",   # Círculo
    "VIIRS": "o"       
}

def actualizar_estado_sistema(exito=True):
    """Genera el JSON de salud para el Dashboard Mirova-OVDAS"""
    estado = {
        "ultima_actualizacion": datetime.now().strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 MONITOR MIROVA-OVDAS OPERATIVO" if exito else "🔴 ERROR DE ENLACE",
        "color": "#2ecc71" if exito else "#e74c3c"
    }
    with open(ARCHIVO_STATUS, "w") as f:
        json.dump(estado, f)

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_tema):
    ahora = datetime.now()
    fecha_limite = ahora - timedelta(days=dias)
    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    ax.set_xlim([fecha_limite, ahora])

    if df_volcan is not None and not df_volcan.empty:
        df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
        if not df_f.empty:
            df_f = df_f.sort_values('Fecha_Obj')
            v_max = df_f['VRP_MW'].max()

            # --- SOMBREADO DINÁMICO INTELIGENTE ---
            # Las zonas solo aparecen si los datos alcanzan la escala
            if v_max < 5:
                ax.axhspan(0, 1, color='green', alpha=0.05, label='Muy Bajo')
                ax.set_ylim(0, max(1.5, v_max * 1.3))
            elif v_max < 50:
                ax.axhspan(0, 1, color='green', alpha=0.03)
                ax.axhspan(1, 10, color='yellow', alpha=0.05, label='Bajo')
                ax.axhspan(10, 50, color='orange', alpha=0.05, label='Moderado')
                ax.set_ylim(0, max(12, v_max * 1.2))
            else:
                ax.axhspan(10, 100, color='orange', alpha=0.05)
                ax.axhspan(100, 1000, color='red', alpha=0.07, label='Alto')
                ax.set_ylim(0, max(110, v_max * 1.2))

            # --- DIBUJO DE PUNTOS POR SENSOR ---
            for sensor, grupo in df_f.groupby('Sensor'):
                m = MAPA_MARCADORES.get(sensor, "o")
                plt.scatter(grupo['Fecha_Obj'], grupo['VRP_MW'], color=color_tema, 
                            marker=m, s=110, edgecolors='white', linewidth=1, 
                            label=f"Sensor: {sensor}", zorder=5)
                plt.vlines(grupo['Fecha_Obj'], 0, grupo['VRP_MW'], color=color_tema, alpha=0.2)

            # LEYENDA TÉCNICA SUPERIOR
            plt.legend(loc='upper left', bbox_to_anchor=(0, 1.15), ncol=3, fontsize=9, frameon=False)
            
            # Anotación de Pico Máximo
            plt.annotate(f"PICO: {v_max} MW", xy=(df_f.loc[df_f['VRP_MW'].idxmax(), 'Fecha_Obj'], v_max),
                         xytext=(8, 8), textcoords='offset points', fontsize=9, fontweight='bold',
                         bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=color_tema, alpha=0.9))
        else:
            plt.text(0.5, 0.5, 'MIROVA-OVDAS: SIN ALERTAS', ha='center', va='center', transform=ax.transAxes, color='gray')
    else:
        plt.text(0.5, 0.5, 'ESTADO NOMINAL\n(Sin anomalías térmicas)', ha='center', va='center', transform=ax.transAxes, color='gray', fontweight='bold')

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.title(f"Mirova-OVDAS VRP: {nombre_volcan}", fontsize=11, fontweight='bold', pad=30)
    plt.ylabel("Potencia Radiada Volcánica (MW)")
    plt.grid(True, linestyle=':', alpha=0.3)
    plt.gcf().autofmt_xdate()
    
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    plt.savefig(os.path.join(ruta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight', dpi=110)
    plt.close()

def procesar():
    try:
        VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]
        df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
        if not df.empty:
            df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC'])
            df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        
        for v in VOLCANES:
            df_v = df[df['Volcan'] == v] if not df.empty and v in df['Volcan'].values else None
            generar_grafico_volcan(df_v, v, 30, "Mensual", "#e67e22")
            generar_grafico_volcan(df_v, v, 365, "Anual", "#2980b9")
        actualizar_estado_sistema(True)
    except:
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
