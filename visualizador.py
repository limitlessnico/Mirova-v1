import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import json
import pytz
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE RUTAS ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_GRAFICOS = "monitoreo_satelital/graficos_tendencia"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"

MAPA_MARCADORES = {"MODIS": "^", "VIIRS375": "s", "VIIRS750": "o", "VIIRS": "o"}

def actualizar_estado_sistema(exito=True):
    # Usamos la hora de Chile para el estado del sistema
    tz_chile = pytz.timezone('America/Santiago')
    ahora_cl = datetime.now(tz_chile)
    estado = {
        "ultima_actualizacion": ahora_cl.strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 MONITOR MIROVA-OVDAS OPERATIVO" if exito else "🔴 ERROR DE ENLACE",
        "color": "#2ecc71" if exito else "#e74c3c"
    }
    with open(ARCHIVO_STATUS, "w") as f:
        json.dump(estado, f)

def generar_grafico_volcan(df_volcan, nombre_volcan, dias, sufijo_archivo, color_tema):
    # Configuramos el tiempo para que el eje X llegue exactamente hasta hoy
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    fecha_limite = ahora - timedelta(days=dias)
    
    plt.figure(figsize=(10, 6.5))
    ax = plt.gca()
    
    # Eje X siempre termina en el momento actual
    ax.set_xlim([fecha_limite, ahora])

    if df_volcan is not None and not df_volcan.empty:
        df_f = df_volcan[df_volcan['Fecha_Obj'] >= fecha_limite].copy()
        if not df_f.empty:
            df_f = df_f.sort_values('Fecha_Obj')
            v_max = df_f['VRP_MW'].max()

            # --- SOMBREADO DINÁMICO ---
            if v_max < 5:
                ax.axhspan(0, 1, color='green', alpha=0.05, label='Nivel Muy Bajo')
                ax.set_ylim(0, max(1.5, v_max * 1.5))
            elif v_max < 50:
                ax.axhspan(0, 10, color='yellow', alpha=0.05, label='Bajo')
                ax.axhspan(10, 50, color='orange', alpha=0.05, label='Moderado')
                ax.set_ylim(0, max(12, v_max * 1.2))
            else:
                ax.axhspan(100, 1000, color='red', alpha=0.07, label='Alto')
                ax.set_ylim(0, max(110, v_max * 1.2))

            for sensor, grupo in df_f.groupby('Sensor'):
                m = MAPA_MARCADORES.get(sensor, "o")
                plt.scatter(grupo['Fecha_Obj'], grupo['VRP_MW'], color=color_tema, 
                            marker=m, s=110, edgecolors='white', linewidth=1, 
                            label=f"Sensor: {sensor}", zorder=5)
                plt.vlines(grupo['Fecha_Obj'], 0, grupo['VRP_MW'], color=color_tema, alpha=0.2)

            plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, fontsize=9, frameon=True, shadow=True)
            
            # Anotación MAX
            plt.annotate(f"MAX: {v_max} MW", xy=(df_f.loc[df_f['VRP_MW'].idxmax(), 'Fecha_Obj'], v_max),
                         xytext=(10, 10), textcoords='offset points', fontsize=9, fontweight='bold',
                         bbox=dict(boxstyle="round", fc="white", ec=color_tema, alpha=0.9))
        else:
            plt.text(0.5, 0.5, 'SIN ANOMALÍAS TÉRMICAS', ha='center', va='center', transform=ax.transAxes, color='gray')
    else:
        plt.text(0.5, 0.5, 'SIN ANOMALÍAS TÉRMICAS', ha='center', va='center', transform=ax.transAxes, color='gray', fontweight='bold')

    # --- MEJORA DE CUADRÍCULA (GRID) ---
    # Marcadores mayores cada 5 días
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    
    # Marcadores menores cada 1 día (para contar visualmente)
    ax.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
    
    # Dibujamos las líneas: sólidas para fechas, punteadas suaves para días intermedios
    ax.grid(which='major', axis='x', linestyle='-', alpha=0.3, color='gray')
    ax.grid(which='minor', axis='x', linestyle=':', alpha=0.2, color='gray')
    ax.grid(axis='y', linestyle=':', alpha=0.3)
    
    plt.title(f"Actividad Térmica: {nombre_volcan}", fontsize=12, fontweight='bold', pad=50)
    plt.ylabel("Potencia Radiada (MW)")
    plt.gcf().autofmt_xdate()
    
    ruta = os.path.join(CARPETA_GRAFICOS, nombre_volcan)
    os.makedirs(ruta, exist_ok=True)
    plt.savefig(os.path.join(ruta, f"Grafico_{nombre_volcan}_{sufijo_archivo}.png"), bbox_inches='tight', dpi=110)
    plt.close()

def procesar():
    try:
        VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]
        
        if os.path.exists(ARCHIVO_POSITIVOS):
            df = pd.read_csv(ARCHIVO_POSITIVOS)
            # Conversión de fechas a la zona horaria de Chile
            df['Fecha_Obj'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
            df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')
        else:
            df = pd.DataFrame()

        for v in VOLCANES:
            df_v = df[df['Volcan'] == v] if not df.empty and v in df['Volcan'].values else None
            # Gráfico Mensual (30 días)
            generar_grafico_volcan(df_v, v, 30, "Mensual", "#e67e22")
            # Gráfico Anual (365 días)
            generar_grafico_volcan(df_v, v, 365, "Anual", "#2980b9")
            
        actualizar_estado_sistema(True)
    except Exception as e:
        print(f"Error procesando gráficos: {e}")
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
