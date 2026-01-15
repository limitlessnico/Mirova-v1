import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import pytz
from datetime import datetime

# --- CONFIGURACIÓN DE RUTAS ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
OUTPUT_INTERACTIVO = "monitoreo_satelital/dashboard_interactivo.html"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"

COLORES_SENSORES = {
    "MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000", "VIIRS": "#C0C0C0"
}

def actualizar_estado_sistema(exito=True):
    tz_chile = pytz.timezone('America/Santiago')
    ahora_cl = datetime.now(tz_chile)
    estado = {
        "ultima_actualizacion": ahora_cl.strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 MONITOR MIROVA-OVDAS OPERATIVO",
        "color": "#2ecc71" if exito else "#e74c3c"
    }
    with open(ARCHIVO_STATUS, "w") as f:
        json.dump(estado, f)

def procesar():
    try:
        if not os.path.exists(ARCHIVO_POSITIVOS):
            actualizar_estado_sistema(False)
            return
        
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        df['Fecha_Chile'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
        df = df.sort_values('Fecha_Chile')

        # Gráfico Base con Estilo del Día 13
        fig = px.scatter(
            df, x="Fecha_Chile", y="VRP_MW", color="Sensor",
            facet_col="Volcan", facet_col_wrap=2,
            color_discrete_map=COLORES_SENSORES,
            hover_data={"VRP_MW": ':.2f', "Sensor": True, "Fecha_Chile": "|%d %b, %H:%M"},
            template="plotly_dark"
        )

        # RESCATE DE ETIQUETAS MÁXIMAS
        volcanes_lista = list(df['Volcan'].unique())
        for i, volcan in enumerate(volcanes_lista):
            df_v = df[df['Volcan'] == volcan]
            if not df_v.empty:
                max_row = df_v.loc[df_v['VRP_MW'].idxmax()]
                fig.add_annotation(
                    x=max_row['Fecha_Chile'], y=max_row['VRP_MW'],
                    text=f"MÁX: {max_row['VRP_MW']:.1f} MW",
                    showarrow=True, arrowhead=1, ax=0, ay=-30,
                    font=dict(color="#e0e0e0", size=10),
                    row=(i // 2) + 1, col=(i % 2) + 1
                )

        # RESCATE DE EJES Y FECHAS
        fig.update_xaxes(tickformat="%d %b", tickangle=-45, showgrid=True, gridcolor="#30363d")
        fig.update_yaxes(title_text="VRP (MW)", gridcolor="#30363d")

        fig.update_layout(
            height=1600,
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(22, 27, 34, 0.5)',
            font=dict(family="Segoe UI", size=11, color="#8b949e"),
            margin=dict(l=60, r=40, t=80, b=100),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        os.makedirs(os.path.dirname(OUTPUT_INTERACTIVO), exist_ok=True)
        fig.write_html(OUTPUT_INTERACTIVO, full_html=False, include_plotlyjs='cdn')
        actualizar_estado_sistema(True)
        
    except Exception as e:
        print(f"Error: {e}")
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
