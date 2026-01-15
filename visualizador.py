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
            return
        
        df = pd.read_csv(ARCHIVO_POSITIVOS)
        df['Fecha_Chile'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
        df['VRP_MW'] = pd.to_numeric(df['VRP_MW'], errors='coerce')

        fig = px.scatter(
            df, x="Fecha_Chile", y="VRP_MW", color="Sensor",
            facet_col="Volcan", facet_col_wrap=2,
            color_discrete_map=COLORES_SENSORES,
            hover_data={"VRP_MW": ':.2f', "Sensor": True, "Fecha_Chile": "|%d %b, %H:%M"},
            template="plotly_dark"
        )

        # --- LÓGICA DE VALOR MÁXIMO (Rescatada del antiguo plt.annotate) ---
        for volcan in df['Volcan'].unique():
            df_v = df[df['Volcan'] == volcan]
            if not df_v.empty:
                max_row = df_v.loc[df_v['VRP_MW'].idxmax()]
                # Añadir etiqueta al punto más alto en cada sub-gráfico
                fig.add_annotation(
                    x=max_row['Fecha_Chile'], y=max_row['VRP_MW'],
                    text=f"Máx: {max_row['VRP_MW']:.1f} MW",
                    showarrow=True, arrowhead=2,
                    patch={ "xref": "x", "yref": "y" },
                    row=(list(df['Volcan'].unique()).index(volcan) // 2) + 1,
                    col=(list(df['Volcan'].unique()).index(volcan) % 2) + 1
                )

        fig.update_layout(
            height=1400,
            title="Análisis Térmico VRP - Red de Monitoreo Volcánico Chile",
            yaxis_title="Potencia Radiada (MW)",
            font=dict(family="Segoe UI", size=11),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        fig.write_html(OUTPUT_INTERACTIVO, full_html=False, include_plotlyjs='cdn')
        actualizar_estado_sistema(True)
    except Exception as e:
        print(f"Error: {e}")
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
