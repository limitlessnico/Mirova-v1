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

# Colores específicos para sensores
COLORES_SENSORES = {
    "MODIS": "#FFA500",    # Naranja
    "VIIRS375": "#FF4500", # Naranja Rojizo
    "VIIRS750": "#FF0000", # Rojo Puro
    "VIIRS": "#C0C0C0"     # Gris (genérico)
}

def actualizar_estado_sistema(exito=True):
    tz_chile = pytz.timezone('America/Santiago')
    ahora_cl = datetime.now(tz_chile)
    estado = {
        "ultima_actualizacion": ahora_cl.strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 MONITOR INTERACTIVO OPERATIVO" if exito else "🔴 ERROR EN GENERACIÓN",
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
        # Convertir a hora de Chile para el visualizador
        df['Fecha_Chile'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
        
        # Crear gráfico interactivo
        fig = px.scatter(
            df,
            x="Fecha_Chile",
            y="VRP_MW",
            color="Sensor",
            facet_col="Volcan",
            facet_col_wrap=2,
            color_discrete_map=COLORES_SENSORES,
            hover_name="Volcan",
            # Aquí personalizamos lo que ves al pasar el mouse
            hover_data={
                "VRP_MW": ':.2f',
                "Sensor": True,
                "Distancia_km": ':.2f',
                "Fecha_Chile": "|%d %b, %H:%M"
            },
            title="Análisis Térmico Interactivo por Volcán",
            template="plotly_dark"
        )

        # Ajustes de diseño
        fig.update_layout(
            height=1200,
            font=dict(family="Segoe UI, Tahoma, sans-serif", size=12),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        fig.update_traces(marker=dict(size=8, opacity=0.8, line=dict(width=1, color='White')))

        # Guardar el HTML
        os.makedirs(os.path.dirname(OUTPUT_INTERACTIVO), exist_ok=True)
        fig.write_html(OUTPUT_INTERACTIVO, full_html=False, include_plotlyjs='cdn')
        
        actualizar_estado_sistema(True)
        print("✅ Dashboard interactivo generado.")

    except Exception as e:
        print(f"❌ Error: {e}")
        actualizar_estado_sistema(False)

if __name__ == "__main__":
    procesar()
