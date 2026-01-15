import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import pytz
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_SALIDA = "monitoreo_satelital/v_html"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]

# Simbología original rescatada
MAPA_SIMBOLOS = {"MODIS": "triangle-up", "VIIRS375": "square", "VIIRS750": "circle", "VIIRS": "circle"}
COLORES_SENSORES = {"MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000", "VIIRS": "#C0C0C0"}

def procesar():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = ahora - timedelta(days=30)

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy() if not df.empty else pd.DataFrame()
        ruta_v = os.path.join(CARPETA_SALIDA, f"{v.replace(' ', '_')}.html")
        
        if not df_v.empty:
            df_v['Fecha_Chile'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
            df_v = df_v[df_v['Fecha_Chile'] >= hace_30_dias]

        if not df_v.empty:
            # Creación del gráfico con simbología y colores rescatados
            fig = px.scatter(df_v, x="Fecha_Chile", y="VRP_MW", color="Sensor", symbol="Sensor",
                           symbol_map=MAPA_SIMBOLOS, color_discrete_map=COLORES_SENSORES,
                           template="plotly_dark", hover_data={"VRP_MW": ':.2f', "Fecha_Chile": "|%d %b"})
            
            # Etiqueta de MÁXIMO rescatada
            max_r = df_v.loc[df_v['VRP_MW'].idxmax()]
            fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], text=f"MÁX: {max_r['VRP_MW']:.1f} MW", 
                               showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black"))
            
            fig.update_xaxes(range=[hace_30_dias, ahora], tickformat="%d %b", tickangle=-45, title=None)
            fig.update_yaxes(title="VRP (MW)", range=[0, df_v['VRP_MW'].max() * 1.2])
            fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10), showlegend=True,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')
        else:
            # Mensaje de NOMINAL rescatado
            with open(ruta_v, "w") as f: 
                f.write("<body style='background:#161b22; display:flex; align-items:center; justify-content:center;'><div style='color:#8b949e; font-family:sans-serif; font-size:16px; font-weight:bold;'>SIN ANOMALÍA TÉRMICA (NOMINAL)</div></body>")

if __name__ == "__main__":
    procesar()
