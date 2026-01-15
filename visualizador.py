import pandas as pd
import plotly.express as px
import os
import json
import pytz
from datetime import datetime

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_SALIDA = "monitoreo_satelital/v_html"
ARCHIVO_STATUS = "monitoreo_satelital/estado_sistema.json"
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]

COLORES_SENSORES = {"MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000", "VIIRS": "#C0C0C0"}

def actualizar_estado(exito=True):
    tz = pytz.timezone('America/Santiago')
    estado = {
        "ultima_actualizacion": datetime.now(tz).strftime("%d-%m-%Y %H:%M"),
        "estado": "🟢 MONITOR MIROVA-OVDAS OPERATIVO",
        "color": "#2ecc71"
    }
    with open(ARCHIVO_STATUS, "w") as f: json.dump(estado, f)

def procesar():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    
    if not df.empty:
        df['Fecha_Chile'] = pd.to_datetime(df['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v] if not df.empty else pd.DataFrame()
        ruta_v = os.path.join(CARPETA_SALIDA, f"{v.replace(' ', '_')}.html")
        
        if not df_v.empty:
            fig = px.scatter(df_v, x="Fecha_Chile", y="VRP_MW", color="Sensor",
                           color_discrete_map=COLORES_SENSORES, template="plotly_dark",
                           hover_data={"VRP_MW": ':.2f', "Fecha_Chile": "|%d %b"})
            
            # Etiqueta de MÁXIMO
            max_r = df_v.loc[df_v['VRP_MW'].idxmax()]
            fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], text=f"MÁX: {max_r['VRP_MW']:.1f}", showarrow=True)
            
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10), showlegend=False,
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig.update_xaxes(tickformat="%d %b", tickangle=-45)
            fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')
        else:
            # Si no hay datos, creamos un HTML vacío o con mensaje
            with open(ruta_v, "w") as f: f.write("<div style='color:#8b949e; text-align:center; padding-top:100px;'>SIN ANOMALÍA TÉRMICA</div>")

    actualizar_estado()

if __name__ == "__main__": procesar()
