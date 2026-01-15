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
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados_de_Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon_Caulle", "Chaiten"]

def actualizar_estado():
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
        # Reemplazamos guiones por espacios solo para filtrar el CSV
        nombre_real = v.replace('_', ' ')
        df_v = df[df['Volcan'] == nombre_real] if not df.empty else pd.DataFrame()
        ruta_v = os.path.join(CARPETA_SALIDA, f"{v}.html")
        
        if not df_v.empty:
            fig = px.scatter(df_v, x="Fecha_Chile", y="VRP_MW", 
                           color_discrete_sequence=["#FFA500"], template="plotly_dark")
            
            fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10), showlegend=False,
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')
        else:
            with open(ruta_v, "w") as f: 
                f.write("<body style='background:#161b22; display:flex; align-items:center; justify-content:center;'><div style='color:#8b949e; font-family:sans-serif; font-size:14px;'>SIN ANOMALÍA TÉRMICA</div></body>")

    actualizar_estado()

if __name__ == "__main__": procesar()
