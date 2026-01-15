import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import pytz
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_SALIDA = "monitoreo_satelital/v_html"
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]

# Simbología y Colores Originales
MAPA_SIMBOLOS = {"MODIS": "triangle-up", "VIIRS375": "square", "VIIRS750": "circle"}
COLORES_SENSORES = {"MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000"}
MESES_ES = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul", 8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}

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
            fig = px.scatter(df_v, x="Fecha_Chile", y="VRP_MW", color="Sensor", symbol="Sensor",
                           symbol_map=MAPA_SIMBOLOS, color_discrete_map=COLORES_SENSORES,
                           template="plotly_dark")

            # --- BANDAS DE COLOR SEGÚN NIVEL MIROVA ---
            niveles = [
                (0.1, 1, "Muy Bajo", "rgba(100, 100, 100, 0.15)"),
                (1, 10, "Bajo", "rgba(150, 150, 0, 0.15)"),
                (10, 100, "Moderado", "rgba(255, 165, 0, 0.15)"),
                (100, 1000, "Alto", "rgba(255, 0, 0, 0.15)")
            ]
            for z_min, z_max, label, color in niveles:
                fig.add_hrect(y0=z_min, y1=z_max, fillcolor=color, annotation_text=label, 
                             annotation_position="inside left", line_width=0)

            # --- ETIQUETA MÁXIMO (2 Decimales) ---
            max_r = df_v.loc[df_v['VRP_MW'].idxmax()]
            fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], 
                               text=f"MÁX: {max_r['VRP_MW']:.2f} MW", 
                               showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black", size=11))

            # --- CONFIGURACIÓN DE EJES Y GRILLA ---
            fig.update_xaxes(showgrid=True, gridcolor='#333', tickangle=-45,
                            tickvals=df_v['Fecha_Chile'], 
                            ticktext=[f"{d.day} {MESES_ES[d.month]}" for d in df_v['Fecha_Chile']])
            
            fig.update_yaxes(showgrid=True, gridcolor='#333', title="Potencia Radiada (MW)", 
                            range=[0, df_v['VRP_MW'].max() * 1.4])
            
            fig.update_layout(height=400, margin=dict(l=50, r=20, t=10, b=50),
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            
            fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')
        else:
            with open(ruta_v, "w") as f: 
                f.write("<body style='background:#161b22; display:flex; align-items:center; justify-content:center; height:100vh; margin:0;'><div style='color:#8b949e; font-family:sans-serif; font-size:16px; border: 1px dashed #30363d; padding: 20px; border-radius:8px;'>SIN ANOMALÍA TÉRMICA (NOMINAL)</div></body>")

if __name__ == "__main__":
    procesar()
