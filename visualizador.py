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

MAPA_SIMBOLOS = {"MODIS": "triangle-up", "VIIRS375": "square", "VIIRS750": "circle", "VIIRS": "circle"}
COLORES_SENSORES = {"MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000", "VIIRS": "#C0C0C0"}

# Diccionario para forzar español
MESES_ES = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul", 8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}

def procesar():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    # Anclamos el inicio a hace exactamente 30 días a las 00:00
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Definimos etiquetas cada 5 días para que no se amontonen los textos
    ticks_principales = [hace_30_dias + timedelta(days=x) for x in range(0, 31, 5)]
    labels_principales = [f"{d.day} {MESES_ES[d.month]}" for d in ticks_principales]

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

            # --- BANDAS MIROVA ---
            fig.add_hrect(y0=0, y1=1, fillcolor="rgba(100,100,100,0.1)", annotation_text="Muy Bajo", line_width=0)
            fig.add_hrect(y0=1, y1=10, fillcolor="rgba(150,150,0,0.1)", annotation_text="Bajo", line_width=0)
            fig.add_hrect(y0=10, y1=100, fillcolor="rgba(255,165,0,0.1)", annotation_text="Moderado", line_width=0)

            # --- ETIQUETA MÁXIMO ---
            max_r = df_v.loc[df_v['VRP_MW'].idxmax()]
            fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], 
                               text=f"MÁX: {max_r['VRP_MW']:.2f} MW", 
                               showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black"))

            # --- CONFIGURACIÓN DE GRILLA DÍA A DÍA ---
            fig.update_xaxes(
                range=[hace_30_dias, ahora],
                tickvals=ticks_principales,
                ticktext=labels_principales,
                showgrid=True,
                gridcolor='rgba(255, 255, 255, 0.2)', # Grilla principal (cada 5 días)
                minor=dict(
                    tickmode="linear",
                    dtick=86400000.0, # 1 día en milisegundos
                    showgrid=True,
                    gridcolor='rgba(255, 255, 255, 0.05)' # Grilla secundaria transparente (cada 1 día)
                ),
                tickangle=-45
            )
            
            fig.update_yaxes(showgrid=True, gridcolor='rgba(255, 255, 255, 0.1)', title="Potencia (MW)")
            
            fig.update_layout(
                height=400, margin=dict(l=50, r=20, t=10, b=60),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')
        else:
            # Mensaje de sin anomalía con diseño corregido
            with open(ruta_v, "w") as f: 
                f.write(f"<body style='background:#161b22; display:flex; align-items:center; justify-content:center; height:100vh; margin:0;'><div style='color:#8b949e; font-family:sans-serif; font-size:14px; border: 1px dashed #30363d; padding: 20px; border-radius:8px; text-align:center;'>SIN ANOMALÍA TÉRMICA<br><small>Últimos 30 días</small></div></body>")

if __name__ == "__main__":
    procesar()
