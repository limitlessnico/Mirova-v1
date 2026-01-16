import pandas as pd
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
MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}

def procesar():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0)

    ticks_x = [hace_30_dias + timedelta(days=x) for x in range(0, 31, 7)]
    labels_x = [f"{d.day} {MESES_ES[d.month]}" for d in ticks_x]

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy() if not df.empty else pd.DataFrame()
        ruta_v = os.path.join(CARPETA_SALIDA, f"{v.replace(' ', '_')}.html")
        fig = go.Figure()

        if not df_v.empty:
            df_v['Fecha_Chile'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
            df_v_30 = df_v[df_v['Fecha_Chile'] >= hace_30_dias]
            v_max = df_v_30['VRP_MW'].max() if not df_v_30.empty else 0

            niveles = [(0, 1, "Muy Bajo", "rgba(100,100,100,0.15)"), (1, 10, "Bajo", "rgba(150,150,0,0.12)"), (10, 100, "Moderado", "rgba(255,165,0,0.12)")]
            for z_min, z_max, label, color in niveles:
                fig.add_hrect(y0=z_min, y1=z_max, fillcolor=color, line_width=0, layer="below")
                if v_max >= z_min:
                    fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', name=label, 
                                           marker=dict(size=7, symbol='square', color=color.replace('0.15', '0.7').replace('0.12', '0.7'))))

            for sensor, grupo in df_v_30.groupby('Sensor'):
                fig.add_trace(go.Scatter(x=grupo['Fecha_Chile'], y=grupo['VRP_MW'], mode='markers', name=sensor,
                    marker=dict(symbol=MAPA_SIMBOLOS.get(sensor, "circle"), color=COLORES_SENSORES.get(sensor, "#C0C0C0"), size=9, line=dict(width=1, color='white')),
                    customdata=grupo['Distancia_km'] if 'Distancia_km' in grupo.columns else [0]*len(grupo),
                    hoverlabel=dict(bgcolor="rgba(20, 24, 33, 0.95)", font=dict(color="white", size=11), bordercolor="#58a6ff"),
                    hovertemplate="<b>%{y:.2f} MW</b><br>%{x|%d %b, %H:%M} | dist: %{customdata}km<extra></extra>"))

            if not df_v_30.empty:
                max_r = df_v_30.loc[df_v_30['VRP_MW'].idxmax()]
                fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], text=f"MÁX: {max_r['VRP_MW']:.2f}", showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black", size=9))

        if df_v.empty or (not df_v.empty and df_v[df_v['Fecha_Chile'] >= hace_30_dias].empty):
            with open(ruta_v, "w", encoding='utf-8') as f:
                f.write(f"<body style='background:#0d1117; display:flex; align-items:center; justify-content:center; height:300px; margin:0; overflow:hidden;'><div style='color:#8b949e; font-family:sans-serif; border: 1px dashed #30363d; padding: 20px; border-radius:8px; text-align:center;'>SIN ANOMALÍA TÉRMICA</div></body>")
            continue

        fig.update_xaxes(type="date", range=[hace_30_dias, ahora], tickvals=ticks_x, ticktext=labels_x, 
                         showgrid=True, gridcolor='rgba(255,255,255,0.08)', minor=dict(dtick=86400000.0, showgrid=True, gridcolor='rgba(255,255,255,0.03)'), 
                         tickangle=-45, fixedrange=True, tickfont=dict(size=9))
        
        fig.update_yaxes(range=[0, max(1.1, v_max * 1.3)], fixedrange=True, gridcolor='rgba(255,255,255,0.05)', tickfont=dict(size=9))
        
        # MW ALINEADO: x=0 en 'paper' lo pega al inicio de la cuadrícula
        fig.add_annotation(xref="paper", yref="paper", x=0, y=1.05, text="<b>MW</b>", showarrow=False, 
                           font=dict(size=10, color="rgba(255,255,255,0.8)"), xanchor="right", yanchor="middle")
        
        fig.update_layout(
            template="plotly_dark", height=300, 
            margin=dict(l=35, r=5, t=15, b=35),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="center", x=0.5, font=dict(size=9), entrywidth=0.2, entrywidthmode="fraction"),
            hovermode="closest"
        )
        fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})

if __name__ == "__main__":
    procesar()
