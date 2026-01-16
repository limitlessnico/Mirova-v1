import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
import pytz
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_LINEAL = "monitoreo_satelital/v_html"
CARPETA_LOG = "monitoreo_satelital/v_html_log"
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]

MAPA_SIMBOLOS = {"MODIS": "triangle-up", "VIIRS375": "square", "VIIRS750": "circle", "VIIRS": "circle"}
COLORES_SENSORES = {"MODIS": "#FFA500", "VIIRS375": "#FF4500", "VIIRS750": "#FF0000", "VIIRS": "#C0C0C0"}
MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}

def crear_grafico(df_v, v, modo_log=False):
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0)
    
    df_v_30 = pd.DataFrame()
    if not df_v.empty:
        df_v['Fecha_Chile'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
        df_v_30 = df_v[df_v['Fecha_Chile'] >= hace_30_dias].copy()

    if df_v_30.empty:
        return None

    # CORRECCIÓN PARA LOG: Asegurar que no haya valores <= 0 que rompan el gráfico
    if modo_log:
        df_v_30.loc[df_v_30['VRP_MW'] < 0.1, 'VRP_MW'] = 0.11 

    fig = go.Figure()
    ticks_x = [hace_30_dias + timedelta(days=x) for x in range(0, 31, 7)]
    labels_x = [f"{d.day} {MESES_ES[d.month]}" for d in ticks_x]
    v_max = df_v_30['VRP_MW'].max()

    # Niveles MIROVA Dinámicos
    niveles = [(0, 1, "Muy Bajo", "rgba(100,100,100,0.15)"), 
               (1, 10, "Bajo", "rgba(150,150,0,0.12)"), 
               (10, 100, "Moderado", "rgba(255,165,0,0.12)")]
    
    for z_min, z_max, label, color in niveles:
        fig.add_hrect(y0=z_min, y1=z_max, fillcolor=color, line_width=0, layer="below")
        hay_datos_en_rango = not df_v_30[(df_v_30['VRP_MW'] > z_min) & (df_v_30['VRP_MW'] <= z_max)].empty
        if hay_datos_en_rango:
            fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', name=label, 
                                     marker=dict(size=8, symbol='square', color=color.replace('0.15', '0.7').replace('0.12', '0.7')),
                                     showlegend=True))

    for sensor, grupo in df_v_30.groupby('Sensor'):
        fig.add_trace(go.Scatter(x=grupo['Fecha_Chile'], y=grupo['VRP_MW'], mode='markers', name=sensor,
            marker=dict(symbol=MAPA_SIMBOLOS.get(sensor, "circle"), color=COLORES_SENSORES.get(sensor, "#C0C0C0"), size=9, line=dict(width=1, color='white')),
            customdata=grupo['Distancia_km'] if 'Distancia_km' in grupo.columns else [0]*len(grupo),
            hoverlabel=dict(bgcolor="rgba(20, 24, 33, 0.95)", font=dict(color="white", size=11), bordercolor="#58a6ff"),
            hovertemplate="<b>%{y:.2f} MW</b><br>%{x|%d %b, %H:%M} | dist: %{customdata}km<extra></extra>",
            showlegend=True))

    # ANOTACIÓN MÁXIMO (Mejorada para Log)
    if not df_v_30.empty:
        max_r = df_v_30.loc[df_v_30['VRP_MW'].idxmax()]
        fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], text=f"MÁX: {max_r['VRP_MW']:.2f}",
                           showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black", size=9),
                           ay=-30, # Desplazamiento fijo
                           cliponaxis=False) # Evita que desaparezca en los bordes

    fig.update_xaxes(type="date", range=[hace_30_dias, ahora], tickvals=ticks_x, ticktext=labels_x, 
                     showgrid=True, gridcolor='rgba(255,255,255,0.08)', minor=dict(dtick=86400000.0, showgrid=True, gridcolor='rgba(255,255,255,0.03)'), 
                     tickangle=-45, fixedrange=True, tickfont=dict(size=9))
    
    if modo_log:
        # Rango logarítmico estable: de 0.1 a un poco más del máximo
        y_max_range = np.log10(max(10, v_max * 2))
        fig.update_yaxes(type="log", range=[np.log10(0.1), y_max_range], 
                         fixedrange=True, gridcolor='rgba(255,255,255,0.05)', tickfont=dict(size=9),
                         dtick="D1") # Muestra ticks en potencias de 10
    else:
        fig.update_yaxes(range=[0, max(1.1, v_max * 1.3)], fixedrange=True, gridcolor='rgba(255,255,255,0.05)', tickfont=dict(size=9))
    
    fig.add_annotation(xref="paper", yref="paper", x=-0.01, y=1.05, text="<b>MW</b>", showarrow=False, 
                       font=dict(size=10, color="rgba(255,255,255,0.8)"), xanchor="right", yanchor="middle")
    
    fig.update_layout(
        template="plotly_dark", height=300, margin=dict(l=40, r=5, t=15, b=35),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="center", x=0.5, font=dict(size=9), entrywidth=0.2, entrywidthmode="fraction"),
        hovermode="closest"
    )
    return fig

def procesar():
    os.makedirs(CARPETA_LINEAL, exist_ok=True)
    os.makedirs(CARPETA_LOG, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()

    config_visual = {
        'displayModeBar': 'hover', 'displaylogo': False,
        'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d'],
        'toImageButtonOptions': {'format': 'png', 'filename': 'monitor_vrp_export', 'height': 500, 'width': 1400, 'scale': 2}
    }

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy() if not df.empty else pd.DataFrame()
        nombre_file = f"{v.replace(' ', '_')}.html"
        
        for carpeta, es_log in [(CARPETA_LINEAL, False), (CARPETA_LOG, True)]:
            fig = crear_grafico(df_v, v, modo_log=es_log)
            ruta_final = os.path.join(carpeta, nombre_file)
            
            if fig is None:
                with open(ruta_final, "w", encoding='utf-8') as f:
                    f.write(f"<body style='background:#0d1117; display:flex; align-items:center; justify-content:center; height:300px; margin:0; overflow:hidden;'><div style='color:#8b949e; font-family:sans-serif; border: 1px dashed #30363d; padding: 20px; border-radius:8px; text-align:center;'>SIN ANOMALÍA TÉRMICA</div></body>")
            else:
                fig.write_html(ruta_final, full_html=False, include_plotlyjs='cdn', config=config_visual)

if __name__ == "__main__":
    procesar()
