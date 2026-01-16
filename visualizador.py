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

BANDAS_LOG = [
    (0, 6, "Muy Bajo", "rgba(85, 85, 85, 0.2)"),
    (6, 7, "Bajo", "rgba(119, 119, 0, 0.15)"),
    (7, 8, "Moderado", "rgba(170, 102, 0, 0.15)"),
    (8, 9, "Alto", "rgba(170, 0, 0, 0.15)")
]

def crear_grafico(df_v, v, modo_log=False):
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0)
    
    df_v_30 = pd.DataFrame()
    if not df_v.empty:
        df_v['Fecha_Chile'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
        df_v_30 = df_v[df_v['Fecha_Chile'] >= hace_30_dias].copy()

    if df_v_30.empty: return None

    unidad = "Watt" if modo_log else "MW"
    fig = go.Figure()
    v_max_mw = df_v_30['VRP_MW'].max()

    def to_log_val(val_mw):
        watts = val_mw * 1e6
        return np.log10(max(watts, 1))

    if modo_log:
        # Dibujar Bandas
        for y0, y1, label, color in BANDAS_LOG:
            fig.add_hrect(y0=y0, y1=y1, fillcolor=color, line_width=0, layer="below")
            if (to_log_val(v_max_mw) >= y0):
                fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', name=label, 
                    marker=dict(size=8, symbol='square', color=color.replace('0.2', '0.8').replace('0.15', '0.8')), showlegend=True))
        
        # Puntos
        for sensor, grupo in df_v_30.groupby('Sensor'):
            fig.add_trace(go.Scatter(x=grupo['Fecha_Chile'], y=[to_log_val(x) for x in grupo['VRP_MW']], 
                mode='markers', name=sensor,
                marker=dict(symbol=MAPA_SIMBOLOS.get(sensor, "circle"), color=COLORES_SENSORES.get(sensor, "#C0C0C0"), size=9, line=dict(width=1, color='white')),
                customdata=grupo['VRP_MW'],
                hovertemplate="<b>%{customdata:.2f} MW</b><br>%{x|%d %b, %H:%M}<extra></extra>"))
        
        # Rango DINÁMICO LOG (Mejora sugerida)
        y_val_max = to_log_val(v_max_mw)
        y_range_min = 5.0 # Mínimo fijo en 10^5 para contexto
        y_range_max = max(8.2, y_val_max + 0.5) # Se estira si el dato es alto
        
        fig.update_yaxes(type="linear", range=[y_range_min, y_range_max], autorange=False,
                         tickvals=[5, 6, 7, 8, 9], ticktext=["10⁵", "10⁶", "10⁷", "10⁸", "10⁹"],
                         gridcolor='rgba(255,255,255,0.05)', tickfont=dict(size=9))
    else:
        # Gráfico Lineal normal
        for sensor, grupo in df_v_30.groupby('Sensor'):
            fig.add_trace(go.Scatter(x=grupo['Fecha_Chile'], y=grupo['VRP_MW'], mode='markers', name=sensor,
                marker=dict(symbol=MAPA_SIMBOLOS.get(sensor, "circle"), color=COLORES_SENSORES.get(sensor, "#C0C0C0"), size=9, line=dict(width=1, color='white')),
                hovertemplate="<b>%{y:.2f} MW</b><br>%{x|%d %b, %H:%M}<extra></extra>"))
        
        fig.update_yaxes(range=[0, max(1.1, v_max_mw * 1.5)], gridcolor='rgba(255,255,255,0.05)', tickfont=dict(size=9))

    # Anotación Máximo
    max_r = df_v_30.loc[df_v_30['VRP_MW'].idxmax()]
    fig.add_annotation(x=max_r['Fecha_Chile'], y=to_log_val(max_r['VRP_MW']) if modo_log else max_r['VRP_MW'],
        text=f"MÁX: {max_r['VRP_MW']:.2f} MW", showarrow=True, arrowhead=2,
        bgcolor="rgba(0,0,0,0.8)", font=dict(color="white", size=9), ay=-40, ax=0)

    # Eje X y Layout
    fig.update_xaxes(type="date", range=[hace_30_dias, ahora], dtick=5*24*60*60*1000, 
                     tickformat="%d %b", gridcolor='rgba(255,255,255,0.12)', tickangle=-45, tickfont=dict(size=9))

    fig.add_annotation(xref="paper", yref="paper", x=-0.01, y=1.12, text=f"<b>{unidad}</b>", 
                       showarrow=False, font=dict(size=10, color="white"), xanchor="right")

    fig.update_layout(template="plotly_dark", height=300, margin=dict(l=40, r=5, t=35, b=40),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', showlegend=True,
                      legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="center", x=0.5, font=dict(size=9)),
                      uirevision='constant')
    return fig

def procesar():
    os.makedirs(CARPETA_LINEAL, exist_ok=True)
    os.makedirs(CARPETA_LOG, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    config_v = {'displayModeBar': 'hover', 'displaylogo': False, 'responsive': True,
                'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']}

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy()
        nombre_f = f"{v.replace(' ', '_')}.html"
        for carpeta, es_log in [(CARPETA_LINEAL, False), (CARPETA_LOG, True)]:
            fig = crear_grafico(df_v, v, modo_log=es_log)
            if fig:
                fig.write_html(os.path.join(carpeta, nombre_f), full_html=False, include_plotlyjs='cdn', config=config_v)

if __name__ == "__main__":
    procesar()
