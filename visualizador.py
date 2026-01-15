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
MESES_ES = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul", 8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}

def procesar():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)

    ticks_principales = [hace_30_dias + timedelta(days=x) for x in range(0, 31, 5)]
    labels_principales = [f"{d.day} {MESES_ES[d.month]}" for d in ticks_principales]

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy() if not df.empty else pd.DataFrame()
        ruta_v = os.path.join(CARPETA_SALIDA, f"{v.replace(' ', '_')}.html")
        fig = go.Figure()

        if not df_v.empty:
            df_v['Fecha_Chile'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
            df_v = df_v[df_v['Fecha_Chile'] >= hace_30_dias]
            v_max_actual = df_v['VRP_MW'].max() if not df_v.empty else 0

            # --- DEFINICIÓN DE NIVELES MIROVA ---
            niveles_config = [
                (0, 1, "Nivel: Muy Bajo", "rgba(100, 100, 100, 0.2)"),
                (1, 10, "Nivel: Bajo", "rgba(150, 150, 0, 0.15)"),
                (10, 100, "Nivel: Moderado", "rgba(255, 165, 0, 0.15)"),
                (100, 1000, "Nivel: Alto", "rgba(255, 0, 0, 0.15)"),
                (1000, 10000, "Nivel: Muy Alto", "rgba(139, 0, 0, 0.2)")
            ]

            for z_min, z_max, label, color in niveles_config:
                # SOLUCIÓN AL ERROR: Convertimos explícitamente a bool de Python
                mostrar_en_leyenda = bool(v_max_actual >= z_min)
                
                fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers',
                    marker=dict(size=10, symbol='square', color=color.replace('0.15', '0.8').replace('0.2', '0.8')),
                    name=label, showlegend=mostrar_en_leyenda))
                
                fig.add_hrect(y0=z_min, y1=z_max, fillcolor=color, line_width=0, layer="below")

            # Datos de sensores
            for sensor, grupo in df_v.groupby('Sensor'):
                fig.add_trace(go.Scatter(x=grupo['Fecha_Chile'], y=grupo['VRP_MW'], mode='markers',
                    name=f"Sensor: {sensor}",
                    marker=dict(symbol=MAPA_SIMBOLOS.get(sensor, "circle"), color=COLORES_SENSORES.get(sensor, "#C0C0C0"), size=10, line=dict(width=1, color='white')),
                    hovertemplate="<b>%{x|%d %b, %H:%M}</b><br>Potencia: %{y:.2f} MW<extra></extra>"))

            # Anotación Máximo
            if not df_v.empty:
                max_r = df_v.loc[df_v['VRP_MW'].idxmax()]
                fig.add_annotation(x=max_r['Fecha_Chile'], y=max_r['VRP_MW'], text=f"MÁX: {max_r['VRP_MW']:.2f} MW", 
                                   showarrow=True, arrowhead=1, bgcolor="white", font=dict(color="black", size=10))

        # --- CONFIGURACIÓN DE EJES ---
        fig.update_xaxes(
            type="date", range=[hace_30_dias, ahora],
            tickvals=ticks_principales, ticktext=labels_principales,
            showgrid=True, gridcolor='rgba(255, 255, 255, 0.2)',
            minor=dict(tickmode="linear", dtick=86400000.0, showgrid=True, gridcolor='rgba(255, 255, 255, 0.05)'),
            tickangle=-45, fixedrange=True
        )
        
        y_max_val = df_v['VRP_MW'].max() if not df_v.empty else 1.2
        fig.update_yaxes(showgrid=True, gridcolor='rgba(255, 255, 255, 0.1)', title="Potencia Radiada (MW)",
                         range=[0, max(1.2, y_max_val * 1.3)], fixedrange=True)

        fig.update_layout(template="plotly_dark", height=400, margin=dict(l=50, r=20, t=10, b=60),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=9)))
        
        fig.write_html(ruta_v, full_html=False, include_plotlyjs='cdn')

if __name__ == "__main__":
    procesar()
