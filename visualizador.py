import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
import pytz
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ARCHIVO_MAESTRO = "monitoreo_satelital/registro_vrp_maestro_publicable.csv"
ARCHIVO_MAESTRO_COMPLETO = "monitoreo_satelital/registro_vrp_maestro.csv"
ARCHIVO_POSITIVOS = "monitoreo_satelital/registro_vrp_positivos.csv"
CARPETA_LINEAL = "monitoreo_satelital/v_html"
CARPETA_LOG = "monitoreo_satelital/v_html_log"
VOLCANES = ["Isluga", "Lascar", "Lastarria", "Peteroa", "Nevados de Chillan", "Copahue", "Llaima", "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"]

MAPA_SIMBOLOS = {"MODIS": "triangle-up", "VIIRS375": "square", "VIIRS750": "circle", "VIIRS": "circle"}
# Colores por confianza (verde para latest.php y OCR alta, amarillo/naranja para OCR media/baja)
COLORES_CONFIANZA = {
    "N/A": "#2ea043",      # Verde - latest.php (legacy)
    "valido": "#2ea043",   # Verde - latest.php (nuevo)
    "alta": "#2ea043",     # Verde - OCR alta
    "media": "#d29922",    # Amarillo - OCR media
    "baja": "#fb8500"      # Naranja - OCR baja
}
MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}

MIROVA_BANDS = [
    (0, 1e6, "Muy Bajo", "rgba(85, 85, 85, 0.2)"),
    (1e6, 1e7, "Bajo", "rgba(119, 119, 0, 0.15)"),
    (1e7, 1e8, "Moderado", "rgba(170, 102, 0, 0.15)"),
    (1e8, 1e9, "Alto", "rgba(170, 0, 0, 0.15)")
]

def crear_grafico(df_v, v, modo_log=False):
    tz_chile = pytz.timezone('America/Santiago')
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(hour=0, minute=0, second=0)
    
    df_v_30 = pd.DataFrame()
    if not df_v.empty:
        # CAMBIO V4.1: Mantener UTC para tooltips (no convertir a Chile)
        df_v['Fecha_UTC'] = pd.to_datetime(df_v['Fecha_Satelite_UTC']).dt.tz_localize('UTC')
        
        # Para filtro temporal seguir usando hora Chile
        df_v['Fecha_Chile_temp'] = df_v['Fecha_UTC'].dt.tz_convert('America/Santiago')
        df_v_30 = df_v[df_v['Fecha_Chile_temp'] >= hace_30_dias].copy()
        
        # FILTRAR VRP = 0 (eventos de rutina sin anomalía térmica)
        df_v_30 = df_v_30[df_v_30['VRP_MW'] > 0].copy()

    if df_v_30.empty: return None

    unidad = "Watt" if modo_log else "MW"
    fig = go.Figure()
    v_max_val = df_v_30['VRP_MW'].max()
    
    # Función transform() - CRÍTICA para modo log (de versión antigua que funciona)
    def transform(val_mw):
        if modo_log:
            watts = val_mw * 1e6
            return np.log10(max(watts, 10000))  # Transforma a log10
        return val_mw
    
    # Para bandas y verificaciones
    mult = 1000000 if modo_log else 1
    v_max_val_check = v_max_val * mult
    
    # Bandas y Simbología Inteligente
    for y0, y1, label, color in MIROVA_BANDS:
        # Transformar a log10 para bandas en modo log
        if modo_log:
            l_y0 = np.log10(max(y0, 1))
            l_y1 = np.log10(y1)
        else:
            l_y0 = y0/1e6
            l_y1 = y1/1e6
        fig.add_hrect(y0=l_y0, y1=l_y1, fillcolor=color, line_width=0, layer="below")
        if (v_max_val_check >= y0):
            fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', name=label, 
                marker=dict(size=8, symbol='square', color=color.replace('0.2', '0.8').replace('0.15', '0.8')), showlegend=True))

    # Traces separadas por confianza (colores) y sensor (símbolos)
    # Agrupar por sensor y confianza
    for sensor in df_v_30['Sensor'].unique():
        df_sensor = df_v_30[df_v_30['Sensor'] == sensor]
        
        for confianza in ['N/A', 'valido', 'alta', 'media', 'baja']:
            # Filtrar por confianza (N/A o valido para latest.php)
            if 'Confianza_Validacion' in df_sensor.columns:
                df_grupo = df_sensor[df_sensor['Confianza_Validacion'] == confianza]
            else:
                # Si no existe la columna, asumir N/A (latest.php legacy)
                df_grupo = df_sensor if confianza in ['N/A', 'valido'] else pd.DataFrame()
            
            if df_grupo.empty:
                continue
            
            # Obtener color y símbolo
            color = COLORES_CONFIANZA.get(confianza, "#2ea043")
            simbolo = MAPA_SIMBOLOS.get(sensor, "circle")
            
            # Nombre de la trace
            if confianza in ['N/A', 'valido']:
                nombre_trace = sensor
            else:
                nombre_trace = f"{sensor} ({confianza})"
            
            # CRÍTICO: Usar transform() para valores Y
            y_vals = [transform(v) for v in df_grupo['VRP_MW']]
            
            fig.add_trace(go.Scatter(
                x=df_grupo['Fecha_UTC'],  # ← CAMBIO: Usar UTC
                y=y_vals,
                mode='markers', 
                name=nombre_trace,
                marker=dict(
                    symbol=simbolo,
                    color=color,
                    size=9, 
                    line=dict(width=1, color='white')
                ),
                customdata=np.column_stack((df_grupo['Fecha_UTC'], df_grupo['VRP_MW'])),  # ← CAMBIO: incluir fecha UTC
                hoverlabel=dict(bgcolor="rgba(20, 24, 33, 0.95)", font=dict(color="white", size=11)),
                hovertemplate="<b>%{customdata[1]:.2f} MW</b><br>%{customdata[0]|%d %b, %H:%M} UTC<extra></extra>",  # ← CAMBIO: agregar "UTC"
                showlegend=True
            ))

    # Anotación Máximo - Se agrega después de configurar ejes
    # (Movido más abajo para modo log)

    # Eje X con grilla cada 5 días
    fig.update_xaxes(type="date", range=[hace_30_dias, ahora],
                     dtick=5 * 24 * 60 * 60 * 1000, tickformat="%d %b",
                     showgrid=True, gridcolor='rgba(255,255,255,0.12)',
                     minor=dict(dtick=86400000.0, showgrid=True, gridcolor='rgba(255,255,255,0.03)'),
                     tickangle=-45, fixedrange=True, tickfont=dict(size=9))
    
    # Eje Y - CRÍTICO: En modo log usar type="linear" con valores transformados
    if modo_log:
        # Usar estrategia de versión antigua: type="linear" con ticktext personalizado
        fig.update_yaxes(
            type="linear",  # ← No "log", sino "linear" con valores log10
            range=[4.7, 9],  # log10(10^4.7) a log10(10^9)
            tickvals=[5, 6, 7, 8],
            ticktext=["10⁵", "10⁶", "10⁷", "10⁸"],
            gridcolor='rgba(255,255,255,0.05)',
            tickfont=dict(size=9),
            autorange=False,
            fixedrange=True
        )
    else:
        fig.update_yaxes(
            type="linear",
            range=[0, max(1.1, v_max_val * 1.5)],
            gridcolor='rgba(255,255,255,0.05)',
            tickfont=dict(size=9),
            fixedrange=True
        )
    
    # Unidad Watt/MW
    fig.add_annotation(xref="paper", yref="paper", x=-0.01, y=1.15, text=f"<b>{unidad}</b>", 
                       showarrow=False, font=dict(size=10, color="white"), xanchor="right")
    
    # Anotación Máximo - CRÍTICO: Usar transform() en modo log
    if not df_v_30.empty:
        max_r = df_v_30.loc[df_v_30['VRP_MW'].idxmax()]
        y_pos = transform(max_r['VRP_MW'])  # ← Usar transform()
        
        fig.add_annotation(x=max_r['Fecha_UTC'], y=y_pos,  # ← CAMBIO: Usar Fecha_UTC
            xref="x", yref="y", text=f"MÁX: {max_r['VRP_MW']:.2f} MW", showarrow=True,
            arrowhead=2, arrowsize=1, arrowwidth=1.5, arrowcolor="white",
            bgcolor="rgba(0,0,0,0.8)", bordercolor="#58a6ff", borderwidth=1,
            font=dict(color="white", size=9), ay=-40, ax=0)
    
    # Layout - CRÍTICO: Sin responsive para gráficos log
    fig.update_layout(
        template="plotly_dark",
        height=300,
        margin=dict(l=40, r=2, t=35, b=40),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="center", x=0.5, font=dict(size=9)),
        # NUEVO: NO responsive para evitar problemas con resize
        autosize=True,
        # NUEVO: Anchura fija para evitar recalculos
        width=None  # Se adapta al contenedor pero sin recalcular ejes
    )
    
    return fig

def procesar():
    os.makedirs(CARPETA_LINEAL, exist_ok=True)
    os.makedirs(CARPETA_LOG, exist_ok=True)
    
    # Leer CSV maestro PUBLICABLE (ya viene filtrado)
    if os.path.exists(ARCHIVO_MAESTRO):
        df = pd.read_csv(ARCHIVO_MAESTRO)
        print(f"📊 Leyendo {ARCHIVO_MAESTRO}: {len(df)} eventos")
    elif os.path.exists(ARCHIVO_MAESTRO_COMPLETO):
        # Fallback: Leer maestro completo y filtrar manualmente
        df = pd.read_csv(ARCHIVO_MAESTRO_COMPLETO)
        print(f"⚠️ Maestro publicable no existe, usando completo: {len(df)} eventos")
        
        # Aplicar filtros manualmente
        if not df.empty:
            antes = len(df)
            
            # Filtrar por tipo
            if 'Tipo_Registro' in df.columns:
                tipos_ok = ['ALERTA_TERMICA', 'ALERTA_TERMICA_OCR', 'EVIDENCIA_DIARIA']
                df = df[df['Tipo_Registro'].isin(tipos_ok)].copy()
            
            # Filtrar VRP > 0
            df = df[df['VRP_MW'] > 0].copy()
            
            # Filtrar confianza baja
            if 'Confianza_Validacion' in df.columns:
                df = df[df['Confianza_Validacion'] != 'baja'].copy()
            
            print(f"   Filtrado: {antes} → {len(df)} eventos")
    else:
        # Fallback final: positivos.csv
        df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()
        if not df.empty:
            df['Confianza_Validacion'] = 'valido'
    
    # Config diferente para lineal vs log
    config_lineal = {
        'displayModeBar': 'hover',
        'displaylogo': False,
        'responsive': True,  # OK para lineales
        'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
        'toImageButtonOptions': {'format': 'png', 'height': 500, 'width': 1400, 'scale': 2}
    }
    
    config_log = {
        'displayModeBar': 'hover',
        'displaylogo': False,
        'responsive': False,  # CRÍTICO: Deshabilitar para log
        'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'],
        'toImageButtonOptions': {'format': 'png', 'height': 500, 'width': 1400, 'scale': 2}
    }

    for v in VOLCANES:
        df_v = df[df['Volcan'] == v].copy()
        nombre_f = f"{v.replace(' ', '_')}.html"
        
        for carpeta, es_log in [(CARPETA_LINEAL, False), (CARPETA_LOG, True)]:
            fig = crear_grafico(df_v, v, modo_log=es_log)
            path = os.path.join(carpeta, nombre_f)
            
            if fig is None:
                with open(path, "w", encoding='utf-8') as f:
                    f.write("<body style='background:#0d1117; color:#8b949e; display:flex; align-items:center; justify-content:center; height:300px; font-family:sans-serif;'>SIN ANOMALÍA TÉRMICA</body>")
            else:
                # CRÍTICO: Usar config apropiada según tipo
                config_usar = config_log if es_log else config_lineal
                fig.write_html(path, full_html=False, include_plotlyjs='cdn', config=config_usar)

if __name__ == "__main__":
    procesar()
