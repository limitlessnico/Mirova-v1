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

VOLCANES = [
    "Isluga", "Lascar", "Lastarria", "Peteroa",
    "Nevados de Chillan", "Copahue", "Llaima",
    "Villarrica", "Puyehue-Cordon Caulle", "Chaiten"
]

MAPA_SIMBOLOS = {
    "MODIS": "triangle-up",
    "VIIRS375": "square",
    "VIIRS750": "circle",
    "VIIRS": "circle"
}

COLORES_SENSORES = {
    "MODIS": "#FFA500",
    "VIIRS375": "#FF4500",
    "VIIRS750": "#FF0000",
    "VIIRS": "#C0C0C0"
}

MIROVA_BANDS_W = [
    (0, 1e6, "Muy Bajo", "rgba(85, 85, 85, 0.2)"),
    (1e6, 1e7, "Bajo", "rgba(119, 119, 0, 0.15)"),
    (1e7, 1e8, "Moderado", "rgba(170, 102, 0, 0.15)"),
    (1e8, 1e9, "Alto", "rgba(170, 0, 0, 0.15)")
]

# ---------------------------------------------------------------------

def crear_grafico(df_v, volcan, modo_log=False):
    tz_chile = pytz.timezone("America/Santiago")
    ahora = datetime.now(tz_chile)
    hace_30_dias = (ahora - timedelta(days=30)).replace(
        hour=0, minute=0, second=0
    )

    if df_v.empty:
        return None

    df_v["Fecha_Chile"] = (
        pd.to_datetime(df_v["Fecha_Satelite_UTC"])
        .dt.tz_localize("UTC")
        .dt.tz_convert("America/Santiago")
    )

    df_v_30 = df_v[df_v["Fecha_Chile"] >= hace_30_dias].copy()
    if df_v_30.empty:
        return None

    fig = go.Figure()
    v_max_mw = df_v_30["VRP_MW"].max()
    v_min_mw = df_v_30["VRP_MW"].min()

    # --- TRANSFORMACIÓN LOG MANUAL ---
    def transform(val_mw):
        if modo_log:
            watts = max(val_mw * 1e6, 1e4)  # evita log(0)
            return np.log10(watts)
        return val_mw

    # --- BANDAS MIROVA ---
    for y0_w, y1_w, label, color in MIROVA_BANDS_W:
        if modo_log:
            y0 = np.log10(max(y0_w, 1))
            y1 = np.log10(y1_w)
        else:
            y0 = y0_w / 1e6
            y1 = y1_w / 1e6

        fig.add_hrect(
            y0=y0, y1=y1,
            fillcolor=color,
            line_width=0,
            layer="below"
        )

        if v_max_mw * 1e6 >= y0_w:
            fig.add_trace(
                go.Scatter(
                    x=[None], y=[None],
                    mode="markers",
                    name=label,
                    marker=dict(
                        symbol="square",
                        size=8,
                        color=color.replace("0.2", "0.8").replace("0.15", "0.8")
                    ),
                    showlegend=True
                )
            )

    # --- PUNTOS ---
    for sensor, grupo in df_v_30.groupby("Sensor"):
        fig.add_trace(
            go.Scatter(
                x=grupo["Fecha_Chile"],
                y=[transform(v) for v in grupo["VRP_MW"]],
                mode="markers",
                name=sensor,
                marker=dict(
                    symbol=MAPA_SIMBOLOS.get(sensor, "circle"),
                    color=COLORES_SENSORES.get(sensor, "#C0C0C0"),
                    size=9,
                    line=dict(width=1, color="white")
                ),
                customdata=grupo["VRP_MW"],
                hovertemplate=(
                    "<b>%{customdata:.2f} MW</b>"
                    "<br>%{x|%d %b, %H:%M}"
                    "<extra></extra>"
                )
            )
        )

    # --- ANOTACIÓN MÁXIMO ---
    idx_max = df_v_30["VRP_MW"].idxmax()
    row_max = df_v_30.loc[idx_max]

    fig.add_annotation(
        x=row_max["Fecha_Chile"],
        y=transform(row_max["VRP_MW"]),
        text=f"MÁX: {row_max['VRP_MW']:.2f} MW",
        showarrow=True,
        arrowhead=2,
        bgcolor="rgba(0,0,0,0.8)",
        font=dict(color="white", size=9),
        ay=-35,
        ax=0
    )

    # --- EJE X ---
    fig.update_xaxes(
        type="date",
        range=[hace_30_dias, ahora],
        dtick=5 * 24 * 60 * 60 * 1000,
        tickformat="%d %b",
        gridcolor="rgba(255,255,255,0.12)",
        tickfont=dict(size=9)
    )

    # --- EJE Y ---
    if modo_log:
        y_min = np.log10(max(v_min_mw * 1e6, 1e4)) - 0.3
        y_max = np.log10(v_max_mw * 1e6) + 0.4

        fig.update_yaxes(
            type="linear",
            range=[y_min, y_max],
            tickvals=[5, 6, 7, 8, 9],
            ticktext=["10⁵", "10⁶", "10⁷", "10⁸", "10⁹"],
            gridcolor="rgba(255,255,255,0.05)",
            tickfont=dict(size=9),
            autorange=False
        )
    else:
        fig.update_yaxes(
            type="linear",
            range=[0, max(1.1, v_max_mw * 1.5)],
            gridcolor="rgba(255,255,255,0.05)",
            tickfont=dict(size=9)
        )

    # --- UNIDAD ---
    fig.add_annotation(
        xref="paper", yref="paper",
        x=-0.01, y=1.12,
        text="<b>Watt</b>" if modo_log else "<b>MW</b>",
        showarrow=False,
        font=dict(size=10, color="white"),
        xanchor="right"
    )

    # --- LAYOUT ---
    fig.update_layout(
        template="plotly_dark",
        autosize=True,
        height=None,
        margin=dict(l=35, r=5, t=35, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.03,
            xanchor="center",
            x=0.5,
            font=dict(size=9)
        ),
        uirevision="constant"
    )

    return fig

# ---------------------------------------------------------------------

def procesar():
    os.makedirs(CARPETA_LINEAL, exist_ok=True)
    os.makedirs(CARPETA_LOG, exist_ok=True)

    df = pd.read_csv(ARCHIVO_POSITIVOS) if os.path.exists(ARCHIVO_POSITIVOS) else pd.DataFrame()

    config_v = {
        "displayModeBar": "hover",
        "displaylogo": False,
        "responsive": True,
        "modeBarButtonsToRemove": [
            "zoom2d", "pan2d", "select2d", "lasso2d",
            "zoomIn2d", "zoomOut2d",
            "autoScale2d", "resetScale2d"
        ]
    }

    for volcan in VOLCANES:
        df_v = df[df["Volcan"] == volcan].copy()
        nombre = f"{volcan.replace(' ', '_')}.html"

        for carpeta, es_log in [(CARPETA_LINEAL, False), (CARPETA_LOG, True)]:
            fig = crear_grafico(df_v, volcan, modo_log=es_log)
            path = os.path.join(carpeta, nombre)

            if fig:
                fig.write_html(
                    path,
                    full_html=False,
                    include_plotlyjs="cdn",
                    config=config_v
                )
            else:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(
                        "<body style='background:#0d1117; color:#8b949e;"
                        "display:flex; align-items:center; justify-content:center;"
                        "height:300px; font-family:sans-serif;'>"
                        "SIN ANOMALÍA TÉRMICA</body>"
                    )

# ---------------------------------------------------------------------

if __name__ == "__main__":
    procesar()
