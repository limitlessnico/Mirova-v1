# 🌋 Mirova-OVDAS VRP Monitor (Chile)

**Mirova-OVDAS VRP Monitor** es una plataforma de **automatización y visualización científica** diseñada para el seguimiento de la Potencia Radiada Volcánica (VRP) en los principales centros eruptivos de Chile. El sistema actúa como un nodo de respaldo y análisis que captura, procesa y grafica la información pública de la plataforma **MIROVA** (Universidad de Turín).

⚠️ **Aclaración:** Este software es una herramienta independiente de soporte técnico. No reemplaza los canales oficiales de alerta temprana de instituciones estatales.
 ---

## 🙏 Acknowledgements
Toda la información térmica utilizada en este proyecto es procesada y obtenida a través de la infraestructura de la plataforma **MIROVA** (Middle InfraRed Observation of Volcanic Activity).

* **Desarrollo y Mantenimiento:** Departamento de Ciencias de la Tierra de la [Universidad de Turín](https://www.unito.it/) (Italia), en colaboración con la [Universidad de Florencia](https://www.unifi.it/).
* **Investigador Principal:** Diego Coppola.
* **Referencias Científicas:** * Coppola, D., et al. (2016). *Enhanced volcanic hot-spot detection using MODIS IR data: results from the MIROVA system*.
    * Coppola, D., et al. (2020). *Thermal Remote Sensing for Global Volcano Monitoring: Experiences From the MIROVA System*.
* Para más información, visite el sitio oficial de MIROVA.
We gratefully acknowledge NASA LANCE for access to MODIS and VIIRS Near Real Time products. Sentinel-2 and Landsat 8 data are accessed through the Copernicus Open Access Hub.
---

## 📡 Dashboard e Interfaz de Auditoría

El sistema cuenta con un **Dashboard Profesional** que permite visualizar el estado de salud del monitor y las tendencias térmicas en tiempo real.

> [!IMPORTANT]
> **[👉 ACCEDER AL MONITOR EN VIVO (Standard OVDAS)](https://mendozavolcanic.github.io/Mirova-v1/)**

### 🟢 Semáforo de Salud del Sistema

El Dashboard integra una **Barra de Auditoría Técnica** que verifica la sincronización con los satélites:

* **Monitor Operativo:** Confirma que el robot ha procesado los datos exitosamente en el último ciclo.
* **Sincronización UTC:** Indica la hora exacta de la última captura de datos desde MIROVA.

---

## 📈 Visualización de Tendencias (V35.1)

El módulo `visualizador.py` genera gráficos de alta precisión con las siguientes características técnicas:

* **Sombreado Dinámico Inteligente:** El fondo del gráfico se colorea automáticamente (Verde, Amarillo, Naranja) solo si la energía detectada alcanza los umbrales de alerta, evitando distorsiones visuales en niveles bajos.
* **Iconografía Multisensor:** Diferenciación visual de la fuente del dato para auditoría científica:
* `▲` **MODIS**: Sensor histórico de amplio espectro.
* `■` **VIIRS 375m**: Alta resolución para detección de anomalías pequeñas.
* `●` **VIIRS 750m**: Alta sensibilidad térmica.


* **Etiquetado Automático:** Marcado dinámico del valor **MAX** (en MW) detectado en el periodo mensual y anual.

---

## 🛰️ Estrategia de Captura y Respaldo (V34.1)

El robot ejecuta un ciclo de vigilancia cada 15-30 minutos aplicando una política de **Evidencia Multisensor**:

1. **Detección de Alerta:** Si se detecta **VRP > 0** dentro del radio de seguridad, el sistema identifica el sensor informante y descarga el set de evidencia disponible (hasta 4 gráficos: `logVRP`, `VRP`, `Latest` y `Dist`).
2. **Soporte Tri-Sensor:** El bot intenta capturar el registro gráfico de los tres sensores principales (**MODIS**, **VIIRS 375m** y **VIIRS 750m**) para el mismo evento, permitiendo una comparación técnica y auditoría visual completa.
3. **Respaldo en Calma:** En ausencia de alertas (VRP = 0), el sistema prioriza al sensor **VIIRS 375m** para descargar una captura de respaldo diaria, documentando la estabilidad del volcán con la mayor resolución espacial.
4. **Auditoría de Datos:** El sistema monitorea cambios en el estado de procesamiento de MIROVA (paso de datos NRT a Standard) y actualiza los registros históricos automáticamente.

---

## 🎯 Red de Vigilancia (Configuración OVDAS)

Se aplica un filtro de precisión geográfica (**Geofencing**) para validar que las anomalías térmicas provengan del cráter activo:

| Volcán | ID MIROVA | Límite (km) | Región |
| --- | --- | --- | --- |
| **Isluga** | 354030 | 5.0 | Tarapacá |
| **Láscar** | 355100 | 5.0 | Antofagasta |
| **Lastarria** | 355120 | 3.0 | Antofagasta |
| **Peteroa** | 357040 | 3.0 | Maule |
| **N. de Chillán** | 357060 | 5.0 | Ñuble |
| **Copahue** | 357090 | 4.0 | Biobío |
| **Llaima** | 357110 | 5.0 | Araucanía |
| **Villarrica** | 357120 | 5.0 | Araucanía |
| **Puyehue-C. Caulle** | 357150 | 20.0 | Los Ríos |
| **Chaitén** | 358041 | 5.0 | Los Lagos |

---

## 📂 Estructura de Datos

* `registro_vrp_positivos.csv`: Base de datos histórica utilizada por el visualizador.
* `imagenes_satelitales/`: Repositorio organizado por volcán y fecha con la evidencia visual de los sensores.
* `graficos_tendencia/`: Gráficos de actividad térmica procesados para el Dashboard.
* `bitacora_robot.txt`: Registro técnico de cada ciclo de ejecución.

---

## 🛠️ Tecnologías y Autoría

* **Motor:** Python 3.9 (Pandas, Matplotlib, BeautifulSoup4).
* **Infraestructura:** GitHub Actions (Automated Workflows).
* **Arquitectura:** Mendoza Volcanic.
* **Asistencia Técnica:** Gemini AI (Google).

---
