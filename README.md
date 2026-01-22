# 🌋 Mirova-OVDAS VRP Monitor (Chile)

**Mirova-OVDAS VRP Monitor** es una plataforma de **automatización y visualización científica** diseñada para el seguimiento de la Potencia Radiada Volcánica (VRP) en los principales centros eruptivos de Chile. El sistema actúa como un nodo de respaldo y análisis que captura, procesa y grafica la información pública de la plataforma **MIROVA** (Universidad de Turín).

⚠️ **Aclaración:** Este software es una herramienta independiente de soporte técnico. No reemplaza los canales oficiales de alerta temprana de instituciones estatales.

---

## 📡 Dashboard e Interfaz de Auditoría 

El sistema cuenta con un **Dashboard** que permite visualizar el estado de salud del monitor y las tendencias térmicas en tiempo real.

> [!IMPORTANT]
> **[👉 ACCEDER AL MONITOR EN VIVO (Standard OVDAS)](https://mendozavolcanic.github.io/Mirova-v1/)**

### 🟢 Semáforo de Salud del Sistema

El Dashboard integra una **Barra de Auditoría Técnica** que verifica la sincronización con los satélites:

* **Monitor Operativo:** Confirma que el robot ha procesado los datos exitosamente en el último ciclo.
* **Sincronización UTC:** Indica la hora exacta de la última captura de datos desde MIROVA.
* **📅 Tiempo Universal:** Todas las fechas se muestran en **hora UTC** para consistencia científica internacional.

---

## 📈 Visualización de Tendencias (V4.1)

El módulo `visualizador.py` genera gráficos de alta precisión con las siguientes características técnicas:

### **Gráficos Duales (Escala Lineal y Logarítmica)**

* **Escala Lineal:** Para visualización intuitiva de tendencias y comparación de magnitudes relativas.
* **Escala Logarítmica:** Permite detectar eventos de baja energía que serían invisibles en escala lineal, esencial para monitoreo de fondo térmico.

### **Características Avanzadas**

* **Sombreado Dinámico Inteligente:** El fondo del gráfico se colorea automáticamente (Verde, Amarillo, Naranja) solo si la energía detectada alcanza los umbrales de alerta, evitando distorsiones visuales en niveles bajos.

* **Iconografía Multisensor:** Diferenciación visual de la fuente del dato para auditoría científica:
  * `▲` **MODIS**: Sensor histórico de amplio espectro.
  * `■` **VIIRS 375m**: Alta resolución para detección de anomalías pequeñas.
  * `●` **VIIRS 750m**: Alta sensibilidad térmica.

* **Etiquetado Automático:** Marcado dinámico del valor **MAX** (en MW) detectado en el periodo mensual y anual.

* **Sistema de Confianza OCR:** Los eventos capturados por OCR se marcan con nivel de confianza:
  * 🟢 **Alta/Validado**: Evento confirmado con píxeles rojos en ROI
  * 🟡 **Media**: Evento en zona límite (mezcla de indicadores)

---

## 🛰️ Estrategia de Captura Dual: latest.php + OCR (V4.0)

El sistema implementa una **arquitectura de doble captura** que combina dos fuentes complementarias:

### **1. Scraper Primario (latest.php)**

Motor principal de captura que ejecuta ciclos cada **15-30 minutos**:

* **Detección de Alerta:** Si se detecta **VRP > 0** dentro del radio de seguridad, el sistema descarga el set de evidencia completo.
* **Soporte Tri-Sensor:** Captura simultánea de **MODIS**, **VIIRS 375m** y **VIIRS 750m** para el mismo evento.
* **Respaldo en Calma:** En ausencia de alertas (VRP = 0), prioriza **VIIRS 375m** para una captura diaria de referencia.
* **Auditoría de Procesamiento:** Detecta cuando MIROVA actualiza datos NRT a Standard y sincroniza el registro histórico.

### **2. Scraper Secundario OCR (Recuperación de Eventos Perdidos)**

Sistema de **detección visual automática** que opera cada **1 hora** para recuperar eventos no capturados por latest.php:

#### **Pipeline OCR (3 etapas):**

**ETAPA 1: Extracción de texto (Latest10NTI.png)**
* Descarga imágenes `Latest10NTI.png` de cada volcán × sensor
* Usa **Tesseract OCR** con estrategias múltiples para extraer fechas y valores VRP
* Detecta hasta 10 eventos simultáneos por imagen
* **Robustez:** 3 estrategias de extracción garantizan 10/10 detecciones

**ETAPA 2: Validación visual (Dist.png)**
* Analiza gráfico de distancia temporal para validar el evento
* Define **ROI** (región de interés) = últimas 24 horas del gráfico
* Cuenta píxeles por densidad (no requiere formas geométricas):
  * 🟢 Filtra **píxeles verdes** (estrella = última detección, puede confundir)
  * 🔴 Cuenta **píxeles rojos** (evento real cercano)
  * ⚫ Cuenta **píxeles negros** (evento fuera de límite)

**ETAPA 3: Clasificación inteligente**
* **Ratio rojos/negros** distingue eventos reales de falsos positivos:
  * `Ratio > 2.0` → 🟢 **Alta** (rojo dominante, evento REAL)
  * `0.5 < Ratio < 2.0` → 🟡 **Media** (zona límite, revisar)
  * `Ratio < 0.5` → ⚫ **Falso positivo** (negro dominante, lejos del cráter)
  * `Sin píxeles` → ⚫ **Falso positivo** (evento fuera de ventana temporal)

#### **Almacenamiento selectivo:**
* **Se guardan imágenes SOLO si:** Confianza alta o media (eventos probables)
* **NO se guardan imágenes si:** Falsos positivos o eventos descartados
* **Auditoría completa:** Todos los eventos (incluso falsos) se registran en `registro_vrp_ocr.csv`

#### **Integración con sistema principal:**
* `merger_maestro.py` combina datos de latest.php + OCR
* Elimina duplicados (mismo timestamp + volcán + sensor)
* Genera `registro_vrp_maestro_publicable.csv` con eventos validados
* **Solo se publican:** ALERTA_TERMICA (alta/media), NO falsos positivos

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

### **Bases de datos maestras:**
* `registro_vrp_consolidado.csv`: Datos capturados por latest.php (fuente primaria)
* `registro_vrp_ocr.csv`: Eventos recuperados por OCR (incluye falsos positivos para auditoría)
* `registro_vrp_maestro_publicable.csv`: Base final combinada y filtrada para el Dashboard

### **Registros por volcán:**
* `registro_[Volcan].csv`: CSV individual por cada volcán (se actualiza automáticamente)

### **Evidencia visual:**
* `imagenes_satelitales/`: Repositorio organizado por volcán y fecha con la evidencia visual de los sensores
* `graficos_tendencia/`: Gráficos de actividad térmica procesados para el Dashboard

### **Logs técnicos:**
* `bitacora_robot.txt`: Registro técnico de cada ciclo de ejecución
* `ocr_logs/`: Logs detallados del sistema OCR

---

## 🔬 Innovaciones Técnicas (V4.0)

### **1. Sistema OCR Robusto**
* **Múltiples estrategias de extracción** evitan pérdida de datos por inconsistencias de Tesseract
* **Filtrado de "Last Update"** con `finditer()` para posiciones exactas
* **Detección R dominante** evita confundir grises con rojos

### **2. Validación Visual Inteligente**
* **Densidad de píxeles** (no circularidad) más robusto para símbolos irregulares
* **Filtrado de estrella verde** (última detección) evita falsos positivos
* **Ratio rojos/negros** distingue eventos reales de falsos con precisión científica

### **3. Almacenamiento Eficiente**
* **Descarga selectiva de imágenes** solo para eventos probables
* **Auditoría completa** mantiene registro de falsos positivos sin desperdiciar espacio
* **Actualización automática** de registros individuales por volcán

---

## 🛠️ Tecnologías y Autoría

* **Motor:** Python 3.9 (Pandas, Matplotlib, Plotly, BeautifulSoup4, Pytesseract, OpenCV)
* **OCR Engine:** Tesseract 4.x/5.x
* **Infraestructura:** GitHub Actions (Automated Workflows)
* **Arquitectura:** Mendoza Volcanic
* **Asistencia Técnica:** Claude AI (Anthropic)

---

## 🙏 Acknowledgements

Toda la información térmica utilizada en este proyecto es procesada y obtenida a través de la infraestructura de la plataforma **MIROVA** (Middle InfraRed Observation of Volcanic Activity).

* **Desarrollo y Mantenimiento:** Departamento de Ciencias de la Tierra de la [Universidad de Turín](https://www.unito.it/) (Italia), en colaboración con la [Universidad de Florencia](https://www.unifi.it/).
* **Investigador Principal:** Diego Coppola.
* **Referencias Científicas:**
  * Coppola, D., et al. (2016). *Enhanced volcanic hot-spot detection using MODIS IR data: results from the MIROVA system*.
  * Coppola, D., et al. (2020). *Thermal Remote Sensing for Global Volcano Monitoring: Experiences From the MIROVA System*.
* Para más información, visite el sitio oficial de MIROVA.
* We gratefully acknowledge NASA LANCE for access to MODIS and VIIRS Near Real Time products. Sentinel-2 and Landsat 8 data are accessed through the Copernicus Open Access Hub.

---

## 📊 Estadísticas del Sistema

* **Cobertura:** 10 volcanes activos de Chile
* **Frecuencia latest.php:** Cada 5 minutos
* **Frecuencia OCR:** Cada 1 hora
* **Sensores monitoreados:** MODIS, VIIRS 375m, VIIRS 750m
* **Tasa de recuperación OCR:** ~5-10% de eventos perdidos
* **Precisión de clasificación:** Alta (ratio-based validation)

---
