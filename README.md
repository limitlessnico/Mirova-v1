# 🌋 VolcanoBot - Monitor de Volcanes (Cliente MIROVA Chile)

**VolcanoBot** es un sistema de vigilancia automatizada que monitorea la actividad volcánica en Chile utilizando los datos procesados por la plataforma **MIROVA** (Middle InfraRed Observation of Volcanic Activity), desarrollada por la Universidad de Turín.

Este bot no se conecta directamente a los satélites, sino que actúa como un "observador virtual" que revisa constantemente el sitio web de MIROVA para detectar nuevas alertas térmicas y recolectar las imágenes procesadas más recientes.

---

## 🚀 Cómo funciona el Sistema (V28.0)

El código se ejecuta en la nube (GitHub Actions) cada **15 minutos**, aplicando una estrategia de doble fase para extraer información de `mirovaweb.it`:

### 1. Fase "El Espía" (Monitor de Alertas VRP) 🕵️
El bot lee la tabla de "Latest Measurements" de MIROVA buscando picos de **Energía Radiativa Volcánica (VRP)**.
* **Fuente:** Datos procesados de sensores **MODIS**, **VIIRS750** y  **VIIRS375**.
* **Función:** Detecta si MIROVA ha publicado una nueva alerta de calor (MW) en los últimos minutos.

### 2. Fase "El Patrullero" (Imágenes Procesadas HD) 🛰️
Como MIROVA integra imágenes de alta resolución que no siempre generan una alerta de VRP inmediata, el bot visita las páginas específicas de cada volcán para buscar nuevos productos visuales.
* **Fuente:** Composiciones visuales de sensores **Sentinel-2 (MSI)** y **Landsat-8/9 (OLI)**.
* **Función:** Descarga las imágenes compuestas ("Latest 6 Images") que MIROVA genera para análisis topográfico.

---

## 🛡️ Filtros de Precisión (Geofencing)

Para filtrar los datos de MIROVA y descartar anomalías que no sean volcánicas (como incendios forestales en las laderas), el bot aplica un filtro de distancia desde el cráter:

| Volcán (Chile) | ID MIROVA | Límite Aplicado | Tipo de Estructura |
| :--- | :--- | :--- | :--- |
| **Láscar** | 355100 | 5.0 km | Cráter central |
| **Lastarria** | 355101 | 3.0 km | Cráter central |
| **Isluga** | 355030 | 5.0 km | Cráter central |
| **Villarrica** | 357120 | 5.0 km | Cráter central|
| **Llaima** | 357110 | 5.0 km | Cráter central |
| **Nevados de Chillán** | 357070 | 5.0 km | Complejo de domos |
| **Copahue** | 357090 | 4.0 km | Cráter central |
| **Puyehue-C. Caulle** | 357150 | **20.0 km** | Complejo Fisural |
| **Chaitén** | 358030 | 5.0 km | Domo |
| **Planchón-Peteroa** | 357040 | 3.0 km | Cráter central |

---

## 📂 Bases de Datos Generadas

El bot organiza la información extraída de MIROVA en tres archivos CSV:

1.  `registro_vrp_consolidado.csv`: **Bitácora Completa.** Historial de todas las detecciones de MIROVA (incluyendo falsos positivos por distancia y días de calma).
2.  `registro_vrp_positivos.csv`: **Alertas Confirmadas.** Solo eventos con VRP > 0 MW validados por el filtro de distancia.
3.  `registro_hd_msi_oli.csv`: **Catálogo HD.** Registro de las imágenes Sentinel/Landsat encontradas en la web.

### 🖼️ Criterio de Descarga de Imágenes
* **Alertas (VRP > 0):** Se descargan todos los gráficos disponibles en MIROVA para ese evento.
* **Calma (VRP = 0):** Se descarga una imagen de referencia diaria (VIIRS 375m) y siempre se guardan las nuevas imágenes HD (MSI/OLI) si están disponibles.

---

## 🌍 Personalización

### Agregar Volcanes
Para sumar otro volcán disponible en MIROVA:
1. Busca el ID en [MIROVA Volcanoes](https://www.mirovaweb.it/NRT/volcanoes.php).
2. Agrégalo al diccionario `VOLCANES_CONFIG` en `scraper.py`.

### Ajuste de Horario
El bot convierte la hora UTC de MIROVA a **Hora Local de Chile** (Continental). Para otros países, ajustar la zona horaria en la función `convertir_utc_a_chile`.

---

## 🛠️ Tecnologías y Créditos

* **Motor:** Python 3.9 (Requests + BeautifulSoup4 + Pandas).
* **Infraestructura:** GitHub Actions.
* **Fuente de Datos Principal:** [MIROVA (Middle InfraRed Observation of Volcanic Activity)](https://www.mirovaweb.it).
    * *Developed by the University of Turin, Italy (Department of Earth Science).*
    * *Este proyecto es una herramienta independiente y no tiene afiliación oficial con la Universidad de Turín.*

---
