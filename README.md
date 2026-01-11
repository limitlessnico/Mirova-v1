Toda la razón, ese es un detalle muy útil para el orden y se nos estaba pasando en la documentación.

Aquí tienes el `README.md` **definitivo y completo**. Agregué el punto **4** en la sección de Bases de Datos explicando que también se generan reportes individuales dentro de cada carpeta.

Copia y pega todo esto:

---

# 🌋 VolcanoBot - Automatización de Vigilancia sobre Plataforma MIROVA

**VolcanoBot** es una herramienta de **automatización de consultas** diseñada para optimizar el seguimiento de la actividad volcánica en Chile. Su función exclusiva es consultar, organizar y respaldar periódicamente la información pública disponible en la plataforma científica **MIROVA** (Middle InfraRed Observation of Volcanic Activity), desarrollada por la Universidad de Turín.

⚠️ **Aclaración Importante:** Este software **no realiza monitoreo satelital directo** ni genera alertas tempranas por cuenta propia. Actúa como un "asistente virtual" que revisa la web de MIROVA cada 15 minutos para asegurar que los datos publicados por dicha institución sean capturados y archivados antes de que sean sobrescritos por nuevas actualizaciones.

---

## 🚀 Cómo funciona el Sistema (V34.0)

El código se ejecuta en la nube (GitHub Actions) siguiendo un ciclo de 15 minutos, aplicando una estrategia de doble fase para extraer información de `mirovaweb.it`:

### 1. Fase "El Espía" (Monitor de Reportes VRP) 🕵️

El bot lee la tabla pública de "Latest Measurements" de MIROVA buscando reportes de **Energía Radiativa Volcánica (VRP)**.

* **Fuente:** Datos procesados por MIROVA basados en sensores **MODIS**, **VIIRS750** y **VIIRS375**.
* **Función:** Identifica si la plataforma ha publicado un nuevo valor de energía (MW) y lo registra en una base de datos histórica.

### 2. Fase "El Patrullero" (Respaldo de Imágenes HD) 🛰️

Dado que MIROVA publica imágenes de alta resolución que son efímeras (se actualizan constantemente), el bot visita las páginas específicas de cada volcán para respaldar estos productos visuales.

* **Fuente:** Composiciones visuales de sensores **Sentinel-2 (MSI)** y **Landsat-8/9 (OLI)** disponibles en la web.
* **Función:** Descarga y organiza las imágenes compuestas ("Latest 6 Images") para mantener un archivo visual permanente que sirva para análisis topográfico posterior.

---

## 🛡️ Robustez e Integridad de Datos (Nuevas Funciones)

Para garantizar que la información extraída sea fidedigna y manejar las particularidades de los datos satelitales, el sistema V34.0 incorpora capas de seguridad lógica:

### 🔍 1. Auditoría de Reprocesamiento (NRT vs. Standard)

Los datos satelitales suelen publicarse en dos etapas: una "Rápida" (NRT) con posición estimada y una "Refinada" (horas después) con GPS corregido.

* **El Bot detecta este cambio:** Si MIROVA corrige la distancia o la energía de un evento pasado, el sistema actualiza el registro y marca el dato como `CORRECCION_DATA`, asegurando que tengamos el dato científico final y no solo el preliminar.

### 🛑 2. Protección de Evidencia Visual

Si ocurre una corrección de datos antiguos, el bot **bloquea la descarga de nuevas imágenes** para ese evento específico. Esto evita que una foto satelital actual (del momento de la corrección) sobrescriba la foto histórica que corresponde verdaderamente al momento de la alerta.

### 🐦 3. Validación Estructural ("Canario en la Mina")

Antes de procesar datos, el bot verifica la integridad del sitio web. Si MIROVA cambia su estructura interna o la tabla de datos desaparece, el sistema aborta la operación y notifica un error crítico, evitando guardar "falsos negativos" o datos vacíos.

### 🧠 4. Filtros de Cordura (Sanity Checks)

Se aplican reglas lógicas para descartar errores de telemetría del sensor original, como valores de energía negativos (MW < 0) o fechas futuras erróneas provocadas por desajustes en relojes satelitales.

---

## 🎯 Filtros de Precisión (Geofencing)

Para clasificar los reportes de MIROVA y distinguir entre anomalías volcánicas probables y otros eventos térmicos (como incendios en laderas), se aplica un filtro de distancia referencial respecto al cráter:

| Volcán (Chile) | ID MIROVA | Límite Aplicado | Tipo de Estructura |
| --- | --- | --- | --- |
| **Láscar** | 355100 | 5.0 km | Cráter central |
| **Lastarria** | 355101 | 3.0 km | Cráter central |
| **Isluga** | 355030 | 5.0 km | Cráter central |
| **Villarrica** | 357120 | 5.0 km | Cráter central |
| **Llaima** | 357110 | 5.0 km | Cráter central |
| **Nevados de Chillán** | 357070 | 5.0 km | Complejo de domos |
| **Copahue** | 357090 | 4.0 km | Cráter central |
| **Puyehue-C. Caulle** | 357150 | **20.0 km** | Complejo Fisural |
| **Chaitén** | 358030 | 5.0 km | Domo |
| **Planchón-Peteroa** | 357040 | 3.0 km | Cráter central |

---

## 📂 Bases de Datos Generadas

El bot organiza la información extraída en cuatro tipos de archivos CSV:

1. `registro_vrp_consolidado.csv`: **Bitácora Maestra.** Historial absoluto de todas las detecciones (incluye datos brutos, correcciones y eventos descartados).
2. `registro_vrp_positivos.csv`: **Resumen de Alertas.** Solo eventos con VRP > 0 MW que cumplen con el criterio de distancia.
3. `registro_hd_msi_oli.csv`: **Catálogo Visual.** Registro de las imágenes Sentinel/Landsat respaldadas.
4. **Reportes Individuales por Volcán:** Dentro de la carpeta de imágenes de cada volcán (ej: `imagenes_satelitales/Villarrica/`), se genera un archivo `registro_Villarrica.csv` exclusivo con el historial filtrado de ese volcán específico.

---

## 🌍 Personalización

### Agregar Volcanes

Para sumar otro volcán disponible en MIROVA:

1. Busca el ID en [MIROVA Volcanoes](https://www.mirovaweb.it/NRT/volcanoes.php).
2. Agrégalo al diccionario `VOLCANES_CONFIG` en `scraper.py`.

### Ajuste de Horario

El bot convierte la hora UTC de los satélites a **Hora Local de Chile** (Continental). Para otros países, ajustar la zona horaria en la función `convertir_utc_a_chile`.

---

## 🛠️ Tecnologías y Créditos

* **Motor:** Python 3.9 (Requests + BeautifulSoup4 + Pandas).
* **Infraestructura:** GitHub Actions (Ejecución programada).
* **Fuente de Datos:** [MIROVA (Middle InfraRed Observation of Volcanic Activity)](https://www.mirovaweb.it).
* *Developed by the University of Turin, Italy (Department of Earth Science).*
* *Este proyecto es una herramienta independiente y no tiene afiliación oficial con la Universidad de Turín.*



## 👨‍💻 Autoría y Diseño

* **Concepto y Arquitectura del Sistema:** Nmendoza
* **Implementación de Código:** Generado con asistencia de IA (Gemini).
