# Proyecto Solvex - Data Engineering Test ＼(≧▽≦)／

Este repositorio contiene la solución completa para la prueba técnica de Ingeniería de Datos. El proyecto abarca desde la extracción de datos mediante web scraping ético, procesamiento y análisis con Pandas y Apache Spark, hasta la integración de pipelines y la implementación de pruebas unitarias.

---

## Estructura del Proyecto


| Carpeta / Archivo | Descripción |
| :--- | :--- |
| **`data/`** | Datasets de entrada (vuelos, población/salud) y salidas procesadas en formato Parquet. |
| **`notebooks/`** | Cuadernos interactivos de análisis exploratorio (`pandas_health.ipynb` y `spark_flights.ipynb`). |
| **`src/`** | Scripts de producción en Python (`scraper.py`, `spark_flights.py` e `integration_job.py`). |
| **`tests/`** | Suite de pruebas unitarias (`test_scraper.py`) y fixtures HTML de prueba. |
| **`requirements.txt`** | Lista de dependencias del entorno de Python. |

---

##  Requisitos e Instalación ( •̀_•́ )

 **Nota de configuración:** Se recomienda ejecutar este proyecto utilizando Python 3.10 o superior para garantizar la compatibilidad de todas las dependencias.

### 1. Clonar el repositorio
```bash
git clone https://github.com/Kheili-F/Proyecto_Slvx_007.git
cd Proyecto_Slvx_007
```

### 2. Configurar el Entorno Virtual
```bash
python -m venv env
# En Windows:
.\env\Scripts\activate
```

### 3. Instalar Dependencias
```bash
pip install -r requirements.txt
```

---

##  Ejecución de los Componentes

Puedes ejecutar y probar cada pieza del pipeline de forma independiente utilizando los siguientes comandos:

| Componente | Comando de Ejecución |
| :--- | :--- |
| **Scraper de Bitcoin** | `python src/scraper.py` |
| **Pipeline de Spark** | `python src/spark_flights.py` |
| **Pruebas Unitarias** | `pytest` |

---

##  Sección 5: Teoría ( ﾟ▽ﾟ)っ

A continuación se presenta el cuestionario teórico completo con las respuestas seleccionadas y sus respectivas justificaciones:

### 1. En Azure, la solución serverless que unifica SQL, Spark y Data Explorer para analítica a gran escala es:
*   a) Azure SQL Database
*   **b) Azure Synapse Analytics (serverless/ dedicated)** 
*   c) Azure Data Factory
*   d) Azure Stream Analytics
*   **Justificación:** Azure Synapse es la plataforma unificada de Microsoft que integra de manera nativa los motores de almacenamiento SQL distribuidos, clústeres de Apache Spark y capacidades de exploración de datos en un solo servicio con esquemas de pago por consulta o dedicados

### 2. En Data Factory / Synapse Pipelines, la actividad gráfica para transformaciones a escala sin escribir código es:
*   a) HDInsight Spark
*   b) Azure Databricks
*   **c) Mapping Data Flows** 
*   d) Azure Stream Analytics
*   **Justificación:** Mapping Data Flows es la herramienta de diseño visual en Azure Data Factory que permite a los ingenieros de datos construir lógica de transformación compleja (ETL) gráficamente, la cual se traduce por debajo en código optimizado ejecutado sobre Spark de manera transparente

### 3. En Spark, la abstracción tolerante a fallos que permite procesamiento en memoria con transformaciones perezosas es:
*   a) Apache Hadoop
*   b) Apache Flink
*   c) Spark Streaming
*   **d) RDD** 
*   **Justificación:** El RDD (Resilient Distributed Dataset) es la estructura de datos básica de Apache Spark. Es tolerante a fallos, se mantiene en memoria principal para un acceso ultrarrápido y evalúa sus transformaciones bajo el principio de *lazy evaluation*

### 4. En Pandas, para eliminar filas duplicadas en un DataFrame se usa:
*   a) df.groupby()
*   **b) df.drop_duplicates()**
*   c) df.fillna()
*   d) df.pivot_table()
*   **Justificación:** El método `df.drop_duplicates()` es la función nativa que provee la librería de Pandas para rastrear e identificar filas repetidas en un DataFrame, eliminándolas de acuerdo a las columnas indicadas

### 5. En Azure Databricks, ¿qué lenguaje se usa con mayor frecuencia para data engineering y notebooks (además de Python)?
*   a) R
*   b) Java
*   **c) Scala** 
*   d) C#
*   **Justificación:** Dado que el motor de Apache Spark está escrito originalmente en Scala, este lenguaje ofrece la mejor integración, velocidad de procesamiento nativa y una adopción masiva junto a Python dentro de los notebooks empresariales de Azure Databricks