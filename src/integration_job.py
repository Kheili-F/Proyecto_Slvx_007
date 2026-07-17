import os
import sys
import time
from functools import wraps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Decorador para medir el tiempo de ejecucion y documentar la eficiencia en los logs
def medir_tiempo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.time()
        print(f"\nIniciando proceso: {func.__name__}...")
        resultado = func(*args, **kwargs)
        fin = time.time()
        print(f"Metrica de rendimiento: {func.__name__} completado en {fin - inicio:.2f} segundos.")
        return resultado
    return wrapper

class MovieIntegrationJob:
    def __init__(self):
        # Inicializacion de la sesion de Spark optimizada para entorno de desarrollo local
        self.spark = (SparkSession.builder
            .appName("MovieIntegrationJob")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Resolucion de rutas relativas dentro de la estructura del proyecto
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.movies_csv_path = os.path.join(self.base_dir, "data", "titulos_peliculas.csv")
        self.parquet_output_path = os.path.join(self.base_dir, "data", "peliculas_integradas.parquet")

    def generar_datos_mock(self):
        """
        Genera archivos de prueba autoejecutables para asegurar la reproducibilidad 
        del test en cualquier otra pc 
        """
        os.makedirs(os.path.join(self.base_dir, "data"), exist_ok=True)
        
        # Creacion del CSV que leera la biblioteca de Pandas
        if not os.path.exists(self.movies_csv_path):
            import pandas as pd
            movies_data = {
                "pelicula_id": [1, 2, 3, 4],
                "titulo": ["Inception", "Interstellar", "The Dark Knight", "The Matrix"],
                "anio": [2010, 2014, 2008, 1999],
                "genero": ["Sci-Fi", "Sci-Fi", "Action", "Sci-Fi"]
            }
            pd.DataFrame(movies_data).to_csv(self.movies_csv_path, index=False)
            print(f"INFO: Dataset de pruebas creado en {self.movies_csv_path}")

        # Dataset de criticas simulado directamente en Spark para la integracion
        criticas_data = [
            # ID de pelicula, critico, puntuacion, fecha
            (1, "CriticoA", 4.5, "2020-05-12"),
            (1, "CriticoA", 5.0, "2020-05-12"),  # Duplicado: se debe conservar la calificacion 5.0
            (1, "CriticoB", 4.0, "2018-11-01"),  # Registro descartado por ser anterior a 2019
            (2, "CriticoA", 4.8, "2021-06-15"),
            (2, "CriticoC", 3.5, "2022-01-20"),
            (3, "CriticoB", 5.0, "2019-07-10"),
            (4, "CriticoC", 2.0, "2020-08-05"),
            (4, "CriticoC", 4.2, "2020-08-05"),  # Duplicado: se debe conservar la calificacion 4.2
        ]
        
        columnas = ["pelicula_id", "critico", "puntuacion", "fecha"]
        return self.spark.createDataFrame(criticas_data, schema=columnas)

    @medir_tiempo
    def run_pipeline(self):
        # 1 Lectura del catalogo de criticas en Spark
        criticas_df = self.generar_datos_mock()
        
        # 2 Lectura del CSV mediante la libreria de Pandas e integracion con Spark
        print("INFO: Cargando catalogo de peliculas mediante Pandas...")
        import pandas as pd
        pandas_movies_df = pd.read_csv(self.movies_csv_path)
        movies_df = self.spark.createDataFrame(pandas_movies_df)

        # 3 Regla del negocio: Considerar unicamente resenas a partir del año 2019
        print("INFO: Filtrando resenas desde 2019-01-01 en adelante...")
        criticas_filtradas = criticas_df.filter(F.col("fecha") >= "2019-01-01")

        # 4  Regla del negocio: Eliminar duplicados por combinacion (pelicula_id, critico, fecha)
        # Se implementa una funcion de ventana particionada para conservar unicamente la puntuacion mas alta
        print("INFO: Resolviendo colisiones de duplicados (preservando maxima puntuacion)...")
        window_spec = Window.partitionBy("pelicula_id", "critico", "fecha").orderBy(F.col("puntuacion").desc())
        
        criticas_limpias = (criticas_filtradas
            .withColumn("rn", F.row_number().over(window_spec))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        # 5 Agregaciones de Spark para calcular metricas resumen agrupadas por pelicula
        print("INFO: Generando metricas agregadas de resenas por pelicula...")
        resumen_criticas = criticas_limpias.groupBy("pelicula_id").agg(
            F.round(F.avg("puntuacion"), 2).alias("promedio_puntuacion"),
            F.count("puntuacion").alias("n_resenas"),
            F.max("fecha").alias("ultima_fecha_resena")
        )

        # 6 Union de datasets mediante optimizacion de Broadcast Join (util para tablas de catalogos pequenas)
        print("INFO: Cruzando dataset de peliculas con resumen de resenas...")
        resultado_final = movies_df.join(F.broadcast(resumen_criticas), on="pelicula_id", how="inner")

        # Seleccion y ordenamiento de columnas finales requeridas por la Seccion 3 del examen
        resultado_final = resultado_final.select(
            "titulo", 
            "anio", 
            "genero", 
            "promedio_puntuacion", 
            "n_resenas", 
            "ultima_fecha_resena"
        )

        # Muestra de resultados para validacion visual por consola
        print("\n Visualizacion de Dataset Integrado ")
        resultado_final.show(truncate=False)

        # 7  Exportacion de los resultados a almacenamiento local en formato Parquet
        # Se realiza una particion fisica en disco por el año de estreno ("anio")
        print(f"INFO: Guardando resultados en Parquet particionado en (づ ᴗ _ᴗ)づ: {self.parquet_output_path} ")
        (resultado_final
            .write
            .mode("overwrite")
            .partitionBy("anio")
            .parquet(self.parquet_output_path)
        )
        print("INFO: Proceso de exportacion de datos finalizado exitosamente (•‿•)")
        
        self.spark.stop()

if __name__ == "__main__":
    job = MovieIntegrationJob()
    job.run_pipeline()
