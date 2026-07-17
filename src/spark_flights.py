import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class FlightAnalysisJob:
    def __init__(self):
        # Inicializar Spark Session optimizada localmente
        self.spark = (SparkSession.builder
            .appName("OnTimePerformanceAnalysis")
            .master("local[1]")  # Yo puse un solo nucleo por que mi humilde pc se estaba quemando 
            .config("spark.sql.shuffle.partitions", "4")  # esto es para optimizar el shuffle en local xd
            .getOrCreate()
        )
        # Usar rutas absolutas para evitar que Hadoop se confunda en Windows
        self.data_dir = os.path.abspath("data/flights")
        self.parquet_output = os.path.abspath("data/flights_processed.parquet")

    def generate_mock_flight_data(self):
        """Genera archivos CSV de prueba, incluyendo registros corruptos para la columna de rescate."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # CSV 1: Datos válidos
        csv_content_1 = (
            "year,month,day,origin,dest,dep_delay,arr_delay,cancelled\n"
            "2026,1,15,MIA,LAX,15,10,0\n"
            "2026,1,15,JFK,LAX,5,-5,0\n"
            "2026,1,16,DFW,ORD,45,50,0\n"
            "2026,1,16,LAX,JFK,-2,0,0\n"
            "2026,1,17,MIA,ORD,120,115,0\n"
        )
        
        # CSV 2: Datos válidos + Registro Corrupto (fila incompleta o con tipos de datos inválidos)
        csv_content_2 = (
            "year,month,day,origin,dest,dep_delay,arr_delay,cancelled\n"
            "2026,2,10,LAX,MIA,0,-10,0\n"
            "CORRUPT_RECORD_THIS_LINE_SHOULD_BE_RESCUED,9999,9999\n"  
            "2026,2,11,JFK,LAX,30,25,0\n"
            "2026,2,12,ORD,DFW,10,,1\n"  # Vuelo cancelado con delay nulo
        )
        
        with open(os.path.join(self.data_dir, "flights_jan.csv"), "w") as f:
            f.write(csv_content_1)
        with open(os.path.join(self.data_dir, "flights_feb.csv"), "w") as f:
            f.write(csv_content_2)
            
        print("INFO: Datos mock generados exitosamente en data/flights/")

    def run_pipeline(self):
        # 1 Definir esquema explícito con columna de rescate para registros corruptos
        schema = StructType([
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("origin", StringType(), True),
            StructField("dest", StringType(), True),
            StructField("dep_delay", IntegerType(), True),
            StructField("arr_delay", IntegerType(), True),
            StructField("cancelled", IntegerType(), True),
            StructField("_corrupt_record", StringType(), True)  
        ])

        # 2 Ingesta de múltiples CSVs con esquema explícito y modo PERMISSIVE
        print("\nINFO: Cargando múltiples archivos CSV en Spark...")
        # Apuntamos a la carpeta absoluta directamente para no usar comodines (*.csv)
        df = (self.spark.read
            .format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")  # Habilita el uso de _corrupt_record por siacaaaaso
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .load(self.data_dir)
        )

        # 3 Mostrar y separar registros corruptos
        print("\nINFO: Detectando registros corruptos...")
        df.cache()  # Guardar en caché ya que usaremos este DF varias veces
        
        corrupt_df = df.filter(df["_corrupt_record"].isNotNull())
        valid_df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
        
        print(" Registros Corruptos Rescatados（˶′◡‵˶）")
        corrupt_df.select("_corrupt_record").show(truncate=False)
        
        print("--- Registros Válidos ---")
        valid_df.show()

        
        # CONSULTAS ANALÍTICAS (´〜｀*) zzz
        
        
        # Consulta A: Retraso promedio de llegada por aeropuerto de destino 
        print("\nINFO: Consulta A: Retraso promedio de llegada en destino ")
        lax_delays = (valid_df
            .filter(F.col("dest") == "LAX")
            .groupBy("dest")
            .agg(F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"))
        )
        lax_delays.show()

        # Consulta B: Top 10 de rutas (origen-destino) por volumen de vuelos y retraso promedio
        print("\nINFO: Consulta B: Top 10 rutas por número de vuelos y su retraso promedio")
        top_routes = (valid_df
            .groupBy("origin", "dest")
            .agg(
                F.count("*").alias("total_vuelos"),
                F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay")
            )
            .orderBy(F.desc("total_vuelos"))
            .limit(10)
        )
        top_routes.show()

        # Consulta C: Ranking mensual por aeropuerto usando Ventanas (Top 3 rutas por volumen)
        print("\nINFO: Consulta C: Top 3 rutas con mayor volumen de vuelos por mes")
        window_spec = Window.partitionBy("month").orderBy(F.desc("total_vuelos"))
        
        monthly_routes = (valid_df
            .groupBy("month", "origin", "dest")
            .agg(F.count("*").alias("total_vuelos"))
            .withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") <= 3)
        )
        monthly_routes.show()

        # OPTIMIZACIÓN Y EXPORTACIÓN
        # Aplicamos dos técnicas de optimización:
        # 1. Reparticionamiento basado en la columna de particionado final (month) para evitar sobrecarga de archivos pequeños.
        # 2. Conversión a Parquet particionado.
        print("\nINFO: Optimizando y exportando resultados a Parquet, un momentito...")

        (valid_df
            .repartition("month")
            .write
            .mode("overwrite")
            .partitionBy("month")
            .parquet(self.parquet_output)
        )

        print(f"INFO: Datos exportados exitosamente a: {self.parquet_output}")

        self.spark.stop()


if __name__ == "__main__":
    job = FlightAnalysisJob()
    job.generate_mock_flight_data()
    job.run_pipeline()

