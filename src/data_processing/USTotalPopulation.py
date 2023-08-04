from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import plotly.graph_objects as go
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, regexp_replace

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/covidData/time_series_covid19_deaths_US.csv")

    df1 = df1.select("Province_State", "Population")
    df1 = df1.groupby("Province_State").agg(F.sum("Population").alias("Population"))

    print("f")
