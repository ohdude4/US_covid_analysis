from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import plotly.graph_objects as go
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, regexp_replace
import plotly.express as px
import pandas as pd
from plotly.subplots import make_subplots

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/covidData/Conditions_Contributing_to_COVID-19_Deaths__by_State_and_Age__Provisional_2020-2023.csv")

    df1 = df1.filter((df1['State'] == "United States") & (df1['Group'] == "By Total")
               & (df1['Age Group'] == "All Ages"))
    df1.select("State", "Condition", "Age Group", "COVID-19 Deaths")
    df1 = df1.withColumn("COVID-19 Deaths", regexp_replace(df1['COVID-19 Deaths'], ",", ""))
    df1 = df1.withColumn("COVID-19 Deaths", df1['COVID-19 Deaths'].cast(FloatType()))

    df2 = df1.groupby("State").sum("COVID-19 Deaths")

    print("f")