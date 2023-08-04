#!/usr/bin/python3
#-*- coding: UTF-8 -*-

import findspark
findspark.init()

import plotly.express as px
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

if __name__ == '__main__':
    conf = SparkConf().setAppName("Line").setMaster("yarn")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("Line") \
        .master("yarn") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("hdfs://172.29.203.82:8020/DataEachState2/*/")\
        .orderBy("Date")

    df1 = df1.withColumn("Confirmed", df1["Confirmed"].cast(FloatType()))
    df1 = df1.withColumn("Deaths", df1["Deaths"].cast(FloatType()))
    df1 = df1.withColumn("diff_Confirmed", df1.diff_Confirmed.cast(FloatType()))
    df1 = df1.withColumn("diff_Deaths", df1.diff_Deaths.cast(FloatType()))

    df2 = df1.groupby("Date").agg(F.sum("Confirmed").alias("Confirmed"))
    df2 = df2.orderBy("Date")

    pdf1 = df2.toPandas()
    pdf2 = df1.toPandas()

    fig_CumulativeConfirmed = px.line(pdf2, x = "Date", y = "Confirmed", color = "Province_State")
    fig_CumulativeDeaths = px.line(pdf2, x = "Date", y = "Deaths", color = "Province_State")
    fig_DailyNewConfirmed = px.line(pdf2, x = "Date", y = "diff_Confirmed", color = "Province_State")
    fig_DailyNewDeaths = px.line(pdf2, x="Date", y="diff_Deaths", color="Province_State")

    fig_CumulativeConfirmed.update_layout(title = dict(text = "累计确诊",
                                    font = dict(family = "Arial",
                                                size = 25,
                                                color = 'red')))

    fig_CumulativeDeaths.update_layout(title = dict(text = "累计死亡",
                                    font = dict(family = "Arial",
                                                size = 25,
                                                color = 'red')))

    fig_DailyNewConfirmed.update_layout(title=dict(text="日新增确诊",
                                  font=dict(family="Arial",
                                            size=25,
                                            color='red')))

    fig_DailyNewDeaths.update_layout(title=dict(text="日新增死亡",
                                  font=dict(family="Arial",
                                            size=25,
                                            color='red')))


    fig_CumulativeConfirmed.write_html("/root/Plot/CumulativeConfirmed.html")
    fig_CumulativeDeaths.write_html("/root/Plot/CumulativeDeaths.html")
    fig_DailyNewConfirmed.write_html("/root/Plot/DailyNewConfirmed.html")
    fig_DailyNewDeaths.write_html("/root/Plot/DailyNewDeaths.html")

    print("success!")