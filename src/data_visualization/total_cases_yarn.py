#!/usr/bin/python3
#-*- coding: UTF-8 -*-

import findspark
findspark.init()


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
import plotly.graph_objects as go

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("test") \
        .master("yarn") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("hdfs://172.29.203.82:8020/DataEachState2/*/")\
        .orderBy("Date")

    df2 = df1.groupby("Date").agg(F.sum("Confirmed").alias("Confirmed"),
                                  F.sum("Deaths").alias("Deaths"))
    df3 = df1.groupby("Date").agg(F.sum("diff_Confirmed").alias("diff_Confirmed"),
                                  F.sum("diff_Deaths").alias("diff_Deaths"))

    pdf = df2.toPandas()
    pdf2 = df3.toPandas()

    fig = go.Figure()
    fig2 = go.Figure()
    fig.add_trace(go.Scatter(x = pdf["Date"], y = pdf["Confirmed"],
                             mode = 'lines',
                             name = "Confirmed Cases"))
    fig2.add_trace(go.Scatter(x = pdf["Date"], y = pdf["Deaths"],
                             mode = 'lines',
                             name = "Deaths"))

    fig.write_html("/root/Plot/CumulativeConfirmed_US.html")
    fig2.write_html("/root/Plot/CumulativeDeaths_US.html")

    fig3 = go.Figure()
    fig4 = go.Figure()
    fig3.add_trace(go.Scatter(x=pdf2["Date"], y=pdf2["diff_Confirmed"],
                             mode='lines',
                             name="Confirmed Cases"))
    fig4.add_trace(go.Scatter(x=pdf2["Date"], y=pdf2["diff_Deaths"],
                              mode='lines',
                              name="diff_Deaths"))

    fig3.write_html("/root/Plot/DailyConfirmed_US.html")
    fig4.write_html("/root/Plot/DailyDeaths_US.html")