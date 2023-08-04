import json

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

f = open('data/USGEOJson.json')
counties = json.load(f)

import plotly.express as px
import pandas as pd
import os

import plotly.io as pio

conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf=conf)
sparkSession = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

df1 = sparkSession.read.format("csv") \
    .option("header", True) \
    .load("data/covidData/Split/01/*") \
    .orderBy("Last_Update")

pdf1 = df1.toPandas()
pdf1['Confirmed'] = pd.to_numeric(pdf1['Confirmed'])

fig = px.choropleth_mapbox(pdf1, geojson=counties,
                           featureidkey="properties.NAME",
                           locations='Province_State',
                           color='Confirmed',
                           animation_frame='Date',
                           animation_group='Province_State',
                           color_continuous_scale='reds',
                           range_color=[0, 2500000],
                           mapbox_style='carto-positron',
                           zoom=3,
                           center={"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           title='COVID-19 Confirmed Cases in the US Over Time',
                           )

fig.write_html("plot_output/map_distribution.html")

fig.show()