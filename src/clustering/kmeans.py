
# Kmeans聚类分析。这里使用平均最高最低气温，新冠死亡数据，以及人口数据作为参数，对其进行聚类分析。

# K-means clustering analysis. Average maximum, and minimum temperatures, 
# COVID-19 death data, and population data are used as parameters for conducting 
# clustering analysis.

from pyspark import SparkConf, SparkContext, StorageLevelA
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
import os
import glob
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go
import json

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

    dir = "./data/US_Temperature/*.csv"
    csv_files = glob.glob(pathname = dir)

    df0 = sparkSession.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(csv_files[0])

    for file in csv_files[1:]:
        df1 = sparkSession.read.format("csv") \
            .option("header", True) \
            .option("inferSchema", True)\
            .option("enforceSchema", False)\
            .load(file)

        df0 = df0.unionByName(df1, allowMissingColumns = True)


    df0 = df0.filter(df0['DATE'] == '2015')

    df_temp = df0.select("STATION", "DATE", "NAME", "TMIN", "TMAX")
    df_temp = df_temp.withColumn("State_b", F.substring(df_temp['NAME'], -5, 2))

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/covidData/Provisional_COVID-19_Deaths_by_Sex_and_Age.csv")
    df_Death = df1.filter((df1.State != "United States") & (df1.Sex == "All Sexes")
                          & (df1["Group"] == "By Total") & (df1["Age Group"] == "All Ages"))
    df_Death = df_Death.select("State", "COVID-19 Deaths")
    df_Death = df_Death.withColumnRenamed("State", "State_D")
    df_Death = df_Death.withColumn("COVID-19 Deaths", F.regexp_replace(df_Death["COVID-19 Deaths"], ",", ""))
    df_Death = df_Death.withColumn("COVID-19 Deaths", df_Death["COVID-19 Deaths"].cast(FloatType()))

    df3 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/US_Population_Demographic/nhgis0001_ds253_2021_state.csv")
    df_Pop = df3.select("STUSAB", "STATE", "ANK8E001")

    df3 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/covidData/Daily_US/01-13-2023.csv")
    df_LatLong_Confirmed = df3.select("Province_State", "Lat", "Long_", "Confirmed")
    df_LatLong_Confirmed = df_LatLong_Confirmed.withColumn("Lat", df_LatLong_Confirmed['Lat'].cast(FloatType()))
    df_LatLong_Confirmed = df_LatLong_Confirmed.withColumn("Long_", df_LatLong_Confirmed['Long_'].cast(FloatType()))
    df_LatLong_Confirmed = df_LatLong_Confirmed.withColumn("Confirmed", df_LatLong_Confirmed['Confirmed'].cast(FloatType()))

    joined_df = df_Pop.join(df_Death, df_Pop['State'] == df_Death['State_D'], "outer")
    joined_df = joined_df.join(df_temp, df_temp['State_b'] == joined_df['STUSAB'], "outer")
    joined_df = joined_df.join(df_LatLong_Confirmed, df_LatLong_Confirmed['Province_State'] == joined_df['State'],
                               'outer')
    joined_df = joined_df.filter(joined_df["STUSAB"] != "None")
    joined_df = joined_df.filter(joined_df["STATE"] != "District of Columbia")

    pdf = joined_df.toPandas()
    array = pdf.iloc[:, [2, 4, 8, 9, 12, 13, 14]].fillna(0).values

    scaler = StandardScaler()
    array_scaled = scaler.fit_transform(array)

    inertia = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(array_scaled)
        inertia.append(kmeans.inertia_)

    scree_x = list(range(1, 11))
    x = [range(1, 11)]
    scree_fig = go.Figure()
    scree_fig.add_trace(go.Scatter(x=scree_x, y=inertia,
                                   mode='lines+markers',
                                   name='碎石图'))
    scree_fig.show()

    kmeans = KMeans(n_clusters=3)
    labels_scaled = kmeans.fit_predict(array_scaled)

    pca = PCA(n_components=2)
    reduced_scaled = pca.fit_transform(array_scaled)

    pdf['X_scaled'] = reduced_scaled[:, 0]
    pdf['Y_scaled'] = reduced_scaled[:, 1]
    pdf['label_scaled'] = labels_scaled

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=pdf['X_scaled'], y=pdf['Y_scaled'], mode='markers',
                              marker=dict(color=labels_scaled, size=10),
                              hovertext=pdf['STATE']))
    fig2.show()

    f = open('data/USGEOJson.json')
    counties = json.load(f)
    map = px.choropleth_mapbox(pdf, geojson=counties,
                               featureidkey="properties.NAME",
                               locations='STATE',
                               color='label_scaled',
                               mapbox_style='carto-positron',
                               zoom=3,
                               center={"lat": 37.0902, "lon": -95.7129},
                               opacity=0.5,
                               title='COVID-19 Confirmed Cases in the US Over Time',
                               height=800
                               )
    map.show()

    pdf_c1 = pdf.loc[pdf['label_scaled'] == 0]
    pdf_c2 = pdf.loc[pdf['label_scaled'] == 1]
    pdf_c3 = pdf.loc[pdf['label_scaled'] == 2]
    fig_c1 = go.Figure()
    fig_c1.add_trace(go.Bar(x = pdf_c1["STATE"], y=pdf_c1["TMIN"], name="Average MIN temperature",
                         text=pdf_c1["TMIN"], textposition='outside',
                         textfont=dict(size=14)))
    fig_c1.add_trace(go.Bar(x=pdf_c1["STATE"], y=pdf_c1["TMAX"], name="Average MAX temperature",
                            text=pdf_c1["TMAX"], textposition='outside',
                            textfont=dict(size=14)))
    fig_c2 = go.Figure()
    fig_c2.add_trace(go.Bar(x = pdf_c2["STATE"], y=pdf_c2["TMIN"], name="Average MIN temperature",
                         text=pdf_c2["TMIN"], textposition='outside',
                         textfont=dict(size=14)))
    fig_c2.add_trace(go.Bar(x=pdf_c2["STATE"], y=pdf_c2["TMAX"], name="Average MAX temperature",
                            text=pdf_c2["TMAX"], textposition='outside',
                            textfont=dict(size=14)))
    fig_c3 = go.Figure()
    fig_c3.add_trace(go.Bar(x = pdf_c3["STATE"], y=pdf_c3["TMIN"], name="Average MIN temperature",
                         text=pdf_c3["TMIN"], textposition='outside',
                         textfont=dict(size=14)))
    fig_c3.add_trace(go.Bar(x=pdf_c3["STATE"], y=pdf_c3["TMAX"], name="Average MAX temperature",
                            text=pdf_c3["TMAX"], textposition='outside',
                            textfont=dict(size=14)))



    fig_c1.show()
    fig_c2.show()
    fig_c3.show()

    print("f")