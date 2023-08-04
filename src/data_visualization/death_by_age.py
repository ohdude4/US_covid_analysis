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
        .load("data/covidData/Provisional_COVID-19_Deaths_by_Sex_and_Age.csv")\

    df_all = df1.filter((df1.State == "United States") & (df1.Sex == "All Sexes")
                      & (df1["Group"] == "By Total") & (df1["Age Group"] != "All Ages")
                        & (df1["Age Group"] != "Under 1 year") & (df1["Age Group"] != "0-17 years")
                        & (df1["Age Group"] != "18-29 years") & (df1["Age Group"] != "30-39 years")
                        & (df1["Age Group"] != "40-49 years") & (df1["Age Group"] != "50-64 years"))

    df_all = df_all.select("Age Group", "COVID-19 Deaths", "Total Deaths")
    df_all = df_all.withColumn("COVID-19 Deaths", regexp_replace(df_all["COVID-19 Deaths"], ",", ""))
    df_all = df_all.withColumn("Total Deaths", regexp_replace(df_all["Total Deaths"], ",", ""))
    df_all = df_all.withColumn("COVID-19 Deaths", df_all["COVID-19 Deaths"].cast(FloatType()))
    df_all = df_all.withColumn("Total Deaths", df_all["Total Deaths"].cast(FloatType()))

    df_all = df_all.withColumn("Percentage", F.round((df_all["COVID-19 Deaths"] / df_all["Total Deaths"] * 100), 2))
    df_all = df_all.withColumn("Percentage", F.concat(df_all["Percentage"], F.lit('%')))
    pdf_all = df_all.toPandas()


    fig = go.Figure()
    fig.add_trace(go.Bar(x = pdf_all["Age Group"], y = pdf_all["COVID-19 Deaths"], name = "COVID-19 Deaths",
                         text = pdf_all["COVID-19 Deaths"], textposition = 'outside',
                         textfont = dict(size = 14)))
    fig.add_trace(go.Bar(x = pdf_all["Age Group"], y=pdf_all["Total Deaths"], name = "Total Deaths",
                         text = pdf_all["Total Deaths"],
                         textposition='outside',
                         textfont=dict(size=14)))
    fig.update_layout(uniformtext_minsize=18, uniformtext_mode='hide')

    fig.update_traces(selector = dict(name = 'Total Deaths',), text = pdf_all["Percentage"],
                      textposition = 'outside')

    bar1 = go.Bar(x=pdf_all["Age Group"], y=pdf_all["COVID-19 Deaths"], name="COVID-19 Deaths",
           text=pdf_all["COVID-19 Deaths"], textposition='outside',
           textfont=dict(size=14))
    bar2 = go.Bar(x = pdf_all["Age Group"], y=pdf_all["Total Deaths"], name = "Total Deaths",
                         text = pdf_all["Total Deaths"],
                         textposition='outside',
                         textfont=dict(size=14))

    new_text_trace = go.Scatter(
        x = pdf_all['Age Group'],
        y = pdf_all['Total Deaths'],
        text = pdf_all['Percentage'],
        textposition = 'top center',
        mode = 'text+markers',
        textfont = dict(size=15),
        name = "Percentages"
    )

    fig2 = go.Figure(data = [bar1, bar2, new_text_trace])
    fig2.update_layout(uniformtext_minsize=18, uniformtext_mode='hide')
    fig2.show()

    fig.show()
