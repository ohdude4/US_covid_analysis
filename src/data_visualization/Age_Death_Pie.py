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
        .load("data/covidData/Provisional_COVID-19_Deaths_by_Sex_and_Age.csv")

    df2 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/US_Population_Demographic/nhgis0001_ds253_2021_state.csv")

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

    df_pop = df2.withColumn("Total_Under_5_years", df2['ANK8E003'] + df2['ANK8E027'])
    df_pop = df_pop.withColumn("Total_5_to_14_years", df2['ANK8E004'] + df2['ANK8E005'] + df2['ANK8E028'] + df2['ANK8E029'])
    df_pop = df_pop.withColumn("Total_15_to_24_years",
                            df2['ANK8E006'] + df2['ANK8E007'] + df2['ANK8E008'] + df2['ANK8E009'] + df2['ANK8E010']
                            + df2['ANK8E030'] + df2['ANK8E031'] + df2['ANK8E032'] + df2['ANK8E033'] + df2['ANK8E034'])
    df_pop = df_pop.withColumn("Total_25_to_34_years",
                               df2['ANK8E011'] + df2['ANK8E012'] + df2['ANK8E035'] + df2['ANK8E036'])
    df_pop = df_pop.withColumn("Total_35_to_44_years",
                               df2['ANK8E013'] + df2['ANK8E014'] + df2['ANK8E037'] + df2['ANK8E038'])
    df_pop = df_pop.withColumn("Total_45_to_54_years",
                               df2['ANK8E015'] + df2['ANK8E016'] + df2['ANK8E039'] + df2['ANK8E040'])
    df_pop = df_pop.withColumn("Total_55_to_64_years",
                               df2['ANK8E017'] + df2['ANK8E018'] + df2['ANK8E041'] + df2['ANK8E042'])
    df_pop = df_pop.withColumn("Total_55_to_64_years",
                               df2['ANK8E017'] + df2['ANK8E018'] + df2['ANK8E019'] + df2['ANK8E041']
                               + df2['ANK8E042'] + df2['ANK8E043'])
    df_pop = df_pop.withColumn("Total_65_to_74_years",
                               df2['ANK8E020'] + df2['ANK8E021'] + df2['ANK8E022'] + df2['ANK8E044']
                               + df2['ANK8E045'] + df2['ANK8E046'])
    df_pop = df_pop.withColumn("Total_75_to_84_years",
                               df2['ANK8E023'] + df2['ANK8E024'] + df2['ANK8E047'] + df2['ANK8E048'])
    df_pop = df_pop.withColumn("Total_85_years_and_over",
                               df2['ANK8E025'] + df2['ANK8E049'])

    df_pop = df_pop.select('STATE', 'Total_Under_5_years', 'Total_5_to_14_years', 'Total_15_to_24_years',
                           'Total_25_to_34_years', 'Total_35_to_44_years', 'Total_45_to_54_years',
                           'Total_55_to_64_years', 'Total_65_to_74_years', 'Total_75_to_84_years',
                           'Total_85_years_and_over')

    df_sum = df_pop.agg(F.sum('Total_Under_5_years').alias('Total_Under_5_years'),
                        F.sum('Total_5_to_14_years').alias('Total_5_to_14_years'),
                        F.sum('Total_15_to_24_years').alias('Total_15_to_24_years'),
                        F.sum('Total_25_to_34_years').alias('Total_25_to_34_years'),
                        F.sum('Total_35_to_44_years').alias('Total_35_to_44_years'),
                        F.sum('Total_45_to_54_years').alias('Total_45_to_54_years'),
                        F.sum('Total_55_to_64_years').alias('Total_55_to_64_years'),
                        F.sum('Total_65_to_74_years').alias('Total_65_to_74_years'),
                        F.sum('Total_75_to_84_years').alias('Total_75_to_84_years'),
                        F.sum('Total_85_years_and_over').alias('Total_85_years_and_over'))
    df_sum = df_sum.withColumn('Country', F.lit('United State'))

    pdf_pop = df_sum.toPandas()
    pdf_all = df_all.toPandas()

    pdf_unpivot = pd.melt(pdf_pop, id_vars = ['Country'], value_vars = ['Total_Under_5_years', 'Total_5_to_14_years',
                                                'Total_15_to_24_years', 'Total_25_to_34_years',
                                                'Total_35_to_44_years', 'Total_45_to_54_years',
                                                'Total_55_to_64_years', 'Total_65_to_74_years',
                                                'Total_75_to_84_years', 'Total_85_years_and_over'],
                           var_name = 'Age_Groups', value_name = 'Population')

    fig1 = px.pie(pdf_all, values = 'COVID-19 Deaths', names = 'Age Group',
                  title = '各年龄段死亡占比',
                  labels = {'Age Group': '年龄段', 'COVID-19 Deaths': '死亡人数'})
    fig2 = px.pie(pdf_unpivot, values = 'Population', names='Age_Groups',
                  title='各年龄段总人口占比',
                  labels = {'Population': '人口总数', 'Age_Groups': '年龄段'})

    fig = make_subplots(rows = 1, cols = 2, specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                        subplot_titles = ("各年龄段死亡占比", "各年龄段总人口占比"))
    fig.add_trace(fig1.data[0], row=1, col=1)
    fig.add_trace(fig2.data[0], row=1, col=2)

    fig.write_html("plot_output/COVID19_age_death_pie.html")
    fig.show()


    print("s")