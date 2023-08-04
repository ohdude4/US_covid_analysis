import string

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
import plotly.express as px
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
        .load("data/covidData/SARS-CoV-2_Variant_Proportions.csv")

    df2 = df1
    df1 = df1.filter((df1['usa_or_hhsregion'] == 'USA') & (df1["published_date"] == "01/06/2023 12:00:00 AM"))
    df1 = df1.select("usa_or_hhsregion", "week_ending", "variant", "share")
    df1 = df1.withColumn("Date", F.substring(df1['week_ending'], 1, 10))
    df1 = df1.withColumn("Date", F.to_date(F.col('Date'), "MM/dd/yyyy"))

    df1 = df1.withColumn("share", df1['share'].cast(FloatType()))
    VariantList = df2.select("variant").distinct().collect()
    VariantList = [row['variant'] for row in VariantList]

    pivotDF = df1.groupby("Date").pivot("variant").sum("share")
    unpivotExpr = "stack(19, 'BA.4.6', `BA.4.6`, 'B.1.617.2', `B.1.617.2`, " \
                  "'BA.5.2.6', `BA.5.2.6`, 'BF.7', `BF.7`,  'B.1.1.529', " \
                  "`B.1.1.529`, 'BQ.1.1', `BQ.1.1`, 'BA.2', `BA.2`, 'BN.1', " \
                  "`BN.1`, 'XBB', `XBB`, 'BA.5', `BA.5`, 'Other', `Other`, " \
                  "'BA.1.1', `BA.1.1`, 'BQ.1', `BQ.1`, 'XBB.1.5', `XBB.1.5`, " \
                  "'BA.2.12.1', `BA.2.12.1`, 'BA.4', `BA.4`, 'BF.11', `BF.11`, " \
                  "'BA.2.75.2', `BA.2.75.2`, 'BA.2.75', `BA.2.75`) as (Variant, Proportions)"
    stackDF = pivotDF.select("Date", F.expr(unpivotExpr))
    stackDF = stackDF.orderBy("Date")

    stackPDF = stackDF.toPandas()
    pdf = pivotDF.toPandas()
    pdf2 = df1.toPandas()
    pdf = pdf.fillna(0)
    fig = px.line(stackPDF, x="Date", y="Proportions", color="Variant")
    fig.show()

    fig.write_html('plot_output/SARS-CoV-2_Variant_Proportions.html')

    print('a')
