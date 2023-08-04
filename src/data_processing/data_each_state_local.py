
# 对原始数据的处理，由于原始数据集没有日新增病例，
# 此处使用spark计算日新增死亡，确诊数据，并保存在/data_output目录下

# Processing of raw data, as the original dataset does not have 
# daily new case data, Spark is used here to calculate the daily new deaths 
# and confirmed cases. The results will be saved in the /data_output directory.

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

if __name__ == '__main__':
    conf = SparkConf().setAppName("DataEachState").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("DataEachState") \
        .master("local[*]") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("data/covidData/Split/01/*.csv")\

    df1 = df1.withColumn("Date", F.substring(F.col("Last_Update"), 1, 10))
    df1 = df1.withColumn("Date", F.to_date(df1["Date"], "yyyy-MM-dd"))
    df1 = df1.withColumn("Date", F.date_sub(F.col("Date"), 1))

    df1 = df1.orderBy("Date")

    states = df1.select(F.collect_set("Province_State")).collect()[0][0]

    result = []
    for i in states:
        result.append(df1.select("*").where(df1['Province_State'] == i))

    WindowSpec = Window.partitionBy().orderBy("Date")

    j = 0
    for i in result:
        i = i.withColumn("prevValue_Confirmed", F.lag("Confirmed").over(WindowSpec))
        i = i.withColumn("diff_Confirmed", F.when(F.isnull(i.Confirmed - i.prevValue_Confirmed), 0)
                             .otherwise(i.Confirmed - i.prevValue_Confirmed))
        i = i.withColumn("prevValue_Deaths", F.lag("Deaths").over(WindowSpec))
        i = i.withColumn("diff_Deaths", F.when(F.isnull(i.Deaths - i.prevValue_Deaths), 0)
                         .otherwise(i.Deaths - i.prevValue_Deaths))
        i.write.format("csv").option("header", "true").mode("overwrite").save("data_output/data_each_state/" + states[j])
        j = j+1

    print("done")
