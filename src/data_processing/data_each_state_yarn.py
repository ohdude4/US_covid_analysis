from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

if __name__ == '__main__':
    conf = SparkConf().setAppName("DataEachState").setMaster("yarn")
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession.builder \
        .appName("DataEachState") \
        .master("yarn") \
        .getOrCreate()

    df1 = sparkSession.read.format("csv") \
        .option("header", True) \
        .load("hdfs://172.29.203.82:8020/DailyUS")\

    df1 = df1.withColumn("Date", F.substring(F.col("Last_Update"), 1, 10))
    df1 = df1.withColumn("Date", F.to_date(df1["Date"], "yyyy-MM-dd"))
    df1 = df1.withColumn("Date", F.date_sub(F.col("Date"), 1))

    # df1 = df1.withColumn("Date", F.coalesce(df1["Date"], F.substring("Last_Update", 1, 10)))
    df1 = df1.orderBy("Date")

    states = df1.select(F.collect_set("Province_State")).collect()[0][0]

    result = []
    for i in states:
        # locals()['df_' + i] = df1.select("*").where(df1['Province_State'] == i)
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
        # i.repartition(1).write.csv("hdfs://172.29.203.82:8020/DataEachState" + states[j], encoding="utf-8", header=True)
        i.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://172.29.203.82:8020/DataEachState/" + states[j])
        j = j+1

    df2 = df1.select("*").where(df1['Province_State'] == "Alabama")

    print(df1)
