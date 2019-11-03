from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def get_grade(value):
    if value <= 50 and value >= 0:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    # 加载数据
    # option("header","true") 不需要重新加表头
    # option("inferSchema", "true") 自动读取字段类型
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/jungle/data/Beijing_2017_HourlyPM25_created20170803.csv")
    df.select("Year","Month","Day","Hour","Value","QC Name").show()

    data2017 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/jungle/data/Beijing_2017_HourlyPM25_created20170803.csv").select("Year","Month","Day","Hour","Value","QC Name")
    data2016 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/jungle/data/Beijing_2016_HourlyPM25_created20170201.csv").select("Year","Month","Day","Hour","Value","QC Name")
    data2015 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/jungle/data/Beijing_2015_HourlyPM25_created20160201.csv").select("Year","Month","Day","Hour","Value","QC Name")

    data2017.show()
    data2016.show()
    data2015.show()

    # udf 转换
    # UDF的全称为user-defined function，用户定义函数
    grade_function_udf = udf(get_grade,StringType().jsonValue())

    # 进来一个Value，出去一个Grade
    # 添加列
    group2017 = data2017.withColumn("Grade",grade_function_udf(data2017['Value'])).groupBy("Grade").count()
    group2016 = data2016.withColumn("Grade",grade_function_udf(data2016['Value'])).groupBy("Grade").count()
    group2015 = data2015.withColumn("Grade",grade_function_udf(data2015['Value'])).groupBy("Grade").count()

    group2015.select("Grade", "count", group2015['count'] / data2015.count()).show()
    group2016.select("Grade", "count", group2016['count'] / data2016.count()).show()
    group2017.select("Grade", "count", group2017['count'] / data2017.count()).show()

    group2017.show()
    group2016.show()
    group2015.show()

    df.show()
    spark.stop()