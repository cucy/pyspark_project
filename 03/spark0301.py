from pyspark import SparkConf,SparkContext

# 创建SparkConf：设置的是Spark相关的参数信息
conf = SparkConf().setMaster("local[2]").setAppName("spark0301")

# 创建SparkContext
sc = SparkContext(conf=conf)

# 业务逻辑
data = [1,2,3,4,5]
distData = sc.parallelize(data)
print(distData.collect())

# 好的习惯
sc.stop()