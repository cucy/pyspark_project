import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage spark0902.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="spark0902")

    # 时间间隔为5秒
    ssc = StreamingContext(sc, 5)

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line:line.split(" "))\
        .map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()