import sys

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    # 没有参数，就退出
    if len(sys.argv) != 3:
        print("Usage: wordcount <input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    def printResult():
        counts = sc.textFile(sys.argv[1]) \
            .flatMap(lambda line: line.split("\t")) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b)

        output = counts.collect()

        for (word, count) in output:
            print("%s: %i" % (word, count))


    def saveFile():
        sc.textFile(sys.argv[1]) \
            .flatMap(lambda line: line.split("\t")) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .saveAsTextFile(sys.argv[2])

    saveFile()

    sc.stop()