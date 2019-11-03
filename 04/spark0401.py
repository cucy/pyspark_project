from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf = conf)

    def my_map():
        data = [1,2,3,4,5]
        # 变成RDD
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x:x*2)
        # 输出collect()
        print(rdd2.collect())


    def my_map2():
        a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", " eagle"])
        b = a.map(lambda x:(x,1))
        print(b.collect())


    def my_filter():
        data = [1,2,3,4,5]
        rdd1 = sc.parallelize(data)
        mapRdd = rdd1.map(lambda x:x*2)
        filterRdd = mapRdd.filter(lambda x:x>5)
        print(filterRdd.collect())

        print(sc.parallelize(data).map(lambda x:x*2).filter(lambda x:x>5).collect())


    def my_flatMap():
        data = ["hello spark","hello world","hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).collect())


    def my_groupBy():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
        groupByRdd = mapRdd.groupByKey()
        print(groupByRdd.collect())
        print(groupByRdd.map(lambda x:{x[0]:list(x[1])}).collect())


    def my_reduceByKey():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a,b:a+b)


    def my_sort():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
        reduceByKeyRdd.sortByKey(False).collect()

        reduceByKeyRdd.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])).collect()

    def my_union():
        a = sc.parallelize([1,2,3])
        b = sc.parallelize([3,4,5])
        print(a.union(b).collect())

    def my_distinct():
        a = sc.parallelize([1, 2, 3])
        b = sc.parallelize([3, 4, 2])
        a.union(b).distinct().collect()

    def my_join():
        a = sc.parallelize([("A", "a1"), ("C", "c1"), ("D", "d1"), ("F", "f1"), ("F", "f2")])
        b = sc.parallelize([("A", "a2"), ("C", "c2"), ("C", "c3"), ("E", "e1")])

        # a.join(b).collect
        # a.leftOuterJoin(b).collect
        # a.rightOuterJoin(b).collect
        a.fullOuterJoin(b).collect



    def my_action():
        data = [1,2,3,4,5,6,7,8,9,10]
        rdd = sc.parallelize(data)
        rdd.collect()
        rdd.reduce(lambda x,y:x+y)

        rdd.foreach(lambda x:print(x))
    my_union()
    sc.stop()