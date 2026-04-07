from pyspark import SparkConf, SparkContext

conf = SparkConf() \
    .setAppName("WordCountDemo") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

sc = SparkContext(conf=conf)

print(sc)
num_cores = conf.get("spark.executor.cores")
print(f"Configured cores: {num_cores}")


"""## Load dữ liệu"""
data = sc.parallelize(list(range(100)))#, numSlices=2)

def f(x):
    #print("Processing:", x)
    return x * 2

print( data.map(f).collect() )
print(sc.uiWebUrl)

input("Press enter to terminate")

sc.stop()
