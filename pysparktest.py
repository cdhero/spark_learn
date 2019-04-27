from __future__ import print_function
import sys
from operator import add
import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "E:\spark-2.4.2-bin-hadoop2\spark-2.4.2-bin-hadoop2.7"
# Append pyspark to Python Path
sys.path.append("E:\spark-2.4.2-bin-hadoop2\spark-2.4.2-bin-hadoop2.7\python")
sys.path.append("E:\spark-2.4.2-bin-hadoop2\spark-2.4.2-bin-hadoop2.7\python\lib\py4j-0.10.7-src.zip")
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == '__main__':
    inputFile = "mg.py"
    outputFile = "result1.txt"
    sc = SparkContext()
    text_file = sc.textFile(inputFile)
    cout = text_file.count()
    contents = text_file.filter(lambda text_file: "contents" in text_file)
    cn = contents.take(3)
    print(cn)
 #   print(contents)
    print(cout)
    #counts = text_file.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
   # contents.saveAsTextFile(outputFile)