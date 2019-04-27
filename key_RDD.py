from pyspark import SparkConf, SparkContext
import os
import sys
if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('key_RDD')
    sc = SparkContext()
    

