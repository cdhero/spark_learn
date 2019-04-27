import pandas as pd
import  numpy as np
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf().setMaster('local').setAppName("MyRDD")


import sys

if __name__ == '__main__':
    print('....')
    sc = SparkContext(conf=conf)
    x = (1, 2)
    print(x[0])
    # line = sc.textFile("dict.txt")
    # print(line.take(3))
    # count = line.count()
    # a = line.filter(lambda line: '阿' in line)
    # print(a.first())
    #
    # #创建RDD 并行化
    # pline = sc.parallelize(['i', 'like', 'spark'])
    # print(pline.first())
    #
    # b = line.filter(lambda line: '艾' in line)
    # print(b.first())
    # #每次行动操作，都从头计算，如果需要多次计算 ，记的要用.persist()
    # print(b.collect())
    #
    # c = a.union(b)
    # print(c.collect())
    # '''
    # 传递函数 important
    # '''
    # def contanA(s):
    #     return '阿' in s
    # word = line.filter(contanA)
    # print(word.first())
    # #map() 接收一个函数，应用于RDD的每一个元素
    # nums = sc.parallelize([1, 2, 3, 4])
    # sqnums = nums.map(lambda x: x * x).collect()
    # for sq in sqnums:
    #     print(sq)
    # # flatmap() 接收一个函数，应用于RDD的每一个元素,生成多个输出元素（迭代器）---切分单词
    #
    # wline = sc.parallelize(['ums.map', '(lambda x.x *', ' x).collect()'])
    # cline = wline.flatMap(lambda x: x.split('.')).collect()
    # print(cline)
    #wline.distinct()去重  wline.union()合并 wline.intersection()交集 wline.subtract()
    # wline.cartesian() 笛卡尔(相似)
    num = sc.parallelize(list(range(0, 10)))
    # lcount = num.reduce(lambda x, y: x + y)
    # print(lcount)
    def avg(RDD):
        return (1, RDD)
    avg = num.map(avg)
    print(avg.collect())
    aa = avg.reduce(lambda x, y:(x[0]+y[0],x[1]+y[1]))
    print(aa)

    data2 = [1,2,3,4,5,6,7,8,9]
    rdd = sc.parallelize(data2, 2)
    print(rdd.collect())
    funa = lambda x, y: (x[0] + y, x[1] + 1)
    funb = lambda x, y: (x[0] + y[0], x[1] + y[1])
    # b = rdd.aggregate(('', ''), funa, funb)
    #     # print(b)
    b = rdd.aggregate((1, 0), funa, funb)
    print(b)








