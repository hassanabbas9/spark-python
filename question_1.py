#!/usr/bin/env python
# Made by Hassan Abbas


# This script gives an example for the second assignment in DSP2015.
# Note that a Spark context depends on specific platform and settings.
# Please modify this file and play with it to get familiar with Spark.
#
# Liang Wang @ CS Dept, Helsinki University, Finland
# 2015.01.19 (modified 2016.01.27)
#

import os
import sys

### Dataset
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

### Some variables you may want to personalize
AppName = "question_1"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.executor.memory", "4g")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 50)
        .set("spark.local.dir", TMPDIR))
sc = SparkContext(conf = conf)

### Put your algorithm here.

def calculate_average(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))
    avg = data.sum() / data.count()
    print "Avg. = %.8f" % avg
    return avg

def calculate_min_max(fn):
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))
    new_data = data.takeOrdered(2)
    minimum = new_data[0]
    new_data = data.takeOrdered(2, key = lambda x: -x)
    maximum = new_data[0]
    print "Minimum = %.8f" % minimum
    print "Maximum = %.8f" % maximum

def calculate_variance(fn, avg):
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))
    new_data = data.map(lambda s: (s-avg)*(s-avg))
    var = new_data.sum()/ data.count()
    print "Variance = %.8f" % var

def calculate_median(fn):
    last_count = 0
    current_count = 0
    is_even_median = None
    data = sc.textFile(fn)
    data = data.map(lambda s: float(s))

    key_values = data.map(lambda d: (int(d),1)).reduceByKey(lambda a, b: a + b)
    data_sets = key_values.sortByKey().collect()

    if(data.count() % 2 == 0):
      median_index = data.count()/2
      is_even_median = True
    else:
      median_index = (data.count()+1)/2

    for x,y in data_sets:
      last_count = current_count
      current_count += y
      if(median_index >= last_count and median_index <= current_count):
        median_data_set = x
        break;

    data_set = data.filter(lambda s: int(s) == median_data_set)
    sorted_data_set = data_set.collect()
    sorted_data_set.sort()

    if not is_even_median:
        final_median = sorted_data_set[median_index - last_count - 1]
    else:
        first_median = sorted_data_set[median_index - last_count - 1]
        data_set_count = data_set.count()
        if (data_set_count == median_index - last_count):
           second_median = data.filter(lambda d: int(d) == median_data_set + 1).min()
        else:
            second_median = sorted_data_set[median_index - last_count]
        final_median = float((first_median + second_median)/2)

    print "Median = %.9f" % final_median

if __name__=="__main__":
    avg = calculate_average(DATA1)
    calculate_min_max(DATA1)
    calculate_variance(DATA1, avg)
    calculate_median(DATA1)
    sys.exit(0)
