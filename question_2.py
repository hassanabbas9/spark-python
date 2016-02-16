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
import numpy as np

### Dataset
DATA1 = '/cs/work/scratch/spark-data/data-2-sample.txt'
DATA2 = '/cs/work/scratch/spark-data/data-2.txt'

### Some variables you may want to personalize
AppName = "question_2"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.maxResultSize", "0")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 50)
        .set("spark.local.dir", TMPDIR))
sc = SparkContext(conf = conf)

### Put your algorithm here.

def multiplyRowCol(row, col):
  res = []
  for r in range(0, len(row)):
    res.append(row[r]*col[r])

  return sum(res)

def multiply(row, mat2):
  result = []
  for c in range(0, len(mat2[0])):
     result.append(multiplyRowCol(row, mat2[:,c]))
  return result

def matrixmultiplication(mat1, mat2):
    if len(mat1[0]) == len(mat2):
      final_result = []

      for row_index in range(0, len(mat1)):
        final_result.append(multiply(mat1[row_index], mat2))

      return final_result
    else:
      print "Row size does not match with column size"

def transpose(mat):
    transpose_matrix = np.zeros((len(mat[0]), len(mat)))
    for r in range(0, len(mat)):
      transpose_matrix[:,r] = mat[r]

    return transpose_matrix

def diag(mat):
  col_size = len(mat[0])
  row_size = len(mat)
  for i in range(0, len(mat)):
    for j in range(0, len(mat[0])):
      if (i != j):
        mat[i][j] = 0

  return mat

#main function
if __name__=="__main__":
    data = sc.textFile(DATA2)
    rows = data.map(lambda line: line.split())
    matrix = rows.map(lambda x: [float(f) for f in x])
    mat = np.array(matrix.collect())
    transpose_matrix = transpose(mat)
    print transpose_matrix
    product_matrix = matrixmultiplication(transpose_matrix, mat)
    final_result = matrixmultiplication(mat, product_matrix)
    print final_result
    print diag(product_matrix)
    sys.exit(0)
