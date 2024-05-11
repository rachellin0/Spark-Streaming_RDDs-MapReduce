

rdd_pattern = sc.textFile('weekly-patterns-nyc-2019-2020-sample.csv').map(lambda x: next(csv.reader([x])))
header = rdd_pattern.first()
rdd_pattern = rdd_pattern.filter(lambda row : row != header)
rdd_task1 = rdd_pattern.map(lambda x: [x[0], '-'.join(x[12].split('T')[0].split('-')[:2]), '-'.join(x[13].split('T')[0].split('-')[:2]), x[18], json.loads(x[19])])

rdd_filter = sc.textFile('core-places-nyc.csv').map(lambda x: next(csv.reader([x]))).filter(lambda x:x[9][:4] == '4451').map(lambda x:[x[9],x[0]])
filter_list = rdd_filter.map(lambda x: x[1]).collect()

rdd_filter.collect()

# !gdown -O nyc_cbg_centroids.csv 196F50FfY1kHJItadHcU_kQCdViV6puHT
# !head nyc_cbg_centroids.csv
# !pip install pyproj
# !pip install pyspark
import csv
import json
# import sys
import pyspark
from pyspark.sql import SparkSession

from pyproj import Transformer
from shapely.geometry import Point



if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)

    #1

    coreplace = sc.textFile('core-places-nyc.csv').map(lambda x: next(csv.reader([x]))).filter(lambda x:x[9][:4] == '4451').map(lambda x:[x[9],x[0]])
    coreplace = coreplace.map(lambda x: x[1]).collect()

    pattern = sc.textFile('weekly-patterns-nyc-2019-2020-sample.csv') \
        .filter(lambda x: next(csv.reader([x]))[0] in coreplace) \
        .filter(lambda x: (''.join(next(csv.reader([x]))[12].split('-')[:2])\
                          in ['201903','201910','202003','202010'])\
                or (''.join(next(csv.reader([x]))[13].split('-')[:2])\
                    in ['201903','201910','202003','202010']))

    rddA = pattern \
        .map(lambda x: next(csv.reader([x]))) \
        .map(lambda x: [x[0], '-'.join(x[12].split('-')[:2]), '-'.join(x[13].split('-')[:2]), x[18], x[19]])

    def getVisits(x):
        vis =json.loads(x[4])
        if vis==[]:
          return None
        else:
          return x[0],x[1],x[2],x[3],vis
    rddD = rddA.map(getVisits).filter(lambda x: x is not None)


    def date_list(date1,date2,cbgs):
      if date1 =='2019-03' or date2 == '2019-03':
        return [cbgs,{},{},{}]
      elif date1 =='2019-10' or date2 == '2019-10':
        return [{},cbgs,{},{}]
      elif date1 =='2020-03' or date2 == '2020-03':
        return [{},{},cbgs,{}]
      elif date1 =='2020-10' or date2 == '2020-10':
        return [{},{},{},cbgs]
      else:
        None

    def merge_by_key(a,b):
      output = [{},{},{},{}]
      for i in range(len(a)):
        output[i].update(a[i])
        output[i].update(b[i])
      return output

    task2 = rddD.map( lambda x: (x[3],date_list(x[1],x[2],x[4]))).filter(lambda x: x[1] is not None).reduceByKey(lambda x,y: merge_by_key(x,y))


    cbg = sc.textFile('nyc_cbg_centroids.csv')
    header2 = cbg.first()
    cbg = cbg.filter(lambda row : row != header2)
    cbg_filter = cbg.map(lambda x: x.split(',')[0]).collect()

    def filter_cbg(dict_in,filter_list):
            output = []
            for dict_ in dict_in:
                if dict_ == {}: output.append('')
                else:
                    dict_out = []
                    for item in dict_:
                        if item in filter_list:
                            dict_out.append((item,dict_[item]))
                    if dict_out != []:
                        output.append(dict_out)
                    else:
                        output.append('')
            return output

    task3 = task2.map(lambda x: [x[0],filter_cbg(x[1],cbg_filter)])

    #4


    rdd_cbg_list = cbg.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2]]).collect()

    def cbg_transfer(input,transfer_list):
      t = Transformer.from_crs(4326, 2263)
      if type(input) == list:
        list_out = []
        for dict_ in input:
          if dict_ == '': list_out.append('')
          else:
            dict_out = []
            for item1 in dict_:
              for item2 in transfer_list:
                if item1[0] == item2[0]:
                  dict_out.append((t.transform(item2[1],item2[2]),item1[1]))
            list_out.append(dict_out)
        return list_out
      else:
        for item in transfer_list:
          if input == item[0]:
            return t.transform(item[1],item[2])

    task4 = task3.map(lambda x: [x[0],cbg_transfer(x[0],rdd_cbg_list),cbg_transfer(x[1],rdd_cbg_list)])

    def distance(start_list,destination):
      output = []
      for item in start_list:
        if item == '':
          output.append('')
        else:
          distance_list=[]
          for start in item:
            distance_list.append((Point(start[0][0],start[0][1]).distance(Point(destination[0],destination[1]))/5280,start[1]))
          output.append(distance_list)
      return output

    task4 = task4.map(lambda x: [x[0],distance(x[2],x[1])])



    def mean_dist(input):
      output = []
      for item in input:
        if item == '':
          output.append('')
        else:
          sum_ = 0
          num_ = 0
          for cuple in item:
            sum_ += cuple[0] * cuple[1]
            num_ += cuple[1]
          if num_ !=0:
              output.append(round(sum_/num_,2))
      return output
    task5 = task4.map(lambda x: [x[0],mean_dist(x[1])])

    ectracredit2 = task5.map(lambda x: [str(x[0]),str(x[1][0]),str(x[1][1]) ,str(x[1][2]),str(x[1][3])])\
            .toDF(['cbg_fips', '2019-03' , '2019-10' , '2020-03' , '2020-10'])\
            .sort('cbg_fips', ascending = True)

    # ectracredit2.coalesce(1).write.options(header='true').csv(sys.argv[1])

ectracredit2.show()

