# Databricks notebook source exported at Thu, 23 Apr 2015 18:33:23 UTC
# MAGIC %md
# MAGIC ## **ASD metric**
# MAGIC Computes the average session duration and exports it to the dashboard

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC _work in progress_

# COMMAND ----------

MOUNT_NAME = "root-001"
PMX_DATE='20150*'

pvRDD = sc.textFile("/mnt/%s/data/pv/10/%s" % (MOUNT_NAME,PMX_DATE))
dbutils.fs.ls("/mnt/%s/data/pv/10/" % MOUNT_NAME)[-5:]

# COMMAND ----------

for row in pvRDD.take(5):
  print '%s, '%row.split('\t')


# COMMAND ----------

def k_gen(s):
  sp=s.split('\t')
  return sp[0],(sp[2],sp[3],sp[4], sp[1].split()[0])  

rdd=pvRDD.map(k_gen).cache() #.takeSample(True, 1000)

# COMMAND ----------

rdd.take(2)

# COMMAND ----------

daily_rdd=rdd.map(lambda x: (x[1][3], (x[0],x[1][1],x[1][2]))).groupByKey()
daily_rdd.take(1)

# COMMAND ----------

def count(x):
  return x[0],len(x[1])

def pprint(x):
  return x

r = daily_rdd.map(count).collect()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *

# The schema is encoded in a string.
schemaString = "date num_sessions"

# TODO: DateType 
fields = [StructField('date', StringType(), True),StructField('num_sessions', LongType(), True)]
schema = StructType(fields)


# COMMAND ----------

nonSchemaRDD = daily_rdd.map(count).sortByKey()
rowRDD = nonSchemaRDD.map(lambda t: Row(key=t[0], value=t[1])) 


# COMMAND ----------


#schemaRDD = sqlContext.inferSchema(rowRDD)
# Apply the schema to the RDD.
schemaRDD = sqlContext.createDataFrame(rowRDD, schema)


# COMMAND ----------

# MAGIC %sql drop table if exists sessionsperday ;

# COMMAND ----------

schemaRDD.saveAsTable('sessionsperday')

# COMMAND ----------

display(sqlContext.sql("Select * from sessionsperday"))


# COMMAND ----------

from  collections import Counter
user_count=Counter(rdd.countByKey())
print '%d unique users'%len(user_count)

# COMMAND ----------

#def myreducer(x,y):
#  return x[1][0]+y[1][0],x[1][1]+y[1][1]

rdd2=rdd.map(lambda v: (int(v[1][0]),int(v[1][1]),int(v[1][2]))).cache()
print rdd2.take(10)

 

# COMMAND ----------

def printASD(the_rdd) :
  _rdd=the_rdd.map(lambda v: (int(v[1][0]),int(v[1][1]),int(v[1][2])))
  avgsesdur,avgbytes,avgpv = _rdd.reduce(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2]))#
  avgsesdur /= _rdd.count()
  avgbytes  /= _rdd.count()
  avgpv     /= _rdd.count()

  print 'avgsesdur:',avgsesdur,'avgbytes:',avgbytes,'avgpv:',avgpv, '%.02f'%(avgsesdur/60.0), _rdd.count()

# COMMAND ----------

printASD(rdd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter on a certain segment of users
# MAGIC A segment is a file with a list of IDs

# COMMAND ----------

segment=set(sorted(sc.textFile("/mnt/%s/data/%s" % (MOUNT_NAME,'msisdn.seg.10000')).collect()))
len(segment)

# COMMAND ----------

list(segment)[:5]

# COMMAND ----------

s_rdd = rdd.filter(lambda x: x[0] in segment).cache()

# COMMAND ----------

s_rdd.count()

# COMMAND ----------

s_rdd.take(10)

# COMMAND ----------

printASD(s_rdd)
#1625 115158 43 27.08 16032639

# COMMAND ----------



# COMMAND ----------

print '%.2f%% of the segmented users were found in the rdd'%(len(s_rdd.countByKey().items())*100.0/len(segment))
