# Databricks notebook source exported at Fri, 24 Apr 2015 15:46:23 UTC
## This notebook is only to setup the account

# COMMAND ----------

import urllib
ACCESS_KEY = "AKIAJ4PBRGXB6KOVJIDQ"
SECRET_KEY = "ih+zb+abbUqOWQaAoZeKRwqUhtKfN%2FzKJh6ZyLWC"
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY)
AWS_BUCKET_NAME = "p-root-001"#"plugger"
MOUNT_NAME = "root-001"

PMX_DATE='20150326'

print AWS_BUCKET_NAME,MOUNT_NAME, PMX_DATE

# COMMAND ----------

sc

# COMMAND ----------

dbutils.fs.mounts()

#for mi in dbutils.fs.mounts():
#  mp = str(mi.mountPoint)
#  print mp
#  dbutils.fs.unmount(mp)

# COMMAND ----------

dbutils.fs.unmount('/mnt/plg_test123')

# COMMAND ----------

dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

dbutils.fs.ls("/mnt/%s/data/pv/10/" % MOUNT_NAME)

# COMMAND ----------

dbutils.fs.refreshMounts()
#dbutils.fs.help()

# COMMAND ----------

pvRDD = sc.textFile("/mnt/%s/data/pv/10/%s" % (MOUNT_NAME,PMX_DATE))

# COMMAND ----------

def k_gen(s):
  sp=s.split('\t')
  return sp[0],tuple(sp[1:])  

rdd=pvRDD.map(k_gen)#.cache() #.takeSample(True, 1000)

# COMMAND ----------

rdd.take(1)

# COMMAND ----------

from  collections import Counter
user_count=Counter(rdd.countByKey())
print '%d unique users'%len(user_count)

# COMMAND ----------



# COMMAND ----------

uc.most_common()[:10]
