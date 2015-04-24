# Databricks notebook source exported at Thu, 23 Apr 2015 18:31:35 UTC
# MAGIC %md
# MAGIC ## **Interest Finder**
# MAGIC Look at user descriptions to find their interests

# COMMAND ----------

MOUNT_NAME = "root-001"

input_fn="/mnt/%s/data/tmp/normalized/descriptions_.tsv" % MOUNT_NAME
friends_fn='/mnt/%s/data/tmp/friends.s.tsv.gz' % MOUNT_NAME

REUSE_PERSISTED_FRIENDS_RDD=True

try:
  for f in dbutils.fs.ls(input_fn):
    print f
except:
  print 'file not found'

# COMMAND ----------

#dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read the normalized data from S3**
# MAGIC see InterestsFinderNorm

# COMMAND ----------

from nltk import metrics

def reset_matches():
  previous_matches.clear()
  
# simple == match
def exact_match(a, b, verbose=False):
  match = a==b
  
  if verbose:
    print '(%s)==(%s) = %s'%(a, b, str(match))

  return match

previous_matches={} # Keeps a list of previously evaluated words

# Simple fuzzy_match, not using stemmer, on already normalized words
def fuzzy_match(a, b, verbose=False):   
  global previous_matches
  
  if a in previous_matches:
    return previous_matches[a]
  
  match=metrics.edit_distance(a,b) <= int((len(a)+1)/4) # this tolerates 1 typo per 4 characters  
  
  if verbose:
    print '(%s)==(%s) = %s'%(a, b, str(match))

  if match:
    previous_matches[a]=match
  
  return match

# Does a fuzzy_match but just look at the first chars of the keyword a
def root_match(a, b, verbose=False):
  return fuzzy_match(a[:len(b)], b)

# COMMAND ----------

def split(x):
  return [str(w) for w in x.split("\t")]

# COMMAND ----------

#  return str(e[1]),str(e[3])
norm_rdd = sc.textFile(input_fn).map(split)#.sample(False, 0.01)
norm_rdd.take(100)

# COMMAND ----------

#keys=norm_rdd.keys()
#print keys.count()#.sample(False, 0.001).count()

# COMMAND ----------

# count the most popular words in a RDD
def find_pop_words(_rdd):
  return _rdd.flatMap(lambda x: x[1].split()).map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda (k,v):(v,k)).sortByKey(False)

popw=find_pop_words(norm_rdd).take(100)
print popw

# COMMAND ----------

print [v for k,v in popw[:100]] 

# COMMAND ----------

# returns True if any words of x matches a word of the given model
def filter_on_model(x, model, verbo=False):
  global previous_matches
  
  if model is None:
    return True
  for w in x.split():
    for i,e in enumerate(model):
        #if w[0]==e[0][0]: #Kludge to improve speed.
        if e[2](w, e[0], verbose=verbo):
          previous_matches[w]=True
          return True
  return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Apply the model**

# COMMAND ----------

# Semantic decision table that associates keywords with their meaning
# the m0, m1, m2 are methods of filtering
SDT_musica=[('musica', 'musica', root_match),
            ('rap', 'musica', fuzzy_match),
            ('banda', 'musica', fuzzy_match),
            ('bailar', 'musica', fuzzy_match),
            ('escuchar', 'musica', root_match),
            ('reggae', 'musica', root_match),
            ('rock', 'musica', exact_match),
            ('hip', 'musica', exact_match),
            ('zapata', 'musica', fuzzy_match),
            ('metal', 'musica', exact_match),
            ('cantar', 'musica', root_match),
            ('corrido','musica', fuzzy_match),
            ('cancion', 'musica', root_match),
            ('electro', 'musica', root_match),
            ('bachata', 'musica', root_match),
            ('sinaloa', 'musica', root_match), #?
            ('durang', 'musica', root_match),
            ('mazatlan', 'musica', root_match),
            ('pop', 'musica', exact_match)]
SDT_table=[('deport', 'desportes', root_match),
            ('fut', 'desportes', exact_match),
            ('basket', 'desportes', exact_match),
            ('skate', 'desportes', root_match)
            ]

#peliculas, durango

filtered_rdd=norm_rdd.filter(lambda x: filter_on_model(x[1], SDT_musica)).cache()


# COMMAND ----------

filter_on_model( '100 cruz kiero konocer amigas pasarla mui bien cantaremos', SDT_musica)

# COMMAND ----------

count=find_pop_words(filtered_rdd)

# COMMAND ----------

print [v for k,v in count.take(100)] #[100:]

# COMMAND ----------

filtered_rdd.take(5)


# COMMAND ----------

#TODO print the words that matched the model
for idx,e in enumerate(SDT_musica):
  # 1. get all words
  print 'Evaluating %s(%s)'%(e[1],e[0])
  print filtered_rdd.flatMap(lambda x: [ i for i in x[1].split() if e[2](i,e[0])]).filter(lambda f: len(f)>2).map(lambda y:(y,1)).reduceByKey(lambda a, b:a+b).map(lambda (k,v):(v,k)).sortByKey(False).collect()

# COMMAND ----------

# This section is to evaluate how a given word was matched
reset_matches()
word='durango'
oneword_rdd=filtered_rdd.map(lambda x: ' '.join([ i for i in x[1].split() if root_match(i,word)])).filter(lambda x: len(x)>0)

# COMMAND ----------

print oneword_rdd.count()
#oneword_rdd.sortByKey(False)#
' '.join(oneword_rdd.distinct().take(50))
#for line in oneword_rdd.take(30):
#  print line

# COMMAND ----------

music = find_pop_words(oneword_rdd)
#' '.join(music.distinct().take(50))



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Apply the model on the full dataset**

# COMMAND ----------

filtered_rdd=norm_rdd.filter(lambda x: filter_on_model(x[1], SDT_musica)).cache()

popw =find_pop_words(filtered_rdd)
popw.take(100)


# COMMAND ----------

filtered_rdd.keys().take(5)#.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Generate Input for Graphx**

# COMMAND ----------

friendsRDD_fn='/mnt/%s/data/tmp/friends_.tsv' % MOUNT_NAME

if REUSE_PERSISTED_FRIENDS_RDD:
  friendsRDD=sc.textFile(friendsRDD_fn).map(split).cache()
else:
  try:
    dbutils.fs.rm(friendsRDD_fn,True)
  except:
    pass
  friendsRDD_in = sc.textFile(friends_fn)
  friendsRDD = friendsRDD_in.repartition(8).map(split) #doing this split will force the repartition. Strange
  friendsRDD.take(4)
  friendsRDD.map(lambda x: '\t'.join([x[0],x[1]])).saveAsTextFile(friendsRDD_fn)

# COMMAND ----------

#vertices_0_set comprises all users with an interest
vertices_0_set = set(filtered_rdd.keys().collect())
sc.broadcast(vertices_0_set)

# find all friends relationships where either the column1 OR column2 is found in vertices_0_set
edges_level1_RDD = friendsRDD.filter(lambda x: (x[0] in vertices_0_set or x[1] in vertices_0_set)).cache()

# COMMAND ----------

print 'we start with %d users'%len(vertices_0_set)

# COMMAND ----------

vertices_1_RDD=edges_level1_RDD.keys().union(edges_level1_RDD.values()).distinct().cache()

# COMMAND ----------

print '%d users and %d friends relationships.'%(vertices_1_RDD.count(), edges_level1_RDD.count())

# COMMAND ----------

vertices_1_set = set(vertices_1_RDD.collect())
sc.broadcast(vertices_1_set)

edges_level2_RDD = friendsRDD.filter(lambda x: (x[0] in vertices_1_set or x[1] in vertices_1_set)).cache()

# COMMAND ----------

vertices_2_RDD=edges_level2_RDD.keys().union(edges_level2_RDD.values()).distinct().cache()

# COMMAND ----------

print '%d users and %d friends relationships.'%(vertices_2_RDD.count(), edges_level2_RDD.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Select connected users only**
# MAGIC Prepare a smaller graph using only the users that have an explicit interest

# COMMAND ----------

# both friends need to like music to be selected
connected_edges_level1_RDD = friendsRDD.filter(lambda x: (x[0] in vertices_0_set and x[1] in vertices_0_set)).cache()

connected_vertices_1_RDD=connected_edges_level1_RDD.keys().union(connected_edges_level1_RDD.values()).distinct().cache()
print connected_edges_level1_RDD.count(), 'relationships'

# COMMAND ----------

connected_vertices_1_set = set(connected_vertices_1_RDD.collect())
sc.broadcast(connected_vertices_1_set)

connected_edges_level2_RDD = friendsRDD.filter(lambda x: (x[0] in connected_vertices_1_set or x[1] in connected_vertices_1_set)).cache()
connected_vertices_2_RDD=connected_edges_level2_RDD.keys().union(connected_edges_level2_RDD.values()).distinct().cache()

print '%d users and %d friends relationships.'%(connected_vertices_2_RDD.count(), connected_edges_level2_RDD.count())

# COMMAND ----------

def map_interests(x):
  likes=''
  if x in vertices_0_set:
    likes='music'
  return x,likes

connected_vertices_interest_RDD=connected_vertices_2_RDD.map(map_interests).cache()
connected_vertices_interest_RDD.take(5)


# COMMAND ----------

b88a1417134a808a_friends=connected_edges_level2_RDD.filter(lambda x: x[1]=='b88a1417134a808a' or x[0]=='b88a1417134a808a').cache()
b88a1417134a808a_friends_set=b88a1417134a808a_friends.keys().union(b88a1417134a808a_friends.values()).distinct().filter(lambda x: (x[0]!='b88a1417134a808a')).collect()


# COMMAND ----------

f_liking_music=connected_vertices_interest_RDD.filter(lambda x: (x[0] in b88a1417134a808a_friends_set and 'music' in x[1])).count()
print '%d friends of b88a1417134a808a out of %d like music'%(f_liking_music, len(b88a1417134a808a_friends_set))

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Save to S3**###

# COMMAND ----------

interestsRDD_fn='/mnt/%s/data/interest/user_interests_small.tsv' % MOUNT_NAME
interests_friendsRDD_fn='/mnt/%s/data/interest/user_interests_friends_small.tsv' % MOUNT_NAME


vertices_to_save=vertices_2_RDD #connected_vertices_2_RDD
edges_to_save=edges_level2_RDD


try:
  dbutils.fs.rm(interestsRDD_fn,True)
except:
  pass

vertices_to_save.map(lambda x: '\t'.join([x[0],x[1]])).saveAsTextFile(interestsRDD_fn)
edges_to_save.map(lambda x: '\t'.join([x[0],x[1]])).saveAsTextFile(interests_friendsRDD_fn)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Graphx###
# MAGIC - compute 
# MAGIC - return a RDD with the probability that the user likes the interest
