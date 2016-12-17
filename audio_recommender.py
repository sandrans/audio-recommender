# AudioRecommender

import pyspark
from pyspark import SparkContext, SparkConf
import re
import sys
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.mllib.recommendation import *
# from pyspark.sql.types import IntegerType

import numpy as np
from numpy.random import rand
from numpy import matrix
# from pyspark.sql import SparkSession


# def parseLine(data):
#     """Parses a urls pair string into urls pair."""
#     parts = re.split(r'\s+', data)
#     return parts[0], parts[1]


######### HELPER FUNCTIONS #########
def makeArtistRow(p):
  if len(p) != 2:
    return None
    # print("skipping")
  else:
    return Row(id=int(p[0]), name=str(p[1]).strip())

def makeAliasRow(p):
  if len(p) != 2:
    return None
  else:
    if p[0] == '' or p[1] == '':
      return None
    return Row(artist=int(p[0]), alias=int(p[1]))

def artist_strip(line):
    # filters out lines that are bad
    words = line.split('\t')
    words = [word.strip() for word in words]
    if words[0] == '' or len(words) < 2:
        return None
    else:
        try:
            return (int(words[0]), words[1])
        except ValueError:
            return None

def alias_strip(line):
    # filters out aliases that are bad
    tokens = line.split('\t')
    tokens = [token.strip() for token in tokens]
    if tokens[0] == '' or len(tokens) < 2:
        return None
    else:
        try:
            return (int(tokens[0]), int(tokens[1]))
        except ValueError:
            return None

def p(x):
  print(x)

def toInt(x):
  return int(x)
####################################

class RunRecommender:
  def __init__(self, context):
    self.sc = sc
    # self.sqlContext = sqlContext
    self.context = context
    self.user_artist_df = None
    self.artist_by_id = None
    self.artist_alias = None
    self.model = None

  def preparation(self, rawUserArtistData, rawArtistData, rawArtistAlias):
    print("preparation...")
    self.artist_by_id = self._build_artist_by_id(rawArtistData)
    self.artist_alias = self._build_artist_alias(rawArtistAlias)

    self.user_artist_df = rawUserArtistData.rdd\
        .map(lambda row: [int(x) for x in row['value'].split()[:2]])\
        .toDF(['user', 'artist'])

    # user and artist id aggregates
    cols = ['user', 'artist']
    funcs = [F.min, F.max]
    exprs = [func(F.col(col)) for func in funcs for col in cols]

    print ("Aggregating...")
    # aggregation takes time
    self.user_artist_df.agg(*exprs).show()

    # parts = rawUserArtistData.map(lambda l: l.split())
    # userArtists = parts.map(lambda p: Row(user=int(p[0]), artist=int(p[1])))
    # userArtistDF = sqlContext.createDataFrame(userArtists)
    # # userArtistDF.show()

    # # NOTE: THE AGG LINE WORKS, BUT TAKES FOREVER (5 min?) TO RUN
    # # userArtistDF.agg(F.min(userArtistDF.user), F.max(userArtistDF.user)).show()

    # artistByID = self._buildArtistByID(rawArtistData)
    # artistByID.show()

    # artistAlias = self._buildArtistAlias(rawArtistAlias)
    # artistAlias.show()

    # Skipping for now:
    # (badID, goodID) = artistAlias.head()
    # artistByID.filter(F.col("id").isin(artistAlias.head())).show()
    # bad = artistByID.where(F.col("id").isin({"artist", "alias"}))


  def model(self, rawUserArtistData, rawArtistData, rawArtistAlias):
    print("modeling ...")
    # bArtistAlias = sc.broadcast(self.buildArtistAlias(rawArtistAlias))
    # bArtistAlias = self.buildArtistAlias(rawArtistAlias)
    print("building counts ...")
    trainData = self._buildCounts(rawUserArtistData, bArtistAlias).cache()
    print("training data...")
    self.model = ALS.trainImplicit(trainData.rdd, 10, iterations=5,
            lambda_=0.01, alpha=1.0)

    print (self.model.userFeatures().mapValues(lambda row: row).first())

    # artist id listened by user_id
    user_id = 2093760
    artist_ids = train_data\
    .filter(train_data.user == user_id)\
    .select(train_data.artist.cast(IntegerType()))\
    .distinct().collect()

    artist_ids.show()
    # [Row(artist=1255340), Row(artist=942), Row(artist=378), Row(artist=1180), Row(artist=813)]

    #convert to python list object
    artist_ids = [row.artist for row in artist_ids]

    # display artists listened by the user_id
    print ("This was listened by: ", user_id)
    __artist_by_id = self.artist_by_id
    __artist_by_id.filter(pysqlf.col('id').isin(artist_ids)).show()

    train_data.rdd.unpersist()

  def evaluate(self, rawUserArtistData, rawArtistAlias):
    print("evaluate...")

  def recommend(self, rawUserArtistData, rawArtistData, rawArtistAlias):
    print("recommend...")

  def _buildArtistByID(self, rawArtistData):
    print("buildArtistByID...")

    artistByIdDF = rawArtistData.rdd\
    .map(lambda row: artist_strip(row['value']))\
    .filter(lambda x: x is not None).toDF(('id', 'name'))

    # parts = rawArtistData.map(lambda l: l.split('\t'))
    # # parts.foreach(p)
    # artistsIDs = parts.map(lambda p: makeArtistRow(p))
    # artistByIdDF = sqlContext.createDataFrame(artistsIDs)

    return artistByIdDF


  def _buildArtistAlias(self, rawArtistAlias):
    print("buildArtistAlias...")

    # parts = rawArtistAlias.map(lambda l: l.split('\t'))
    # # parts.foreach(p)
    # artistAlias = parts.map(lambda p: makeAliasRow(p))
    # artistAliasDF = sqlContext.createDataFrame(artistAlias)

    artistAliasDF = rawArtistAlias.rdd\
    .map(lambda row: alias_strip(row['value']))\
    .filter(lambda x: x is not None).collectAsMap()

    return artistAliasDF

  def _buildCounts(self, rawUserArtistData, bArtistAlias):
    # rawUserArtistData.map { line =>
    # val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    #   val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
    #   (userID, finalArtistID, count)
    # }.toDF("user", "artist", "count")

    # lines = rawUserArtistData.map(lambda l: l.split())
    # lines.foreach(toInt)
    # lines.foreach(p)
    # # finalArtists = bArtistAlias.value
    # countsDF = sqlContext.createDataFrame(finalArtists)

    # parts = rawUserArtistData.map(lambda l: l.split())
    # userArtists = parts.map(lambda p: Row(user=int(p[0]), artist=int(p[1]), count=int(p[2])))
    # userArtistDF = sqlContext.createDataFrame(userArtists)

    # userArtistDF.show()
    # bArtistAlias.show()
    # join = userArtistDF.join(bArtistAlias, userArtistDF['artist'] == bArtistAlias['artist'], 'inner')
    # count = join.count()
    # print(count)

    join = rawUserArtistData.rdd\
        .map(lambda row: count_final_play(row['value'], bArtistAlias))\
        .toDF(('user', 'artist', 'count'))

    return join

def count_final_plays(line, bad_artist_alias):
    info = map(int, line.split()) # info[0] - user_id, info[1] - artist_id, info[2] - play count
    final_artist_id = bad_artist_alias.value.get(info[1], info[1])
    return (info[0], final_artist_id, info[2])

if __name__ == '__main__':

  sc = SparkContext("local", "Audio Recommender App")

  # replace base eventually with s3 bucket url
  # base = ("s3://aws-logs-858798505425-us-east-1/audio_data/")
  base = "../profiledata_06-May-2005/"

  rawUserArtistData = sc.textFile(base+"user_artist_data.txt").cache()
  rawArtistData = sc.textFile(base+"artist_data.txt").cache()
  rawArtistAlias = sc.textFile(base+"artist_alias.txt").cache()

  context = SQLContext(sparkContext=sc)
  # Make a new object of the RunRecommender class
  runRecommender = RunRecommender(context)

  runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
  runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
  runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
  runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)



