# AudioRecommender

import pyspark
from pyspark import SparkContext
import re
import sys
from pyspark.sql import *
from pyspark.sql import functions as F


# def parseLine(data):
#     """Parses a urls pair string into urls pair."""
#     parts = re.split(r'\s+', data)
#     return parts[0], parts[1]

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

def p(x):
	print(x)


class RunRecommender:
	def __init__(self, sc, sqlContext):
		self.sc = sc
		self.sqlContext = sqlContext

	def preparation(self, rawUserArtistData, rawArtistData, rawArtistAlias):
		print("preparation")
		# userArtistDF = rawUserArtistData.map(lambda line: [line.split()[0], line.split()[1]]).toDF("user","artist")
		
		# rdd = sc.parallelize(rawUserArtistData)
		# hasattr(rdd, "toDF")

		# sqlContext = SQLContext(sc)
		# hasattr(rdd, "toDF")

		parts = rawUserArtistData.map(lambda l: l.split())
		userArtists = parts.map(lambda p: Row(user=int(p[0]), artist=int(p[1])))
		userArtistDF = sqlContext.createDataFrame(userArtists)
		userArtistDF.show()

		# NOTE: THE AGG LINE WORKS, BUT TAKES FOREVER (5 min?) TO RUN
		# userArtistDF.agg(F.min(userArtistDF.user), F.max(userArtistDF.user)).show()

		artistByID = self.buildArtistByID(rawArtistData)
		artistByID.show()

		artistAlias = self.buildArtistAlias(rawArtistAlias)
		artistAlias.show()

		

		# print(userArtistDF)

	def model(self, rawUserArtistData, rawArtistData, rawArtistAlias):
		print("model")

	def evaluate(self, rawUserArtistData, rawArtistAlias):
		print("evaluate")

	def recommend(self, rawUserArtistData, rawArtistData, rawArtistAlias):
		print("recommend")

	def buildArtistByID(self, rawArtistData):
		print("buildArtistByID")
		parts = rawArtistData.map(lambda l: l.split('\t'))
		# parts.foreach(p)
		artistsIDs = parts.map(lambda p: makeArtistRow(p))
		artistByIdDF = sqlContext.createDataFrame(artistsIDs)

		return artistByIdDF


	def buildArtistAlias(self, rawArtistAlias):
		print("buildArtistAlias")
		parts = rawArtistAlias.map(lambda l: l.split('\t'))
		# parts.foreach(p)
		artistAlias = parts.map(lambda p: makeAliasRow(p))
		artistAliasDF = sqlContext.createDataFrame(artistAlias)

		return artistAliasDF




if __name__ == '__main__':

	sc = SparkContext("local", "Audio Recommender App")
	base = "../profiledata_06-May-2005/"
	
	rawUserArtistData = sc.textFile(base+"user_artist_data.txt").cache()
	rawArtistData = sc.textFile(base+"artist_data.txt").cache()
	rawArtistAlias = sc.textFile(base+"artist_alias.txt").cache()

	sqlContext = SQLContext(sc)
	# Make a new object of the RunRecommender class
	runRecommender = RunRecommender(sc, sqlContext)

	runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
	runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)



