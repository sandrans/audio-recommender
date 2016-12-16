# AudioRecommender

import pyspark
from pyspark import SparkContext




if __name__ == '__main__':

	sc = SparkContext("local", "Audio Recommender App")
	base = "../profiledata_06-May-2005/"
	
	rawUserArtistData = sc.textFile(base+"user_artist_data.txt").cache()
	rawArtistData = sc.textFile(base+"artist_data.txt").cache()
	rawArtistAlias = sc.textFile(base+"artist_alias.txt").cache()

	# Make a new object of the RunRecommender class
	runRecommender = RunRecommender(sc)
	
	runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
	runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
	runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)




class RunRecommender:
	def __init__(self, sc):
		self.sc = sc

	def preparation(rawUserArtistData, rawArtistData, rawArtistAlias):
		print("preparation")

	def model(rawUserArtistData, rawArtistData, rawArtistAlias):
		print("model")

	def evaluate(rawUserArtistData, rawArtistAlias):
		print("evaluate")

	def recommend(rawUserArtistData, rawArtistData, rawArtistAlias):
		print("recommend")

