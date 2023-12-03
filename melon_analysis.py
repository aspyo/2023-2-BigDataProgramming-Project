from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk


if __name__ == "__main__":

	def categorize_decades(year):
		return F.floor((F.col(year).cast("int") - 1960) / 10) * 10 + 1960

	spark = SparkSession.builder.appName("MelonDF").getOrCreate()

	nltk.download('vader_lexicon')
	analyzer = SentimentIntensityAnalyzer()

	data = spark.read.load("/user/maria_dev/melon/melon_top10.csv",format="csv", sep=",",
			inferSchema="true", header="true")
	data = data.na.fill('', subset=['가사'])

	data = data.withColumn('연도_범주', categorize_decades('연도')) 

	@udf(DoubleType())
	def analyze_sentiment(text):
		return analyzer.polarity_scores(text)['compound']

	data = data.withColumn('감정_점수', analyze_sentiment('가사'))

	result = data.groupBy('연도_범주').agg({'감정_점수': 'mean'}).withColumnRenamed('avg(감정_점수)', '평균_감정_점수')

	data.show()

	result.show()

