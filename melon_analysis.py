from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType
from googletrans import Translator
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

if __name__=="__main__":

	def categorize_decades(year):
		return F.floor((F.col(year).cast("int") - 1960) / 10) * 10 + 1960

	spark = SparkSession.builder.appName("MelonDF").getOrCreate()

	directory_path = "/user/maria_dev/melon_data/"

	file_names = ["melon" + str(year) + ".csv" for year in range(1964, 2023)]

	data_frames = []

	for file_name in file_names:
		file_path = directory_path + file_name
		df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
		data_frames.append(df)

	merged_data = reduce(DataFrame.union, data_frames)

	data = merged_data.dropna(subset=['가사'])

	data = data.withColumn('연도_범주', categorize_decades('연도')) 

	def translate_to_english(text):
		translator = Translator()
		translated_text = translator.translate(text, dest='en').text
		return translated_text

	translate_to_english_udf = udf(translate_to_english, StringType())

	data_translated = data.withColumn('영어_가사', translate_to_english_udf('가사'))

	nltk.download('vader_lexicon')

	analyzer = SentimentIntensityAnalyzer()

	@udf(DoubleType())
	def analyze_sentiment_vader(text):
		return analyzer.polarity_scores(text)['compound']

	data_with_sentiment_vader = data_translated.withColumn('감정_점수', analyze_sentiment_vader('영어_가사'))

	result = data_with_sentiment_vader.groupBy('연도_범주').agg({'감정_점수': 'mean'}).withColumnRenamed('avg(감정_점수)', '평균_감정_점수')

	result.show()


