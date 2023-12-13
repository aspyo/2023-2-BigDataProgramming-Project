# -*- coding: utf-8 -*-
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, rank



spark = SparkSession.builder.appName("Music Trend Analysis").getOrCreate()

hdfs_directory = "hdfs:///user/maria_dev/melon_data/"

def load_dataframe(year):
    file_path = "{}melon{}.csv".format(hdfs_directory, year)
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df.withColumn("Year", lit(year))

year_range = range(1964, 2019)
dataframes = [load_dataframe(year) for year in year_range]
all_years_df = dataframes[0]
for df in dataframes[1:]:
    all_years_df = all_years_df.union(df)

genre_popularity_df = all_years_df.groupBy("장르", "Year").count()



# 연도별로 Window를 정의하고, 각 연도 내에서 'count' 값에 따라 순위를 매김
windowSpec = Window.partitionBy("Year").orderBy(col("count").desc())

# 각 연도별로 가장 많이 출현한 장르만 선택
top_genre_per_year_df = genre_popularity_df.withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") == 1) \
    .drop("rank")

# 결과 출력 (연도별로 정렬)
top_genre_per_year_df.orderBy("Year").show(100)


# 결과 데이터를 Pandas DataFrame으로 변환
result_df = top_genre_per_year_df.orderBy("Year").toPandas()

# CSV 파일로 저장
result_df.to_csv('result.csv', index=False, encoding='utf-8-sig')