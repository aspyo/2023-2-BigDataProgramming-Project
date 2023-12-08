import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
import subprocess
import os

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")

my_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
chrome_options.add_argument(f"--user-agent={my_user_agent}")

driver = webdriver.Chrome(executable_path="chromedriver-linux64/chromedriver", options=chrome_options)
driver.get("https://www.melon.com/chart/age/index.htm?chartType=YE&chartGenre=KPOP&chartDate=2021")

html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

def melon_collector(url, year, count):
	time.sleep(5)
	driver.get(url)
	html = driver.page_source
	soup = BeautifulSoup(html, 'html.parser')

	title=driver.find_elements(by=By.CLASS_NAME, value='ellipsis.rank01')
	title2=[]
	for i in title:
		title2.append(i.text)

	del title2[count:]

	singer=driver.find_elements(by=By.CLASS_NAME, value='ellipsis.rank02')

	singer2=[]
	for i in singer:
		singer2.append(i.text)
	
	del singer2[count:]

	song_info = soup.find_all('div', attrs={'class': 'ellipsis rank01'})
	
	songid = []

	for i in range(count):
		try:
			songid.append(re.sub(r'[^0-9]', '', song_info[i].find("a")["href"][43:]))
		except:
			songid.append('')
			continue

	lyrics=[]
	genres=[]

	for i in songid:
		try:
			genres_append = False
			if(not i):
				genres.append('')
				genres_append = True

			driver.get("https://www.melon.com/song/detail.htm?songId=" + i)
			html_content = driver.page_source
			soup = BeautifulSoup(html_content, 'html.parser')

			time.sleep(3)

			if(genres_append == False):
				genre_element = soup.find('dt', text='장르')
				genre = genre_element.find_next('dd').text.strip()
				genres.append(genre)

			lyric=driver.find_element(by=By.CLASS_NAME, value="lyric")
			lyrics.append(lyric.text)
		except:
			lyrics.append('')
			continue
	lyrics2 = []
	years = []

	for i in lyrics:
		lyrics2.append(i.replace("\n"," "))
		years.append(year)

	df=pd.DataFrame({"title":title2, "singer":singer2, "genre":genres, "years":years, "lyrics":lyrics2})
	
	hdfs_folder = "/user/maria_dev/melon_data/"
	csv_filename = f"melon{start}.csv"
	local_filepath = csv_filename

	df.to_csv(local_filepath, index=False, encoding='utf-8-sig')
	hdfs_filepath = hdfs_folder + csv_filename
	subprocess.run(["hadoop", "fs", "-copyFromLocal", local_filepath, hdfs_filepath])
	os.remove(local_filepath)

start = 1964
url = 'https://www.melon.com/chart/age/index.htm?chartType=YE&chartGenre=KPOP&chartDate='

while start<=2022:
	new_url = url + str(start)
	if(start<1990):
		melon_collector(new_url, start,10)
	else:
		melon_collector(new_url, start, 50)
	print(start,'complete')
	start += 1



