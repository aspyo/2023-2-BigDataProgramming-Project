import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc

# 한글 폰트 설정
font_name = "HYPost"
font_path = font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
if font_name in font_path:
    rc('font', family=font_name)
else:
    print(f"{font_name} 폰트가 설치되어 있지 않습니다. 다른 폰트로 설정해주세요.")

# 데이터 불러오기
data = pd.read_csv(r"C:\result\result.csv")  # 파일 경로를 정확하게 수정하세요.

# 데이터를 장르별로 그룹화하고 연도에 따른 평균 count 값을 계산
genre_year_avg = data.groupby(['장르', 'Year'])['count'].mean().unstack().T

# 그래프 그리기
plt.figure(figsize=(12, 8))

# 각 장르를 다른 색상으로 플롯
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'orange', 'purple', 'brown']
for i, genre in enumerate(genre_year_avg.columns):
    plt.plot(genre_year_avg.index, genre_year_avg[genre], marker='o', linestyle='-', color=colors[i], label=genre)

plt.title('장르별 연도별 평균 Count')
plt.xlabel('연도')
plt.ylabel('평균 Count')
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.grid(True)

plt.show()
