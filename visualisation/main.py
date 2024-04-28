import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO

standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats"
data = requests.get(standings_url)

soup = BeautifulSoup(data.text)
standings_table = soup.select("table.stats_table")[0]
links = standings_table.find_all("a")
links = [l.get("href") for l in links]
links = [l for l in links if "/squads/" in l]

team_urls = [f"https://fbref.com{l}" for l in links]

squad_dfs = []

for team_url in team_urls:
    data = requests.get(team_url)
    squads = pd.read_html(StringIO(data.text), match="Standard Stats")[0]
    squads = squads.droplevel(level=0, axis=1)
    squad_dfs.append(squads)

    time.sleep(1)
