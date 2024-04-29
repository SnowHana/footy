import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import matplotlib.pyplot as plt
from io import StringIO

standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats"


def get_team_urls(standings_url: str) -> list[str]:
    """Get team urls

    Args:
        standings_url (str): Link to certain leagues' stats

    Returns:
        list[str]: List of teams url
    """
    data = requests.get(standings_url)
    soup = BeautifulSoup(data.text, features="lxml")
    standings_table = soup.select("table.stats_table")[0]

    # links store href of each team
    links = standings_table.find_all("a")
    links = [l.get("href") for l in links]
    links = [l for l in links if "/squads/" in l]

    team_urls = [f"https://fbref.com{l}" for l in links]

    return team_urls


def get_squad_dfs(team_urls: list[str]) -> list[pd.DataFrame]:

    squad_dfs = []

    for team_url in team_urls:
        data = requests.get(team_url)
        squads = pd.read_html(StringIO(data.text), match="Standard Stats")[0]
        squads = squads.droplevel(level=0, axis=1)

        # Get team name
        team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        # Add a column team_name
        squads["team"] = team_name

        # Change index to lower case
        squads.columns = [c.lower() for c in squads.columns]
        squad_dfs.append(squads)

        time.sleep(1)

    return squad_dfs


def store_squad_df():
    # Store squad_dfs into a single csv file
    team_urls = get_team_urls(standings_url)
    squad_dfs = get_squad_dfs(team_urls)

    dfs_modified = []

    # Remove the last two rows from each DataFrame and append them to dfs_modified
    for df in squad_dfs:
        df_modified = df.iloc[:-2]  # Exclude the last two rows
        dfs_modified.append(df_modified)

    # Concatenate the modified DataFrames into a single DataFrame
    result_df = pd.concat(dfs_modified, ignore_index=True)
    # Drop 2nd column because it is now meaningless
    # result_df = result_df.drop(columns=result_df.columns[0])
    result_df.to_csv("squad.csv", index=True)

    # Store avg info
    squad_avg_rows = []
    for df in squad_dfs:
        squad_avg_row = df.iloc[-2]
        squad_avg_rows.append(squad_avg_row)
    # Concatenate squad infos to a single df
    squad_avg_df = pd.concat(squad_avg_rows, axis=1).T
    # Remove columns with NaN values
    squad_avg_df = squad_avg_df.dropna(axis=1)
    squad_avg_df.set_index("team", inplace=True)
    squad_avg_df.to_csv("squad_avg.csv", index=True)


store_squad_df()
