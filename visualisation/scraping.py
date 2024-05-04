from matplotlib import pyplot as plt
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO
import os
import time


class Scrape:
    def __init__(self) -> None:
        self.standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats"

    def __get_team_urls(self) -> list[str]:
        """Get team urls

        Args:
            standings_url (str): Link to certain leagues' stats

        Returns:
            list[str]: List of teams url
        """
        data = requests.get(self.standings_url)
        soup = BeautifulSoup(data.text, features="lxml")
        standings_table = soup.select("table.stats_table")[0]

        # links store href of each team
        links = standings_table.find_all("a")
        links = [l.get("href") for l in links]
        links = [l for l in links if "/squads/" in l]

        team_urls = [f"https://fbref.com{l}" for l in links]

        return team_urls

    def __get_squad_dfs(self, team_urls: list[str]) -> list[pd.DataFrame]:

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

    def store_squad_to_csv(self):
        """Store squad info into 2 separate csv files.
        squad_avg.csv : will store avg info about squad
        squad.csv : will store info about players in all teams
        """
        # Store squad_dfs into a single csv file
        team_urls = self.__get_team_urls()
        squad_dfs = self.__get_squad_dfs(team_urls)

        dfs_modified = []

        # Remove the last two rows from each DataFrame and append them to dfs_modified
        for df in squad_dfs:
            df_modified = df.iloc[:-2]  # Exclude the last two rows
            dfs_modified.append(df_modified)

        # Concatenate the modified DataFrames into a single DataFrame
        result_df = pd.concat(dfs_modified, ignore_index=True)
        # Drop 2nd column because it is now meaningless
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


class Squad:
    def __init__(self) -> None:
        # Check if csv file exists
        # NOTE: this will break if cwd changes....
        squad_path = "./squad.csv"
        squad_avg_path = "./squad_avg.csv"
        if not (os.path.isfile(squad_path) and os.path.isfile(squad_avg_path)):
            # File doesnt exist
            try:
                s = Scrape()
                s.store_squad_to_csv()
            except:
                print("Error: Scraping didn't work properly.")
            else:
                print("Scraping process executed successfully.")
        else:
            print("File exist!")

        # File exists
        self.squad_avg_df = pd.read_csv(squad_avg_path, index_col=0)

    # def get_squad_df(self):
    # print(self.squad_avg_df)

    def avg_age_graph(self):
        self.squad_avg_df["age"] = self.squad_avg_df["age"].astype(float)
        ax = self.squad_avg_df["age"].plot(kind="bar", color="lightgreen")

        # Set the title and labels
        plt.title("Average Age of Players by Team")
        plt.xlabel("Team")
        plt.ylabel("Average Age")

        # for i, val in enumerate(df['age']):
        #     ax.text(i, val, str(val), ha='center', va='bottom')
        # plt.xticks(rotation=0)
        # Show the plot
        plt.show()
        
    def avg_age_info(self):
        
        


def main():
    s = Squad()
    # s.get_squad_df()
    s.age()


if __name__ == "__main__":
    main()
