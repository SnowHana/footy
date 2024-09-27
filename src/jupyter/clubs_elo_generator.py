import os
from pathlib import Path
import pandas as pd
import datetime
from datetime import datetime


def import_data_from_csv() -> list[pd.DataFrame]:
    """Read data from csv files (prepared by transfermrkt dataset)

    Returns:
        list[pd.DataFrame] : list of dataframes (single csv to single dataframe)
    """

    # NOTE: This wont work for jupyter so we are using sth else for now
    # # Get the absolute path of the current script (jupyter dir)
    BASE_DIR = Path(__file__).resolve().parent

    # # Build the path to the data directory
    DATA_DIR = BASE_DIR / "data"
    # BASE_DIR = Path.cwd()

    # Build the path to the data directory

    # # Example of using the path to the data directory
    # csv_file = DATA_DIR / 'your_file.csv'
    # print(csv_file)

    # import all files in Data folder and read into dataframes
    dataframes = []

    # Below is just to suppress warnings...
    competitions_df = pd.DataFrame
    appearances_df = pd.DataFrame
    player_valuations_df = pd.DataFrame
    game_events_df = pd.DataFrame
    players_df = pd.DataFrame
    games_df = pd.DataFrame
    club_games_df = pd.DataFrame
    clubs_df = pd.DataFrame

    # Actual reading csv flies
    for dirpath, dirname, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            file = filename.split(".")
            file = file[0] + "_df"
            if file != "_df":
                filepath = os.path.join(dirpath, filename)
                df = pd.read_csv(filepath, sep=",", encoding="UTF-8")
                exec(f"{file} = df.copy()")
                print(file, df.shape)
                dataframes.append(df)
    print("Data imported")

    return dataframes


def calculate_clubs_elo():
    dataframes = import_data_from_csv()

    home_exists = games_df["home_club_id"].isin(clubs_df["club_id"])
    away_exists = games_df["away_club_id"].isin(clubs_df["club_id"])
    filtered_games_df = games_df[home_exists & away_exists]
    filtered_games_df.shape
