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
    dataframes = {}

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
                dataframes[file] = df.copy()
    print("Data imported")

    return dataframes


def sort_games_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort club_games_df by date
    It handles changing the date to datetime format as well

    Args:
        club_games_df (pd.DataFrame): Assume club_games_df have "date"

    Returns:
        pd.DataFrame: _description_
    """

    # NOTE: This is why it might fail? Chagne to index or columns if this fails
    if "date" not in df.columns:
        raise ValueError("The 'date' column does not exist in the DataFrame.")

    df["date"] = df["date"].str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    df.dropna(subset=["date"], inplace=True)
    # Sort by date
    df = df.sort_values(by=["date"])

    return df


def expected_chance_of_success(home_team_elo, away_team_elo) -> int:
    """Calculate chance of success E_home

    Args:
        home_team_elo (int): _description_
        away_team_elo (int): _description_

    Returns:
        int : Expected chance of home team winning
    """
    return 1 / (1 + 10 ** ((away_team_elo - home_team_elo) / 400))


def calculate_new_elo(home_team_elo, away_team_elo, result, K=30):
    """Calculate new elo

    Args:
        home_team_elo (_type_): _description_
        away_team_elo (_type_): _description_
        result (_type_): result (should be 1, 0, -1)
        K (int, optional): _description_. Defaults to 30.

    Returns:
        tuple (int, int): new elos of home and away team
    """
    if result not in [-1, 0, 1]:
        raise AssertionError("Invalid result value passed!")

    expected_home_team = expected_chance_of_success(home_team_elo, away_team_elo)
    expected_away_team = 1 - expected_home_team

    new_home_elo = home_team_elo + K * (result - expected_home_team)
    new_away_elo = away_team_elo + K * ((1 - result) - expected_away_team)

    return new_home_elo, new_away_elo


def calculate_clubs_elo():
    """
    Basically calculates clubs elo from scratch

    1. Read csv files (from transfermrkt dataset) to create a dictionary of DataFrames
    2. Clean games and filter them by date
    3. Calculate ELO using very basic ELO

    Returns:
        pd.DataFrame : clubs_df with updated club ELO
    """

    # Import data
    dataframes = import_data_from_csv()
    games_df = dataframes["games_df"]
    clubs_df = dataframes["clubs_df"]
    club_games_df = dataframes["club_games_df"]

    # Clean games_df so that it only contains games that have valid club id (ie. Clubs in our club_df)
    home_exists = games_df["home_club_id"].isin(clubs_df["club_id"])
    away_exists = games_df["away_club_id"].isin(clubs_df["club_id"])
    cleaned_games_df = games_df[home_exists & away_exists]

    # Merge
    club_games_merged_df = pd.merge(
        cleaned_games_df, club_games_df, on="game_id", how="inner"
    )

    # Now, we will sort by date (Oldest to Latest)
    # To calculate and update ELO by time
    # Change data type to datetime format
    club_games_merged_df = sort_games_by_date(club_games_merged_df)

    # Initialise club elos
    # NOTE: Here we are using 1000, but better init. can be done like considering team market value
    # or level of league they are playing for etc
    clubs_df["elo"] = 1000
    test_games_df = club_games_merged_df.head(1000)

    for idx, row in test_games_df.iterrows():
        home_club_id = row["home_club_id"]
        away_club_id = row["away_club_id"]

        # Get current ELOs for home and away clubs from clubs_df
        home_elo = clubs_df.loc[clubs_df["club_id"] == home_club_id, "elo"].values[0]
        away_elo = clubs_df.loc[clubs_df["club_id"] == away_club_id, "elo"].values[0]

        # Get result for the game (assuming 1 for home win, 0 for away win, 0.5 for draw)
        game_result = row["is_win"]  # Adjust this based on your game logic

        # Update ELOs based on the result
        new_home_elo, new_away_elo = calculate_new_elo(home_elo, away_elo, game_result)

        clubs_df.loc[clubs_df["club_id"] == home_club_id, "elo"] = new_home_elo
        clubs_df.loc[clubs_df["club_id"] == away_club_id, "elo"] = new_away_elo

    return clubs_df
