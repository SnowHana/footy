import pandas as pd
from scipy.stats import zscore
from utils import *

BASE_ELO = 1500
ELO_RANGE = 300


# Create new df

# Create season column for players df...for each season he ran?
def init_players_elo_df(players_df: pd.DataFrame, player_valuations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Init players elo df based on a players_df
    @param players_df:
    @return:
    """
    players_elo_df = players_df.copy()
    players_elo_df['elo'] = None
    # Find seasons player played for each season
    df_sorted = add_season_column(player_valuations_df)
    df_sorted = df_sorted.loc[df_sorted.groupby(['player_id', 'season'])['date'].idxmin()]
    # seasons = sorted(oldest_player_valuations_df['season'].unique())
    players_elo_df = players_elo_df.merge(
        df_sorted[['player_id', 'season']],
        on='player_id',
        how='left'
    )

    return players_elo_df


def is_enough_data_to_init_elo(appearances_df: pd.DataFrame, games_df: pd.DataFrame, players_elo_df: pd.DataFrame,
                               player_id, game_id):
    """
    Decide if we should use
    1. Squad's avg elo
    2. Player's market value of that time
    to init. player's elo
    Assume. Player's elo should be initialised.
    @param games_df:
    @param player_id:
    @param game_id:
    @param season:
    @return:
    """
    teammates = appearances_df.loc[(appearances_df['game_id'] == game_id) & (appearances_df['player_id'] != player_id)]
    # This might go wrong?
    season = games_df.loc[games_df['game_id'] == game_id, 'season'].iloc[0]
    if teammates.empty:
        # No teammate at all, so probs just use mrkt value
        return None
    else:
        # Teammate exists. More than half's elo exists?
        res = []

        for player_id in teammates['player_id']:
            elo = players_elo_df.loc[(players_elo_df['player_id'] == player_id) & (players_elo_df['season'] == season)]
            if elo is not None:
                res.append(elo)

        if len(res) >= len(teammates) / 2:
            # More than half of teammates' elo exists, so we just avg them
            return None
        else:
            return sum(res) / len(res)


def init_player_elo_with_player_value(player_valuations_df: pd.DataFrame, players_elo_df: pd.DataFrame, player_id,
                                      season, base_elo=BASE_ELO, elo_range=ELO_RANGE):
    """
    Init. player's elo based on a player value of that time.
    Assume: His teammates didn't have much data as well...
    @param player_id:
    @param season:
    """
    player_value = player_valuations_df.loc[(player_valuations_df['player_id'] == player_id) \
                                            & (player_valuations_df['season'] == season)]

    if player_value.empty:
        # So we have no value of that player at that season..
        # TODO: Better value for very first initialisation
        return 1500
    else:
        season_df = player_valuations_df[player_valuations_df['season'] == season].copy()
        # Normalise
        # Check if we have enough data to calculate mean and std
        if len(season_df['market_value'].dropna()) > 1:
            # Calculate z-scores for market values
            season_df['market_value_z'] = zscore(season_df['market_value'].fillna(season_df['market_value'].mean()))
        else:
            season_df['market_value_z'] = 0  # Default to zero if only one player or no valid market values

        # Map z-scores to ELO
        season_df['elo'] = base_elo + (season_df['market_value_z'] * (elo_range / 2))

        # Handle cases with missing market value (assign base ELO or another strategy)
        season_df['elo'].fillna(base_elo, inplace=True)

        # Merge back to main DataFrame
        players_elo_df = players_elo_df.merge(season_df[['player_id', 'season', 'elo']], on=['player_id', 'season'],
                                              how='left')

        return players_elo_df


def init_player_elo(appearances_df: pd.DataFrame, games_df: pd.DataFrame, players_elo_df: pd.DataFrame, player_id,
                    game_id):
    """
    Init player's elo of player id, at game_id
    @param player_id:
    @param game_id:
    @return:
    """
    # 1. Get player's club (at that time)
    player_appearance = appearances_df.loc[(appearances_df['game_id'] == game_id) \
                                           & (appearances_df['player_id'] == player_id)]
    if player_appearance.empty:
        # Error: No such player
        raise ValueError("No result found for player {} in game {}".format(player_id, game_id))
    club = player_appearance['player_club_id']

    # 2. and check if we have enough elo data of that club
    elo_value = is_enough_data_to_init_elo(player_id, game_id)
    # season = games_df.loc[games_df['game_id'] == game_id]['season']
    season = games_df.loc[games_df['game_id'] == game_id, 'season'].iloc[0]
    if elo_value is None:
        # We need to manually calculate elo_value based on his market value of taht time
        # print(season)
        elo_value = init_player_elo_with_player_value(player_id, season)

    print(f"Elo value {elo_value} for player {player_id}")
    # 3. Now set player elo of that time
    players_elo_df.loc[(players_elo_df['player_id'] == player_id) \
                       & (players_elo_df['season'] == season), 'elo'] = elo_value
    # NOTE: We can also add feature like looking up player's elo of prev / next season.
    # Note, that, since we chronologically loop through games, his teammate's elo will be elo of that time!
    # However, be careful finding out which club he was playing for at THAT time.
    # return


def get_player_elo(players_df: pd.DataFrame, player_id, game_id):
    """
    Get player {player_id} elo, and if elo is not initialised, it will initialise the player's elo.
    @param player_id: player's id
    @return: player['elo'], integer
    """
    player: pd.DataFrame = players_df.loc[players_df['player_id'] == player_id]
    # If there is multiple....
    if len(player) > 1:
        raise ValueError(f"Multiple results found for player {player_id}. Expected only one")
    elif len(player) == 0:
        raise ValueError(f"No results found for player {player_id}")

    # Nothing went wrong
    elo_value = player.get('elo')
    if elo_value is not None:
        # Elo value exists, do something with it
        print(f"Elo value exists for player {player_id}!")
        return player['elo']
    else:
        # Handle the case where 'elo' column doesn't exist or is empty
        # print("Player DataFrame doesn't have an 'elo' column or values are empty")
        print(f"Empty elo_value for player {player_id}, initialising.")
        return init_player_elo(player_id, game_id)


def test_is_enough_data(appearances_df: pd.DataFrame, games_df: pd.DataFrame, players_elo_df: pd.DataFrame,
                        player_id, game_id):
    print(is_enough_data_to_init_elo(appearances_df, games_df, players_elo_df, player_id, game_id))


def main():
    """
    Main fn.
    """
    # Define all DataFrames globally as empty initially
    global competitions_df, appearances_df, player_valuations_df, game_events_df
    global players_df, games_df, club_games_df, clubs_df, players_elo_df

    # Initialize each as empty DataFrame
    competitions_df = pd.DataFrame()
    appearances_df = pd.DataFrame()
    player_valuations_df = pd.DataFrame()
    game_events_df = pd.DataFrame()
    players_df = pd.DataFrame()
    games_df = pd.DataFrame()
    club_games_df = pd.DataFrame()
    clubs_df = pd.DataFrame()
    players_elo_df = pd.DataFrame()

    # Import CSV data
    dataframes = import_data_from_csv()  # Load dataframes from CSVs

    # Map your predefined variables to their respective keys in the dictionary
    variable_mapping = {
        'competitions_df': 'competitions',
        'appearances_df': 'appearances',
        'player_valuations_df': 'player_valuations',
        'game_events_df': 'game_events',
        'players_df': 'players',
        'games_df': 'games',
        'club_games_df': 'club_games',
        'clubs_df': 'clubs',
        'players_elo_df': 'players_elo'
    }

    # Update each predefined variable with its corresponding data from the dictionary
    for var_name, file_name in variable_mapping.items():
        if file_name in dataframes:
            globals()[var_name] = dataframes[file_name].copy()

    # for file_name, dataframe in dataframes.items():
    #     exec(f"{file_name} = dataframe.copy()")
    #     # print(file_name)
    #     # print(dataframe)
    #
    # dataframes = import_data_from_csv()  # Load dataframes from CSVs
    #
    # # Map your predefined variables to their respective keys in the dictionary
    # variable_mapping = {
    #     'competitions_df': 'competitions',
    #     'appearances_df': 'appearances',
    #     'player_valuations_df': 'player_valuations',
    #     'game_events_df': 'game_events',
    #     'players_df': 'players',
    #     'games_df': 'games',
    #     'club_games_df': 'club_games',
    #     'clubs_df': 'clubs',
    #     'players_elo_df': 'players_elo'
    # }
    #
    # # Update each predefined variable with its corresponding data from the dictionary
    # for var_name, file_name in variable_mapping.items():
    #     globals()[var_name] = dataframes.get(file_name, pd.DataFrame()).copy()

    # print(games_df)
    games_df = sort_df_by_date(games_df)
    games_df = add_season_column(games_df)
    player_valuations_df = add_season_column(player_valuations_df)

    players_elo_df = init_players_elo_df(players_df, player_valuations_df)

    # Now Testing
    ronaldo = players_df.loc[players_df['name'].str.contains('Cristiano Ronaldo')]
    ronaldo_id = ronaldo['player_id'].values[0]
    ronaldo_games = appearances_df.loc[appearances_df['player_id'] == ronaldo_id]
    print(ronaldo)
    print(ronaldo_games)
    res = is_enough_data_to_init_elo(appearances_df, games_df, players_elo_df, ronaldo_id, ronaldo_games['game_id'].values[0])
    # print(res)
    if res is None:
        print("Not enough data of teammates to init ronlaod's elo based on them!")
    else:
        print("We have enough data???")
    print("DONE")
    #
    # sample_df = appearances_df.head(10)
    # for index, col in sample_df.iterrows():
    #     print(get_player_elo(col['player_id'], col['game_id']))


if __name__ == "__main__":
    main()
