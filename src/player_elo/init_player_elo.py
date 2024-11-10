import os
from pathlib import Path
import time
import numpy as np
import pandas as pd

BASE_ELO = 1500
ELO_RANGE = 300


class PlayerEloInitializer:
    """Initialize Player ELO based on various criteria."""

    def __init__(self, base_dir: Path = None, base_elo=BASE_ELO, elo_range=ELO_RANGE):
        self.elo_range = elo_range
        self.base_elo = base_elo
        self.base_dir = base_dir or Path(__file__).resolve().parent
        self.data_dir = self.base_dir.parents[0] / 'data' / 'transfer_data'
        self.dataframes = self._import_dataframes()

        # Assign dataframes
        self.appearances_df = self.dataframes.get('appearances_df')
        self.games_df = self.dataframes.get('games_df')
        self.players_df = self.dataframes.get('players_df')
        self.player_valuations_df = self.dataframes.get('player_valuations_df')
        # TODO: Right now we init players elo df instead of reading it, because players_elo df only contains partial info
        self.players_elo_df = self.dataframes.get('players_elo_df', self._init_players_elo_df())
        # self.players_elo_df = self._init_players_elo_df()
        # Initialize season valuations
        self.season_valuations = self._init_season_valuations()

        # Initialise SQL

    def _import_dataframes(self) -> dict:
        """Read data from CSV files and store as DataFrames."""
        dataframes = {}
        for dirpath, _, filenames in os.walk(self.data_dir):
            for filename in filenames:
                file_key = f"{filename.split('.')[0]}_df"
                filepath = os.path.join(dirpath, filename)
                dataframes[file_key] = pd.read_csv(filepath, sep=",", encoding="UTF-8")
                # Convert season column to integer if they exist
                # if 'season' in dataframes[file_key].columns:
                #     dataframes[file_key]['season'] = dataframes[file_key]['season'].astype(int)
                print(f"{file_key}: {dataframes[file_key].shape}")
        print("Data imported successfully.")
        return dataframes

    def _init_players_elo_df(self) -> pd.DataFrame:
        """Initialize players' ELO DataFrame with a season column if not available."""
        df = self.players_df.copy()
        df['elo'] = None
        player_val_df_with_season = self._add_season_column(self.player_valuations_df)
        player_val_df_with_season = self._fill_season_gaps(player_val_df_with_season)

        df_sorted = player_val_df_with_season.loc[
            player_val_df_with_season.groupby(['player_id', 'season'])['date'].idxmin()
        ]
        # TODO: Add missing season.
        df = df.merge(df_sorted[['player_id', 'season']], on='player_id', how='left')
        # Now, we add missing seasons.
        # ie) There are cases where ther3 is a gap in the data (ie. 2011, 2012, 2014, 2017)

        # df = df.drop(df.columns.difference(['player_id', 'season', 'first_name', 'last_name', 'name',
        #                                     'current_club_id', 'player_code', 'country_of_birth',
        #                                     'market_value_in_eur', 'date_of_birth']), axis=1, inplace=)
        #
        # # Check available columns after the merge
        # print("Columns after merge:", df.columns.tolist())
        #
        # # Select only the specified columns
        expected_columns = [
            'player_id', 'season', 'first_name', 'last_name', 'name', 'player_code',
            'country_of_birth', 'date_of_birth', 'elo'
        ]
        #
        # # Filter by expected columns only if they exist in df
        existing_columns = [col for col in expected_columns if col in df.columns]
        #
        # # Display missing columns for debugging, if any
        missing_columns = set(expected_columns) - set(existing_columns)
        if missing_columns:
            print("Warning: Missing columns in df:", missing_columns)
        #
        return df[existing_columns]
        # return df

    @staticmethod
    def _fill_season_gaps(df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill season gaps for each player individually, ensuring each player has continuous season entries
        from their minimum to maximum season.
        """
        # Check for required columns
        if 'player_id' not in df.columns or 'season' not in df.columns:
            raise ValueError("Dataframe must contain 'player_id' and 'season' columns.")

        # Store columns to preserve other than `elo`
        other_columns = df.columns.difference(['elo']).tolist()
        # other_columns = [col for col in df.columns if col not in ['elo']]

        # Create a list to store filled data for each player
        filled_dfs = []

        # Process each player individually
        for player_id, group in df.groupby('player_id'):
            # Reset index on group to avoid issues with multi-indexing
            # group = group.reset_index(drop=True)

            # Cases when there is no data of season
            min_season, max_season = group['season'].min(), group['season'].max()
            if pd.isna(min_season) or pd.isna(max_season):
                continue

            df2 = pd.concat(
                [group.set_index('season').reindex(np.arange(group['season'].min(), group['season'].max() + 1))])

            # df2['elo'] = df2['elo'].ffill()
            with pd.option_context('future.no_silent_downcasting', True):
                df2 = df2.ffill()
            # df2 = df2.infer_objects()
            df2 = df2.reset_index()

            # Append the filled data for this player
            filled_dfs.append(df2)

        # Concatenate all filled data into one DataFrame
        filled_df = pd.concat(filled_dfs, ignore_index=True)

        return filled_df

    @staticmethod
    def _add_season_column(df: pd.DataFrame) -> pd.DataFrame:
        """Add a season column based on the date column."""
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        if 'season' not in df_copy.columns:
            df_copy['season'] = df_copy['date'].apply(
                lambda x: f"{x.year}" if x >= pd.Timestamp(x.year, 7, 1) else f"{x.year - 1}"
            )
            # Set data type accordingly
            df_copy['season'] = np.int64(df_copy['season'])
        return df_copy

    def _init_season_valuations(self):
        """Create a dictionary of mean and std for player valuations per season with consistent types."""
        season_valuations = {}

        # Ensure consistent 'season' column type in player_valuations_df
        self.player_valuations_df = self._add_season_column(self.player_valuations_df)
        self.player_valuations_df['season'] = np.int64(self.player_valuations_df['season'])

        for season in self.player_valuations_df['season'].unique():
            season_values = self.player_valuations_df.loc[
                self.player_valuations_df['season'] == season, 'market_value_in_eur'
            ].dropna()
            season_values_log = np.log1p(season_values)
            season_valuations[season] = {
                'mean_log': season_values_log.mean(),
                'std_log': season_values_log.std()
            }

        return season_valuations

    def is_enough_data_to_init_elo(self, player_id, game_id):
        """Check if enough data exists to initialize ELO based on teammates' average ELO."""
        if game_id not in self.games_df['game_id'].values:
            return None

        season = self._get_season(game_id)
        teammates = self._get_teammates(player_id, game_id)
        if teammates.empty:
            return None

        teammate_elo_df = self.players_elo_df[
            (self.players_elo_df['player_id'].isin(teammates['player_id'])) &
            (self.players_elo_df['season'] == season)
            ]
        teammate_elos = teammate_elo_df['elo'].dropna()
        return teammate_elos.mean() if len(teammate_elos) >= len(teammates) / 2 else None

    def init_player_elo_with_value(self, player_id, season):
        """Initialize a player's ELO based on their market value for a given season."""
        season = np.int64(season)  # Ensure consistent season type for dictionary lookup
        if season not in self.season_valuations:
            return self.base_elo

        season_mean_log = self.season_valuations[season]['mean_log']
        season_std_log = self.season_valuations[season]['std_log']

        player_value = self.player_valuations_df.loc[
            (self.player_valuations_df['player_id'] == player_id) &
            (self.player_valuations_df['season'] == season), 'market_value_in_eur'
        ]

        if player_value.empty:
            return self.base_elo

        player_z_score = (np.log1p(player_value.values[0]) - season_mean_log) / season_std_log
        return self.base_elo + (player_z_score * (self.elo_range / 2))

    def init_player_elo(self, player_id, game_id):
        """Initialize a player's ELO based on game and player data."""
        player_appearance = self.appearances_df[
            (self.appearances_df['game_id'] == game_id) & (self.appearances_df['player_id'] == player_id)
            ]
        if player_appearance.empty:
            raise ValueError(f"No result found for player {player_id} in game {game_id}")

        season = self._get_season(game_id)
        elo_value = self.is_enough_data_to_init_elo(player_id, game_id) or \
                    self.init_player_elo_with_value(player_id, season)

        # print(f"ELO Value: {elo_value} for player {player_id}")
        # Check matching row exists
        row_exists = ((self.players_elo_df['player_id'] == player_id) &
                      (self.players_elo_df['season'] == season)).any()
        if not row_exists:
            # Copy first occurence
            existing_row = self.players_elo_df[self.players_elo_df['player_id'] == player_id].iloc[0].copy()
            existing_row['season'] = season
            existing_row['elo'] = elo_value

            # Append the new row to the DataFrame
            self.players_elo_df = pd.concat([self.players_elo_df, pd.DataFrame([existing_row])], ignore_index=True)

            # This is the case where there is a missing season
            # raise ValueError(f'No result found for player {player_id} in season {season} in player elos file')
        else:
            # print("Matching row found")
            self.players_elo_df.loc[
                (self.players_elo_df['player_id'] == player_id) &
                (self.players_elo_df['season'] == season), 'elo'
            ] = elo_value
        # Confirm update by printing relevant rows
        # print("Updated DataFrame rows:\n",
        #       self.players_elo_df[(self.players_elo_df['player_id'] == player_id) &
        #                           (self.players_elo_df['season'] == season)])

        return self.players_elo_df

    # def fill_missing_season_elo(self, player_id, season):
    # def find_debut_matches(self, player_id):

    def init_all_players_elo(self):
        """Initialize ELOs for a sample of players."""

        # for index, row in self.appearances_df.iterrows():
        #     player_id = row['player_id']
        #     game_id = row['game_id']
        #     self.players_elo_df = self.init_player_elo(player_id, game_id)
        #     # print("########################################")
        start_time = time.time()
        uninit_player_ids = set(self.players_df['player_id'].values)

        # Batch process all appearances for players who need ELOs
        # test_df = self.appearances_df[self.appearances_df['player_id'] == 28003]
        for index, row in enumerate(self.appearances_df.itertuples(), start=1):
            # for index, row in enumerate(test_df.itertuples(), start=1):
            player_id = row.player_id
            game_id = row.game_id
            # Only process if the player's ELO isn't initialized
            if player_id in uninit_player_ids:
                # print(f"{player_id} is uninitialised!")
                # Update player's ELO
                self.players_elo_df = self.init_player_elo(player_id, game_id)
                # Remove player from the uninitialized set
                uninit_player_ids.remove(player_id)

            # Print a message every 100 and 1000 iterations
            if index % 10000 == 0:
                print(f"Processed {index} players- ({time.time() - start_time})")

        # NOTE: Store only not NA Values
        # self.players_elo_df = self.players_elo_df[self.players_elo_df['elo'].notna()]
        self.players_elo_df.sort_values(by=['player_id', 'season'], inplace=True)
        data_path = os.path.join(self.data_dir, 'players_elo.csv')
        self.players_elo_df.to_csv(data_path, index=False)

        return self.players_elo_df

    def _get_season(self, game_id):
        """Retrieve season for a given game_id."""
        season = self.games_df.loc[self.games_df['game_id'] == game_id, 'season'].iloc[0]
        return np.int64(season)

    def _get_teammates(self, player_id, game_id):
        """Retrieve teammates for a player in a specific game."""
        return self.appearances_df[
            (self.appearances_df['game_id'] == game_id) & (self.appearances_df['player_id'] != player_id)
            ]


# Usage

@staticmethod
def _fill_season_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill season gaps for each player individually, ensuring each player has continuous season entries
    from their minimum to maximum season.
    """
    # Check for required columns
    if 'player_id' not in df.columns or 'season' not in df.columns:
        raise ValueError("Dataframe must contain 'player_id' and 'season' columns.")

    # Store original data types
    original_dtypes = df.dtypes

    # Store columns to preserve other than `elo`
    other_columns = df.columns.difference(['elo']).tolist()

    # Create a list to store filled data for each player
    filled_dfs = []

    # Process each player individually
    for player_id, group in df.groupby('player_id'):
        # Reset index on group to avoid issues with multi-indexing
        min_season, max_season = group['season'].min(), group['season'].max()
        if pd.isna(min_season) or pd.isna(max_season):
            continue

        # Reindex seasons to fill the gaps
        group = group.set_index('season')
        df_filled = group.reindex(np.arange(min_season, max_season + 1))

        # Forward fill missing values
        with pd.option_context('future.no_silent_downcasting', True):
            df_filled = df_filled.ffill().infer_objects(copy=False)

        # Reset index and restore player_id
        df_filled = df_filled.reset_index()
        df_filled['player_id'] = player_id

        # Append the filled data for this player
        filled_dfs.append(df_filled)

    # Concatenate all filled data into one DataFrame
    filled_df = pd.concat(filled_dfs, ignore_index=True)

    # Restore original data types
    for col in filled_df.columns:
        if col in original_dtypes:
            filled_df[col] = filled_df[col].astype(original_dtypes[col])

    # Reorder columns to have 'player_id' first and 'season' second
    cols = ['player_id', 'season'] + [col for col in filled_df.columns if col not in ['player_id', 'season']]
    filled_df = filled_df[cols]

    return filled_df
#
# def _fill_season_gaps(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Fill season gaps for each player individually, ensuring each player has continuous season entries
#     from their minimum to maximum season.
#     """
#     # Check for required columns
#     if 'player_id' not in df.columns or 'season' not in df.columns:
#         raise ValueError("Dataframe must contain 'player_id' and 'season' columns.")
#
#     # Store columns to preserve other than `elo`
#     other_columns = df.columns.difference(['elo']).tolist()
#     # other_columns = [col for col in df.columns if col not in ['elo']]
#
#     # Create a list to store filled data for each player
#     filled_dfs = []
#
#     # Process each player individually
#     for player_id, group in df.groupby('player_id'):
#         # Reset index on group to avoid issues with multi-indexing
#         # group = group.reset_index(drop=True)
#
#         # Cases when there is no data of season
#         min_season, max_season = group['season'].min(), group['season'].max()
#         if pd.isna(min_season) or pd.isna(max_season):
#             continue
#
#
#         df2 = pd.concat(
#             [group.set_index('season').reindex(np.arange(group['season'].min(), group['season'].max() + 1))])
#
#         # df2['elo'] = df2['elo'].ffill()
#         with pd.option_context('future.no_silent_downcasting', True):
#             df2 = df2.ffill()
#         # df2 = df2.infer_objects()
#         df2 = df2.reset_index()
#
#
#         # Append the filled data for this player
#         filled_dfs.append(df2)
#
#     # Concatenate all filled data into one DataFrame
#     filled_df = pd.concat(filled_dfs, ignore_index=True)
#
#     return filled_df


base_dir = Path(__file__).resolve().parent
data_dir = base_dir.parents[0] / 'data' / 'transfer_data'
df = pd.read_csv(os.path.join(data_dir, 'players_elo.csv'))
res = _fill_season_gaps(df)
data_path = os.path.join(data_dir, 'players_elo_copy.csv')
res.to_csv(data_path, index=False)



# initializer = PlayerEloInitializer()
# players_elo_df = initializer.init_all_players_elo()
