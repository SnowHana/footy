import os
from pathlib import Path
import pandas as pd
import numpy as np

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
        # self.players_elo_df = self.dataframes.get('players_elo_df', self._init_players_elo_df())
        self.players_elo_df = self._init_players_elo_df()
        # Initialize season valuations
        self.season_valuations = self._init_season_valuations()

    def _import_dataframes(self) -> dict:
        """Read data from CSV files and store as DataFrames."""
        dataframes = {}
        for dirpath, _, filenames in os.walk(self.data_dir):
            for filename in filenames:
                file_key = f"{filename.split('.')[0]}_df"
                filepath = os.path.join(dirpath, filename)
                dataframes[file_key] = pd.read_csv(filepath, sep=",", encoding="UTF-8")
                print(f"{file_key}: {dataframes[file_key].shape}")
        print("Data imported successfully.")
        return dataframes

    def _init_players_elo_df(self) -> pd.DataFrame:
        """Initialize players' ELO DataFrame with a season column if not available."""
        df = self.players_df.copy()
        df['elo'] = None
        player_val_df_with_season = self._add_season_column(self.player_valuations_df)
        df_sorted = player_val_df_with_season.loc[
            player_val_df_with_season.groupby(['player_id', 'season'])['date'].idxmin()
        ]
        df = df.merge(df_sorted[['player_id', 'season']], on='player_id', how='left')
        # df = df.drop(df.columns.difference(['player_id', 'season', 'first_name', 'last_name', 'name',
        #                                     'current_club_id', 'player_code', 'country_of_birth',
        #                                     'market_value_in_eur', 'date_of_birth']), axis=1, inplace=)
        #
        # # Check available columns after the merge
        # print("Columns after merge:", df.columns.tolist())
        #
        # # Select only the specified columns
        # expected_columns = [
        #     'player_id', 'season', 'first_name', 'last_name', 'name',
        #     'current_club_id', 'player_code', 'country_of_birth',
        #     'market_value_in_eur', 'date_of_birth' , 'elo'
        # ]
        #
        # # Filter by expected columns only if they exist in df
        # existing_columns = [col for col in expected_columns if col in df.columns]
        #
        # # Display missing columns for debugging, if any
        # missing_columns = set(expected_columns) - set(existing_columns)
        # if missing_columns:
        #     print("Warning: Missing columns in df:", missing_columns)
        #
        # return df[existing_columns]
        return df

    @staticmethod
    def _add_season_column(df: pd.DataFrame) -> pd.DataFrame:
        """Add a season column based on the date column."""
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        if 'season' not in df_copy.columns:
            df_copy['season'] = df_copy['date'].apply(
                lambda x: f"{x.year}" if x >= pd.Timestamp(x.year, 7, 1) else f"{x.year - 1}"
            )
        return df_copy

    def _init_season_valuations(self):
        """Create a dictionary of mean and std for player valuations per season with consistent types."""
        season_valuations = {}

        # Ensure consistent 'season' column type in player_valuations_df
        self.player_valuations_df = self._add_season_column(self.player_valuations_df)
        self.player_valuations_df['season'] = self.player_valuations_df['season'].astype(str)

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
        season = str(season)  # Ensure consistent season type for dictionary lookup
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

        print(f"ELO Value: {elo_value} for player {player_id}")
        self.players_elo_df.loc[
            (self.players_elo_df['player_id'] == player_id) &
            (self.players_elo_df['season'] == season), 'elo'
        ] = elo_value
        return self.players_elo_df

    def init_all_players_elo(self):
        """Initialize ELOs for a sample of players."""
        for index, row in self.appearances_df.sample(n=5).iterrows():
            player_id = row['player_id']
            game_id = row['game_id']
            self.players_elo_df = self.init_player_elo(player_id, game_id)
            print("########################################")

        # lastly save it as a csv file
        data_path = os.path.join(self.data_dir, 'players_elo.csv')
        self.players_elo_df.to_csv(data_path, index=True)
        return self.players_elo_df

    def _get_season(self, game_id):
        """Retrieve season for a given game_id."""
        return self.games_df.loc[self.games_df['game_id'] == game_id, 'season'].iloc[0]

    def _get_teammates(self, player_id, game_id):
        """Retrieve teammates for a player in a specific game."""
        return self.appearances_df[
            (self.appearances_df['game_id'] == game_id) & (self.appearances_df['player_id'] != player_id)
            ]


# Usage
initializer = PlayerEloInitializer()
players_elo_df = initializer.init_all_players_elo()
