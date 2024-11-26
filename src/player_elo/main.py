from src.player_elo.club_analysis import ClubAnalysis
from src.player_elo.database_connection import DatabaseConnection, DATABASE_CONFIG
from src.player_elo.game_analysis import GameAnalysis
from src.player_elo.player_analysis import PlayerAnalysis


class EloUpdater:
    """Class for updating ELOs based on game data."""

    def __init__(self, cur):
        self.cur = cur

    def is_game_data_valid(self, game_id):
        """Check if game data is valid"""
        # Get player data from Appearances Table
        appearances_data = {}
        self.cur.execute(f"SELECT player_club_id, player_id FROM appearances WHERE game_id = {game_id};")
        for club_id, player_id in self.cur.fetchall():
            appearances_data.get(club_id, []).append(player_id)

        # Now, get players data from appearances + game events table
        game_analysis = GameAnalysis(self.cur, game_id=game_id)

        if game_analysis.players != appearances_data:
            return False
        else:
            return True




    def update_elo_for_all_games(self):
        """Iterate through games to analyze and update ELOs for players and clubs."""
        # with self.conn.cursor() as cur:
        # self.cur.execute("SELECT game_id FROM games ORDER BY date;")
        self.cur.execute("SELECT game_id FROM games WHERE game_id = 2246172 ORDER BY date;")
        game_ids = self.cur.fetchall()

        for index, (game_id,) in enumerate(game_ids, start=1):
            if self.is_game_data_valid(game_id):
                print(f"Game {game_id} is a valid game.")
                self.update_elo_for_game(game_id)

            # Display progress information every 100,000 games
            if index % 10000 == 0:
                print(f"Processed {index} games so far...")



    def update_elo_for_game(self, game_id):
        """Update ELO for a single game."""
        # Initialize game analysis
        game_analysis = GameAnalysis(self.cur, game_id=game_id)
        # TODO: Check club ratings field is correctly initialised for Gameanalysis class

        # Initialize Club Analysis for both clubs
        home_club_analysis = ClubAnalysis(game_analysis, game_analysis.home_club_id)
        away_club_analysis = ClubAnalysis(game_analysis, game_analysis.away_club_id)

        # Calculate new club ELOs
        new_home_club_elo = home_club_analysis.new_elo()
        new_away_club_elo = away_club_analysis.new_elo()

        # Update club ELOs in the database
        # @note: Skipping this cuz we don't really use club elo
        # self._update_club_elo(self.cur, game_analysis.home_club_id, new_home_club_elo)
        # self._update_club_elo(self.cur, game_analysis.away_club_id, new_away_club_elo)

        # Iterate through each player in the game
        players = game_analysis.players.values()  # Adjust this based on your `GameAnalysis` API
        for player_id in players:
            player_analysis = PlayerAnalysis(game_analysis, player_id)
            team_change = new_home_club_elo if player_analysis.club_id == game_analysis.home_club_id \
                                            else new_away_club_elo
            new_player_elo = player_analysis.new_elo(team_change)

            # Update player ELO in the database
            self._update_player_elo(player_id, game_analysis.season, new_player_elo)
    #
    # def _update_club_elo(self, club_id, new_elo):
    #     """Helper function to update club ELO."""
    #     self.cur.execute(
    #         "UPDATE clubs SET elo = %s WHERE club_id = %s;",
    #         (new_elo, club_id)
    #     )

    def _update_player_elo(self, player_id, season, new_elo):
        """Helper function to update player ELO."""
        self.cur.execute(
            """
            INSERT INTO players_elo (player_id, season, elo)
            VALUES (%s, %s, %s)
            ON CONFLICT (player_id, season) 
            DO UPDATE SET elo = EXCLUDED.elo;
            """,
            (player_id, season, new_elo)
        )


# Usage
if __name__ == "__main__":
    with DatabaseConnection(DATABASE_CONFIG) as conn:
        with conn.cursor() as cur:
            elo_updater = EloUpdater(cur)
            elo_updater.update_elo_for_all_games()
