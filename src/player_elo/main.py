# import signal
# import logging
# import sys
# from pathlib import Path
#
# # Add the src directory to sys.path
# BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Adjust parent calls to reach the root
# sys.path.append(str(BASE_DIR))
#
# from src.player_elo.club_analysis import ClubAnalysis
# from src.player_elo.database_connection import DatabaseConnection, DATABASE_CONFIG
# from src.player_elo.game_analysis import GameAnalysis
# from src.player_elo.player_analysis import PlayerAnalysis
#
#
# class EloUpdater:
#     """Class for updating ELOs based on game data."""
#
#     BATCH_SIZE = 100  # Number of games processed per batch
#     interrupted = False  # Flag to check if the process was interrupted
#
#     def __init__(self, cur):
#         self.cur = cur
#         self.current_game_id = None  # Track the current game ID being processed
#
#         # Register signal handler
#         signal.signal(signal.SIGINT, self._handle_interrupt)
#
#     def _get_last_processed_game(self) -> tuple:
#         """Fetch the last processed game date and game ID from the progress tracker."""
#         self.cur.execute("""
#             SELECT last_processed_date, last_processed_game_id
#             FROM process_progress
#             WHERE process_name = 'elo_update';
#         """)
#         result = self.cur.fetchone()
#         return result if result else (None, None)
#
#     def _update_progress(self, last_game_date: str, last_game_id: int):
#         """Update the progress tracker with the last processed game date and game ID."""
#         self.cur.execute("""
#             UPDATE process_progress
#             SET last_processed_date = %s, last_processed_game_id = %s
#             WHERE process_name = 'elo_update';
#         """, (last_game_date, last_game_id))
#         self.cur.connection.commit()
#
#     def _handle_interrupt(self, signum, frame):
#         """Handle user interrupt (e.g., Ctrl+C) to save progress and exit gracefully."""
#         logging.warning("Process interrupted by user.")
#         self.interrupted = True
#
#         if self.current_game_id:
#             # Save progress before exiting
#             self.cur.execute("""
#                 SELECT date FROM valid_games WHERE game_id = %s;
#             """, (self.current_game_id,))
#             game_date = self.cur.fetchone()[0]
#             self._update_progress(game_date, self.current_game_id)
#             logging.info(f"Progress saved. Last processed game: {self.current_game_id} on {game_date}.")
#         # Gracefully close the connection and exit
#         self.cur.connection.rollback()
#         sys.exit(0)  # Force exit
#
#     def update_elo_for_all_games(self):
#         """Iterate through games to analyze and update ELOs for players and clubs."""
#         last_processed_date, last_processed_game_id = self._get_last_processed_game()
#
#         # Query games starting after the last processed game
#         if last_processed_date:
#             self.cur.execute("""
#                 SELECT game_id, date
#                 FROM valid_games
#                 WHERE (date::DATE > %s::DATE OR (date::DATE = %s::DATE AND game_id > %s))
#                 ORDER BY date, game_id ASC;
#             """, (last_processed_date, last_processed_date, last_processed_game_id))
#         else:
#             self.cur.execute("""
#                 SELECT game_id, date
#                 FROM valid_games
#                 ORDER BY date, game_id ASC;
#             """)
#
#         games_to_process = self.cur.fetchall()
#         logging.info(f"Starting ELO update for {len(games_to_process)} games.")
#
#         for batch_start in range(0, len(games_to_process), self.BATCH_SIZE):
#             if self.interrupted:  # Check if the process was interrupted
#                 logging.info("Stopping gracefully as requested by the user.")
#                 break
#
#             batch_games = games_to_process[batch_start:batch_start + self.BATCH_SIZE]
#
#             logging.info(f"Processing batch from {batch_games[0][1]} to {batch_games[-1][1]}.")
#
#             for game_id, game_date in batch_games:
#                 if self.interrupted:  # Break inner loop if interrupted
#                     break
#                 self.current_game_id = game_id  # Track the current game being processed
#                 try:
#                     self.update_elo_for_game(game_id)
#                     # Update progress after processing each game
#                     self._update_progress(game_date, game_id)
#                 except Exception as e:
#                     logging.error(f"Error processing game {game_id}: {e}. Rolling back.")
#                     self.cur.connection.rollback()  # Rollback the transaction
#                     self.current_game_id = None
#
#         logging.info("ELO update completed.")
#
#     def update_elo_for_game(self, game_id):
#         """Update ELO for a single game."""
#         try:
#             # Initialize game analysis
#             game_analysis = GameAnalysis(self.cur, game_id=game_id)
#
#             # Initialize Club Analysis for both clubs
#             home_club_analysis = ClubAnalysis(game_analysis, game_analysis.home_club_id)
#             away_club_analysis = ClubAnalysis(game_analysis, game_analysis.away_club_id)
#
#             # Calculate new club ELOs
#             new_home_club_elo = home_club_analysis.new_elo()
#             new_away_club_elo = away_club_analysis.new_elo()
#
#             # Iterate through each player in the game
#             players = [player for club_players_list in game_analysis.players.values() for player in club_players_list]
#             for player_id in players:
#                 player_analysis = PlayerAnalysis(game_analysis, player_id)
#                 team_change = new_home_club_elo if player_analysis.club_id == game_analysis.home_club_id else new_away_club_elo
#                 new_player_elo = player_analysis.new_elo(team_change)
#
#                 # Update player ELO in the database
#                 self._update_player_elo(player_id, game_analysis.season, new_player_elo)
#
#             # Commit changes after processing the game
#             self.cur.connection.commit()
#
#         except Exception as e:
#             logging.error(f"Error processing game {game_id}: {e}. Rolling back.")
#             self.cur.connection.rollback()  # Rollback transaction in case of error
#             raise  # Re-raise the exception for retry mechanism
#
#     def _update_player_elo(self, player_id, season, new_elo):
#         """Helper function to update player ELO."""
#         self.cur.execute(
#             """
#             INSERT INTO players_elo (player_id, season, elo)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (player_id, season)
#             DO UPDATE SET elo = EXCLUDED.elo;
#             """,
#             (player_id, season, new_elo)
#         )
#
#
# # Usage
# if __name__ == "__main__":
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s"
#     )
#     with DatabaseConnection(DATABASE_CONFIG) as conn:
#         with conn.cursor() as cur:
#             elo_updater = EloUpdater(cur)
#             elo_updater.update_elo_for_all_games()


import logging
import sys
from pathlib import Path

# Add the src directory to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.player_elo.club_analysis import ClubAnalysis
from src.player_elo.database_connection import DatabaseConnection, DATABASE_CONFIG
from src.player_elo.game_analysis import GameAnalysis
from src.player_elo.player_analysis import PlayerAnalysis


class EloUpdater:
    """Class for updating ELOs based on game data."""

    MAX_GAMES_TO_PROCESS = 1000  # Number of games to process before exiting
    BATCH_SIZE = 1000  # Number of games processed per batch

    def __init__(self, cur):
        self.cur = cur
        self.current_game_id = None  # Track the current game ID being processed
        self.games_processed = 0  # Counter for games processed in this run

    def _get_last_processed_game(self) -> tuple:
        """Fetch the last processed game date and game ID from the progress tracker."""
        self.cur.execute("""
            SELECT last_processed_date, last_processed_game_id
            FROM process_progress
            WHERE process_name = 'elo_update';
        """)
        result = self.cur.fetchone()
        return result if result else (None, None)

    def _update_progress(self, last_game_date: str, last_game_id: int):
        """Update the progress tracker with the last processed game date and game ID."""
        self.cur.execute("""
            UPDATE process_progress
            SET last_processed_date = %s, last_processed_game_id = %s
            WHERE process_name = 'elo_update';
        """, (last_game_date, last_game_id))
        self.cur.connection.commit()

    def update_elo_for_all_games(self):
        """Iterate through games to analyze and update ELOs for players and clubs."""
        last_processed_date, last_processed_game_id = self._get_last_processed_game()

        # Query games starting after the last processed game
        if last_processed_date:
            self.cur.execute("""
                SELECT game_id, date 
                FROM valid_games 
                WHERE (date::DATE > %s::DATE OR (date::DATE = %s::DATE AND game_id > %s))
                ORDER BY date, game_id ASC;
            """, (last_processed_date, last_processed_date, last_processed_game_id))
        else:
            self.cur.execute("""
                SELECT game_id, date 
                FROM valid_games 
                ORDER BY date, game_id ASC;
            """)

        games_to_process = self.cur.fetchall()
        logging.info(f"Starting ELO update for {len(games_to_process)} games.")

        for batch_start in range(0, len(games_to_process), self.BATCH_SIZE):
            batch_games = games_to_process[batch_start:batch_start + self.BATCH_SIZE]

            logging.info(f"""Processing batch from Game {batch_games[0][0]} {batch_games[0][1]} to {batch_games[-1][0]} {batch_games[-1][1]}.""")

            for game_id, game_date in batch_games:
                self.current_game_id = game_id  # Track the current game being processed
                try:
                    self.update_elo_for_game(game_id)
                    # Update progress after processing each game
                    self._update_progress(game_date, game_id)
                    self.games_processed += 1

                    # Stop processing if the limit is reached
                    if self.games_processed >= self.MAX_GAMES_TO_PROCESS:
                        logging.info(f"Processed {self.games_processed} games. Exiting...")
                        return
                except Exception as e:
                    logging.error(f"Error processing game {game_id}: {e}. Rolling back.")
                    self.cur.connection.rollback()  # Rollback the transaction
                    self.current_game_id = None

        logging.info("ELO update completed.")

    def update_elo_for_game(self, game_id):
        """Update ELO for a single game."""
        try:
            # Initialize game analysis
            game_analysis = GameAnalysis(self.cur, game_id=game_id)

            # Initialize Club Analysis for both clubs
            home_club_analysis = ClubAnalysis(game_analysis, game_analysis.home_club_id)
            away_club_analysis = ClubAnalysis(game_analysis, game_analysis.away_club_id)

            # Calculate new club ELOs
            new_home_club_elo = home_club_analysis.new_elo()
            new_away_club_elo = away_club_analysis.new_elo()

            # Iterate through each player in the game
            players = [player for club_players_list in game_analysis.players.values() for player in club_players_list]
            for player_id in players:
                player_analysis = PlayerAnalysis(game_analysis, player_id)
                team_change = new_home_club_elo if player_analysis.club_id == game_analysis.home_club_id else new_away_club_elo
                new_player_elo = player_analysis.new_elo(team_change)

                # Update player ELO in the database
                self._update_player_elo(player_id, game_analysis.season, new_player_elo)

            # Commit changes after processing the game
            self.cur.connection.commit()

        except Exception as e:
            logging.error(f"Error processing game {game_id}: {e}. Rolling back.")
            self.cur.connection.rollback()  # Rollback transaction in case of error
            raise  # Re-raise the exception for retry mechanism

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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    with DatabaseConnection(DATABASE_CONFIG) as conn:
        with conn.cursor() as cur:
            elo_updater = EloUpdater(cur)
            elo_updater.update_elo_for_all_games()