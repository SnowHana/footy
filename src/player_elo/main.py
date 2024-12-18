import logging
import sys
from pathlib import Path
from multiprocessing import Pool, Manager
from functools import partial

# Add the src directory to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.player_elo.club_analysis import ClubAnalysis
from src.player_elo.database_connection import DatabaseConnection, DATABASE_CONFIG
from src.player_elo.game_analysis import GameAnalysis
from src.player_elo.player_analysis import PlayerAnalysis


class EloUpdater:
    """Class for updating ELOs based on game data."""

    BATCH_SIZE = 100  # Number of games processed per batch

    def __init__(self, cur, max_games_to_process=1000):
        self.cur = cur
        self.current_game_id = None  # Track the current game ID being processed
        self.games_processed = 0  # Counter for the total games processed
        self.MAX_GAMES_TO_PROCESS = max_games_to_process

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

    def fetch_games_to_process(self):
        """Fetch the list of games to process."""
        last_processed_date, last_processed_game_id = self._get_last_processed_game()

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

        return self.cur.fetchall()

    @staticmethod
    def process_game(game, db_config):
        """Static method to process a single game."""
        game_id, game_date = game
        try:
            with DatabaseConnection(db_config) as conn:
                with conn.cursor() as cur:
                    game_analysis = GameAnalysis(cur, game_id=game_id)

                    # Club analysis
                    home_club_analysis = ClubAnalysis(game_analysis, game_analysis.home_club_id)
                    away_club_analysis = ClubAnalysis(game_analysis, game_analysis.away_club_id)

                    # Calculate new ELOs
                    new_home_club_elo = home_club_analysis.new_elo()
                    new_away_club_elo = away_club_analysis.new_elo()

                    # Update players' ELO
                    players = [player for club_players_list in game_analysis.players.values() for player in club_players_list]
                    for player_id in players:
                        player_analysis = PlayerAnalysis(game_analysis, player_id)
                        team_change = new_home_club_elo if player_analysis.club_id == game_analysis.home_club_id else new_away_club_elo
                        new_player_elo = player_analysis.new_elo(team_change)

                        cur.execute(
                            """
                            INSERT INTO players_elo (player_id, season, elo)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (player_id, season) 
                            DO UPDATE SET elo = EXCLUDED.elo;
                            """,
                            (player_id, game_analysis.season, new_player_elo)
                        )
                    conn.commit()
            return game_id, game_date
        except Exception as e:
            logging.error(f"Error processing game {game_id}: {e}")
            return None

    def update_elo_with_multiprocessing(self, db_config, games_to_process):
        """Parallel processing of games using multiprocessing."""
        logging.info(f"Starting ELO update for {len(games_to_process)} games.")

        # Divide the games into batches
        batches = [games_to_process[i:i + self.BATCH_SIZE] for i in range(0, len(games_to_process), self.BATCH_SIZE)]

        for batch in batches:
            if self.games_processed >= self.MAX_GAMES_TO_PROCESS:
                logging.info(f"Processed {self.games_processed} games. Exiting...")
                return  # Exit after processing the maximum allowed games

            with Pool(processes=4) as pool:  # Adjust the number of processes
                results = pool.map(partial(self.process_game, db_config=db_config), batch)

            # Update progress tracker for completed games
            for result in results:
                if result:
                    game_id, game_date = result
                    self._update_progress(game_date, game_id)
                    self.games_processed += 1

            logging.info(f"Batch completed. Processed {len(batch)} games.")


# Usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        process_game_num = int(input("Enter number of games you want to process (recommended 100+): "))
        if process_game_num <= 0:
            raise ValueError("Number of games must be greater than 0.")
    except ValueError as e:
        logging.error(f"Invalid input: {e}. Exiting...")
        sys.exit(1)

    with DatabaseConnection(DATABASE_CONFIG) as conn:
        with conn.cursor() as cur:
            elo_updater = EloUpdater(cur, max_games_to_process=process_game_num)
            games_to_process = elo_updater.fetch_games_to_process()
            elo_updater.update_elo_with_multiprocessing(DATABASE_CONFIG, games_to_process)

