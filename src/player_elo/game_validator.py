from src.player_elo.database_connection import DatabaseConnection


class GameValidator:
    """
    Class for validating and selecting valid games.

    Attributes:
        cur: Database cursor for executing SQL queries.
    """

    BATCH_SIZE = 5000  # Adjust batch size based on performance

    def __init__(self, cur):
        """
        Initialize the GameValidator class.

        Args:
            cur: Database cursor for executing SQL queries.
        """
        self.cur = cur
        self._ensure_valid_games_table_exists()

    def _ensure_valid_games_table_exists(self):
        """
        Ensure the `valid_games` table exists in the database.
        If not, create it based on the structure of the `games` table.
        """
        self.cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'valid_games'
                ) THEN
                    CREATE TABLE valid_games AS
                    SELECT * FROM games WHERE 1 = 0;
                    ALTER TABLE valid_games ADD CONSTRAINT valid_games_game_id_pk PRIMARY KEY (game_id);
                END IF; 
            END
            $$;
        """)
        print("Ensured `valid_games` table exists.")

    def _fetch_players_batch(self, game_ids):
        """
        Fetch players for multiple games in a single query.

        Args:
            game_ids (list): List of game IDs to fetch players for.

        Returns:
            dict: {game_id: {club_id: [player_id]}}.
        """
        # Combine players from `appearances` and `game_events`
        self.cur.execute("""
            SELECT game_id, player_club_id AS club_id, player_id
            FROM appearances
            WHERE game_id = ANY(%s)
        """, (game_ids,))

        players = {}
        for game_id, club_id, player_id in self.cur.fetchall():
            players.setdefault(game_id, {}).setdefault(club_id, []).append(player_id)

        self.cur.execute("""
            SELECT game_id, club_id, player_in_id
            FROM game_events
            WHERE type = 'Substitutions' AND game_id = ANY(%s)
        """, (game_ids,))

        for game_id, club_id, player_in_id in self.cur.fetchall():
            players.setdefault(game_id, {}).setdefault(club_id, []).append(player_in_id)

        return players

    def _validate_batch(self, game_ids):
        """
        Validate a batch of games.

        Args:
            game_ids (list): List of game IDs.
            players_by_game (dict): Pre-fetched player data for each game.

        Returns:
            list: Valid game IDs.
        """
        valid_game_ids = []

        # Check appearances for each game in the batch
        self.cur.execute("""
            SELECT DISTINCT game_id
            FROM appearances
            WHERE game_id = ANY(%s)
        """, (game_ids,))
        valid_appearances = {row[0] for row in self.cur.fetchall()}

        for game_id in game_ids:
            if game_id not in valid_appearances:
                print(f"Skipping game_id={game_id}: No record in appearances table.")
                continue

            # NOTE: @note: Skipping this for now
            # Compare fetched players and players in `appearances`
            # table_players = players_by_game.get(game_id, {})
            # self.cur.execute("""
            #     SELECT player_club_id, player_id
            #     FROM appearances
            #     WHERE game_id = %s
            # """, (game_id,))
            # db_players = {}
            # for club_id, player_id in self.cur.fetchall():
            #     db_players.setdefault(club_id, []).append(player_id)
            #
            # if db_players != table_players:
            #     print(f"Skipping game_id={game_id}: Players mismatch.")
            #     continue

            valid_game_ids.append(game_id)

        return valid_game_ids

    def add_valid_games(self):
        """
        Iterate through the Games table in batches, validate each game, and add valid games to the `valid_games` table.
        """
        print("Starting validation...")

        for batch_game_ids in self._fetch_game_ids_batch():
            # Fetch players for the batch
            # players_by_game = self._fetch_players_batch(batch_game_ids)

            # Validate games in the batch
            valid_game_ids = self._validate_batch(batch_game_ids)

            # Insert valid games into the `valid_games` table
            self.cur.executemany("""
                INSERT INTO valid_games SELECT * FROM games WHERE game_id = %s
                ON CONFLICT (game_id) DO NOTHING;
            """, [(game_id,) for game_id in valid_game_ids])

            # print(f"Processed batch: {len(valid_game_ids)} valid games added.")

    def _fetch_game_ids_batch(self):
        """
        Generator to fetch game IDs in batches.

        Yields:
            list: A batch of game IDs.
        """
        self.cur.execute("SELECT game_id FROM games ORDER BY game_id;")
        while True:
            batch = self.cur.fetchmany(self.BATCH_SIZE)
            if not batch:
                break
            yield [row[0] for row in batch]


# Usage
if __name__ == "__main__":
    from src.player_elo.database_connection import DATABASE_CONFIG

    with DatabaseConnection(DATABASE_CONFIG) as conn:
        with conn.cursor() as cur:
            validator = GameValidator(cur)
            validator.add_valid_games()
