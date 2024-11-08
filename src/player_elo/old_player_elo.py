from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg

#
# DATABASE_URI = 'postgresql+psycopg://postgres:1234@localhost:5432/football'
# engine = create_engine(DATABASE_URI)
#
# # Get table names
# inspector = inspect(engine)
# tables = inspector.get_table_names()
#
# for table_name in tables:
#     print(table_name)
#     columns = inspector.get_columns(table_name)
#     for column in columns:
#         print(f" - Column: {column['name']} | Type: {column['type']}")
#     print("\n")

#
# Globals
FULL_GAME_MINUTES = 90
# Connection configuration
DATABASE_CONFIG = {
    'dbname': 'football',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}


# Step 1: Connect to PostgreSQL
def connect_to_db():
    """

    @return:
    """
    conn = psycopg.connect(**DATABASE_CONFIG)
    # conn.autocommit = True
    return conn


def get_goals_in_game_per_club(cur, game_id) -> dict:
    """
    Find goals per club in a single game
    @param cur: Cursor
    @param game_id: Game ID of a game to analyse
    @return Dictionary of goals per club in a single game {club_id: [minutes when they scored a goal]
    """
    # Return list of goals scored in a game
    cur.execute(f"""SELECT game_id, minute, club_id, player_id
                FROM game_events
                WHERE type = 'Goals'
                AND game_id = {game_id};""")
    res = {}
    for result in cur.fetchall():
        club_id = result[2]
        minute = result[1]
        res[club_id] = res.get(club_id, []) + [minute]
    return res


def get_players_playing_time(cur, game_id) -> dict:
    """
    Find out starting-ending minutes of players in a game
    @param cur:
    @param game_id: Game ID of a game to analyse
    @return: {(club_id, player_id) :(mp_start, mp_end)}
    """
    # Find out when player was subbed in / subbed off
    cur.execute(f"""SELECT game_id, minute, type, club_id, player_id, player_in_id
                    FROM game_events
                    WHERE type = 'Substitutions' AND game_id = {game_id};""")
    players_in = {}
    players_out = {}
    for result in cur.fetchall():
        minute = result[1]
        club_id = result[3]
        player_id = result[4]
        player_in_id = result[5]
        players_in[(club_id, player_in_id)] = minute
        players_out[(club_id, player_id)] = minute

    cur.execute(f"""SELECT game_id, player_id, minutes_played, player_club_id
                    FROM appearances
                    WHERE game_id = {game_id};""")
    starting_players = {}
    for result in cur.fetchall():
        # Starting players
        club_id = result[-1]
        player_id = result[1]
        minute = result[2]
        starting_players[(club_id, player_id)] = minute

    play_time = {}
    # Iterate through all players played in the game
    # if player in starting_players:
    global FULL_GAME_MINUTES
    # For startings.
    for (club_id, player_id), minute in starting_players.items():
        play_time[(club_id, player_id)] = (0, players_out.get((club_id, player_id), minute))
    # For subbed-ins
    for (club_id, player_id), in_minute in players_in.items():
        # This handles the case of
        # Subbed player getting subbed off lol
        play_time[(club_id, player_id)] = (in_minute, players_out.get((club_id, player_id), FULL_GAME_MINUTES))

    return play_time


def get_match_impact_players(cur, game_id):
    """
    Get match impact (goal difference while player was on the pitch)
    of players at the game
    @param cur:
    @param game_id:
    @return: {(club_id, player_id): goal_diff}
    """
    goal_minutes = get_goals_in_game_per_club(cur, game_id)
    play_times = get_players_playing_time(cur, game_id)
    player_goal_impact = {}
    for (club_id, player_id), (start_time, end_time) in play_times.items():
        # Basically we wanna know GOAL diff when player was on the pitch
        # 1. Goal scored
        goal_scored = 0
        goal_conceded = 0
        # Check goals scored by the player's team while the player was on the pitch
        for minute in goal_minutes.get(club_id, []):
            if start_time <= minute <= end_time:
                goal_scored += 1
        # Check goals conceded by the opposing teams while the player was on the pitch
        for opp_club_id, opp_goal_minutes in goal_minutes.items():
            if opp_club_id != club_id:
                # Opponent club scored
                for minute in opp_goal_minutes:
                    if start_time <= minute <= end_time:
                        goal_conceded += 1

        # Calculate goal diff.
        goal_difference = goal_scored - goal_conceded
        player_goal_impact[(club_id, player_id)] = goal_difference

    return player_goal_impact

def get_players_rating(cur, game_id):
    # Get players rating for a game.
    pass
def get_team_rating(cur, game_id):
    cur.execute(f"""SELECT home_club_id, away_club_id
                    FROM games
                    WHERE game_id = {game_id};""")
    res = cur.fetchone()
    home_club_id = res[0]
    away_club_id = res[1]
    # home_club_goals = res[2]
    # away_club_goals = res[3]

    avg_rating= {}
    play_time = {}
    cur.execute(f"""SELECT appearances.player_id, appearances.player_club_id, appearances.minutes_played, elo
                    FROM players_elo
                    INNER JOIN appearances ON players_elo.player_id = appearances.player_id
                    WHERE game_id = {game_id} AND date_part('year', appearances.date::date) = players_elo.season;
                """)
    for res in cur.fetchall():
        player_club_id = res[1]
        minutes_played = res[2]
        elo = res[3]
        # Play time
        play_time[player_club_id] = play_time.get(player_club_id, 0) + minutes_played
        # Rating
        avg_rating[player_club_id] = avg_rating.get(player_club_id, 0) + (minutes_played * elo)

    # Lets get summed playing time
    # Multiply Rating of indiv player * Minutes played
    team_rating = {home_club_id: avg_rating[home_club_id] / play_time[home_club_id],
                   away_club_id: avg_rating[away_club_id] / play_time[away_club_id]}
    return team_rating






def calc_indiv_change(cur, game_id):
    pass


# Sample usage
conn = None
cur = None
try:
    # Establish the connection
    conn = connect_to_db()
    cur = conn.cursor()

    # goal_dict = goals_in_game_per_club(cur, 2224008)

    # get_players_playing_time(cur, 2224008)
    res = get_team_rating(cur, 3079452)
    print(res)

except (psycopg.OperationalError, psycopg.ProgrammingError) as e:
    print(f"Database error: {str(e)}")
    raise ValueError('Could not connect to database or execute query')
finally:
    # Close the connection
    if cur:
        cur.close()
    if conn:
        conn.close()

#
# # Step 2: Create the database (if it doesn't exist)
# def create_database():
#     with psycopg.connect(
#         dbname="football",
#         user=DATABASE_CONFIG["user"],
#         password=DATABASE_CONFIG["password"],
#         host=DATABASE_CONFIG["host"],
#         port=DATABASE_CONFIG["port"]
#     ) as conn:
#         conn.autocommit = True
#         with conn.cursor() as cur:
#             cur.execute(f"CREATE DATABASE {DATABASE_CONFIG['dbname']}")
#
# # Step 3: Create table based on CSV schema
# def create_table(conn, table_name, schema):
#     with conn.cursor() as cur:
#         create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
#         cur.execute(create_table_query)
#         print(f"Table {table_name} created.")
#
# # Step 4: Load CSV data into the table
# def load_csv_to_table(conn, csv_file, table_name):
#     data = pd.read_csv(csv_file)
#     with conn.cursor() as cur:
#         for index, row in data.iterrows():
#             columns = ', '.join(row.index)
#             values = ', '.join([f"%s"] * len(row))
#             insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
#             cur.execute(insert_query, tuple(row))
#         print(f"Data from {csv_file} inserted into {table_name}")
