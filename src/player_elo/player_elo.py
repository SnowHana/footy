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
    conn = psycopg.connect(**DATABASE_CONFIG)
    # conn.autocommit = True
    return conn


def goals_in_game_per_club(cur, game_id) -> dict:
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
        res[result[2]] = res.get(result[2], []) + [result[1]]
    return res


def goal_diff_when_playing(cur, game_id) -> dict:
    """
    Find out starting-ending minutes of players in a game
    @param cur:
    @param game_id: Game ID of a game to analyse
    @return: {player_id:(mp_start, mp_end)}
    """
    # Find out when player was subbed in / subbed off
    cur.execute(f"""SELECT game_id, minute, type, club_id, player_id, player_in_id
                    FROM game_events
                    WHERE type = 'Substitutions' AND game_id = {game_id};""")
    players_in = {}
    players_out = {}
    for result in cur.fetchall():
        players_in[result[-1]] = result[1]
        players_out[result[-2]] = result[1]

    cur.execute(f"""SELECT game_id, player_id, minutes_played
                    FROM appearances
                    WHERE game_id = {game_id};""")
    starting_players = {}
    for result in cur.fetchall():
        starting_players[result[1]] = result[-1]

    # print(players_in)
    # print(players_out)
    # print(starting_players)
    # played_players = starting_players | players_in
    play_duration = {}
    # for player, minute in (starting_players | players_in).items():
    # Iterate through all players played in the game
    # if player in starting_players:
    global FULL_GAME_MINUTES
    # For startings.
    for player, minute in starting_players.items():
        play_duration[player] = (0, players_out.get(player, minute))
    # For subbed-ins
    for player, in_minute in players_in.items():
        play_duration[player] = (in_minute, players_out.get(player, FULL_GAME_MINUTES))

    return play_duration

def match_impact_players(cur, game_id):
    goal_minutes =
# Sample usage
conn = None
cur = None
try:
    # Establish the connection
    conn = connect_to_db()
    cur = conn.cursor()

    # goal_dict = goals_in_game_per_club(cur, 2224008)

    goal_diff_when_playing(cur, 2224008)

    # cur.execute("""
    # SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
    # """)
    # tables = cur.fetchall()
    #
    # for table in tables:
    #     table_name = table[0]
    #     print(f"Table: {table_name}")
    #
    #     # Get columns for the current table
    #     cur.execute(
    #         f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    #     columns = cur.fetchall()
    #     for column in columns:
    #         print(f" - Column: {column[0]} | Type: {column[1]}")
    #     print("\n")
except:
    raise ValueError('Could not connect to database')
finally:
    # Close the connection
    cur.close()
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
