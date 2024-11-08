from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg
from typing import Dict, List, Tuple

# Database configuration
DATABASE_CONFIG = {
    'dbname': 'football',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

FULL_GAME_MINUTES = 90


# Connect to the database with connection pooling
def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    return psycopg.connect(**DATABASE_CONFIG)


# Optimized query to get goals scored in a game, grouped by club
def get_goals_in_game_per_club(cur, game_id: int) -> Dict[int, List[int]]:
    """Find goals per club in a single game."""
    cur.execute("""
        SELECT club_id, minute
        FROM game_events
        WHERE type = %s AND game_id = %s
    """, ('Goals', game_id))

    goals_by_club = {}
    for club_id, minute in cur.fetchall():
        goals_by_club.setdefault(club_id, []).append(minute)
    return goals_by_club


# Optimized query for getting player play times in a single call
def get_players_playing_time(cur, game_id: int) -> Dict[Tuple[int, int], Tuple[int, int]]:
    """Calculate playing time for each player in a game."""
    cur.execute("""
        WITH substitutions AS (
            SELECT club_id, player_id, player_in_id, minute
            FROM game_events
            WHERE type = %s AND game_id = %s
        ), appearances AS (
            SELECT player_club_id AS club_id, player_id, minutes_played
            FROM appearances
            WHERE game_id = %s
        )
        SELECT COALESCE(s.club_id, a.club_id) AS club_id,
               COALESCE(s.player_id, a.player_id) AS player_id,
               a.minutes_played AS start_minute,
               s.minute AS end_minute
        FROM substitutions s
        FULL JOIN appearances a ON s.player_id = a.player_id
    """, ('Substitutions', game_id, game_id))

    play_time = {}
    for club_id, player_id, start_minute, end_minute in cur.fetchall():
        play_time[(club_id, player_id)] = (start_minute or 0, end_minute or FULL_GAME_MINUTES)
    return play_time


# Calculate match impact for players based on goal differential while on pitch
def get_match_impact_players(cur, game_id: int) -> Dict[Tuple[int, int], int]:
    """Calculate goal differential impact for each player in a game."""
    goal_minutes = get_goals_in_game_per_club(cur, game_id)
    play_times = get_players_playing_time(cur, game_id)
    player_goal_impact = {}

    for (club_id, player_id), (start_time, end_time) in play_times.items():
        goals_scored = sum(1 for minute in goal_minutes.get(club_id, []) if start_time <= minute <= end_time)
        goals_conceded = sum(1 for opp_club_id, opp_minutes in goal_minutes.items()
                             if
                             opp_club_id != club_id and any(start_time <= minute <= end_time for minute in opp_minutes))
        player_goal_impact[(club_id, player_id)] = goals_scored - goals_conceded
    return player_goal_impact


# Calculate team rating based on player ELO and minutes played
def get_team_rating(cur, game_id: int) -> Dict[int, float]:
    """Calculate the average ELO rating per team based on player participation."""
    cur.execute("""
        SELECT g.home_club_id, g.away_club_id
        FROM games g
        WHERE g.game_id = %s
    """, (game_id,))
    home_club_id, away_club_id = cur.fetchone()

    cur.execute("""
        SELECT a.player_club_id, a.minutes_played, e.elo
        FROM appearances a
        JOIN players_elo e ON a.player_id = e.player_id
        WHERE a.game_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
    """, (game_id,))

    total_rating = {home_club_id: 0, away_club_id: 0}
    total_playtime = {home_club_id: 0, away_club_id: 0}
    for club_id, minutes_played, elo in cur.fetchall():
        total_rating[club_id] += minutes_played * elo
        total_playtime[club_id] += minutes_played

    return {
        club_id: total_rating[club_id] / total_playtime[club_id]
        for club_id in (home_club_id, away_club_id) if total_playtime[club_id] > 0
    }

# TOOD: apply home adavantage

def get_indiv_expectation(conn, game_id: int):

    pass

# Bulk load CSV data into a table using COPY for optimal performance
def load_csv_to_table(conn, csv_file: str, table_name: str):
    """Load data from a CSV file into the specified PostgreSQL table."""
    with conn.cursor() as cur, open(csv_file, 'r') as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
    print(f"Data from {csv_file} loaded into {table_name}")


# Sample usage with optimized queries and connection handling
try:
    conn = connect_to_db()
    with conn.cursor() as cur:
        team_rating = get_team_rating(cur, 3079452)
        print("Team Ratings:", team_rating)
except (psycopg.OperationalError, psycopg.ProgrammingError) as e:
    print(f"Database error: {e}")
finally:
    if conn:
        conn.close()
