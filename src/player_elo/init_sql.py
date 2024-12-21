
import os
from pathlib import Path
from typing import Dict

# Make sure your environment uses psycopg 3, e.g. 'psycopg==3.1.8'
# and your SQLAlchemy URL is 'postgresql+psycopg://...' not 'psycopg2'
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean, PrimaryKeyConstraint, text
from sqlalchemy.orm import declarative_base
from psycopg import sql  # psycopg 3
from src.player_elo.database_connection import DATABASE_CONFIG
# Data dir
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parents[0] / 'data' / 'transfer_data'


def create_sqlalchemy_engine(config: Dict[str, str]):
    """Create SQLAlchemy engine using psycopg 3 driver."""
    return create_engine(
        f"postgresql+psycopg://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    )


engine = create_sqlalchemy_engine(DATABASE_CONFIG)
Base = declarative_base()

# Define SQLAlchemy models for each DataFrame
class GameLineup(Base):
    __tablename__ = 'game_lineups'

    game_lineups_id = Column(String, primary_key=True)
    date = Column(Date)
    game_id = Column(Integer)
    player_id = Column(Integer)
    club_id = Column(Integer)
    player_name = Column(String)
    type = Column(String)
    position = Column(String)
    number = Column(String)
    team_captain = Column(Integer)


class Competition(Base):
    __tablename__ = 'competitions'

    competition_id = Column(String, primary_key=True)
    competition_code = Column(String)
    name = Column(String)
    sub_type = Column(String)
    type = Column(String)
    country_id = Column(Integer)
    country_name = Column(String)
    domestic_league_code = Column(String)
    confederation = Column(String)
    url = Column(String)
    is_major_national_league = Column(Boolean)


class Appearance(Base):
    __tablename__ = 'appearances'

    appearance_id = Column(String, primary_key=True)
    game_id = Column(Integer)
    player_id = Column(Integer)
    player_club_id = Column(Integer)
    player_current_club_id = Column(Integer)
    date = Column(Date)
    player_name = Column(String)
    competition_id = Column(String)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    goals = Column(Integer)
    assists = Column(Integer)
    minutes_played = Column(Integer)


class PlayerValuation(Base):
    __tablename__ = 'player_valuations'

    player_id = Column(Integer)
    date = Column(Date)
    market_value_in_eur = Column(Integer)
    current_club_id = Column(Integer)
    player_club_domestic_competition_id = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint('player_id', 'date', name='player_valuation_pk'),
    )


class GameEvent(Base):
    __tablename__ = 'game_events'

    game_event_id = Column(String, primary_key=True)
    date = Column(Date)
    game_id = Column(Integer)
    minute = Column(Integer)
    type = Column(String)
    club_id = Column(Integer)
    player_id = Column(Integer)
    description = Column(String)
    player_in_id = Column(Integer)
    player_assist_id = Column(Integer)


class Transfer(Base):
    __tablename__ = 'transfers'

    player_id = Column(Integer)
    transfer_date = Column(Date)
    transfer_season = Column(String)
    from_club_id = Column(Integer)
    to_club_id = Column(Integer)
    from_club_name = Column(String)
    to_club_name = Column(String)
    transfer_fee = Column(Float)
    market_value_in_eur = Column(Float)
    player_name = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint('player_id', 'from_club_id', 'to_club_id', name='transfer_pk'),
    )


class Player(Base):
    __tablename__ = 'players'

    player_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    name = Column(String)
    last_season = Column(Integer)
    current_club_id = Column(Integer)
    player_code = Column(String)
    country_of_birth = Column(String)
    city_of_birth = Column(String)
    country_of_citizenship = Column(String)
    date_of_birth = Column(Date)
    sub_position = Column(String)
    position = Column(String)
    foot = Column(String)
    height_in_cm = Column(Float)
    contract_expiration_date = Column(Date)
    agent_name = Column(String)
    image_url = Column(String)
    url = Column(String)
    current_club_domestic_competition_id = Column(String)
    current_club_name = Column(String)
    market_value_in_eur = Column(Float)
    highest_market_value_in_eur = Column(Float)


class Game(Base):
    __tablename__ = 'games'

    game_id = Column(Integer, primary_key=True)
    competition_id = Column(String)
    season = Column(Integer)
    round = Column(String)
    date = Column(Date)
    home_club_id = Column(Integer)
    away_club_id = Column(Integer)
    home_club_goals = Column(Integer)
    away_club_goals = Column(Integer)
    home_club_position = Column(Integer)
    away_club_position = Column(Integer)
    home_club_manager_name = Column(String)
    away_club_manager_name = Column(String)
    stadium = Column(String)
    attendance = Column(Integer)
    referee = Column(String)
    url = Column(String)
    home_club_formation = Column(String)
    away_club_formation = Column(String)
    home_club_name = Column(String)
    away_club_name = Column(String)
    aggregate = Column(String)
    competition_type = Column(String)


class ClubGame(Base):
    __tablename__ = 'club_games'

    game_id = Column(Integer, primary_key=True)
    club_id = Column(Integer)
    own_goals = Column(Integer)
    own_position = Column(Integer)
    own_manager_name = Column(String)
    opponent_id = Column(Integer)
    opponent_goals = Column(Integer)
    opponent_position = Column(Integer)
    opponent_manager_name = Column(String)
    hosting = Column(String)
    is_win = Column(Integer)


class PlayerElo(Base):
    __tablename__ = 'players_elo'

    player_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    name = Column(String)
    player_code = Column(String)
    country_of_birth = Column(String)
    date_of_birth = Column(Date)
    elo = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('player_id', 'season', name='player_elo_pk'),
    )


class Club(Base):
    __tablename__ = 'clubs'

    club_id = Column(Integer, primary_key=True)
    club_code = Column(String)
    name = Column(String)
    domestic_competition_id = Column(String)
    total_market_value = Column(Float)
    squad_size = Column(Integer)
    average_age = Column(Float)
    foreigners_number = Column(Integer)
    foreigners_percentage = Column(Float)
    national_team_players = Column(Integer)
    stadium_name = Column(String)
    stadium_seats = Column(Integer)
    net_transfer_record = Column(String)
    coach_name = Column(Float)
    last_season = Column(Integer)
    filename = Column(String)
    url = Column(String)


def drop_all_tables(engine):
    """Drops all tables in the current database schema."""
    print("Dropping all tables...")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
    print("Schema reset complete.")


def recreate_tables(engine):
    """Recreate tables using SQLAlchemy models."""
    Base.metadata.create_all(engine)
    print("Tables recreated successfully.")

def load_csv_to_postgres(table_name, csv_file_path, engine):
    """
    Load a CSV file into a PostgreSQL table using psycopg 3's new COPY interface.
    """
    from psycopg import sql

    # Our COPY statement: Make sure it matches your CSV format
    copy_sql = sql.SQL("""
        COPY {} FROM STDIN
        WITH (FORMAT csv, HEADER, DELIMITER ',')
    """).format(sql.Identifier(table_name))

    print(f"Loading data into table: {table_name} from file: {csv_file_path}")

    raw_conn = engine.raw_connection()  # raw DB-API connection (should be psycopg 3 if your URL is `postgresql+psycopg://`)
    try:
        with raw_conn.cursor() as cur:
            # Open the CSV file in binary mode
            with open(csv_file_path, 'rb') as f:
                # Use the psycopg 3 COPY context manager
                with cur.copy(copy_sql) as copy:
                    # Read the CSV and write it line-by-line to the copy stream
                    for line in f:
                        copy.write(line)

        raw_conn.commit()
        print(f"Data loaded successfully into table: {table_name}")

    except Exception as e:
        raw_conn.rollback()
        print(f"Error loading data into table {table_name}: {e}")

    finally:
        raw_conn.close()



def load_all_csv(data_dir, engine):
    """Load all CSV files in the data directory into corresponding PostgreSQL tables."""

    # We map "filename.csv" -> "filename" as the table name
    # Make sure your CSV file names match your table names
    csv_to_table_map = {}
    for dirpath, _, filenames in os.walk(data_dir):
        for filename in filenames:
            file_key = filename.split('.')[0]  # e.g. "players" from "players.csv"
            filepath = os.path.join(dirpath, filename)
            csv_to_table_map[filepath] = file_key

    # Load each CSV into its matching table
    for csv_file_path, table_name in csv_to_table_map.items():
        if os.path.exists(csv_file_path):
            load_csv_to_postgres(table_name, csv_file_path, engine)
        else:
            print(f"File {csv_file_path} not found. Skipping.")


def create_process_table(engine):
    """
    Create a table to track the progress of processes such as ELO updates.
    """
    print("Creating or updating process progress table...")
    with engine.begin() as conn:
        try:
            print("Creating process_progress table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS process_progress (
                    process_name VARCHAR PRIMARY KEY,
                    last_processed_date DATE,
                    last_processed_game_id INTEGER
                );
            """))
            print("Table creation successful.")

            print("Inserting default values into process_progress table...")
            conn.execute(text("""
                INSERT INTO process_progress (process_name, last_processed_date, last_processed_game_id)
                VALUES ('elo_update', NULL, NULL)
                ON CONFLICT (process_name) DO NOTHING;
            """))
            print("Default values inserted successfully.")

        except Exception as e:
            print(f"Error creating or updating process progress table: {e}")
            raise


def main():
    data_dir = DATA_DIR  # Path to your CSV directory

    # 1) Drop existing tables
    drop_all_tables(engine)
    # 2) Recreate tables from SQLAlchemy models
    recreate_tables(engine)
    # 3) Load all CSVs with psycopg 3 "copy" approach
    load_all_csv(data_dir, engine)

    # STEP 1: Check row counts after loading each table
    print("\nVerifying row counts after loading CSVs...")
    with engine.connect() as conn:
        for table_name in Base.metadata.tables.keys():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
            count = result.scalar()
            print(f"Table '{table_name}' has {count} rows.")

    # 4) Create process_progress table
    create_process_table(engine)


if __name__ == "__main__":
    main()