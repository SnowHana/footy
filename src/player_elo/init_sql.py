
import os
from pathlib import Path
from typing import Dict

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean, PrimaryKeyConstraint, text
from sqlalchemy.orm import declarative_base
from src.player_elo.database_connection import DATABASE_CONFIG
# Data dir
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parents[0] / 'data' / 'transfer_data'

# Replace with your actual database credentials
# Create SQLAlchemy engine dynamically using DATABASE_CONFIG
def create_sqlalchemy_engine(config: Dict[str, str]):
    return create_engine(
        f"postgresql+psycopg://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}",
        # echo=True
    )

engine = create_sqlalchemy_engine(DATABASE_CONFIG)
# DATABASE_URI = 'postgresql+psycopg://postgres:1234@localhost:5432/football'
# engine = create_engine(DATABASE_URI)

Base = declarative_base()
#
# def add_indexes_and_constraints(engine):
#     """Add indexes and constraints to optimize database queries."""
#     with engine.connect() as conn:
#         try:
#             conn.execute(text("CREATE INDEX IF NOT EXISTS idx_valid_games_date ON valid_games (date);"))
#             conn.execute(text("CREATE INDEX IF NOT EXISTS idx_players_player_id ON players (player_id);"))
#             conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clubs_club_id ON clubs (club_id);"))
#             conn.execute(text("CREATE INDEX IF NOT EXISTS idx_appearances_game_id ON appearances (game_id);"))
#             conn.execute(text("CREATE INDEX IF NOT EXISTS idx_players_elo_player_season ON players_elo (player_id, season);"))
#             conn.execute(text("""
#                 ALTER TABLE players_elo
#                 ADD CONSTRAINT IF NOT EXISTS player_elo_pk PRIMARY KEY (player_id, season);
#             """))
#             print("Indexes and constraints added successfully.")
#         except Exception as e:
#             print(f"Error adding indexes or constraints: {e}")
#             raise
#
#
#



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
    Load a CSV file into a PostgreSQL table using the COPY command.

    :param table_name: Name of the PostgreSQL table.
    :param csv_file_path: Path to the CSV file.
    :param engine: SQLAlchemy engine connected to the database.
    """
    conn = engine.raw_connection()  # Get raw connection
    cursor = conn.cursor()  # Get raw psycopg2 cursor
    try:
        print(f"Loading data into table: {table_name} from file: {csv_file_path}")
        copy_sql = f"""
            COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ',';
        """
        with open(csv_file_path, 'r') as f:
            cursor.copy(copy_sql, f)  # Use psycopg2's copy_expert
        conn.commit()  # Commit transaction
        print(f"Data loaded successfully into table: {table_name}")
    except Exception as e:
        conn.rollback()  # Rollback on error
        print(f"Error loading data into table {table_name}: {e}")
    finally:
        cursor.close()  # Close cursor
        conn.close()  # Close connection
def load_all_csv(data_dir, engine):
    """Load all CSV files in the data directory into corresponding PostgreSQL tables."""

    global DATA_DIR
    csv_to_table_map = {}
    for dirpath, _, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            file_key = f"{filename.split('.')[0]}"
            filepath = os.path.join(dirpath, filename)
            csv_to_table_map[filename] = file_key

    for csv_file, table_name in csv_to_table_map.items():
        csv_file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(csv_file_path):
            load_csv_to_postgres(table_name, csv_file_path, engine)
        else:
            print(f"File {csv_file_path} not found. Skipping.")


def create_process_table(engine):
    """
    Create a table to track the progress of processes such as ELO updates.

    :param engine: SQLAlchemy engine connected to the database.
    """

    print("Creating or updating process progress table...")
    with engine.begin() as conn:
        try:
            # Create the table if it doesn't exist
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
    # with engine.connect() as conn:
    #     drop_all_tables(conn)
    #

    # with engine.connect() as conn:
    #     result = conn.execute(text("SELECT 1;"))
    #     print("Database connection is active:", result.scalar())
    drop_all_tables(engine)
    recreate_tables(engine)
    load_all_csv(data_dir, engine)

    # STEP 1: Check row counts after loading each table
    print("\nVerifying row counts after loading CSVs...")
    with engine.connect() as conn:
        # Loop over the tables defined in your SQLAlchemy models
        for table_name in Base.metadata.tables.keys():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
            count = result.scalar()
            print(f"Table '{table_name}' has {count} rows.")


    create_process_table(engine)


if __name__ == "__main__":
    main()
