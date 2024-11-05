import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

# Data dir
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parents[0] / 'data' / 'transfer_data'

# Replace with your actual database credentials
DATABASE_URI = 'postgresql+psycopg://postgres:1234@localhost:5432/football'
engine = create_engine(DATABASE_URI)

Base = declarative_base()


class PlayerElo(Base):
    __tablename__ = 'players_elo'

    player_id = Column(Integer, primary_key=True)
    season = Column(Integer)
    first_name = Column(String(50))
    last_name = Column(String(50))
    name = Column(String(100))
    player_code = Column(String(50))
    country_of_birth = Column(String(50))
    date_of_birth = Column(Date)
    elo = Column(Float)


# Create the table in the database
Base.metadata.create_all(engine)


players_elo_df = pd.read_csv(os.path.join(DATA_DIR, 'players_elo.csv'))
players_elo_df.to_sql('players_elo', engine, if_exists='replace', index=False)

# Verify data load
#
# with engine.connect() as conn:
#     result = conn.execute("SELECT * FROM players_elo LIMIT 5")
#     for row in result:
#         print(row)