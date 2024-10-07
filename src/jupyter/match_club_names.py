import pandas as pd
from pathlib import Path
import time
import os

from fuzzywuzzy import fuzz
from fuzzywuzzy import process


BASE_DIR = Path.cwd()

# Build the path to the data directory
DATA_DIR = BASE_DIR.parents[0] / "transfer_data"
CLUBS_FILE = "clubs.csv"
ELOS_FILE = "club_elos.csv"
# res.to_csv(DATA_DIR / 'club_elos.csv')


filepath = os.path.join(DATA_DIR, CLUBS_FILE)
clubs_df = pd.read_csv(filepath, sep=",", encoding="UTF-8")
elos_df = pd.read_csv(os.path.join(DATA_DIR, ELOS_FILE), sep=",", encoding="UTF-8")


def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=2):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with both keys and matches
    """
    s = df_2[key2].tolist()

    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
    df_1["matches"] = m

    m2 = df_1["matches"].apply(
        lambda x: ", ".join([i[0] for i in x if i[1] >= threshold])
    )
    df_1["matches"] = m2

    return df_1


# Example usage of fuzzy_merge
df_result = fuzzy_merge(elos_df, clubs_df, "Club", "name", threshold=90, limit=2)

# Identify unmatched teams
# df_unmatched = pd.concat([df_result['matches'], elos_df['C']]).drop_duplicates(keep=False

df_result.to_csv("match.csv")
