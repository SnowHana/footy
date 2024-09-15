import pandas as pd
from django.conf import settings
from django.utils.text import slugify
from base.models import Player, PlayerStat, Team


def run():
    # Read CSV file into a DataFrame
    csv_file_path = settings.BASE_DIR / "data" / "standard_stats_big5.csv"
    df = pd.read_csv(csv_file_path)

    # Fill missing values with default values or drop rows with missing essential information
    df.fillna(
        {
            "Age": 0,
            "Born": 0,
            "MP": 0,
            "Starts": 0,
            "Min": 0,
            "90s": 0.0,
            "Gls": 0,
            "Ast": 0,
            "G+A": 0,
            "G-PK": 0,
            "PK": 0,
            "PKatt": 0,
            "CrdY": 0,
            "CrdR": 0,
            "xG": 0.0,
            "npxG": 0.0,
            "xAG": 0.0,
            "npxG+xAG": 0.0,
            "PrgC": 0,
            "PrgP": 0,
            "PrgR": 0,
            "Gls-90": 0.0,
            "Ast-90": 0.0,
            "G+A-90": 0.0,
            "G-PK-90": 0.0,
            "G+A-PK": 0,
            "xG-90": 0.0,
            "xAG-90": 0.0,
            "xG+xAG": 0.0,
            "npxG-90": 0.0,
            "npxG+xAG-90": 0.0,
        },
        inplace=True,
    )

    # Convert numerical columns to correct types
    df["Age"] = df["Age"].astype(int)
    df["Born"] = df["Born"].astype(int)
    df["MP"] = df["MP"].astype(int)
    df["Starts"] = df["Starts"].astype(int)
    df["Min"] = df["Min"].astype(int)
    df["90s"] = df["90s"].astype(float)
    df["Gls"] = df["Gls"].astype(int)
    df["Ast"] = df["Ast"].astype(int)
    df["G+A"] = df["G+A"].astype(int)
    df["G-PK"] = df["G-PK"].astype(int)
    df["PK"] = df["PK"].astype(int)
    df["PKatt"] = df["PKatt"].astype(int)
    df["CrdY"] = df["CrdY"].astype(int)
    df["CrdR"] = df["CrdR"].astype(int)
    df["xG"] = df["xG"].astype(float)
    df["npxG"] = df["npxG"].astype(float)
    df["xAG"] = df["xAG"].astype(float)
    df["npxG+xAG"] = df["npxG+xAG"].astype(float)
    df["PrgC"] = df["PrgC"].astype(int)
    df["PrgP"] = df["PrgP"].astype(int)
    df["PrgR"] = df["PrgR"].astype(int)
    df["Gls-90"] = df["Gls-90"].astype(float)
    df["Ast-90"] = df["Ast-90"].astype(float)
    df["G+A-90"] = df["G+A-90"].astype(float)
    df["G-PK-90"] = df["G-PK-90"].astype(float)
    df["G+A-PK"] = df["G+A-PK"].astype(int)
    df["xG-90"] = df["xG-90"].astype(float)
    df["xAG-90"] = df["xAG-90"].astype(float)
    df["xG+xAG"] = df["xG+xAG"].astype(float)
    df["npxG-90"] = df["npxG-90"].astype(float)
    df["npxG+xAG-90"] = df["npxG+xAG-90"].astype(float)

    # Lists to hold instances for bulk operations
    teams_to_update_or_create = []
    players_to_update_or_create = []
    player_stats_to_create = []
    players_to_update = []
    player_stats_to_update = []

    # Helper dictionaries for lookup
    player_lookup = {}

    # Iterate through the DataFrame
    for index, row in df.iterrows():
        # Handle Team
        team_name = row["Squad"]
        team_slug = slugify(team_name)
        team, created = Team.objects.update_or_create(
            name=team_name, defaults={"slug": team_slug}
        )
        if not created:
            # Means we need to update
            teams_to_update_or_create.append(team)

        # Handle Player
        player_name = row["Player"]
        player_slug = slugify(f"{player_name}-{team.name}")
        player, created = Player.objects.update_or_create(
            slug=player_slug,
            defaults={
                "name": player_name,
                "age": row["Age"],
                "born": row["Born"],
                "nation": row["Nation"],
                "position": row["Pos"],
                "team": team,
            },
        )
        if created:
            players_to_update_or_create.append(player)
        else:
            # Alr exists
            players_to_update.append(player)

        player_lookup[player_name] = player

        # Handle PlayerStat
        # stats_slug = slugify(f"{player.name}-{team.name}-{row['Comp']}")
        stats, created = PlayerStat.objects.update_or_create(
            player=player,
            defaults={
                # "slug": stats_slug,
                "competition": row["Comp"],
                "mp": row["MP"],
                "starts": row["Starts"],
                "minutes": row["Min"],
                "nineties": row["90s"],
                "goals": row["Gls"],
                "assists": row["Ast"],
                "goals_assists": row["G+A"],
                "goals_minus_pens": row["G-PK"],
                "penalties": row["PK"],
                "penalties_attempted": row["PKatt"],
                "yellow_cards": row["CrdY"],
                "red_cards": row["CrdR"],
                "xg": row["xG"],
                "npxg": row["npxG"],
                "xag": row["xAG"],
                "npxg_plus_xag": row["npxG+xAG"],
                "prog_carries": row["PrgC"],
                "prog_passes": row["PrgP"],
                "prog_runs": row["PrgR"],
                "goals_per_90": row["Gls-90"],
                "assists_per_90": row["Ast-90"],
                "goals_assists_per_90": row["G+A-90"],
                "goals_minus_pens_per_90": row["G-PK-90"],
                "goals_assists_minus_pens": row["G+A-PK"],
                "xg_per_90": row["xG-90"],
                "xag_per_90": row["xAG-90"],
                "xg_plus_xag": row["xG+xAG"],
                "npxg_per_90": row["npxG-90"],
                "npxg_plus_xag_per_90": row["npxG+xAG-90"],
            },
        )
        if created:
            player_stats_to_create.append(stats)
        else:
            player_stats_to_update.append(stats)

    # Perform bulk create and update operations
    Team.objects.bulk_update(
        teams_to_update_or_create,
        fields=[field.name for field in Team._meta.fields if field.name != "id"],
    )
    Player.objects.bulk_create(players_to_update_or_create)
    Player.objects.bulk_update(
        players_to_update,
        fields=[field.name for field in Player._meta.fields if field.name != "id"],
    )
    PlayerStat.objects.bulk_create(player_stats_to_create)
    PlayerStat.objects.bulk_update(
        player_stats_to_update,
        fields=[field.name for field in PlayerStat._meta.fields if field.name != "id"],
    )

    print("Data import complete.")
