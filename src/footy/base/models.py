from django.db import models


class PlayerStats(models.Model):
    player = models.CharField(max_length=100)
    nation = models.CharField(max_length=100)
    position = models.CharField(max_length=10)
    squad = models.CharField(max_length=100)
    competition = models.CharField(max_length=100)
    age = models.IntegerField()
    born = models.IntegerField()
    mp = models.IntegerField()  # Matches played
    starts = models.IntegerField()
    minutes = models.IntegerField()
    nineties = models.FloatField()  # 90s
    goals = models.IntegerField()
    assists = models.IntegerField()
    goals_assists = models.IntegerField()  # G+A
    goals_minus_pens = models.IntegerField()  # G-PK
    penalties = models.IntegerField()  # PK
    penalties_attempted = models.IntegerField()  # PKatt
    yellow_cards = models.IntegerField()  # CrdY
    red_cards = models.IntegerField()  # CrdR
    xg = models.FloatField()  # Expected goals
    npxg = models.FloatField()  # Non-penalty expected goals
    xag = models.FloatField()  # Expected assists
    npxg_plus_xag = models.FloatField()  # Non-penalty xG + xAG
    prog_carries = models.IntegerField()  # Progressive carries
    prog_passes = models.IntegerField()  # Progressive passes
    prog_runs = models.IntegerField()  # Progressive runs
    goals_per_90 = models.FloatField()  # Gls-90
    assists_per_90 = models.FloatField()  # Ast-90
    goals_assists_per_90 = models.FloatField()  # G+A-90
    goals_minus_pens_per_90 = models.FloatField()  # G-PK-90
    goals_assists_minus_pens = models.FloatField()  # G+A-PK
    xg_per_90 = models.FloatField()  # xG-90
    xag_per_90 = models.FloatField()  # xAG-90
    xg_plus_xag = models.FloatField()  # xG+xAG
    npxg_per_90 = models.FloatField()  # npxG-90
    npxg_plus_xag_per_90 = models.FloatField()  # npxG+xAG-90

    def __str__(self):
        return self.player

    class Meta:
        ordering = ["player", "age"]
