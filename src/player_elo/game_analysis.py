
from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg
from typing import Dict, List, Tuple, Optional, Union
from .elo_calculation_mixin import ELOCalculationMixin
from .base_analysis import BaseAnalysis
# Typing
ClubGoals = Dict[int, List[int]]
PlayersPlayTimes = Dict[Tuple[int, int], Tuple[int, int]]
MatchImpacts = Dict[Tuple[int, int], int]

DATABASE_CONFIG = {
    'dbname': 'football',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}


class DatabaseConnection:
    """
    Set up connection with PostgreSQL database
    """

    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn = None

    def __enter__(self):
        self.conn = psycopg.connect(**self.config)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

class GameAnalysis:
    """
    Analysis of a Single Game. Doesn't focus on a single player,
    but contains information about the game so that it can further be used
    in PlayerAnalysis class
    """

    FULL_GAME_MINUTES = 90

    def __init__(self, cur, game_id: int, weight: float):
        """
        Initialize.

        @param cur: DB cursor
        @param game_id: ID of the game to analyze
        """
        self.cur = cur
        self.game_id = game_id
        self.weight = weight
        self.home_club_id, self.away_club_id = self._fetch_club_ids()

        # Lazy-loaded attributes
        self._club_ratings = None
        self._goals_per_club = None
        self._players_play_times = None
        self._match_impact_players = None
        self._club_elo_change = None

    def _fetch_club_ids(self) -> Tuple[int, int]:
        """
        Retrieve the home and away club IDs for the game.

        @return: Tuple[int, int]: Home and away club IDs
        """
        self.cur.execute("""
            SELECT g.home_club_id, g.away_club_id
            FROM games g
            WHERE g.game_id = %s
        """, (self.game_id,))
        return self.cur.fetchone()

    @property
    def club_ratings(self) -> Dict[int, float]:
        """
        Lazy load the club ratings for each team.

        @return: Dict[int, float]: Dictionary mapping each club ID to its average ELO rating
        """
        if self._club_ratings is None:
            self._club_ratings = self._calculate_club_ratings()
        return self._club_ratings

    def _calculate_club_ratings(self) -> Dict[int, float]:
        """
        Calculate the average ELO rating for each team based on player participation.

        @return: Dict[int, float]: Dictionary of club rating for each club
        """
        self.cur.execute("""
            SELECT a.player_club_id, a.minutes_played, e.elo
            FROM appearances a
            JOIN players_elo e ON a.player_id = e.player_id
            WHERE a.game_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
        """, (self.game_id,))

        total_rating = {self.home_club_id: 0, self.away_club_id: 0}
        total_playtime = {self.home_club_id: 0, self.away_club_id: 0}
        for club_id, minutes_played, elo in self.cur.fetchall():
            total_rating[club_id] += minutes_played * elo
            total_playtime[club_id] += minutes_played

        return {
            club_id: total_rating[club_id] / total_playtime[club_id]
            for club_id in (self.home_club_id, self.away_club_id) if total_playtime[club_id] > 0
        }

    @property
    def goals_per_club(self) -> ClubGoals:
        """
        Lazy load the goals scored by each club in the game.

        @return: ClubGoals: Dictionary mapping each club ID to a list of goal-scoring minutes
        """
        if self._goals_per_club is None:
            self._goals_per_club = self._fetch_goals_per_club()
        return self._goals_per_club

    def _fetch_goals_per_club(self) -> ClubGoals:
        """
        Retrieve goals scored by each club in the game.

        @return: ClubGoals: Dictionary mapping each club ID to a list of goal-scoring minutes
        """
        self.cur.execute("""
            SELECT club_id, minute
            FROM game_events
            WHERE type = 'Goals' AND game_id = %s
        """, (self.game_id,))

        goals_by_club = {self.home_club_id: [], self.away_club_id: []}
        for club_id, minute in self.cur.fetchall():
            goals_by_club.setdefault(club_id, []).append(minute)
        return goals_by_club

    @property
    def players_play_times(self) -> PlayersPlayTimes:
        """
        Lazy load the playing time for each player in the game.

        @return: PlayersPlayTimes: Dictionary mapping (club_id, player_id) to (start_min, end_min)
        """
        if self._players_play_times is None:
            self._players_play_times = self._fetch_players_play_times()
        return self._players_play_times

    def _fetch_players_play_times(self) -> PlayersPlayTimes:
        """
        Retrieve playing times for each player in the game.

        @return: PlayersPlayTimes: Dictionary mapping (club_id, player_id) to (start_min, end_min)
        """
        # Starting players
        self.cur.execute("""
            SELECT player_club_id AS club_id, player_id, minutes_played
            FROM appearances
            WHERE game_id = %s
        """, (self.game_id,))

        starting_players = {}
        for club_id, player_id, minutes_played in self.cur.fetchall():
            end_time = minutes_played if minutes_played > 0 else self.FULL_GAME_MINUTES
            starting_players[(club_id, player_id)] = (0, end_time)

        # Substituted players
        self.cur.execute("""
            SELECT club_id, player_id, player_in_id, minute
            FROM game_events
            WHERE type = 'Substitutions' AND game_id = %s
        """, (self.game_id,))

        play_time = starting_players.copy()
        for club_id, player_id, player_in_id, minute in self.cur.fetchall():
            if (club_id, player_id) in play_time:
                play_time[(club_id, player_id)] = (play_time[(club_id, player_id)][0], minute)
            play_time[(club_id, player_in_id)] = (minute, self.FULL_GAME_MINUTES)
        return play_time

    @property
    def match_impact_players(self) -> MatchImpacts:
        """
        Lazy load the match impact for each player in the game.

        @return: MatchImpacts: Dictionary mapping (club_id, player_id) to match impact
        """
        if self._match_impact_players is None:
            self._match_impact_players = self._calculate_match_impact_players()
        return self._match_impact_players

    def _calculate_match_impact_players(self) -> MatchImpacts:
        """
        Calculate the match impact of all players who participated in this game.

        @note 'Match Impact': Goal difference while player was on the pitch.
        @return: MatchImpacts: Dictionary mapping (club_id, player_id) to match impact
        """
        goal_minutes = self.goals_per_club
        play_times = self.players_play_times
        player_goal_impacts = {}

        for (club_id, player_id), (start_time, end_time) in play_times.items():
            goals_scored = sum(1 for minute in goal_minutes.get(club_id, []) if start_time <= minute <= end_time)
            goals_conceded = sum(1 for opp_club_id, opp_minutes in goal_minutes.items()
                                 if opp_club_id != club_id and any(
                start_time <= minute <= end_time for minute in opp_minutes))
            player_goal_impacts[(club_id, player_id)] = goals_scored - goals_conceded
        return player_goal_impacts

    # @property
    # def club_elo_change(self) -> Dict[int, float]:
    #     if self._club_elo_change is None:
    #         self._club_elo_change = self._calculate_club_elo_change()
    #     return self._club_elo_change
    #
    # def _calculate_club_elo_change(self) -> Dict[int, float]:
    #     """
    #     Calculate Club ELO change
    #     @return: Dict[int, float]: {club_id : Club ELO Change (C_A)}
    #     """
    #
    #     home_change = 0
    #     away_change = 0
    #     # Get G.D.
    #     home_club_goals = self.goals_per_club[self.home_club_id]
    #     away_club_goals = self.goals_per_club[self.away_club_id]
    #
    #     gd = len(home_club_goals) - len(away_club_goals)
    #
    #     self.
    #
    #     # {self.home_club_id: len(home_club_goals), self.away_club_id: len(away_club_goals)}

    def analyze_team_performance(self):
        """
        Example function to analyze overall team performance.

        @return: Dict[int, Dict[str, int]]: Team performance metrics based on goals and ratings
        """
        # TODO: Finish this
        performance = {
            club_id: {
                "rating": self.club_ratings[club_id],
                "goals_scored": len(self.goals_per_club.get(club_id, []))
            } for club_id in self.club_ratings
        }
        return performance

# Usage
with DatabaseConnection(DATABASE_CONFIG) as conn:
    with conn.cursor() as cur:
        # Initialize game-level analysis
        game_analysis = GameAnalysis(cur, game_id=3079452)

        # Analyze overall team performance
        team_performance = game_analysis.analyze_team_performance()
        print("Team Performance:", team_performance)

        # Individual player analysis based on shared game data
        player_analytics = PlayerAnalysis(game_analysis, player_id=20506)
        playing_time = player_analytics.playing_time
        print(f"Player {player_analytics.player_id} Playing Time:", playing_time)

        expectation = player_analytics.player_expectation
        print(f"Player {player_analytics.player_id} Expectation:", expectation)
