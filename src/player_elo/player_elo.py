
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

class GameAnalysis(ELOCalculationMixin):
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

    @property
    def club_elo_change(self) -> Dict[int, float]:
        if self._club_elo_change is None:
            self._club_elo_change = self._calculate_club_elo_change()
        return self._club_elo_change

    def _calculate_club_elo_change(self) -> Dict[int, float]:
        """
        Calculate Club ELO change
        @return: Dict[int, float]: {club_id : Club ELO Change (C_A)}
        """

        home_change = 0
        away_change = 0
        # Get G.D.
        home_club_goals = self.goals_per_club[self.home_club_id]
        away_club_goals = self.goals_per_club[self.away_club_id]

        gd = len(home_club_goals) - len(away_club_goals)

        self.

        # {self.home_club_id: len(home_club_goals), self.away_club_id: len(away_club_goals)}

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


class PlayerAnalysis(ELOCalculationMixin):
    """
    Analyse single player (Player ID)'s performance in a single game (Game ID)
    """

    def __init__(self, game_analysis: GameAnalysis, player_id: int):
        """
        Init.

        @param game_analysis: Game Analysis Class (Analyse entire game, treating club as a single entity)
        @param player_id: ID of a player to analyse
        """
        self.game_analysis = game_analysis  # Reference to shared GameAnalysis instance
        self.player_id = player_id

        # Lazy Loaded attr.
        self.club_id, self.opponent_club_id = self._fetch_club_ids()
        self._player_elo = None
        self._player_expectation = None
        self._playing_time = None
        self._match_impact = None
        self._game_score = None

    def _fetch_club_ids(self) -> Tuple[int, int]:
        """
        Fetch and store the player's club ID and the opponent club ID for this game.

        @return Tuple[int, int]: Player's club ID and opponent's club ID.
        """
        # Fetch the player's club ID from appearances
        self.game_analysis.cur.execute("""
            SELECT player_club_id
            FROM appearances
            WHERE game_id = %s AND player_id = %s
        """, (self.game_analysis.game_id, self.player_id))

        result = self.game_analysis.cur.fetchone()
        if result:
            club_id = result[0]
            # Determine opponent club ID
            opponent_club_id = (
                self.game_analysis.home_club_id if club_id == self.game_analysis.away_club_id
                else self.game_analysis.away_club_id
            )
            return club_id, opponent_club_id
        else:
            raise ValueError(f"Player with ID {self.player_id} not found in game {self.game_analysis.game_id}")

    def _fetch_player_elo(self) -> float:
        self.game_analysis.cur.execute("""
                    SELECT e.elo
                    FROM players_elo e
                    JOIN appearances a ON e.player_id = a.player_id
                    WHERE a.game_id = %s AND e.player_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
                """, (self.game_analysis.game_id, self.player_id))

        elo = self.game_analysis.cur.fetchone()[0]
        return float(elo)

    def _fetch_player_playing_time(self) -> Optional[Tuple[int, int]]:
        """Calculate playing time for a specific player in the game."""
        (start_min, end_min) = self.game_analysis.players_play_times[(self.club_id, self.player_id)]
        # Add
        if start_min is None or end_min is None:
            raise ValueError(f"Player with ID {self.player_id} and Club ID {self.club_id} is not "
                             f"in game {self.game_analysis.game_id}!")
        return start_min, end_min

    def _calculate_player_expectation(self) -> float:
        """
        Calculate E_A_i (Indiv. Expectation)
        @return: float:  Player Expectation in this game
        """

        try:
            opponent_rating = self.game_analysis.club_ratings[self.opponent_club_id]
        except KeyError:
            raise ValueError(
                f"Opponent club rating for club ID {self.opponent_club_id} not found in game {self.game_analysis.game_id}")

        mod = (opponent_rating - self.player_elo) / 400
        return 1 / (1 + pow(10, mod))

    def _calculate_game_score(self) -> float:
        """
        Calculate Game Score S_A_i
        @note: We assumed D_A_i (G.D when A_i was playing) == Match Impact. But this can change later
        @return: float: Game Score of Player A_i (S_A_i)
        """
        # Get match impact (or g.d.)
        # match_impact = self.game_analysis.match_impact_players[(self.club_id, self.player_id)]

        game_score = 0
        # Assume match_impact == Goal Diff atm
        # if self.match_impact > 0:
        #     game_score = 1
        # elif self.match_impact == 0:
        #     game_score = 0.5
        # else:
        #     game_score = 0
        super().calculate_game_score(self, self.player_id)
        return float(game_score)

    def _fetch_match_impact(self) -> int:
        """

        @return:
        """
        match_impact = self.game_analysis.match_impact_players[(self.club_id, self.player_id)]
        return match_impact

    @property
    def match_impact(self) -> int:
        """

        @return:
        """
        if self._match_impact is None:
            self._match_impact = self._fetch_match_impact()
        return self._match_impact

    # def _calculate_change(self) -> float:

    @property
    def game_score(self) -> int:
        """
        Game Score S_A_i (Indiv. Score)
        """

        if self._game_score is None:
            self._game_score = self._calculate_game_score()
        return self._game_score

    @property
    def player_elo(self) -> float:
        """
        Lazy load player elo

        @return: float: Player elo of the season when game GameID was played
        """

        if self._player_elo is None:
            self._player_elo = self._fetch_player_elo()
        return self._player_elo

    @property
    def playing_time(self) -> Optional[Tuple[int, int]]:
        """
        Get Playing time
        @return: (start_min, end_min)
        """
        if self._playing_time is None:
            self._playing_time = self._fetch_player_playing_time()
        return self._playing_time

    @property
    def player_expectation(self) -> float:
        """
        Get E_A_i (Indiv. Expectation)
        @return:float:  Player Expectation
        """
        if self._player_expectation is None:
            self._player_expectation = self._calculate_player_expectation()
        return self._player_expectation

class ClubAnalysis(BaseAnalysis):

    def __init__(self, game_analysis: GameAnalysis, club_id: int):
        super().__init__(game_analysis, entity_id=club_id, is_club=True)


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
