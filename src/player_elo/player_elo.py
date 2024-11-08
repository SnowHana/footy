from sqlalchemy import create_engine, inspect
import pandas as pd
import psycopg
from typing import Dict, List, Tuple, Optional

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


#
# class GameAnalysis:
#     """
#     Analysis of a Single Game. Doesn't focus on a single player,
#     but contains information about the game so that it can further be used
#     in PlayerAnalysis Calass
#     """
#     FULL_GAME_MINUTES = 90
#
#     def __init__(self, cur, game_id: int):
#         """
#         Initialise.
#
#         @param cur: DB cursor
#         @param game_id: ID of the game to analyse
#         """
#         self.cur = cur
#         self.game_id = game_id
#         self._game_data_cache = {}  # Cache for storing shared game-level data
#         self.home_club_id, self.away_club_id = self._fetch_club_ids()
#
#     def _fetch_club_ids(self) -> Tuple[int, int]:
#         """
#         Retrieve the home and away club IDs for the game and store as class fields.
#
#         @return:
#             Tuple[int, int]: Home and away club IDs.
#         """
#         self.cur.execute("""
#             SELECT g.home_club_id, g.away_club_id
#             FROM games g
#             WHERE g.game_id = %s
#         """, (self.game_id,))
#
#         return self.cur.fetchone()
#
#     def fetch_game_data(self):
#         """
#         Fetch and cache game-level data if not already done.
#         Checks if we alr have game_data
#         """
#         if "club_ratings" not in self._game_data_cache:
#             self._game_data_cache["club_ratings"] = self.get_club_ratings()
#         if "goals_per_club" not in self._game_data_cache:
#             self._game_data_cache["goals_per_club"] = self.get_goals_in_game()
#         if 'players_play_times' not in self._game_data_cache:
#             self._game_data_cache["players_play_times"] = self.get_players_playing_time()
#         # if "match_impact_players" not in self._game_data_cache:
#         #     self._game_data_cache["match_impact_players"] = self.get_match_impact_players()
#
#     def get_goals_in_game(self) -> ClubGoals:
#         """
#         Get goals scored by each club in the game.
#         @return: dictionary mapping each club ID (int) to a list of goal-scoring minutes (List[int]).
#         """
#         self.cur.execute("""
#             SELECT club_id, minute
#             FROM game_events
#             WHERE type = 'Goals' AND game_id = %s
#         """, (self.game_id,))
#
#         goals_by_club = {}
#         for club_id, minute in self.cur.fetchall():
#             goals_by_club.setdefault(club_id, []).append(minute)
#
#         return goals_by_club
#
#     def get_club_ratings(self) -> Dict[int, float]:
#         """Calculate the average ELO rating for each team based on player participation.
#         @return: Dictionary of club rating for each club. """
#         # self.cur.execute("""
#         #     SELECT g.home_club_id, g.away_club_id
#         #     FROM games g
#         #     WHERE g.game_id = %s
#         # """, (self.game_id,))
#         # home_club_id, away_club_id = self.cur.fetchone()
#
#         self.cur.execute("""
#             SELECT a.player_club_id, a.minutes_played, e.elo
#             FROM appearances a
#             JOIN players_elo e ON a.player_id = e.player_id
#             WHERE a.game_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
#         """, (self.game_id,))
#
#         total_rating = {self.home_club_id: 0, self.away_club_id: 0}
#         total_playtime = {self.home_club_id: 0, self.away_club_id: 0}
#         for club_id, minutes_played, elo in self.cur.fetchall():
#             total_rating[club_id] += minutes_played * elo
#             total_playtime[club_id] += minutes_played
#
#         return {
#             club_id: total_rating[club_id] / total_playtime[club_id]
#             for club_id in (self.home_club_id, self.away_club_id) if total_playtime[club_id] > 0
#         }
#
#     def get_players_playing_time(self) -> PlayersPlayTimes:
#         """
#         Get players playing time data.
#
#         @return: PlayersPlayTimes (club_id, player_id) : (Start min, End min)
#         """
#         # Step 1: Get starting players' information (assumed to start at 0 and play until subbed out or full game)
#         self.cur.execute("""
#                SELECT player_club_id AS club_id, player_id, minutes_played
#                FROM appearances
#                WHERE game_id = %s
#            """, (self.game_id,))
#
#         starting_players = {}
#         for club_id, player_id, minutes_played in cur.fetchall():
#             # Start at minute 0, play until substituted out or full game
#             end_time = minutes_played if minutes_played > 0 else self.FULL_GAME_MINUTES
#             starting_players[(club_id, player_id)] = (0, end_time)
#
#         # Step 2: Get substitution information
#         cur.execute("""
#                SELECT club_id, player_id, player_in_id, minute
#                FROM game_events
#                WHERE type = %s AND game_id = %s
#            """, ('Substitutions', self.game_id))
#
#         # Initialize play time dictionary with starting players
#         play_time = starting_players.copy()
#
#         for club_id, player_id, player_in_id, minute in cur.fetchall():
#             # Update end time for the player who was subbed out
#             if (club_id, player_id) in play_time:
#                 play_time[(club_id, player_id)] = (play_time[(club_id, player_id)][0], minute)
#
#             # Set start and end times for the player who was subbed in
#             play_time[(club_id, player_in_id)] = (minute, self.FULL_GAME_MINUTES)
#
#         return play_time
#
#     def get_match_impact_players(self) -> MatchImpacts:
#         """
#         Get Match impact of all players played in this game
#
#         @note 'Match Impact': Simple gd(Goal Difference) while player was playing on the pitch.
#         @note 'Match Impact' can change later
#
#         @return:MatchImpacts: Dictionary [(club_id, player_id): Match Impact)]
#         """
#         # Check cache first
#         if "match_impact_players" in self._game_data_cache:
#             return self._game_data_cache["match_impact_players"]
#
#         # Fetch?
#         self.fetch_game_data()
#
#         goal_minutes = self._game_data_cache['goals_per_club']
#         play_times = self._game_data_cache['players_play_time']
#         player_goal_impacts = {}
#
#         for (club_id, player_id), (start_time, end_time) in play_times.items():
#             goals_scored = sum(1 for minute in goal_minutes.get(club_id, []) if start_time <= minute <= end_time)
#             goals_conceded = sum(1 for opp_club_id, opp_minutes in goal_minutes.items()
#                                  if
#                                  opp_club_id != club_id and any(
#                                      start_time <= minute <= end_time for minute in opp_minutes))
#             player_goal_impacts[(club_id, player_id)] = goals_scored - goals_conceded
#         return player_goal_impacts
#
#     def analyze_team_performance(self):
#         """Example function to analyze overall team performance."""
#         self.fetch_game_data()
#         club_ratings = self._game_data_cache["club_ratings"]
#         goals_per_club = self._game_data_cache["goals_per_club"]
#
#         # TODO: Finish this?
#
#         # Example: Calculate performance metrics based on goals and ratings
#         # performance = {
#         #     club_id: {
#         #         "rating": club_ratings[club_id],
#         #         "goals_scored": len(goals_per_club.get(club_id, []))
#         #     } for club_id in club_ratings
#         # }
#         # return performance
#
class GameAnalysis:
    """
    Analysis of a Single Game. Doesn't focus on a single player,
    but contains information about the game so that it can further be used
    in PlayerAnalysis class
    """

    FULL_GAME_MINUTES = 90

    def __init__(self, cur, game_id: int):
        """
        Initialize.

        @param cur: DB cursor
        @param game_id: ID of the game to analyze
        """
        self.cur = cur
        self.game_id = game_id
        self.home_club_id, self.away_club_id = self._fetch_club_ids()

        # Lazy-loaded attributes
        self._club_ratings = None
        self._goals_per_club = None
        self._players_play_times = None
        self._match_impact_players = None

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

        goals_by_club = {}
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

    def analyze_team_performance(self):
        """
        Example function to analyze overall team performance.

        @return: Dict[int, Dict[str, int]]: Team performance metrics based on goals and ratings
        """
        #TODO: Finish this
        performance = {
            club_id: {
                "rating": self.club_ratings[club_id],
                "goals_scored": len(self.goals_per_club.get(club_id, []))
            } for club_id in self.club_ratings
        }
        return performance


class PlayerAnalytics:
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
        self.club_id, self.opponent_club_id = self._fetch_club_ids()

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

    def get_player_playing_time(self) -> Optional[Tuple[int, int]]:
        """Calculate playing time for a specific player in the game."""
        self.game_analysis.cur.execute("""
            SELECT minutes_played
            FROM appearances
            WHERE game_id = %s AND player_id = %s
        """, (self.game_analysis.game_id, self.player_id))

        result = self.game_analysis.cur.fetchone()
        if result:
            club_id, player_id, minutes_played = result
            end_time = minutes_played if minutes_played > 0 else self.game_analysis.FULL_GAME_MINUTES
            return 0, end_time
        return None

    def get_individual_expectation(self, opponent_club_id: int) -> float:
        """Calculate the expectation of this player's performance against an opponent."""
        self.game_analysis.fetch_game_data()
        club_ratings = self.game_analysis._game_data_cache["club_ratings"]

        self.game_analysis.cur.execute("""
            SELECT e.elo
            FROM players_elo e
            JOIN appearances a ON e.player_id = a.player_id
            WHERE a.game_id = %s AND e.player_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
        """, (self.game_analysis.game_id, self.player_id))

        elo = self.game_analysis.cur.fetchone()[0]
        mod = (club_ratings[opponent_club_id] - elo) / 400
        return 1 / (1 + pow(10, mod))


# Usage
with DatabaseConnection(DATABASE_CONFIG) as conn:
    with conn.cursor() as cur:
        # Initialize game-level analysis
        game_analysis = GameAnalysis(cur, game_id=3079452)

        # Analyze overall team performance
        team_performance = game_analysis.analyze_team_performance()
        print("Team Performance:", team_performance)

        # Individual player analysis based on shared game data
        player_analytics = PlayerAnalytics(game_analysis, player_id=20506)
        playing_time = player_analytics.get_player_playing_time()
        print(f"Player {player_analytics.player_id} Playing Time:", playing_time)

        expectation = player_analytics.get_individual_expectation(opponent_club_id=131)
        print(f"Player {player_analytics.player_id} Expectation:", expectation)
