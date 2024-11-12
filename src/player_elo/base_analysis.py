# from .elo_calculation_mixin import ELOCalculationMixin
from .player_elo import GameAnalysis


class BaseAnalysis:
    """
    Base class providing common ELO calculation and expectation methods for both Club and Player analysis.
    """

    def __init__(self, game_analysis: GameAnalysis, entity_id: int, is_club: bool = True):
        """
        Initialize.

        @param game_analysis: Instance of GameAnalysis providing game context
        @param entity_id: ID of the entity (club or player) to analyze
        @param is_club: Boolean indicating if the entity is a club (True) or a player (False)
        """
        self.game_analysis = game_analysis
        self.entity_id = entity_id
        self.is_club = is_club
        self._elo = None
        self._expectation = None
        self._game_score = None

    def fetch_elo(self) -> float:
        """
        Retrieve or calculate the ELO for the entity (club or player).
        """
        if self.is_club:
            # Get rating of CLUB
            return self.game_analysis.club_ratings.get(self.entity_id, 0)
        else:
            # Get rating of PLAYER from player elos with ID, Season
            self.game_analysis.cur.execute("""
                SELECT e.elo
                FROM players_elo e
                JOIN appearances a ON e.player_id = a.player_id
                WHERE a.game_id = %s AND e.player_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
            """, (self.game_analysis.game_id, self.entity_id))
            return self.game_analysis.cur.fetchone()[0]

    def calculate_expectation(self) -> float:
        """
        Calculate the expected performance based on ELO ratings.
        """
        if self.is_club:
            opponent_id = (
                self.game_analysis.home_club_id if self.entity_id == self.game_analysis.away_club_id
                else self.game_analysis.away_club_id)
            opponent_elo = self.game_analysis.club_ratings[opponent_id]
        else:
            opponent_elo = self.game_analysis.club_ratings[self._get_opponent_club_id()]

        return 1 / (1 + pow(10, (opponent_elo - self.elo) / 400))

    def update_elo(self, actual_score: float, weight: float) -> float:
        """
        Update the entity's ELO based on actual performance vs. expected.
        """
        expected_score = self.expectation
        goal_difference = self._get_goal_difference()
        change = self.calculate_change(
            expectation=expected_score,
            game_score=actual_score,
            weight=weight,
            goal_difference=goal_difference,
            minutes_played=self._get_minutes_played()
        )
        self._elo += change
        return self._elo

    def _get_goal_difference(self) -> int:
        """
        Retrieve goal difference for clubs or match impact for players.
        """
        if self.is_club:
            goals_for = len(self.game_analysis.goals_per_club[self.entity_id])
            opponent_id = (
                self.game_analysis.home_club_id if self.entity_id == self.game_analysis.away_club_id else self.game_analysis.away_club_id)
            goals_against = len(self.game_analysis.goals_per_club[opponent_id])
            return goals_for - goals_against
        else:
            return self.game_analysis.match_impact_players[(self._get_club_id(), self.entity_id)]

    def _get_minutes_played(self) -> int:
        """
        Get the minutes played by the player or return full game duration for clubs.
        """
        if self.is_club:
            return self.game_analysis.FULL_GAME_MINUTES
        else:
            start_min, end_min = self.game_analysis.players_play_times[(self._get_club_id(), self.entity_id)]
            return end_min - start_min

    @property
    def elo(self) -> float:
        if self._elo is None:
            self._elo = self.fetch_elo()
        return self._elo

    @property
    def expectation(self) -> float:
        if self._expectation is None:
            self._expectation = self.calculate_expectation()
        return self._expectation

    @property
    def game_score(self) -> float:
        if self._game_score is None:
            self._game_score = self.calculate_game_score(analysis=self)
        return self._game_score

    # @staticmethod
    def calculate_game_score(self) -> float:
        """
        Calculate Game Score based on match impact (goal difference).
        """
        # TODO: Not done yet.
        match_impact = None

        if self.is_club:
            home_goals = len(self.game_analysis.goals_per_club.get(self.game_analysis.home_club_id, []))
            away_goals = len(self.game_analysis.goals_per_club.get(self.game_analysis.away_club_id, []))
            match_impact = home_goals - away_goals

        else:
            # Player
            match_impact = self.game_analysis.match_impact_players()

        if match_impact > 0:
            return 1.0
        elif match_impact == 0:
            return 0.5
        else:
            return 0.0

    @staticmethod
    def calculate_change(expectation: float, game_score: float, weight: float,
                         goal_difference: int = 0, minutes_played: int = 0,
                         minutes_max: int = 90) -> float:
        """
        Calculate the change in score.
        """
        res = weight * (game_score - expectation)
        if goal_difference == 0:
            res *= (minutes_played / minutes_max)
        else:
            res *= (abs(goal_difference) ** (1 / 3))
        return res


class PlayerAnalysis(BaseAnalysis):
    """
    Analysis specific to player performance, inheriting shared logic from BaseAnalysis.
    """

    def __init__(self, game_analysis: GameAnalysis, player_id: int):
        super().__init__(game_analysis, entity_id=player_id, is_club=False)

    def _get_club_id(self) -> int:
        """
        Retrieve the club ID for the player.
        """
        self.game_analysis.cur.execute("""
                SELECT player_club_id
                FROM appearances
                WHERE game_id = %s AND player_id = %s
            """, (self.game_analysis.game_id, self.entity_id))
        return self.game_analysis.cur.fetchone()[0]


class ClubAnalysis(BaseAnalysis):

    def __init__(self, game_analysis: GameAnalysis, club_id: int):
        super().__init__(game_analysis, entity_id=club_id, is_club=True)
