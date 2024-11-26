import logging

from src.player_elo.base_analysis import BaseAnalysis
from src.player_elo.club_analysis import ClubAnalysis
from src.player_elo.database_connection import DatabaseConnection, DATABASE_CONFIG
from src.player_elo.game_analysis import GameAnalysis


class PlayerAnalysis(BaseAnalysis):
    """
    Analysis specific to player performance, inheriting shared logic from BaseAnalysis.
    """

    def __init__(self, game_analysis: GameAnalysis, player_id: int):
        super().__init__(game_analysis, entity_id=player_id, k_value=1, q_value=1)
        # IDK maybe delete this cuz it's confusing?
        self.player_id = player_id
        self._club_id = None

    @property
    def club_id(self) -> int:
        if self._club_id is None:
            self._club_id = self._get_club_id()
        return self._club_id

    def _fetch_elo(self) -> float:
        # self.game_analysis.cur.execute("""
        #                 SELECT e.elo
        #                 FROM players_elo e
        #                 JOIN appearances a ON e.player_id = a.player_id
        #                 WHERE a.game_id = %s AND e.player_id = %s AND EXTRACT(YEAR FROM a.date::date) = e.season
        #             """, (self.game_analysis.game_id, self.entity_id))
        # TODO: Make this use game_analysis elo attr instead of querying
        # Because there are tooooooo many empty info in appearances table
        return self.game_analysis.cur.fetchone()[0]

    def _calculate_expectation(self) -> float:
        opponent_elo = self.game_analysis.club_ratings[self.opponent_id]
        return 1 / (1 + pow(10, (opponent_elo - self.elo) / 400))

    def _get_goal_difference(self) -> int:
        return self.game_analysis.match_impact_players[(self._get_club_id(), self.entity_id)]

    def _get_minutes_played(self) -> int:
        start_min, end_min = self.game_analysis.players_play_times[(self._get_club_id(), self.entity_id)]
        return end_min - start_min

    def _get_club_id(self) -> int:
        """
        Retrieve the club ID for the player.
        """
        # self.game_analysis.cur.execute("""
        #         SELECT player_club_id
        #         FROM appearances
        #         WHERE game_id = %s AND player_id = %s
        #     """, (self.game_analysis.game_id, self.entity_id))
        # return self.game_analysis.cur.fetchone()[0]
        try:
            for club_id, club_players in self.game_analysis.players.items():
                if self.entity_id in club_players:
                    return club_id
            raise KeyError(f"Error: Could not find Player {self.entity_id} in game {self.game_analysis.game_id}")
        except KeyError as e:
            logging.error(e)
            raise

    def _get_opponent_id(self) -> int:
        """
        Get Opponent Club's ID
        @todo: Later instead of using player's play time, do sth else, like creating a team data for GameAnalysis?
        @return:
        """
        for club_id, player_id in self.game_analysis.players_play_times.keys():
            if player_id == self.entity_id:
                return club_id

    def new_elo(self, team_elo_change: float) -> float:
        """
        @param: team_elo_change: Team ELO Change (C_A)
        @return:
        """

        return self.elo + self.k_value * ((self.q_value * self.calculate_change())
                                          + ((1 - self.q_value) * team_elo_change * (
                        self.minutes_played / self.MINUTES_MAX)))
#
#
# with DatabaseConnection(DATABASE_CONFIG) as conn:
#     with conn.cursor() as cur:
#         # Initialize game analysis with game ID
#         game_analysis = GameAnalysis(cur, game_id=3079452)
#
#         # Initialize BaseAnalysis for a club entity
#         home_club_analysis = ClubAnalysis(game_analysis, game_analysis.home_club_id)
#         away_club_analysis = ClubAnalysis(game_analysis, game_analysis.away_club_id)
#         # Calculate ELO and expectation
#         print("Club ELO:", home_club_analysis.elo)
#         print("Club ELO:", away_club_analysis.elo)
#         print("Club Expectation:", home_club_analysis.expectation)
#         print("Club Expectation:", away_club_analysis.expectation)
#         print("Club MP", home_club_analysis.minutes_played)
#         print("Club GD", home_club_analysis.goal_difference)
#         print("Club GD", away_club_analysis.goal_difference)
#
#         # Test Player analysis\
#         players = [player for club, player in game_analysis.players_play_times.keys()]
#         player_analysis = PlayerAnalysis(game_analysis, players[5])
#         club_analysis = ClubAnalysis(game_analysis, player_analysis.club_id)
#         # Testing player analysis
#         print("Player: ", player_analysis.entity_id)
#         print("Player Expectation:", player_analysis.expectation)
#         print("Player MP", player_analysis.minutes_played)
#         print("Player GD", player_analysis.goal_difference)
#         print("Player ELO:", player_analysis.elo)
#         print("Player ELO Change", player_analysis.new_elo(club_analysis.calculate_change()))

        # Update the club's ELO based on actual game score (e.g., actual_score=1.0 if they won)
        # updated_elo = club_analysis.update_elo(actual_score=1.0, weight=0.5)
        # print("Updated Club ELO:", updated_elo)

        # Initialize BaseAnalysis for a player entity
        # player_analysis = BaseAnalysis(game_analysis=game_analysis, entity_id=20506, is_club=False)
        #
        # # Calculate ELO and expectation for the player
        # print("Player ELO:", player_analysis.elo)
        # print("Player Expectation:", player_analysis.expectation)
        #
        # # Update the player's ELO based on actual performance score
        # updated_player_elo = player_analysis.update_elo(actual_score=0.5, weight=0.3)
        # print("Updated Player ELO:", updated_player_elo)
