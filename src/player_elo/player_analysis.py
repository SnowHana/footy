from src.player_elo.base_analysis import BaseAnalysis
from src.player_elo.player_elo import GameAnalysis


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
