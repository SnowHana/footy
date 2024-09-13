from django.http import HttpResponse
from django.shortcuts import render
from django.views.generic import DetailView
from .models import Player, Team, PlayerStat


# Create your views here.


def home(request):
    context = {}
    # return render(request, "base/home.html", context)
    return HttpResponse("HELLO")


def players_all(request):
    players = Player.objects.all()[:50]
    context = {"players": players}

    return render(request, "base/players_all.html", context)


# class (DetailView):
#     model = Player
#     template_name = "player_detail.html"
#     context_object_name = "player"


# class TeamDetailView(DetailView):
#     model = Team
#     template_name = "team_detail.html"
#     context_object_name = "team"


# class PlayerStatDetailView(DetailView):
#     model = PlayerStat
#     template_name = "player_stats_detail.html"
#     context_object_name = "player_stats"
