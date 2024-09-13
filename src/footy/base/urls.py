from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("players_all/", views.players_all, name="players_all"),
]
