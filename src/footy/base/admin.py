from django.contrib import admin

from .models import Player, PlayerStat, Team


# Register your models here.
@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    list_display = ("name",)  # Fields to display in the list view
    search_fields = ("name",)  # Fields to include in the search functionality
    ordering = ("name",)  # Default ordering of the list


@admin.register(Player)
class PlayerAdmin(admin.ModelAdmin):
    list_display = ("name", "position", "team")  # Fields to display in the list view
    search_fields = (
        "name",
        "position",
        "team__name",
    )  # Fields to include in the search functionality
    list_filter = ("team",)  # Add filters for easy navigation
    ordering = ("name",)  # Default ordering of the list


@admin.register(PlayerStat)
class PlayerStatAdmin(admin.ModelAdmin):
    list_display = (
        "player",
        "competition",
        "goals",
        "assists",
    )  # Fields to display in the list view
    search_fields = (
        "player__name",
        "competition",
    )  # Fields to include in the search functionality
    list_filter = ("player", "competition")  # Add filters for easy navigation
    ordering = ("-player",)  # Default ordering of the list
