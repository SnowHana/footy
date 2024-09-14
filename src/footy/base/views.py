import io
import base64
import matplotlib.pyplot as plt
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView
from .models import Player, Team, PlayerStat
import matplotlib

matplotlib.use("Agg")  # Ensure non-GUI backend for rendering on macOS
# Create your views here.


def home(request) -> HttpResponse:
    context = {}
    # return render(request, "base/home.html", context)
    return HttpResponse("HELLO")


def players_all(request) -> HttpResponse:
    players = Player.objects.all()[:50]
    context = {"players": players}

    return render(request, "base/players_all.html", context)


def player_stats(request, slug):
    player_stats = get_object_or_404(
        PlayerStat,
        slug=slug,
    )

    # Get fileds of Player Stats value

    fields = [
        (field.verbose_name, field.value_from_object(player_stats))
        for field in PlayerStat._meta.fields
        if field.name not in ["id", "slug", "player", "competition"]
    ]

    context = {"player_stats": player_stats, "fields": fields}
    return render(request, "base/player_stats.html", context)


def generate_graph(request, slug):
    """Generate a graph for a player stat detailed view

    Args:
        request (_type_): _description_
        slug (_type_): _description_

    Returns:
        _type_: _description_
    """
    feature = request.GET.get("feature", None)  # Get the feature from the AJAX request

    # Ensure correct object retrieval or 404 if not found
    player_stat = get_object_or_404(PlayerStat, slug=slug)

    # Create a simple graph based on the feature clicked
    plt.figure(figsize=(6, 4))
    plt.title(f"{player_stat.player.name} - {feature}")

    # Example: Display the clicked feature value as a bar graph
    feature_value = getattr(
        player_stat, feature.lower().replace(" ", "_"), None
    )  # Convert feature name to field name

    if feature_value is not None:
        plt.bar([feature], [feature_value])
    else:
        plt.text(
            0.5,
            0.5,
            "Feature not found",
            horizontalalignment="center",
            verticalalignment="center",
        )

    # Convert the graph to a PNG image and encode it as base64
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    graph_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
    buffer.close()

    return JsonResponse({"graph": graph_base64})


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
