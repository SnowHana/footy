from django.http import HttpResponse
from django.shortcuts import render

# Create your views here.


def home(request):
    context = {}
    # return render(request, "base/home.html", context)
    return HttpResponse("HELLO")
