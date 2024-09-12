from itertools import islice
from typing import Any
import csv

from django.conf import settings
from base.models import PlayerStats
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Load data from standard_stas_big5 file"

    def handle(self, *args: Any, **options: Any) -> str | None:
        datafile = settings.BASE_DIR / "data" / "standard_stats_big5.csv"

        with open(datafile, "r") as csvfile:
            reader = csv.DictReader(
                islice(
                    csvfile,
                )
            )
            
            for row in reader:
                

        raise NotImplementedError
        return super().handle(*args, **options)
