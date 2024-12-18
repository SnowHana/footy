# Generated by Django 5.1.1 on 2024-09-12 08:07

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='PlayerStats',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('player', models.CharField(max_length=100)),
                ('nation', models.CharField(max_length=100)),
                ('position', models.CharField(max_length=10)),
                ('squad', models.CharField(max_length=100)),
                ('competition', models.CharField(max_length=100)),
                ('age', models.IntegerField()),
                ('born', models.IntegerField()),
                ('mp', models.IntegerField()),
                ('starts', models.IntegerField()),
                ('minutes', models.IntegerField()),
                ('nineties', models.FloatField()),
                ('goals', models.IntegerField()),
                ('assists', models.IntegerField()),
                ('goals_assists', models.IntegerField()),
                ('goals_minus_pens', models.IntegerField()),
                ('penalties', models.IntegerField()),
                ('penalties_attempted', models.IntegerField()),
                ('yellow_cards', models.IntegerField()),
                ('red_cards', models.IntegerField()),
                ('xg', models.FloatField()),
                ('npxg', models.FloatField()),
                ('xag', models.FloatField()),
                ('npxg_plus_xag', models.FloatField()),
                ('prog_carries', models.IntegerField()),
                ('prog_passes', models.IntegerField()),
                ('prog_runs', models.IntegerField()),
                ('goals_per_90', models.FloatField()),
                ('assists_per_90', models.FloatField()),
                ('goals_assists_per_90', models.FloatField()),
                ('goals_minus_pens_per_90', models.FloatField()),
                ('goals_assists_minus_pens', models.FloatField()),
                ('xg_per_90', models.FloatField()),
                ('xag_per_90', models.FloatField()),
                ('xg_plus_xag', models.FloatField()),
                ('npxg_per_90', models.FloatField()),
                ('npxg_plus_xag_per_90', models.FloatField()),
            ],
            options={
                'ordering': ['player', 'age'],
            },
        ),
    ]
