# Generated by Django 4.1 on 2024-09-14 02:08

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0004_rename_playerstats_playerstat'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='playerstat',
            name='slug',
        ),
    ]