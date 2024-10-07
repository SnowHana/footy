# Generated by Django 4.1 on 2024-09-27 23:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0008_auto_20240927_2250'),
    ]

    operations = [
        migrations.AddField(
            model_name='club',
            name='avg_age',
            field=models.PositiveIntegerField(blank=True, default=1000, null=True),
        ),
        migrations.AddField(
            model_name='club',
            name='club_code',
            field=models.CharField(default='CR7', max_length=100),
        ),
        migrations.AddField(
            model_name='club',
            name='elo',
            field=models.FloatField(default=1000),
        ),
        migrations.AddField(
            model_name='club',
            name='squad_size',
            field=models.PositiveIntegerField(blank=True, default=1000, null=True),
        ),
        migrations.AddField(
            model_name='club',
            name='total_market_value',
            field=models.DecimalField(blank=True, decimal_places=2, default=1000, max_digits=20, null=True),
        ),
    ]
