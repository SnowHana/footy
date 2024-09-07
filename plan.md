# Footy project
## Objective
1. Use django to create a website
2. Full premier league squad, with footballer profile etc?
    - allow user to re-direct to fbref, transfermarket webiste with a hyperlink
3. Being able to plan and form user's custom squad
4. Being able to 'simulate' or battle against other squad
5. Custom *budget*, *team-ranking* etc?
6. Use Postgre sql cuz we have A LOT of data

---
## Plan
1. Website - Django should be fine
2. Battle..I'm not sure atm but probs use some deep learning shit
3. Scraping : https://www.youtube.com/watch?v=rfM3Jli81fU
https://github.com/hoyishian/footballwebscraper


### PostgreSQL vs Sqlite
Migration from SQLite to PostgreSQL can be relatively straightforward, especially if you're starting with a small project.

 Here are the general steps involved:

    Create a PostgreSQL database: Set up a PostgreSQL instance and create a new database for your project.
    Install Django's PostgreSQL adapter: Ensure you have the psycopg2 library installed, which is required for Django to connect to PostgreSQL.
    Configure Django settings: Update your settings.py file to use the PostgreSQL database settings.
    Run migrations: Use python manage.py migrate to apply your existing migrations to the new PostgreSQL database.
    Test your application: Verify that your application works as expected with the PostgreSQL database.

However, there are a few things to keep in mind:

    Data types: Ensure that your SQLite data types are compatible with PostgreSQL. There might be minor differences, such as the default precision of floating-point numbers.
    Indexes: If you have custom indexes defined in SQLite, you'll need to recreate them in PostgreSQL.
    Constraints: Check if any constraints (e.g., UNIQUE, FOREIGN KEY) need to be adjusted or recreated.
    Full-text search (if applicable): If you're using full-text search features, you might need to configure them differently in PostgreSQL.

For more complex applications or large datasets, it's recommended to use a tool like pg_dump to create a database dump from SQLite and then import it into PostgreSQL. This can help automate the migration process and reduce the risk of errors.


> So use SQLite for now on, and move on to postgre sql later on


### Progress
#### 8th Sep

- Forget about automatic update about scraping cuz thats too aids
    - Later on we can add it. 
    - https://www.youtube.com/watch?v=rfM3Jli81fU

- Right now finish scraping Standard Stats about Current season or any other season
- Then, we can do sth like importing an alr existing csv file into a django model
- Display a single player profile?