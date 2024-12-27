import subprocess


def reset_players_elo():
    """
    Resets the players ELO table of Postgresql DB
    @return:
    """
    try:
        print("\nResetting Players ELO table...")
        # subprocess.run(["python", "init_player_elo.py"], check=True)
        subprocess.run(["python", "reset_players_elo.py"], check=True)

        print("Players ELO Table reset successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during database reset: {e}\n")


def reset_db():
    """
    Function to reset the database by executing necessary scripts.
    """
    try:
        print("\nResetting database...")
        # subprocess.run(["python", "init_player_elo.py"], check=True)
        subprocess.run(["python", "init_sql.py"], check=True)
        subprocess.run(["python", "game_validator.py"], check=True)
        # subprocess.run(["python", "reset_players_elo.py"], check=True)
        # reset_players_elo()
        print("Database reset successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during database reset: {e}\n")


def run_analysis():
    """
    Function to run the analysis by executing the analysis script.
    """
    try:
        print("\nRunning analysis...")
        subprocess.run(["python", "elo_updater.py"], check=True)
        print("Analysis completed successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during analysis: {e}\n")


def main():
    """
    Main function to display menu and handle user input.
    """
    while True:
        print("\nFootball Database Management Tool")
        print("1. Reset Database : Delete and Create whole SQL DB from scratch. "
              "(Takes up to 3min, don't do this process unless you HAVE TO.)")
        print("2. Reset Players ELO : Re-init. players ELO (Takes less than a minute)")
        print("3. Run Analysis : Continue on analysing ELO.")
        print("4. Exit")

        choice = input("Enter your choice (1/2/3/4): ").strip()

        if choice == "1":
            confirm = input("Do you really want to reset database? (y/n): ").strip()
            if confirm == "y":
                reset_db()
            else:
                print("Exiting...")
        elif choice == "2":
            reset_players_elo()
        elif choice == "3":
            run_analysis()
        elif choice == "4":
            print("Exiting the program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
