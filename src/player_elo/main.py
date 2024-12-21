import os
import subprocess


def reset_db():
    """
    Function to reset the database by executing necessary scripts.
    """
    try:
        print("\nResetting database...")
        # subprocess.run(["python", "init_player_elo.py"], check=True)
        subprocess.run(["python", "init_sql.py"], check=True)
        subprocess.run(["python", "reset_players_elo.py"], check=True)
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
        print("1. Reset Database : Delete and Create whole SQL DB from scratch. (Takes up to 10min)")
        print("2. Run Analysis : Continue on analysing ELO.")
        print("3. Exit")

        choice = input("Enter your choice (1/2/3): ").strip()

        if choice == "1":
            reset_db()
        elif choice == "2":
            run_analysis()
        elif choice == "3":
            print("Exiting the program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
