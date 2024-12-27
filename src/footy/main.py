import subprocess
from pathlib import Path
import sys


def reset_players_elo():
    """
    Resets the players ELO table of Postgresql DB
    """
    # Build the absolute path to init_player_elo.py
    script_path = Path(__file__).parent / "player_elo" / "init_player_elo.py"
    try:
        print("\nResetting Players ELO table...")
        subprocess.run([sys.executable, str(script_path)], check=True)
        print("Players ELO Table reset successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during database reset: {e}\n")


def reset_db():
    """
    Function to reset the database by executing necessary scripts.
    """
    script_init_path = Path(__file__).parent / "player_elo" / "init_player_elo.py"
    script_reset_path = Path(__file__).parent / "player_elo" / "reset_players_elo.py"
    try:
        print("\nResetting database...")
        subprocess.run([sys.executable, str(script_init_path)], check=True)
        subprocess.run([sys.executable, str(script_reset_path)], check=True)
        print("Database reset successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during database reset: {e}\n")


def run_analysis():
    try:
        print("\nRunning analysis...")
        # Use the module path "footy.player_elo.elo_updater" instead of a .py file
        subprocess.run(
            [sys.executable, "-m", "footy.player_elo.elo_updater"], check=True
        )
        print("Analysis completed successfully!\n")
    except subprocess.CalledProcessError as e:
        print(f"Error during analysis: {e}\n")


def main():
    """
    Main function to display menu and handle user input.
    """
    while True:
        print("\nFootball Database Management Tool")
        print(
            "1. Reset Database : Delete and Create whole SQL DB from scratch. "
            "(Takes up to 3min, don't do this process unless you HAVE TO.)"
        )
        print("2. Reset Players ELO : Re-init. players ELO (Takes less than a minute)")
        print("3. Run Analysis : Continue on analysing ELO.")
        print("4. Exit")

        choice = input("Enter your choice (1/2/3/4): ").strip()

        if choice == "1":
            confirm = input("Do you really want to reset database? (y/n): ").strip()
            if confirm.lower() == "y":
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
