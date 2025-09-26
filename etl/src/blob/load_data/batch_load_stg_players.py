import os
import sys
import subprocess

def main():
    """
    Calls stg_load_players.py multiple times (for 5 leagues).
    Adjust league_folder and season_str as needed.
    This approach runs them one after another in the same Python process,
    so you don't have to run them manually.
    """
    # Suppose you have these league folders (the same you used in fetch script):
    leagues = [
        ("premier_league", "2024_25"),
        ("la_liga",        "2024_25"),
        ("bundesliga",     "2024_25"),
        ("serie_a",        "2024_25"),
        ("ligue_1",        "2024_25")
    ]

    for (league_folder, season_str) in leagues:
        print(f"\n=== Loading stg_players for {league_folder} / {season_str} ===")
        cmd = [
            sys.executable,  # python interpreter
            "load_stg_players.py",  # your script's name
            "--league_folder", league_folder,
            "--season_str",    season_str
        ]
        try:
            # Run the command
            subprocess.check_call(cmd)
            print(f"Successfully loaded stg_players for {league_folder}, {season_str}")
        except subprocess.CalledProcessError as e:
            print(f"Error loading stg_players for {league_folder}, {season_str}: {e}")
    
    print("\nAll leagues loaded into stg_players.")

if __name__ == "__main__":
    main()
