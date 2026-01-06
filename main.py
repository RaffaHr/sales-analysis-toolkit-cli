from pathlib import Path

from analysis.cli import run_cli

if __name__ == "__main__":
    default_file = Path("BASE.xlsx")
    run_cli(default_file)