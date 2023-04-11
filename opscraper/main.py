import argparse
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
import yaml

from opscraper.browser import Browser


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


WAIT_TIME = 1.5
NUM_WORKERS = 5
ROOT_FOLDER = os.path.dirname(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(ROOT_FOLDER, "config", "config.yaml")
OUTPUT_FOLDER = os.path.join(ROOT_FOLDER, "data", "odds")
SPORTS = [
    "football",
    "basketball",
    "esports",
    "darts",
    "tennis",
    "baseball",
    "rugby-union",
    "rugby-league",
    "american-football",
    "hockey",
    "volleyball",
    "handball",
]


def scrape_tournament(
    sport: str, country: str, tournament: str, start_year: int, end_year: int, output_path: str, wait_time: int
) -> None:
    driver = Browser(wait_time=wait_time)
    df_tournament_games = scrape_tournament_season(
        driver, sport, country, tournament, start_year, end_year, output_path
    )
    df_tournament_games_data = scrape_tournament_games(driver, df_tournament_games, output_path)
    save_collection_as_per_game_json(df_tournament_games_data, output_path)
    logger.info(f"Done with {sport}|{country}|{tournament}|From {start_year} to {end_year}")


def save_collection_as_per_game_json(df: pd.DataFrame, output_path: str) -> None:
    for _, game_row in df.iterrows():
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        sport = game_row["sport"]
        country = game_row["country"]
        tournament = game_row["tournament"]
        season_index = game_row["season_index"]
        game_uuid = game_row["game_uuid"]
        output_folder_path = os.path.join(output_path, sport, country, tournament, str(season_index))
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path, exist_ok=True)
        logger.info(f"Storing game {game_uuid} to {output_folder_path}")
        output_file_path = os.path.join(output_folder_path, f"{game_uuid}.json")
        with open(output_file_path, "w") as output_file:
            json.dump(game_row.to_dict(), output_file)


def scrape_game(
    driver: Browser, game_url: str, asian_handicap: bool, over_under: bool, correct_score: bool
) -> dict[str, Any]:
    logger.info(f"Scraping game {game_url}")
    driver.get_page(game_url)
    game_odds: dict[str, list[dict[str, Any]]] = {}
    if asian_handicap:
        odds_text = "Asian Handicap"
        odds_key = odds_text.lower().replace(" ", "_").replace("/", "_")
        game_odds[odds_key] = driver.get_game_odds_data(odds_text)
    if over_under:
        odds_text = "Over/Under"
        driver.odds_click(odds_text)
        odds_key = odds_text.lower().replace(" ", "_").replace("/", "_")
        game_odds[odds_key] = driver.get_game_odds_data(odds_text)
    return game_odds


def scrape_tournament_games(driver: Browser, df_tournament_games: pd.DataFrame, output_path: str) -> pd.DataFrame:
    game_data_list = []
    for _, game_row in df_tournament_games.iterrows():
        sport = game_row["sport"]
        country = game_row["country"]
        tournament = game_row["tournament"]
        season_index = game_row["season_index"]
        game_uuid = game_row["game_url"].split("/")[-2]
        output_file_path = os.path.join(output_path, sport, country, tournament, str(season_index), game_uuid)
        if os.path.exists(output_file_path):
            logger.info(f"Game {sport}|{tournament}|{season_index}{game_uuid} already existent...")
            continue
        game_data = scrape_game(driver, game_row["game_url"], asian_handicap=True, over_under=True, correct_score=False)
        game_data_list.append({**game_row, **game_data, **{"game_uuid": game_uuid}})
    return pd.DataFrame(game_data_list)


def scrape_tournament_season(
    driver: Browser,
    sport: str,
    country: str,
    tournament: str,
    start_year: int,
    end_year: int,
    output_path: str,
) -> pd.DataFrame:
    logger.info(f"Scrape tournament seasons for {sport}|{country}|{tournament}|from {start_year} to {end_year}")
    tmp_folder = os.path.join(output_path, sport, country, tournament)
    tmp_file = os.path.join(tmp_folder, "games.csv")
    if os.path.exists(tmp_file):
        return pd.read_csv(tmp_file)
    current_season_url = f"https://www.oddsportal.com/{sport}/{country}/{tournament}/results/"
    tournament_season_pages_data = []
    base_info = {
        "sport": sport,
        "country": country,
        "tournament": tournament,
    }
    tournament_seasons_urls = driver.get_tournament_seasons_urls(url=current_season_url)
    for season, season_url in tournament_seasons_urls.items():
        # Take 2020 from 2020/2021 (or 2020-2021) and 2021 if only 2021
        season_index = int(re.split(r"[/\-]", season)[0])
        if not (start_year <= season_index < end_year):
            logger.info(f"\tSkipping {sport}|{country}|{tournament}|{season}...")
            continue
        logger.info(f"Working on {sport}|{country}|{tournament}|{season}")
        num_pages = driver.get_num_pages(season_url)
        for num_page in range(1, num_pages + 1):
            page_url = f"{season_url.rstrip('/')}/#/page/{num_page}"
            page_games = driver.get_page_games_data(page_url)
            for page_game in page_games:
                tournament_season_pages_data.append(
                    {**base_info, "season": season, "season_index": season_index, "page_url": page_url, **page_game}
                )
    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder, exist_ok=True)
    df_tournament_season_games = pd.DataFrame(tournament_season_pages_data)
    df_tournament_season_games.to_csv(tmp_file, index=False)
    return df_tournament_season_games


def main(config_path: str, output_path: str, wait_time: int, num_workers: int) -> None:
    with open(config_path) as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        _ = [
            executor.submit(
                scrape_tournament,
                entry["sport"],
                entry["country"],
                entry["tournament"],
                entry.get("start_year", -1),
                entry.get("end_year", 2100),
                output_path,
                wait_time,
            )
            for entry in config["scraper"]
        ]


def choose_sport() -> str:
    while True:
        logger.info("What do you wanna scrape?")
        for idx, sport in enumerate(SPORTS):
            logger.info(f"\t[{idx + 1}] {sport}")
        chosen_sport = input("Selection: ")
        if not chosen_sport.isdigit():
            logger.error("Invalid selection, try again and insert a number within the list")
            continue
        idx_sport = int(chosen_sport)
        if not (0 < idx_sport < len(SPORTS)):
            logger.error("Selection out of bounds, try again and insert a number within the list")
            continue
        return SPORTS[idx_sport]


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OddsPortal Scraper v0.1")
    parser.add_argument("--wait-time", "-w", type=int, default=WAIT_TIME, help="Seconds to wait")
    parser.add_argument("--config", "-c", type=str, default=CONFIG_FILE, help="Path to config file")
    parser.add_argument("--output-path", "-o", type=str, default=OUTPUT_FOLDER, help="Output path")
    parser.add_argument("--workers", "-n", type=str, default=NUM_WORKERS, help="Number of threads")
    return parser


if __name__ == "__main__":
    args = create_parser().parse_args()
    main(config_path=args.config, output_path=args.output_path, wait_time=args.wait_time, num_workers=args.workers)
