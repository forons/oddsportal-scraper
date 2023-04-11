import logging
import time
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_all_elements_located
from selenium.webdriver.support.wait import WebDriverWait


logger = logging.getLogger()


class Browser:
    def __init__(self, wait_time: int, timeout: int = 30):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        # Un-comment next line to supress logging:
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.driver = webdriver.Chrome(options=options)
        self.wait_time = wait_time
        self.timeout = timeout
        logger.info("Chrome browser opened in headless mode")

    def quit(self):
        self.driver.quit()
        logger.info("The driver has been quit")

    def get_page(self, url: str, css_selector: Optional[str] = None, scroll_to_end: bool = False):
        self.driver.get(url)
        if css_selector:
            WebDriverWait(self.driver, self.timeout).until(
                presence_of_all_elements_located((By.CSS_SELECTOR, css_selector))
            )
        if scroll_to_end:
            self.scroll_to_until_page_end()
        time.sleep(self.wait_time)

    def scroll_to_until_page_end(self) -> None:
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(self.wait_time)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_tournament_seasons_urls(self, url: str) -> dict[str, str]:
        css_selector = ".mt-5:nth-child(4)>div:last-child:not(.hidden)>div:last-child>a"
        self.get_page(url, css_selector)
        page_soup = BeautifulSoup(self.driver.page_source, "lxml")
        season_links = {}
        for row in page_soup.select(css_selector):
            season_name = row.text.strip()
            season_links[season_name] = row.attrs["href"].strip()
        return season_links

    def get_num_pages(self, season_url: str) -> int:
        self.get_page(season_url, css_selector="#overflow_text>.about-text:first-child>.cms")
        page_soup = BeautifulSoup(self.driver.page_source, "lxml")
        has_odds = self.has_odds(page_soup)
        if not has_odds:
            return 0
        css_selector = "#pagination>a"
        pagination_links = page_soup.select(css_selector)
        if len(pagination_links) == 0:
            logger.warning(f"Only current page in html, please check {season_url}")
            return 1
        return max(int(link.attrs["x-page"]) for link in pagination_links)

    @staticmethod
    def has_odds(page_soup: BeautifulSoup) -> bool:
        missing_odds_text = (
            "Unfortunately, no matches can be displayed because there are no odds available "
            "from your selected bookmakers."
        )
        missing_odds_soup = page_soup.select_one(r".gap-\[2px\]>p")
        if missing_odds_soup:
            if missing_odds_soup.get_text(" ").strip() == missing_odds_text:
                return False
        return True

    def get_page_games_data(self, page_url: str) -> list[dict[str, Any]]:
        self.get_page(page_url, "div[set]>div:last-child a:has(a[title])~div:not(.hidden)")
        page_soup = BeautifulSoup(self.driver.page_source, "lxml")
        page_game_data: list[dict[str, Any]] = []
        game_group_data: dict[str, Any] = {}

        for row_soup in page_soup.select("div[set]>div:last-child"):
            game_group_data.update(self.get_page_game_group_data(row_soup))
            game_data = self.get_game_data(row_soup)

            page_game_data.append({**game_group_data, **game_data})
        return page_game_data

    @staticmethod
    def get_page_game_group_data(row_soup: Tag) -> dict[str, Any]:
        game_group_data = {}
        if row_soup.parent.select(":scope>div:first-child+div+div"):
            game_group_css_selectors = {
                "date": ":scope>div:first-child+div>div:first-child",
                "game_country": ":scope>div:first-child>a:nth-of-type(2):nth-last-of-type(2)",
                "game_league": ":scope>div:first-child>a:nth-of-type(3):last-of-type",
            }
            for key, css_selector in game_group_css_selectors.items():
                game_group_data[key] = Browser.parse_value(row_soup, css_selector, default=None)
        return game_group_data

    @staticmethod
    def parse_value(row_soup: Tag, css_selector: str, default: Any) -> Any:
        target_soup = row_soup.parent.select_one(css_selector)
        return target_soup.get_text(" ").strip() if target_soup else default

    @staticmethod
    def get_game_data(row_soup: Tag) -> dict[str, Any]:
        game_url = row_soup.select_one("a").attrs["href"].strip()
        base_url = "https://www.oddsportal.com/"
        game_data = {
            "game_url": f"{base_url.rstrip('/')}/{game_url.lstrip('/')}",
            "retrieval_datetime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }
        game_css_selectors = {
            "time": "p.whitespace-nowrap",
            "home_team": "a div>a[title]:first-of-type>div",
            "away_team": "a div>a[title]:last-of-type>div",
            "home_score": "a:has(a[title]) .hidden:not(.liveSize):first-of-type",
            "away_score": "a:has(a[title]) .hidden:not(.liveSize):last-of-type",
            "home_odds": "a:has(a[title])~div:not(.hidden)",
            "draw_odds": "a:has(a[title])~div:not(.hidden)+div:nth-last-of-type(3)",
            "away_odds": "a:has(a[title])~div:nth-last-of-type(2)",
        }
        for key, css_selector in game_css_selectors.items():
            game_data[key] = Browser.parse_value(row_soup, css_selector, default=None)
        teams_soup = row_soup.select("a div>a[title]")
        if len(teams_soup) != 2:
            raise ValueError()
        return game_data

    def odds_click(self, text: str):
        self.driver.find_element(By.XPATH, f"//div[@class[contains(.,'prio-odds')]]//li[. = '{text}']").click()
        time.sleep(self.wait_time)

    def get_game_odds_data(self, odds_text: str):
        self.odds_click(odds_text)
        page_soup = BeautifulSoup(self.driver.page_source, "lxml")
        odds_names = {
            "Over/Under": {"first": "over", "second": "under"},
            "Asian Handicap": {"first": "home", "second": "away"},
        }

        game_odds = {}
        for row_soup in page_soup.select("div[set]>div.cursor-pointer:last-child"):
            odds_value = Browser.parse_value(row_soup, "div>p:first-of-type", default=None)
            first_entry_value = Browser.parse_value(
                row_soup, "div:nth-of-type(3)>div:first-of-type>div>div>p", default=None
            )
            second_entry_value = Browser.parse_value(
                row_soup, "div:nth-of-type(3)>div:nth-of-type(2)>div>div>p", default=None
            )
            payout_value = Browser.parse_value(row_soup, ".colaps-btn", default=None)
            game_odds[odds_value] = {
                odds_names[odds_text]["first"]: first_entry_value,
                odds_names[odds_text]["second"]: second_entry_value,
                "payout": payout_value,
            }
        return game_odds
