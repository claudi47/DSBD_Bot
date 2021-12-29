from selenium import webdriver  # necessary for web dynamic pages
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from models.betting_data import BettingData


def run(category):
    betting_data = BettingData()

    url = 'https://sports.bwin.it/it/sports/calcio-4'
    url_list = {
        'serie_A': '/scommesse/italia-20/serie-a-102846',
        'champions_league': '/scommesse/europa-7/champions-league-0:3',
        'europa_league': '/scommesse/europa-7/uefa-europa-league-0:5',
        'premier_league': '/scommesse/inghilterra-14/premier-league-102841',
        'la_liga': '/scommesse/spagna-28/laliga-102829',
        'bundesliga': '/scommesse/germania-17/bundesliga-102842',
        'ligue_1': '/scommesse/francia-16/ligue-1-102843',
        'serie_B': '/scommesse/italia-20/serie-b-102848'
    }

    serv = Service('chromedriver.exe')
    chrome_opts = webdriver.ChromeOptions()
    chrome_opts.add_argument('window-size=1920,1080')
    chrome_opts.add_argument('headless')

    with webdriver.Chrome(service=serv, options=chrome_opts) as driver:
        driver.implicitly_wait(3)
        driver.get(url + url_list.get(category, url_list['serie_A']))
        web_element = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.CSS_SELECTOR, '.grid')))
        ms_event_groups = WebDriverWait(driver, 10).until(
            lambda x: web_element.find_elements(By.CSS_SELECTOR, '.grid > ms-event-group'))

        for ms_event_group in ms_event_groups:

            header_date = WebDriverWait(driver, 10).until(
                lambda x: ms_event_group.find_element(By.CSS_SELECTOR, 'ms-event-group > .header-wrapper'))

            day = WebDriverWait(driver, 10).until(
                lambda x: header_date.find_element(By.CSS_SELECTOR, 'ms-event-group .date-group'))

            header_1X2 = WebDriverWait(driver, 10).until(
                lambda x: ms_event_group.find_element(By.CSS_SELECTOR, 'ms-event-group > div > ms-group-header'))
            digit_list = WebDriverWait(driver, 10).until(
                lambda x: header_1X2.find_elements(By.CSS_SELECTOR, 'ms-group-header > div'))

            header_uo = WebDriverWait(driver, 10).until(
                lambda x: ms_event_group.find_element(By.CSS_SELECTOR, 'ms-event-group > div > ms-group-header:nth-of-type(2)'))

            over_under = WebDriverWait(driver, 10).until(
                lambda x: header_uo.find_elements(By.CSS_SELECTOR,'ms-group-header > div'))

            # map allows us to apply a function to every single element of a list and returns the new object iterator
            # list allows us to convert an iterable object to a list
            list_digit = list(map(lambda x: x.get_property('textContent'), digit_list))
            new_list_digit = []
            for element in list_digit:
                new_list_digit.append(element.replace('\n', ''))

            list_ou = list(map(lambda x: x.get_property('textContent'), over_under))
            new_ou_list = []
            for element in list_ou:
                new_ou_list.append(element.replace('\n', ''))

            ms_events = WebDriverWait(driver, 10).until(
                lambda x: ms_event_group.find_elements(By.CSS_SELECTOR, 'ms-event-group > ms-event'))

            for ms_event in ms_events:
                team1 = WebDriverWait(driver, 10).until(
                    lambda x: ms_event.find_element(By.CSS_SELECTOR, '.grid-event-detail > .grid-event-name  .participants-pair-game > div:nth-of-type(1)'))
                team2 = WebDriverWait(driver, 10).until(
                    lambda x: ms_event.find_element(By.CSS_SELECTOR, '.grid-event-detail > .grid-event-name  .participants-pair-game > div:nth-of-type(2)'))

                previsions = WebDriverWait(driver, 10).until(
                    lambda x: ms_event.find_elements(By.CSS_SELECTOR, '.grid-group-container > ms-option-group:nth-of-type(1) > ms-option'))
                list_previsions = list(map(lambda x: x.get_property('textContent'), previsions))
                quotes = []
                for element in list_previsions:
                    quotes.append(element)

                gol_ou = WebDriverWait(driver, 5).until(
                    lambda x: ms_event.find_element(By.CSS_SELECTOR, 'ms-event .option-group-attribute'))
                over_under = WebDriverWait(driver, 10).until(
                    lambda x: ms_event.find_elements(By.CSS_SELECTOR, 'ms-event .grid-group-container > ms-option-group:nth-of-type(2) > ms-option .option-value'))
                list_prev_ou = list(map(lambda x: x.get_property('textContent'), over_under))
                ov_un = []
                for element in list_prev_ou:
                    ov_un.append(element)

                betting_data.add_row(day.get_property('textContent').upper(),
                                     team1.get_property('textContent') + " -" + team2.get_property('textContent'),
                                     quotes[0].replace(",", "."), quotes[1].replace(",", "."), quotes[2].replace(",", "."),
                                     gol_ou.get_property('textContent').replace(",", "."), ov_un[0].replace(",", "."), ov_un[1].replace(",", "."))

    return betting_data