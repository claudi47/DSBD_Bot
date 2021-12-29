import requests
from models.betting_data import BettingData


def run(category):
    betting_data = BettingData()
    url_list = {
        'serie_A': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=93',
        'champions_league': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=26534',
        'europa_league': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=247944',
        'premier_league': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=26604',
        'la_liga': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=95',
        'bundesliga': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=84',
        'ligue_1': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=86',
        'serie_B': 'https://www.goldbet.it/getOverviewEvents/0?idDiscipline=0&idTournament=1626630'
    }
    response = requests.get(url_list.get(category, url_list['serie_A']))
    json_data = response.json()

    matches = json_data['leo']
    for match in matches:
        date = match['ed']
        teams = match['en'].split(' - ')

        quotes_1X2 = match['mmkW']['111;355;4001;0;0']
        quote_values = quotes_1X2['spd']['0.0']['asl']
        list_values = []
        for value in quote_values:
            list_values.append(str(value['ov']))

        quotes_ov = match['mmkW']['111;355;4002;0;0']
        ov_values = quotes_ov['spd']['2.5']['asl']
        list2_values = []
        for value in ov_values:
            list2_values.append(str(value['ov']))

        betting_data.add_row(date, ' - '.join(teams), list_values[0].replace(",", "."), list_values[1].replace(",", "."), list_values[2].replace(",", "."),
                             '2.5', list2_values[1].replace(",", "."), list2_values[0].replace(",", "."))

    return betting_data