from models.betting_data import BettingData
from models.user_data import UserData

class SearchData:
    def __init__(self, bet_data:BettingData, user_data:UserData, website):
        self._bet_data = bet_data
        self._user_data = user_data
        self._web_site = website

    @property
    def data(self):
        return {
            **self._user_data.data, # returning the dict returned by UserData class
            'data': self._bet_data.data,
            'web_site': self._web_site
        }