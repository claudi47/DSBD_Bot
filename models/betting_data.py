import json


class BettingData:
    def __init__(self):
        self._data = [] # _data in this way means private <field> data

    # the decorator "property" is used when the function below is a getter/setter of a class
    @property
    def data(self):
        return self._data

    def add_row(self, date, match, one, ics, two, gol, over, under):
        self._data.append({
            "date": date,
            "match": match,
            "one": one,
            "ics": ics,
            "two": two,
            "gol": gol,
            "over": over,
            "under": under
        })

    def to_json(self):
        return json.dumps(self._data)