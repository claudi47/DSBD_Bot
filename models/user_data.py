class UserData:
    def __init__(self, user_id, username):
         self._user_id = user_id
         self._username = username

    @property
    def data(self):
        return {
            'user_id': self._user_id,
            'username': self._username
        }