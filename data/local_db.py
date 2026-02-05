from config import INITIAL_BALANCE

class LocalDB:
    def __init__(self):
        self.users = {}

    def create_user(self, user_id):
        if user_id not in self.users:
            self.users[user_id] = {
                "balance": INITIAL_BALANCE,
                "portfolio": {}
            }

    def get_user(self, user_id):
        return self.users.get(user_id)
