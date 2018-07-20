class BrokerAPIUser():
    def __init__(self, raw):
        self.id = raw[0]
        self.api_key = raw[1]
        self.display_name = raw[2]

class BrokerUser():
    def __init__(self, raw):
        self.id = raw[0]
        self.created = raw[1]
        self.display_name = raw[2]
        self.balance = raw[3]
        self.stocks = dict()
    
    def to_dict(self):
        result = {
            'id': self.id,
            'created_date': self.created,
            'balance': self.balance,
            'display_name': self.display_name
        }

        stock_dict = {}
        for symbol in self.stocks:
            stock_dict[symbol] = [x.to_dict() for x in self.stocks[symbol]]
        
        result['stocks'] = stock_dict
        
        return result

class BrokerStock():
    def __init__(self, raw):
        self.id = raw[0]
        self.stock_type = raw[1]
        self.user_id = raw[2]
        self.transaction_id = raw[3]
        self.ticker_symbol = raw[4]
        self.purchase_cost = raw[5]
        self.purchase_time = raw[6]
        self.expiration_time = raw[7]
        self.sell_cost = raw[8]
        self.sell_time = raw[9]

    def to_dict(self):
        return {
            'id': self.id,
            'stock_type': self.stock_type,
            'symbol': self.ticker_symbol,
            'purchase_cost': self.purchase_cost,
            'purchase_time': self.purchase_time,
            'expiration_time': self.expiration_time,
            'sell_cost': self.sell_cost,
            'sell_time': self.sell_time
        }