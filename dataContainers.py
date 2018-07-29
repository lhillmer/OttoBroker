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
        self.historical_stocks = dict()
        self.watches = dict()
    
    def to_dict(self, shallow=False, historical=False):
        result = {
            'id': self.id,
            'created_date': self.created,
            'balance': self.balance,
            'display_name': self.display_name
        }
        
        stock_dict = {}
        historical_stock_dict = {}
        watch_dict = {}
        
        if not shallow:
            for symbol in self.stocks:
                stock_dict[symbol] = [x.to_dict() for x in self.stocks[symbol]]

            if historical:
                for symbol in self.historical_stocks:
                    historical_stock_dict[symbol] = [x.to_dict() for x in self.historical_stocks[symbol]]
            
            for symbol in self.watches:
                watch_dict[symbol] = self.watches[symbol].watch_cost

        result['historical'] = historical_stock_dict
        result['stocks'] = stock_dict
        result['watches'] = watch_dict
        
        return result

class BrokerStock():
    def __init__(self, raw):
        self.stock_type = raw[0]
        self.user_id = raw[1]
        self.ticker_symbol = raw[2]
        self.purchase_cost = raw[3]
        self.sell_cost = raw[4]
        self.count = raw[5]

    def to_dict(self):
        return {
            'stock_type': self.stock_type,
            'symbol': self.ticker_symbol,
            'purchase_cost': self.purchase_cost,
            'sell_cost': self.sell_cost,
            'count': self.count
        }

class BrokerWatch():
    def __init__(self, raw):
        self.id = raw[0]
        self.user_id = raw[1]
        self.ticker_symbol = raw[2]
        self.watch_cost = raw[3]

    def to_dict(self):
        return {
            'symbol': self.ticker_symbol,
            'watch_cost': self.watch_cost
        }