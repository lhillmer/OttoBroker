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
        self.longs = dict()
        self.historical_longs = dict()
        self.shorts = dict()
        self.historical_shorts = dict()
        self.watches = dict()
    
    def to_dict(self, shallow=False, historical=True):
        result = {
            'id': self.id,
            'created_date': self.created,
            'balance': self.balance,
            'display_name': self.display_name
        }
        
        long_dict = {}
        short_dict = {}
        historical_long_dict = {}
        historical_short_dict = {}
        watch_dict = {}
        
        if not shallow:
            for symbol in self.longs:
                long_dict[symbol] = [x.to_dict() for x in self.longs[symbol]]
            for symbol in self.shorts:
                short_dict[symbol] = [x.to_dict() for x in self.shorts[symbol]]

            if historical:
                for symbol in self.historical_longs:
                    historical_long_dict[symbol] = [x.to_dict() for x in self.historical_longs[symbol]]
                for symbol in self.historical_shorts:
                    historical_short_dict[symbol] = [x.to_dict() for x in self.historical_shorts[symbol]]
            
            for symbol in self.watches:
                watch_dict[symbol] = self.watches[symbol].watch_cost

        result['historical_longs'] = historical_long_dict
        result['longs'] = long_dict
        result['shorts'] = short_dict
        result['historical_shorts'] = historical_short_dict
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