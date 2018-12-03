import logging

_logger = logging.getLogger()

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
    
    def to_dict(self, assets, liabilities, stock_vals, shallow=False):
        result = {
            'id': self.id,
            'created_date': self.created,
            'balance': self.balance,
            'display_name': self.display_name,
            'assets': assets,
            'liabilities': liabilities
        }
        
        long_dict = {}
        short_dict = {}
        historical_long_dict = {}
        historical_short_dict = {}
        watch_dict = {}
        
        if not shallow:
            long_dict = self._get_stock_dict(self.longs, stock_vals, False)
            short_dict = self._get_stock_dict(self.shorts, stock_vals, False)

            historical_long_dict = self._get_stock_dict(self.historical_longs, stock_vals, True)
            historical_short_dict = self._get_stock_dict(self.historical_shorts, stock_vals, True)
            
            for symbol in self.watches:
                watch_dict[symbol] = self.watches[symbol].watch_cost

        result['historical_holdings'] = historical_long_dict
        result['holdings'] = long_dict
        result['shorts'] = short_dict
        result['historical_shorts'] = historical_short_dict
        result['watches'] = watch_dict
        
        return result
    
    @staticmethod
    def _get_stock_dict(stock_dict, stock_vals, is_historical):
        result = {}
        for symbol in stock_dict:
            result[symbol] = {
                'name': stock_vals[symbol]['name'],
                'stocks': [x.to_dict() for x in stock_dict[symbol]],
                'stock_count': sum([x.count for x in stock_dict[symbol]])
            }

            if not is_historical:
                result[symbol]['per_value'] = stock_vals[symbol]['value']
                result[symbol]['total_value'] = sum([x.count for x in stock_dict[symbol]]) * stock_vals[symbol]['value']
        
        return result

class BrokerStock():
    def __init__(self, raw):
        self.stock_type = raw[0]
        self.user_id = raw[1]
        self.ticker_symbol = raw[2]
        self.purchase_cost = raw[3]
        self.sell_cost = raw[4]
        self.count = int(raw[5])

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

class BrokerLimitOrder():
    def __init__(self, raw):
        self.id = raw[0]
        self.user_id = raw[1]
        self.stock_type = raw[2]
        self.transaction_type = raw[3]
        self.ticker_symbol = raw[4]
        self.target_price = raw[5]
        self.quantity = raw[6]
        self.expiration = raw[7]
        self.active = raw[8]
        self.filled = raw[9]

    def to_dict(self):
        return {
            'symbol': self.ticker_symbol,
            'type': self.stock_type,
            'transaction_type': self.transaction_type,
            'target_price': self.target_price,
            'quantity': self.quantity,
            'expiration': self.expiration,
            'active': self.active,
            'filled': self.filled
        }