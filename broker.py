import json
import logging
import datetime
import copy
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_DOWN
import signal
import functools

import pytz

from webWrapper import RestWrapper
from postgresWrapper import PostgresWrapper

_logger = logging.getLogger()

class OttoBroker():

    STATUS_KEY = 'status'
    MESSAGE_KEY = 'message'
    VALUE_KEY = 'value'

    STATUS_SUCCESS = 'success'
    STATUS_ERROR = 'error'

    def __init__(self, db_connection_string, api_key, test_user_id):
        self._rest = RestWrapper("https://api.iextrading.com/1.0", {})
        self._db = PostgresWrapper(db_connection_string)
        self._user_cache = {}
        self._user_stocks = {}
        self._api_users = {}

        self._test_mode = False
        self._test_user_id = test_user_id

        self._populate_user_cache()
        self._populate_api_user_cache()
    
    def _populate_api_user_cache(self):
        self._api_users = {}
        api_user_list = self._db.broker_get_all_api_users()

        for user in api_user_list:
            self._api_users[user.api_key] = user

    def _populate_user_cache(self):
        self._user_cache = {}
        # TODO:update this to just get all user_ids, since we're kinda just dumping the user objects
        user_list = self._db.broker_get_all_users()

        for user in user_list:
            self._update_single_user(user.id)
    
    def _update_single_user(self, user_id):
        user = self._db.broker_get_single_user(user_id)

        stock_list = self._db.broker_get_stocks_by_user(user_id)
        
        # create a dictionary of the stocks, grouped by ticker
        stock_dict = {}
        for stock in stock_list:
            if stock.ticker_symbol in stock_dict:
                stock_dict[stock.ticker_symbol].append(stock)
            else:
                stock_dict[stock.ticker_symbol] = [stock]

        user.stocks = stock_dict

        historical_stock_list = self._db.broker_get_historical_stocks_by_user(user_id)
        
        # create a dictionary of the stocks, grouped by ticker
        stock_dict = {}
        for stock in historical_stock_list:
            if stock.ticker_symbol in stock_dict:
                stock_dict[stock.ticker_symbol].append(stock)
            else:
                stock_dict[stock.ticker_symbol] = [stock]

        user.historical_stocks = stock_dict
        
        self._user_cache[user.id] = user

    def _get_user(self, user_id):
        if self._test_mode:
            return self._user_cache[self._test_user_id]
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        else:
            return None
            
    def _is_valid_api_user(self, api_key):
        if api_key in self._api_users:
            return True

    def is_market_live(self, time=None):
        if self._test_mode:
            return True
            
        if time is None:
            time = datetime.datetime.now(pytz.timezone('EST5EDT'))
        
        return (time.weekday() < 5) and ((time.hour > 9 or (time.hour == 9 and time.minute >= 30)) and time.hour < 16)
    
    @staticmethod
    def get_type(value, expected_type, var_name):
        if isinstance(value, expected_type):
            return value
        elif expected_type == bool:
            if isinstance(value, str):
                if value.lower() == 'false':
                    return False
                elif value.lower() == 'true':
                    return True
        else:
            try:
                value = expected_type(value)
                return value
            except Exception:
                pass

        raise Exception('Couldn\'t convert {} with value {} to type {}'.format(var_name, value, expected_type))
    
    @staticmethod
    def return_failure(failure_message, exc_info=None, do_log=True, extra_vals=None):
        if do_log:
            _logger.error(failure_message)
        if exc_info:
            _logger.exception(exc_info)

        result = {
            OttoBroker.STATUS_KEY: OttoBroker.STATUS_ERROR,
            OttoBroker.MESSAGE_KEY: failure_message
        }

        if isinstance(extra_vals, dict):
            result.update(extra_vals)

        return result
    
    def get_stock_value(self, symbol_list):
        result = dict()
        unparsed = self._rest.request(
            '/stock/market/batch/',
            {
                'types': 'quote',
                'symbols': ','.join(symbol_list)}
            )
        data = None

        try:
            data = json.loads(unparsed)
        except Exception as e:
            return self.return_failure('Invalid API response', exc_info=e)
        if data is None:
            return self.return_failure('Got None from api response')
        elif not isinstance(data, dict):
            return self.return_failure('Unexpected data type ' + str(type(data)))

        try:
            for symbol in symbol_list:
                if symbol not in data:
                    result[symbol] = {
                        self.STATUS_KEY: self.STATUS_ERROR,
                        self.MESSAGE_KEY: 'Unknown symbol'
                    }
                else:
                    result[symbol] = {
                        self.STATUS_KEY: self.STATUS_SUCCESS,
                        self.VALUE_KEY: Decimal(str(data[symbol]['quote']['latestPrice']))
                    }

            result[self.STATUS_KEY] = self.STATUS_SUCCESS
        except Exception as e:
            return self.return_failure('Unexpected response format', exc_info=e)

        return result
    
    def buy_stock(self, symbol, quantity, user_id, api_key):
        user = self._get_user(user_id)

        if not user:
            return self.return_failure('Invalid user_id: {}'.format(user_id), do_log=False)
        if not isinstance(quantity, int):
            return self.return_failure('Quantity, \'{}\' must be an int'.format(quantity), do_log=False)
        if quantity < 1:
            return self.return_failure('Gotta buy at least 1 stock!', do_log=False)
        if not self.is_market_live():
            return self.return_failure('No trading after hours', do_log=False)

        stock_val = self.get_stock_value([symbol])

        if stock_val[self.STATUS_KEY] != self.STATUS_SUCCESS:
            # don't need to log here, because the error is presumably also logged in get_stock_value
            return self.return_failure('Failed getting stock value: {}'.format(stock_val[self.MESSAGE_KEY]), do_log=False)
        if symbol not in stock_val:
            # bwuh? This really shouldn't happen
            return self.return_failure('Symbol {} missing from stock response. Please check the logs...'.format(symbol))
        if stock_val[symbol][self.STATUS_KEY] != self.STATUS_SUCCESS:
            return self.return_failure('Failed to get stock value for symbol {}. Messsage: {}'.format(symbol,
                                                                                                      stock_val[symbol][self.MESSAGE_KEY]))

        per_stock_cost = stock_val[symbol][self.VALUE_KEY]
        total_cost = per_stock_cost * quantity

        if user.balance < total_cost:
            extra_vals = {
                'per_stock_amt': per_stock_cost,
                'total_amt': total_cost,
                'quantity': quantity,
                'symbol': symbol,
                'user': user.to_dict()
            }
            return self.return_failure('Insufficient funds', extra_vals=extra_vals, do_log=False)
        
        if self._db.broker_buy_regular_stock(user.id, symbol, per_stock_cost, quantity, api_key) is None:
            return self.return_failure('buying stocks failed. Ensure you have a valid API key')
        
        self._update_single_user(user.id)
        user = self._get_user(user.id)
        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': user.to_dict(),
            'total_amt': total_cost,
            'per_stock_amt': per_stock_cost,
            'quantity': quantity,
            'symbol': symbol
        }
    
    def sell_stock(self, symbol, quantity, user_id, api_key):
        user = self._get_user(user_id)

        if not user:
            return self.return_failure('Invalid user_id: {}'.format(user_id), do_log=False)
        if not isinstance(quantity, int):
            return self.return_failure('Quantity, \'{}\' must be an int'.format(quantity), do_log=False)
        if quantity < 1:
            return self.return_failure('Gotta sell at least 1 stock!', do_log=False)
        if not self.is_market_live():
            return self.return_failure('No trading after hours', do_log=False)

        stock_val = self.get_stock_value([symbol])

        if stock_val[self.STATUS_KEY] != self.STATUS_SUCCESS:
            # don't need to log here, because the error is presumably also logged in get_stock_value
            return self.return_failure('Failed getting stock value: {}'.format(stock_val[self.MESSAGE_KEY]), do_log=False)
        if symbol not in stock_val:
            # bwuh? This really shouldn't happen
            return self.return_failure('Symbol {} missing from stock response. Please check the logs...'.format(symbol))
        if stock_val[symbol][self.STATUS_KEY] != self.STATUS_SUCCESS:
            return self.return_failure('Failed to get stock value for symbol {}. Messsage: {}'.format(symbol,
                                                                                                      stock_val[symbol][self.MESSAGE_KEY]))

        per_stock_cost = stock_val[symbol][self.VALUE_KEY]
        total_cost = per_stock_cost * quantity

        cur_stocks = 0
        if symbol in user.stocks:
            cur_stocks = len(user.stocks[symbol])

        if cur_stocks < quantity:
            extra_vals = {
                'quantity': quantity,
                'symbol': symbol,
                'user': user.to_dict()
            }
            return self.return_failure('Insufficient stocks', extra_vals=extra_vals, do_log=False)
        
        if self._db.broker_sell_regular_stock(user.id, symbol, per_stock_cost, quantity, api_key) is None:
            return self.return_failure('selling stocks failed. Ensure you have a valid API key')
        
        self._update_single_user(user.id)
        user = self._get_user(user.id)
        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': user.to_dict(),
            'total_amt': total_cost,
            'per_stock_amt': per_stock_cost,
            'quantity': quantity,
            'symbol': symbol
        }
    
    def withdraw(self, user_id, amount, reason, api_key):
        if not isinstance(amount, Decimal):
            return self.return_failure('amount must be a Decimal', do_log=False)

        user = self._get_user(user_id)

        if not user:
            return self.return_failure('Invalid user_id: {}'.format(user_id), do_log=False)
        # make sure the user can afford the transaction
        if user.balance < amount:
            return self.return_failure('Insufficient cash to withdraw', do_log=False, extra_vals={'user': user.to_dict()})

        if self._db.broker_give_money_to_user(user.id, -amount, reason, api_key) is None:
            return self.return_failure('withdraw failed. Ensure you have a valid API key')

        self._update_single_user(user.id)
        user = self._get_user(user.id)

        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': user.to_dict(),
            'amount': amount,
        }

    def deposit(self, user_id, amount, reason, api_key):
        if not isinstance(amount, Decimal):
            return self.return_failure('amount must be a Decimal', do_log=False)

        user = self._get_user(user_id)

        if not user:
            return self.return_failure('Invalid user_id: {}'.format(user_id), do_log=False)

        if self._db.broker_give_money_to_user(user.id, amount, reason, api_key) is None:
            return self.return_failure('deposit failed. Ensure you have a valid API key')

        self._update_single_user(user.id)
        user = self._get_user(user.id)

        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': user.to_dict()
        }

    def get_user_info(self, user_id, shallow, historical):
        user = self._get_user(user_id)

        if not isinstance(shallow, bool):
            return self.return_failure('shallow must be either \'True\' or \'False\'', do_log=False)

        if not isinstance(historical, bool):
            return self.return_failure('historical must be either \'True\' or \'False\'', do_log=False)

        if not user:
            return self.return_failure('Invalid user_id: {}'.format(user_id), do_log=False)

        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': user.to_dict(shallow, historical)
        }
    
    def get_all_users(self, shallow, historical):
        if not isinstance(shallow, bool):
            return self.return_failure('shallow must be either \'True\' or \'False\'', do_log=False)

        if not isinstance(historical, bool):
            return self.return_failure('historical must be either \'True\' or \'False\'', do_log=False)

        result = {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user_list': []
        }

        for u in self._user_cache:
            result['user_list'].append(self._user_cache[u].to_dict(shallow, historical))

        return result


    def register_user(self, user_id, display_name, api_key):
        user = self._get_user(user_id)

        if user is not None:
            return self.return_failure('User with id {} already exists'.format(user.id), do_log=False)
        
        if self._db.broker_create_user(user_id, display_name, api_key) is None:
            return self.return_failure('User could not be created. Ensure you have a valid API key')

        self._update_single_user(user_id)
        new_user = self._get_user(user_id)
        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'user': new_user.to_dict(shallow=False, historical=True)
        }

    def toggle_test_mode(self, api_key):
        if not self._is_valid_api_user(api_key):
            return self.return_failure('Invalid api_key', do_log=False)
        
        self._test_mode = not self._test_mode
        return {
            self.STATUS_KEY: self.STATUS_SUCCESS,
            'test_mode': self._test_mode
        }