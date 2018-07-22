from dataContainers import *

import psycopg2
import psycopg2.extras

import datetime
import logging
import pickle
import copy

_logger = logging.getLogger()

class PostgresWrapper():
    def __init__(self, connectionString, force_quiet=False):
        self.connection_string = connectionString
        self.force_quiet = force_quiet
    
    def _query_wrapper(self, query, vals=None, doFetch=True, do_log=True):
        if vals is None:
            vals = []
        retry = True
        connection = None
        cursor = None
        while(retry):
            try:
                connection = psycopg2.connect(self.connection_string)
                cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                if do_log and not self.force_quiet:
                    _logger.info('making Query: ' + query + ' with vals: {}'.format(vals))
                cursor.execute(query, vals)
                connection.commit()
                result = None
                if(doFetch):
                    result = cursor.fetchall()
                cursor.close()
                connection.close()
                return result
            except psycopg2.InternalError as e:
                cursor.close()
                connection.close()
                if e.pgcode:
                    _logger.error("psycopg2 error code: " + str(e.pgcode))
                if not retry:
                    raise e
                retry = False

    def broker_create_user(self, user_id, display_name, api_key):
        result_table = self._query_wrapper("SELECT ottobroker.createuser(%s, %s, %s);", [user_id, display_name, api_key])
        return result_table[0][0]
    
    def broker_get_single_user(self, user_id):
        return BrokerUser(self._query_wrapper("SELECT * FROM ottobroker.users WHERE id=%s;", [user_id])[0])
    
    def broker_get_all_users(self):
        rawVals = self._query_wrapper("SELECT * FROM ottobroker.users;", [])
        result = []
        for raw in rawVals:
            result.append(BrokerUser(raw))
        return result
    
    def broker_get_all_api_users(self):
        rawVals = self._query_wrapper("SELECT * FROM ottobroker.apiusers;", [])
        result = []
        for raw in rawVals:
            result.append(BrokerAPIUser(raw))
        return result
    
    def broker_get_stocks_by_user(self, user_id):
        rawVals = self._query_wrapper("SELECT * FROM ottobroker.fakestocks WHERE userid=%s and sold is NULL;", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_get_historical_stocks_by_user(self, user_id):
        rawVals = self._query_wrapper("SELECT * FROM ottobroker.fakestocks WHERE userid=%s and sold is not NULL;", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_give_money_to_user(self, user_id, amount, reason, api_key):
        result_table =  self._query_wrapper("SELECT ottobroker.givemoney(%s, %s, %s, %s);", [user_id, amount, reason, api_key])
        return result_table[0][0]
    
    def broker_buy_regular_stock(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table =  self._query_wrapper("SELECT ottobroker.buyregularstock(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]
    
    def broker_sell_regular_stock(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table = self._query_wrapper("SELECT ottobroker.sellregularstock(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]