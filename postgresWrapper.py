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
        result = self._query_wrapper("SELECT * FROM ottobroker.users WHERE id=%s;", [user_id])
        if len(result) > 0:
            return BrokerUser(result[0])
        else:
            return None

    def broker_get_single_user_by_name(self, user_id):
        result = self._query_wrapper("SELECT * FROM ottobroker.users WHERE displayname=%s;", [user_id])
        if len(result) > 0:
            return BrokerUser(result[0])
        else:
            return None
    
    def broker_get_all_user_ids(self):
        rawVals = self._query_wrapper("SELECT id FROM ottobroker.users;", [])
        result = []
        for row in rawVals:
            result.append(row[0])
        return result
    
    def broker_get_single_api_users(self, api_key):
        result = self._query_wrapper("SELECT * FROM ottobroker.apiusers where apikey=%s;", [api_key])
        if len(result) > 0:
            return BrokerAPIUser(result[0])
        else:
            return None
    
    def broker_get_longs_by_user(self, user_id):
        rawVals = self._query_wrapper("""SELECT stocktypeid, userid, ticker, purchase_cost, sell_cost, COUNT(id)
        FROM ottobroker.fakestocks
        WHERE userid=%s AND
            stocktypeid=(SELECT id FROM ottobroker.fakestocktypes WHERE stocktype='LONG') AND
            sold IS NULL
        GROUP BY stocktypeid, userid, ticker, purchase_cost, sell_cost;""", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_get_historical_longs_by_user(self, user_id):
        rawVals = self._query_wrapper("""SELECT stocktypeid, userid, ticker, purchase_cost, sell_cost, COUNT(id)
        FROM ottobroker.fakestocks
        WHERE userid=%s AND
            stocktypeid=(SELECT id FROM ottobroker.fakestocktypes WHERE stocktype='LONG') AND
            sold IS NOT NULL
        GROUP BY stocktypeid, userid, ticker, purchase_cost, sell_cost;""", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_get_shorts_by_user(self, user_id):
        rawVals = self._query_wrapper("""SELECT stocktypeid, userid, ticker, purchase_cost, sell_cost, COUNT(id)
        FROM ottobroker.fakestocks
        WHERE userid=%s AND
            stocktypeid=(SELECT id FROM ottobroker.fakestocktypes WHERE stocktype='SHORT') AND
            purchased IS NULL
        GROUP BY stocktypeid, userid, ticker, purchase_cost, sell_cost;""", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_get_historical_shorts_by_user(self, user_id):
        rawVals = self._query_wrapper("""SELECT stocktypeid, userid, ticker, purchase_cost, sell_cost, COUNT(id)
        FROM ottobroker.fakestocks
        WHERE userid=%s AND
            stocktypeid=(SELECT id FROM ottobroker.fakestocktypes WHERE stocktype='SHORT') AND
            purchased IS NOT NULL
        GROUP BY stocktypeid, userid, ticker, purchase_cost, sell_cost;""", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerStock(raw))
        return result
    
    def broker_give_money_to_user(self, user_id, amount, reason, api_key):
        result_table =  self._query_wrapper("SELECT ottobroker.givemoney(%s, %s, %s, %s);", [user_id, amount, reason, api_key])
        return result_table[0][0]
    
    def broker_buy_long(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table =  self._query_wrapper("SELECT ottobroker.buylong(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]
    
    def broker_sell_long(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table = self._query_wrapper("SELECT ottobroker.selllong(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]
    
    def broker_buy_short(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table =  self._query_wrapper("SELECT ottobroker.buyshort(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]
    
    def broker_sell_short(self, user_id, ticker_symbol, ticker_value, quantity, api_key):
        result_table = self._query_wrapper("SELECT ottobroker.sellshort(%s, %s, %s, %s, %s);", [user_id, ticker_symbol, ticker_value, quantity, api_key])
        return result_table[0][0]
    
    def broker_get_watches(self, user_id):
        rawVals = self._query_wrapper("SELECT * from ottobroker.watches WHERE userid=%s;", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerWatch(raw))
        return result
    
    def broker_update_watch(self, user_id, symbol, value):
        # TODO: figure out how to verify this actually 'worked'
        self._query_wrapper("UPDATE ottobroker.watches set watch_cost=%s WHERE userid=%s and ticker=%s;", [value, user_id, symbol], doFetch=False)
    
    def broker_create_watch(self, user_id, symbol, value):
        return self._query_wrapper("INSERT INTO ottobroker.watches (userid, ticker, watch_cost) VALUES (%s, %s, %s) RETURNING id;", [user_id, symbol, value])[0][0]
    
    def broker_remove_watch(self, user_id, symbol):
        self._query_wrapper("DELETE FROM ottobroker.watches WHERE userid=%s and ticker=%s;", [user_id, symbol], doFetch=False)
    
    def broker_set_limit_order(self, user_id, stocktypeid, symbol, target_price, quantity, expiration):
        self._query_wrapper("INSERT INTO ottobroker.limitorders (userid, stocktypeid, ticker, target_price, quantity, expiration) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
            [user_id, stocktypeid, symbol, target_price, quantity, expiration]
        )[0][0]
    
    def broker_get_active_limit_orders(self, user_id):
        rawVals = self._query_wrapper("SELECT * from ottobroker.limitorders WHERE userid=%s and active='yes';", [user_id])
        result = []
        for raw in rawVals:
            result.append(BrokerWatch(raw))
        return result
    
    def broker_get_recent_inactive_limit_orders(self, user_id, expiration):
        rawVals = self._query_wrapper("SELECT * from ottobroker.limitorders WHERE userid=%s and active='no' and expiration > %s;", [user_id, expiration])
        result = []
        for raw in rawVals:
            result.append(BrokerWatch(raw))
        return result