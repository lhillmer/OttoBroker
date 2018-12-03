from flask import Flask, request, Response

import argparse
import configparser
import signal
import logging
from logging import handlers
import os
import json
from decimal import Decimal
import datetime
import pytz
# yes, threading is not actually truly parallel in cpython, and potentially very slow
# however, this API is not currently developed to handle a large number of requests, and is not processor intensive
# if performance ever becomes a concern, the decision to use threading can be revisited
import threading
import atexit

from broker import OttoBroker
from jsonEncoder import CustomJSONEncoder

handler = handlers.TimedRotatingFileHandler("logs/log_broker.log", when="midnight", interval=1)
logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    filename=os.devnull,
    level=logging.INFO)
_logger = logging.getLogger()
_logger.addHandler(handler)

# START CONSTANTS
# api params
SYMBOLS_KEY = 'symbols'
SYMBOL_KEY = 'symbol'
USERID_KEY = 'userid'
APIKEY_KEY = 'apikey'
DISPLAYNAME_KEY = 'displayname'
AMOUNT_KEY = 'amount'
REASON_KEY = 'reason'
QUANTITY_KEY = 'quantity'
SHALLOW_KEY = 'shallow'
MAX_VALUE = 'MAX'

# error messages
MISSING_PARAM_MSG = 'missing required parameter: {param}'
INVALID_TYPE_MSG = 'param \'{param}\' could not be converted to type \'{type}\''

# bool conversion consts
STR_TRUE = 'True'
STR_FALSE = 'False'

# time constant - just need the time, really
# currently set to 8 am
BG_RUN_TIME = datetime.datetime(1, 1, 1, 8)
# END CONSTANTS

data_lock = threading.Lock()
# just creating a handle to assign to later. Is this strictly necessary?
# this was in the example i'm following, so I'll keep it for now
background_thread = None

# the next sections are ordered weirdly because of needing to rely on ~~global variables~~
# yes i know it's bad. I'll fix it later. hopefully
def get_broker():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", 
            dest="configFile",
            help="Relative Path to config file",
            required=True)
    args = parser.parse_args()
    config = configparser.ConfigParser(delimiters=('='))
    config.read(args.configFile)

    return OttoBroker(
        config.get('DEFAULT', 'connection_string'),
        config.get('DEFAULT', 'test_connection_string'),
        int(config.get('DEFAULT', 'max_liabilities_ratio')),
    )

# create the broker object now that the function is defined
broker = get_broker()

# functions needed to create the flask app (and threading stuff)
def get_seconds_until_time(dt):
    # looking to get seconds until next occurence of the supplied time (not counting date)

    # we want to run this during off-hours, so grab the timezone that has wallstreet
    # but then we need to remove the timezone so we can actually compare it to the other datetime
    cur_time = datetime.datetime.now(pytz.timezone('EST5EDT')).replace(tzinfo=None)
    # get the target time, but then replace the date information so it's the same day as cur_time
    target_time = dt.replace(year=cur_time.year, month=cur_time.month, day=cur_time.day)

    # if we are after target time on the current day, then we need to add a day
    if cur_time.time() > target_time.time():
        target_time += datetime.timedelta(days=1)

    return (target_time - cur_time).total_seconds()

def bg_process_run(broker):
    #_logger.warn('here we are!')
    pass

def get_app():
    # setting up threading taken from: https://stackoverflow.com/a/22900255
    app = Flask(__name__)

    def handle_signals(self, signum, frame):
        global app
        # taken from: https://stackoverflow.com/a/17053522
        func = app.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    signal.signal(signal.SIGINT, handle_signals)
    signal.signal(signal.SIGTERM, handle_signals)

    def queue_bg_thread():
        global background_thread
        global BG_RUN_TIME
        global broker
        #TODO:use this line once we have all the testing ironed out
        #background_thread = threading.Timer(get_seconds_until_time(BG_RUN_TIME), bg_thread_process)
        #TODO: just doing testing with this. ba;lksdjfj;alskdjfa;lksdjf;alkjsjk;df remove
        background_thread = threading.Timer(5, bg_thread_process)
        background_thread.daemon = True
        background_thread.start()
    
    def bg_thread_process():
        global data_lock
        global broker
        global background_thread

        with data_lock:
            bg_process_run(broker)
        
        queue_bg_thread()

    def kill_bg_thread():
        global background_thread
        background_thread.cancel()
    
    queue_bg_thread()
    atexit.register(kill_bg_thread)

    return app

# cool, now we can make the app
app = get_app()

# now, all the functions related to actually routing the queries
def jsonify(obj):
    return Response(json.dumps(obj, cls=CustomJSONEncoder), mimetype='application/json')

@app.route('/broker/hello')
def flask_test():
    return "I am OttoBroker yes hello"

@app.route('/broker/stock_info')
def get_stock_info():
    if SYMBOLS_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOLS_KEY)))
    return jsonify(broker.get_stock_value([s.upper() for s in request.args[SYMBOLS_KEY].split(',')]))

@app.route('/broker/toggle_test_mode')
def toggle_test():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    return jsonify(broker.toggle_test_mode(request.args[APIKEY_KEY]))

@app.route('/broker/test_mode')
def view_test():
    return jsonify({
        broker.STATUS_KEY: broker.STATUS_SUCCESS,
        'test_mode': broker._test_mode
    })

@app.route('/broker/user_info')
def get_user_info():
    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SHALLOW_KEY not in request.args:
        shallow = False
    else:
        shallow = request.args[SHALLOW_KEY]
        if shallow.lower() == STR_TRUE.lower():
            shallow = True
        elif shallow.lower() == STR_FALSE.lower():
            shallow = False
        
    return jsonify(broker.get_user_info(request.args[USERID_KEY], shallow))

@app.route('/broker/all_users')
def get_all_users():
    if SHALLOW_KEY not in request.args:
        shallow = False
    else:
        shallow = request.args[SHALLOW_KEY]
        if shallow.lower() == STR_TRUE.lower():
            shallow = True
        elif shallow.lower() == STR_FALSE.lower():
            shallow = False
        
    return jsonify(broker.get_all_users(shallow))

@app.route('/broker/register')
def register_user():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))
    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))
    if DISPLAYNAME_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=DISPLAYNAME_KEY)))

    return jsonify(broker.register_user(request.args[USERID_KEY], request.args[DISPLAYNAME_KEY], request.args[APIKEY_KEY]))

@app.route('/broker/deposit')
def deposit():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))
    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))
    if AMOUNT_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=AMOUNT_KEY)))
    amount = request.args[AMOUNT_KEY]
    try:
        amount = Decimal(amount)
    except Exception:
        return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=AMOUNT_KEY, type='Decimal')))
        
    if REASON_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=REASON_KEY)))

    return jsonify(broker.deposit(request.args[USERID_KEY], amount, request.args[REASON_KEY], request.args[APIKEY_KEY]))

@app.route('/broker/withdraw')
def withdraw():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if AMOUNT_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=AMOUNT_KEY)))
    amount = request.args[AMOUNT_KEY]
    try:
        amount = Decimal(amount)
    except Exception:
        return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=AMOUNT_KEY, type='Decimal')))
        
    if REASON_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=REASON_KEY)))

    return jsonify(broker.withdraw(request.args[USERID_KEY], amount, request.args[REASON_KEY], request.args[APIKEY_KEY]))

@app.route('/broker/buy_long')
def buy_long():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    if QUANTITY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=QUANTITY_KEY)))
    quantity = request.args[QUANTITY_KEY]

    if quantity.upper() == MAX_VALUE.upper():
        quantity = -1
        use_max = True
    else:
        use_max = False
        try:
            quantity = int(quantity)
        except Exception:
            return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=QUANTITY_KEY, type='int')))

    return jsonify(broker.buy_long(request.args[SYMBOL_KEY].upper(), quantity, request.args[USERID_KEY], request.args[APIKEY_KEY], use_max))

@app.route('/broker/sell_long')
def sell_long():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    if QUANTITY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=QUANTITY_KEY)))
    quantity = request.args[QUANTITY_KEY]

    if quantity.upper() == MAX_VALUE.upper():
        quantity = -1
        use_max = True
    else:
        use_max = False
        try:
            quantity = int(quantity)
        except Exception:
            return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=QUANTITY_KEY, type='int')))

    return jsonify(broker.sell_long(request.args[SYMBOL_KEY].upper(), quantity, request.args[USERID_KEY], request.args[APIKEY_KEY], use_max))

@app.route('/broker/buy_short')
def buy_short():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    if QUANTITY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=QUANTITY_KEY)))
    quantity = request.args[QUANTITY_KEY]

    if quantity.upper() == MAX_VALUE.upper():
        quantity = -1
        use_max = True
    else:
        use_max = False
        try:
            quantity = int(quantity)
        except Exception:
            return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=QUANTITY_KEY, type='int')))

    return jsonify(broker.buy_short(request.args[SYMBOL_KEY].upper(), quantity, request.args[USERID_KEY], request.args[APIKEY_KEY], use_max))

@app.route('/broker/sell_short')
def sell_short():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    if QUANTITY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=QUANTITY_KEY)))
    quantity = request.args[QUANTITY_KEY]

    if quantity.upper() == MAX_VALUE.upper():
        quantity = -1
        use_max = True
    else:
        use_max = False
        try:
            quantity = int(quantity)
        except Exception:
            return jsonify(broker.return_failure(INVALID_TYPE_MSG.format(param=QUANTITY_KEY, type='int')))

    return jsonify(broker.sell_short(request.args[SYMBOL_KEY].upper(), quantity, request.args[USERID_KEY], request.args[APIKEY_KEY], use_max))

@app.route('/broker/set_watch')
def set_watch():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    return jsonify(broker.set_watch(request.args[USERID_KEY], request.args[SYMBOL_KEY].upper(), request.args[APIKEY_KEY]))

@app.route('/broker/remove_watch')
def remove_watch():
    if APIKEY_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=APIKEY_KEY)))

    if USERID_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=USERID_KEY)))

    if SYMBOL_KEY not in request.args:
        return jsonify(broker.return_failure(MISSING_PARAM_MSG.format(param=SYMBOL_KEY)))

    return jsonify(broker.remove_watch(request.args[USERID_KEY], request.args[SYMBOL_KEY].upper(), request.args[APIKEY_KEY]))
    


if __name__ == '__main__':
    app.run(
        debug=False,
        port=8888
    )
