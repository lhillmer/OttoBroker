

CREATE OR REPLACE FUNCTION ottobroker.createuser(_user_id varchar(256), _display_name varchar(256), _api_key char(32))
RETURNS varchar(256) AS $BODY$
    DECLARE
        user_exists int = 0;
        result_id varchar(256) = Null;
        api_user_id int = null;
    BEGIN
        select count(id) into user_exists from ottobroker.users where id = _user_id;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if user_exists <= 0 AND api_user_id IS NOT NULL THEN
            insert into ottobroker.users (id, displayname, created, balance)
            values (_user_id, _display_name, now(), 0) returning id into result_id;
        end if;
        return result_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.givemoney(_user_id varchar(256), _amount numeric(100, 2), _reason varchar(256), _api_key char(32))
RETURNS int AS $BODY$
    DECLARE 
        user_exists int = 0;
        user_balance numeric(100, 2) = 0;
        txtype_id int = -1;
        transaction_id int = null;
        api_user_id int = null;
    BEGIN
        select count(id) into user_exists from ottobroker.users where id = _user_id;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if user_exists = 1 AND FOUND THEN
            select balance into user_balance from ottobroker.users where id =_user_id;
            update ottobroker.users set balance = user_balance + _amount where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'CAPITAL';
            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, executed, reason, apiuserid)
            values (txtype_id, _user_id, _amount, 0, now(), _reason, api_user_id) returning id into transaction_id;
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.buylong(_user_id varchar(256), _ticker varchar(10), _per_cost numeric(100, 2), _quantity int, _api_key char(32))
RETURNS INTEGER AS $BODY$
    DECLARE
        total_cost numeric(100, 2) = -1;
        user_balance numeric(100, 2) = -1;
        stock_index int = -1;
        transaction_id int = null;
        txtype_id int = -1;
        stocktype_id int = -1;
        _now timestamp = now();
        api_user_id int = null;
    BEGIN
        select balance into user_balance from ottobroker.users where id = _user_id;
        total_cost := _quantity * _per_cost;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if user_balance >= total_cost and FOUND THEN
            update ottobroker.users set balance = user_balance - total_cost where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'BUY';
            select id into stocktype_id from ottobroker.fakestocktypes where stocktype = 'LONG';
            
            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_cost, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;

            insert into ottobroker.fakestocks (stocktypeid, userid, txid, ticker, purchase_cost, purchased)
            select stocktype_id, _user_id, transaction_id, _ticker, _per_cost, _now
            from generate_series(1, _quantity);
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.selllong(_user_id varchar(256), _ticker varchar(10), _per_value numeric(100, 2), _quantity int, _api_key char(32))
RETURNS INTEGER AS $BODY$
    DECLARE
        total_value numeric(100, 2) = -1;
        user_balance NUMERIC(100, 2) = -1;
        stock_count int = -1;
        transaction_id int = -1;
        txtype_id int = null;
        stocktype_id int = (select id from ottobroker.fakestocktypes where stocktype = 'LONG');
        _now timestamp = now();
        api_user_id int = null;
        update_ids int[] := ARRAY(select id from ottobroker.fakestocks where userid = _user_id and sold is null and ticker = _ticker and stocktypeid = stocktype_id order by purchased asc limit _quantity);
    BEGIN
        select count(id) into stock_count from ottobroker.fakestocks where userid = _user_id AND ticker = _ticker AND sold is Null AND stocktypeid = stocktype_id;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if stock_count >= _quantity AND FOUND THEN
            total_value := _quantity * _per_value;
            select balance into user_balance from ottobroker.users where id = _user_id;
            update ottobroker.users set balance = user_balance + total_value where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'SELL';

            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_value, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;

            update ottobroker.fakestocks set sold = _now, sell_cost = _per_value
            where id = ANY(update_ids);
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.buyshort(_user_id varchar(256), _ticker varchar(10), _per_cost numeric(100, 2), _quantity int, _api_key char(32))
RETURNS INTEGER AS $BODY$
    DECLARE
        total_cost numeric(100, 2) = -1;
        user_balance numeric(100, 2) = -1;
        stock_count int = -1;
        stock_index int = -1;
        transaction_id int = null;
        txtype_id int = -1;
        stocktype_id int = (select id from ottobroker.fakestocktypes where stocktype = 'SHORT');
        _now timestamp = now();
        api_user_id int = null;
        update_ids int[] = ARRAY(select id from ottobroker.fakestocks where userid = _user_id and purchased is null and ticker = _ticker and stocktypeid = stocktype_id order by sold asc limit _quantity);
    BEGIN
        select count(id) into stock_count from ottobroker.fakestocks where userid = _user_id AND ticker = _ticker AND purchased is Null AND stocktypeid = stocktype_id;
        select balance into user_balance from ottobroker.users where id = _user_id;
        total_cost := _quantity * _per_cost;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if user_balance >= total_cost AND stock_count >= _quantity AND FOUND THEN
            update ottobroker.users set balance = user_balance - total_cost where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'BUY';

            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_cost, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;

            update ottobroker.fakestocks set purchased = _now, purchase_cost = _per_cost
            where id = ANY(update_ids);
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.sellshort(_user_id varchar(256), _ticker varchar(10), _per_value numeric(100, 2), _quantity int, _api_key char(32))
RETURNS INTEGER AS $BODY$
    DECLARE
        total_value numeric(100, 2) = -1;
        user_balance NUMERIC(100, 2) = -1;
        transaction_id int = -1;
        txtype_id int = null;
        stocktype_id int = -1;
        _now timestamp = now();
        api_user_id int = null;
    BEGIN
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if FOUND THEN
            total_value := _quantity * _per_value;
            select balance into user_balance from ottobroker.users where id = _user_id;
            update ottobroker.users set balance = user_balance + total_value where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'SELL';
            select id into stocktype_id from ottobroker.fakestocktypes where stocktype = 'SHORT';
            
            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_value, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;

            insert into ottobroker.fakestocks (stocktypeid, userid, txid, ticker, sell_cost, sold)
            select stocktype_id, _user_id, transaction_id, _ticker, _per_value, _now
            from generate_series(1, _quantity);
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;
