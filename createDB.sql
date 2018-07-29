CREATE SCHEMA IF NOT EXISTS ottobroker;
CREATE TABLE ottobroker.apiusers(
    id serial NOT NULL,
    apikey char(32) NOT NULL,
    displayname varchar(256) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE ottobroker.users(
    id varchar(256) NOT NULL,
    created TIMESTAMP NOT NULL,
    displayname varchar(256) NOT NULL,
    balance NUMERIC(100, 2) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE ottobroker.faketransactiontypes(
    id serial NOT NULL,
    txtype varchar(256) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE ottobroker.faketransactions(
    id serial NOT NULL,
    txtypeid int NOT NULL,
    userid varchar(256) NOT NULL,
    dollaramount NUMERIC(100, 2) NOT NULL,
    stockamount int NOT NULL,
    ticker varchar(10),
    executed TIMESTAMP NOT NULL,
    reason varchar(256),
    apiuserid serial NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY(txtypeid) REFERENCES ottobroker.faketransactiontypes(id),
    FOREIGN KEY(userid) REFERENCES ottobroker.users(id),
    FOREIGN KEY(apiuserid) REFERENCES ottobroker.apiusers(id)
);
CREATE TABLE ottobroker.fakestocktypes(
    id serial NOT NULL,
    stocktype varchar(256) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE ottobroker.fakestocks(
    id serial NOT NULL,
    stocktypeid int NOT NULL,
    userid varchar(256) NOT NULL,
    txid int NOT NULL,
    ticker varchar(10) NOT NULL,
    purchase_cost NUMERIC(100, 2) NOT NULL,
    purchased TIMESTAMP NOT NULL,
    expiration TIMESTAMP,
    sell_cost NUMERIC(100, 2),
    sold TIMESTAMP, 
    PRIMARY KEY(id),
    FOREIGN KEY(stocktypeid) REFERENCES ottobroker.fakestocktypes(id),
    FOREIGN KEY(userid) REFERENCES ottobroker.users(id),
    FOREIGN KEY(txid) REFERENCES ottobroker.faketransactions(id)
);

INSERT INTO ottobroker.faketransactiontypes (txtype) values ('BUY'), ('SELL'), ('CAPITAL');
INSERT INTO ottobroker.fakestocktypes (stocktype) values ('REGULAR');

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

CREATE OR REPLACE FUNCTION ottobroker.buyregularstock(_user_id varchar(256), _ticker varchar(10), _per_cost numeric(100, 2), _quantity int, _api_key char(32))
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
            select id into stocktype_id from ottobroker.fakestocktypes where stocktype = 'REGULAR';
            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_cost, _per_cost, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;
            for i in 1.._quantity LOOP
            	insert into ottobroker.fakestocks (stocktypeid, userid, txid, ticker, purchase_cost, purchased)
                values (stocktype_id, _user_id, transaction_id, _ticker, _per_cost, _now);
            end loop;
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION ottobroker.sellregularstock(_user_id varchar(256), _ticker varchar(10), _per_value numeric(100, 2), _quantity int, _api_key char(32))
RETURNS INTEGER AS $BODY$
    DECLARE
        total_value numeric(100, 2) = -1;
        user_balance NUMERIC(100, 2) = -1;
        stock_count int = -1;
        transaction_id int = -1;
        txtype_id int = null;
        stock_id int = 0;
        _now timestamp = now();
        api_user_id int = null;
    BEGIN
        select count(id) into stock_count from ottobroker.fakestocks where userid = _user_id AND ticker = _ticker AND sold is Null;
        select id into api_user_id from ottobroker.apiusers where apikey = _api_key;
        if stock_count >= _quantity AND FOUND THEN
            total_value := _quantity * _per_value;
            select balance into user_balance from ottobroker.users where id = _user_id;
            update ottobroker.users set balance = user_balance + total_value where id = _user_id;
            select id into txtype_id from ottobroker.faketransactiontypes where txtype = 'SELL';
            insert into ottobroker.faketransactions (txtypeid, userid, dollaramount, stockamount, ticker, executed, apiuserid)
            values (txtype_id, _user_id, total_value, _quantity, _ticker, _now, api_user_id) returning id into transaction_id;
            for i in 1.._quantity LOOP
                select id into stock_id from ottobroker.fakestocks where userid = _user_id and sold is null and ticker = _ticker order by purchased asc limit 1;
            	update ottobroker.fakestocks set sold = _now, sell_cost = _per_value where id = stock_id;
            end loop;
        end if;
        return transaction_id;
    END;
    $BODY$
LANGUAGE 'plpgsql' VOLATILE;
