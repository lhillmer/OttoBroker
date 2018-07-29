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
    purchase_cost NUMERIC(100, 2),
    purchased TIMESTAMP,
    expiration TIMESTAMP,
    sell_cost NUMERIC(100, 2),
    sold TIMESTAMP, 
    PRIMARY KEY(id),
    FOREIGN KEY(stocktypeid) REFERENCES ottobroker.fakestocktypes(id),
    FOREIGN KEY(userid) REFERENCES ottobroker.users(id),
    FOREIGN KEY(txid) REFERENCES ottobroker.faketransactions(id)
);
CREATE TABLE ottobroker.watches(
    id serial NOT NULL,
    userid varchar(256) NOT NULL,
    ticker varchar(10) NOT NULL,
    watch_cost NUMERIC(100, 2) NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY(userid) REFERENCES ottobroker.users(id)
);

INSERT INTO ottobroker.faketransactiontypes (txtype) values ('BUY'), ('SELL'), ('CAPITAL');
INSERT INTO ottobroker.fakestocktypes (stocktype) values ('LONG'), ('SHORT');
