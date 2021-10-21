from datetime import timezone, datetime
from decimal import Decimal

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class TimeScaleDBUtil:
    def __init__(self, user = None, password = None, host = None, port = None, database = None):
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        
        self._sqlalchemy_config = f"postgresql+psycopg2://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        
        self._engine = create_engine(self._sqlalchemy_config)
            
    def _get_table_name(self, symbol = "BTC-PERP", suffix = ""):
        return (symbol.replace("-", "_") + "_" + suffix).lower()
    
    def get_dollar_trades_table_name(self, symbol = "BTC-PERP"):
        return self._get_table_name(symbol, "dollar_trades")
    
    def read_sql_query(self, sql = "", index = "", debug = False):
        if debug == True:
            print(sql)
        df = pd.read_sql_query(sql, self._engine)
        
        if len(index) > 0:
            df = df.set_index(index)
        return df

    def sql_execute(self, sql = "", debug = False):
        if debug == True:
            print(sql)
        return self._engine.execute(sql)
    
    def df_to_sql(self, df = None, schema = None, if_exists = 'fail'):
        if df.empty or schema == None:
            return
        
        return df.to_sql(schema, con = self._engine, if_exists = if_exists, index = False) 
    
    def get_latest_trade_datetime(self, symbol = "BTC-PERP", suffix = "dollar_trades", debug = False):
        _trades_table_name = self._get_table_name(symbol, suffix)
        df = self.read_sql_query(f"SELECT max(time) AS max_time, max(id) AS max_id FROM {_trades_table_name}", debug = debug)
        if df['max_time'][0] is not None:
            return df['max_time'][0].to_pydatetime(), df['max_id'][0]
        return datetime(2019, 3, 1, 0, 0, 0, tzinfo=timezone.utc), -1
    
    def get_latest_trade_id(self, symbol = "BTC-PERP", suffix = "dollar_trades", debug = False):
        _trades_table_name = self._get_table_name(symbol, suffix)
        df = self.read_sql_query(f"SELECT max(id) FROM {_trades_table_name}", debug = debug)
        if df['max'][0] is not None:
            return df['max'][0]
        return -1
        
    def get_latest_dollar_cumsum(self, symbol = "BTC-PERP", suffix = "dollar_trades", debug = False):
        _trades_table_name = self._get_table_name(symbol, suffix)
        
        _latest_id = self.get_latest_trade_id(symbol, suffix = suffix, debug = debug)
        if _latest_id < 0:
            return 0
        else:
            df = self.read_sql_query(f"SELECT dollar_cumsum FROM {_trades_table_name} where id = {_latest_id}", debug = debug)
            if df['dollar_cumsum'][0] is not None:
                return Decimal(df['dollar_cumsum'][0])
        return 0
    
    def init_dollar_table(self, symbol = "BTC-PERP", force = False, debug = False):
        _trades_table_name = self._get_table_name(symbol, "dollar_trades")
        
        df = self.read_sql_query(f"select * from information_schema.tables where table_name='{_trades_table_name}'", debug = debug)
        
        if df.empty == False and force == False:
            return
        
        # trades記録用テーブルと時間足用Materialized viewを作成
        # Chunk time intervalは2019年3月から2021年10月までのFTXの総取引高(約1兆ドル)を945/7で割り算した。Timescaledbの時間側のchunk割が毎週単位なのに合わせている。
        # Chunk timeの値が小さすぎるとChunkテーブルが大量にできすぎて、手動でしか削除できなくなる
        # https://docs.timescale.com/timescaledb/latest/how-to-guides/data-retention/manually-drop-chunks/#dropping-chunks-manually
        sql = (f"DROP TABLE IF EXISTS {_trades_table_name} CASCADE;"
               f"CREATE TABLE IF NOT EXISTS {_trades_table_name} (time TIMESTAMP WITH TIME ZONE NOT NULL, id BIGINT NOT NULL, price NUMERIC NOT NULL, size NUMERIC NOT NULL, dollar NUMERIC NOT NULL, dollar_cumsum NUMERIC NOT NULL, dollar_time BIGINT NOT NULL, side enum_side NOT NULL, liquidation BOOLEAN NOT NULL, UNIQUE(dollar_time, id));"
               f" CREATE INDEX ON {_trades_table_name} (id, dollar_time DESC);"
               f" CREATE INDEX ON {_trades_table_name} (dollar_cumsum, dollar_time DESC);"
               f" SELECT create_hypertable ('{_trades_table_name}', 'dollar_time', chunk_time_interval => 20000000000);"
               f" DROP FUNCTION IF EXISTS current_dollar_time;"
               f" CREATE FUNCTION current_dollar_time() RETURNS BIGINT STABLE AS $$DECLARE time BIGINT; BEGIN SELECT MAX(dollar_time) from {_trades_table_name} INTO time; IF NOT FOUND THEN time := 0; END IF; RETURN time; END;$$ LANGUAGE plpgsql;"
               f" SELECT set_integer_now_func('{_trades_table_name}', 'current_dollar_time');")
        self.sql_execute(sql, debug)
    
    def init_dollar_aggregate_table(self, symbol = "BTC-PERP", force = False, debug = False):
        _trades_table_name = self._get_table_name(symbol, "dollar_trades")

        for _period in [(100_000_000, 'dollar_100m'),
                        (10_000_000, 'dollar_10m'),
                        (1_000_000, 'dollar_1m')]:
            _period_table_name = self._get_table_name(symbol, _period[1])
                
            if force == True:
                sql = (f"DROP MATERIALIZED VIEW IF EXISTS {_period_table_name}")
                self.sql_execute(sql, debug = debug)

            sql = (f"CREATE MATERIALIZED VIEW {_period_table_name} WITH (timescaledb.continuous) AS "
                   f"SELECT first(time, dollar_time) AS time_from,"
                   f"last(time, dollar_time) AS time_to,"
                   f"first(id, dollar_time) AS id_from,"
                   f"last(id, dollar_time) AS id_to,"
                   f"time_bucket('{_period[0]}', dollar_time) AS time_bucket,"
                   f"first(price, dollar_time) AS open,"
                   f"MAX(price) AS high,"
                   f"MIN(price) AS low,"
                   f"last(price, dollar_time) AS close,"
                   f"SUM(dollar) AS volume,"
                   f"SUM(CASE WHEN side='buy' THEN dollar ELSE 0 END) AS buy_volume,"
                   f"SUM(CASE WHEN side='sell' THEN dollar ELSE 0 END) AS sell_volume,"
                   f"SUM(CASE WHEN liquidation=TRUE THEN dollar ELSE 0 END) AS liquidation_volume,"
                   f"SUM(CASE WHEN side='buy' AND liquidation=TRUE THEN dollar ELSE 0 END) AS liquidation_buy_volume,"
                   f"SUM(CASE WHEN side='sell' AND liquidation=TRUE THEN dollar ELSE 0 END) AS liquidation_sell_volume,"
                   f"COUNT(id) AS count_trades "
                   f"FROM {_trades_table_name} GROUP BY time_bucket WITH NO DATA;"
                   f"SELECT add_continuous_aggregate_policy('{_period_table_name}',start_offset => NULL,end_offset => '{_period[0]}',schedule_interval => INTERVAL '30 minutes');"
                   f"ALTER MATERIALIZED VIEW {_period_table_name} set (timescaledb.materialized_only = false);")
            self.sql_execute(sql, debug = debug)
