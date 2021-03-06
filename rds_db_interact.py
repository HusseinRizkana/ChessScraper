#%%
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
from Secrets import (username, access_key_ID, secret_access_ID,
                     DB_password, DB_username, hostname, DB_name)
import pycountry
import pytz
import ChessComData
import pandas as pd

class Table_sqls:
    def __init__(self, hostname, username, DB_username, DB_password):
        self = self
        self.hostname = hostname
        self.DB_username = username
        self.user = DB_username
        self.password = DB_password
        

    def connect(self):
        cnn = None
        try:
            cnn = psycopg2.connect(
                host=self.hostname, dbname=self.DB_username, user=self.user, password=self.password)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return cnn
    # create postgresql table and if exists return description of columns

    def create_table(self, sql):
        """ query data from the vendors table """
        cnn = None
        try:
            cnn = self.connect()
            cnn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            Curs = cnn.cursor()
            # select current version of db
            Curs.execute('''select version()''')
            Curs.execute(sql)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if cnn is not None:
                cnn.close()

    def describe_table(self, table):
        cnn = None
        try:
            cnn = self.connect()
            Curs = cnn.cursor()
            # select current version of db
            Curs.execute('''select version()''')
            Curs.execute(f"Select * FROM {table} LIMIT 0")
            colnames = [desc[0] for desc in Curs.description]
            coltypes = [desc[1] for desc in Curs.description]
            print(colnames)
            print(coltypes)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if cnn is not None:
                cnn.close()

    def create_needed_tables(self):
        crt_countries = '''CREATE TABLE if not exists countries(
        alpha_2 VARCHAR(2),
        alpha_3 VARCHAR(3),
        country VARCHAR(50),
        timezone INT,
        PRIMARY KEY(alpha_2)
        '''
        # sql string to create games table
        crt_games = '''CREATE TABLE if not exists games(
        game_id int, player_black_username varchar(60),player_white_username varchar(60),
        result_black varchar(30), result_white varchar(30),game_mode varchar(30),time_control int, inc int,
        date varchar(25), opening varchar(90),start_time varchar(50), white_elo int, black_elo int,
        PRIMARY KEY(game_id)
        )'''
        # sql string to create players table
        crt_players = '''CREATE TABLE if not exists players(
        player_id int, username varchar(60), name varchar(80),join_date int,
        country_code varchar(3), streamer_status varchar(5),title varchar(3), PRIMARY KEY(player_id)
        )'''
        # sql string to create moves table
        crt_moves = '''CREATE TABLE if not exists moves(
        game_id int, move_num int, white_move varchar(6), white_clck int, black_move varchar(6),
        black_clck int
        )'''
        self.create_table(crt_players)
        self.describe_table("players")
        self.create_table(crt_countries)
        self.describe_table("countries")
        self.create_table(crt_games)
        self.describe_table("games")
        self.create_table(crt_moves)
        self.describe_table("moves")

    def select_single_table(self, table, where=None, orderby=None, direction=None, limit=None):
        '''creates select statement from table with optional additions
        inputs: table = tablename as string
        where = where statement as string
        orderby = column name as string
        direction = asc or desc as string
        limit = number as integer'''
        sqlstat = f"SELECT * FROM {table}"
        if where != None:
            sqlstat += where
        if orderby != None:
            if direction != None:
                sqlstat += "ORDER BY " + orderby + direction
            else:
                sqlstat += "ORDER BY " + orderby
        if limit != None:
            sqlstat += f"LIMIT {limit}"
        return sqlstat

    def get_table_as_pd(self, table_name):
        """ query data from postgreSQL table return pd.dataframe
        input: table_name as string
        """
        # clear variable
        conn = None
        try:
            # connect to database
            conn = self.connect()
            sql_string = f"SELECT * FROM {table_name}"
            # read values to dataframe
            table = pd.read_sql_query(sql=sql_string, con=conn)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            table = pd.DataFrame()
        finally:
            if conn is not None:
                conn.close()
        return table
    # completely clear table
    def clear_table(self,table):    
        '''clear chosen table... use with caution'''
        conn = None
        try:
            conn = self.connect()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            # select current version of the db
            cur.execute("SELECT version()")

            sql_str = sql.SQL(f"Delete FROM {table}")
            cur.execute(sql_str)
            print("The number of parts: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    def insert_into_table(self, table, columns, values):
        ''' insert data into table 
        table as string, columns as string, values as string
        only accepts single insert'''
        conn = None
        try:
            conn = self.connect()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            # select current version of the db
            cur.execute("SELECT version()")
            sql_str = sql.SQL(
                f"INSERT INTO {table}({columns}) VALUES({values}) ON CONFLICT DO NOTHING")
            cur.execute(sql_str)
            print("The number of parts: ", cur.rowcount)

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    def insert_multi_into_table(self,table, columns, values):
        ''' insert data into table 
        table as string, columns as string, values as string list of tuples
        for multiple inserts'''
        conn = None
        try:
            conn = self.connect()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # select current version of the db
            cur.execute("SELECT version()")

            sql_str = sql.SQL(f"INSERT INTO {table}({columns}) VALUES{values}")
            # print(sql_str)
            cur.execute(sql_str)
            print("The number of parts: ", cur.rowcount)

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    def insert_to_moves_games(self,table_moves, table_games, values_games, moves_data):
        ''' inserts to both the moves and games table to ensure 
        the game_id is alligned
        '''
        columns_moves = "game_id, move_num, white_move, white_clck, black_move, black_clck"
        columns_games = "player_black_username, player_white_username, result_black, result_white, game_mode, time_control, inc, date, opening, white_elo, black_elo, start_time"
       
        conn = None
        try:
            conn = self.connect()
            cur = conn.cursor()

            # select current version of the db
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur.execute("SELECT version()")

            sql_str1 = sql.SQL(
                f"INSERT INTO {table_games}({columns_games}) VALUES({values_games}) returning game_id;")
            cur.execute(sql_str1)
            # return game_id from sql_str1 insert
            game_id1 = cur.fetchone()[0]
            MD = [[game_id1]+moves_data[i] for i in range(len(moves_data))]
            # map into a list of tuples for single multiple insert with game_id set
            MD = list(map(tuple, MD))

            # moves to string of tuples per move
            values_moves = ','.join("({}, {}, '{}', {}, '{}', {})".format(
                i, j, k, x, y, z) for i, j, k, x, y, z in MD)
            sql_str2 = sql.SQL(
                f"INSERT INTO {table_moves}({columns_moves}) VALUES{values_moves} returning game_id;")
            cur.execute(sql_str2)
            print("The number of parts: ", cur.rowcount)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

