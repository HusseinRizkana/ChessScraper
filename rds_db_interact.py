# %%
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
from Secrets import (username, access_key_ID, secret_access_ID,
                     DB_password, DB_username, hostname, DB_name)

# %%
if __name__ == "__main__":
    cnn = psycopg2.connect(host=hostname,
                       dbname=DB_username,
                       user=DB_username,
                       password=DB_password)
    cnn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = cnn.cursor()
# %%
# retrieve all countries table data


def get_moves():
    """ query data from the countries table """
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        cur = conn.cursor()
        cur.execute("SELECT * FROM moves")
        print("The number of parts: ", cur.rowcount)
        # row = cur.fetchone()

        # while row is not None:
        #     print(row)
        #     row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_countries():
    """ query data from the countries table """
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        cur = conn.cursor()
        cur.execute("SELECT * FROM countries")
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


if __name__ == "__main__":
    get_countries()
    get_moves()
# %%
# retrieve all players table data


def get_players():
    """ query data from the games table """
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        cur = conn.cursor()
        cur.execute("SELECT * FROM players")
        print("The number of parts: ", cur.rowcount)
        # row = cur.fetchone()

        # while row is not None:
        #     # print(row)
        #     row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    get_players()
# %%
# retrieve all games table data


def get_games():
    """ query data from the games table """
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        cur = conn.cursor()
        cur.execute("SELECT * FROM games")
        print("The number of parts: ", cur.rowcount)
        # row = cur.fetchone()

        # while row is not None:
        #     print(row)
        #     row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    get_games()

# execute sql string (for creating table if it doesnt exist) and print column names


def create_table(sql, tablename):
    """ query data from the vendors table """
    cnn = None
    try:
        cnn = psycopg2.connect(host=hostname,
                           dbname=DB_username,
                           user=DB_username,
                           password=DB_password)

        cnn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        Curs = cnn.cursor()
        #select current version of db
        Curs.execute('''select version()''')
        Curs.execute(sql)
        Curs.execute(f"Select * FROM {tablename} LIMIT 0")
        colnames = [desc[0] for desc in Curs.description]
        coltypes = [desc[1] for desc in Curs.description]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnn is not None:
            cnn.close()
    print(colnames)
    print(coltypes)
    Curs.close()


# %%
# sql string to create games table
crt_games = '''CREATE TABLE if not exists games(
    game_id int, player_black_username varchar(60),player_white_username varchar(60),
    result_black varchar(30), result_white varchar(30),game_mode varchar(30),time_control int, inc int,
    date varchar(25), opening varchar(90),start_time varchar(50), PRIMARY KEY(game_id)

)'''
if __name__ == "__main__":
    create_table(crt_games, 'games')
# %%
# sql string to create players table
crt_players = '''CREATE TABLE if not exists players(
    player_id int, username varchar(60), name varchar(80),join_date int,
   country_code varchar(3), streamer_status varchar(5),title varchar(3), PRIMARY KEY(player_id)
)'''
if __name__ == "__main__":
    create_table(crt_players, 'players')
# %%
# sql string to create moves table
crt_moves = '''CREATE TABLE if not exists moves(
    game_id int, move_num int, white_move varchar(6), white_clck int, black_move varchar(6),
    black_clck int
)'''
if __name__ == "__main__":
    create_table(crt_moves, 'moves')
# %%


def insert_into_table(table, columns, values):

    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        # select current version of the db
        cur.execute("SELECT version()")
        sql_str = sql.SQL(f"INSERT INTO {table}({columns}) VALUES({values})")
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


def insert_multi_into_table(table, columns, values):
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # select current version of the db
        cur.execute("SELECT version()")

        sql_str = sql.SQL(f"INSERT INTO {table}({columns}) VALUES{values}")
        # print(sql_str)
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
def clear_table(table):
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        # select current version of the db
        cur.execute("SELECT version()")
        
        sql_str = sql.SQL(f"Delete FROM {table}")
        print(sql_str)
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
def get_max_games():
    """ query data from the games table """
    conn = None
    try:
        conn = psycopg2.connect(host=hostname,
                                dbname=DB_username,
                                user=DB_username,
                                password=DB_password)
        cur = conn.cursor()
        cur.execute("SELECT max(game_id) FROM games")
        row = cur.fetchall()
        max_id = row[0][0]
            
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        if max_id == None:
            max_id = 0
    return max_id
if __name__ == "__main__":
    print(get_max_games())

# %%
