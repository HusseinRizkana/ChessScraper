# %%
from ChessComData import (get_player_data, get_names_by_title,
                          retrieve_games_per_month_series, save_player_games, extract_game_pgn)
from boto3 import resource
from rds_db_interact import (insert_into_table, get_players, get_moves, insert_multi_into_table,
                             clear_table, get_max_games, get_games)

from Secrets import (username, access_key_ID, secret_access_ID,
                     DB_password, DB_username, hostname, DB_name)

s3Client = resource("s3", aws_access_key_id=access_key_ID,
                    aws_secret_access_key=secret_access_ID)
import multiprocessing as mp
import os
from functools import partial
def scrape(player,table_players, columns_players, months_list, years_list,columns_games,columns_moves):
        # return dictionary of player data

        print(player)
        pd = get_player_data(player)

        # values to be inputted in rds players sql table
        values_players = f"{pd['player_id']},'{player}','{pd['name']}',{pd['join_date']}," + \
            f"'{pd['country_code']}','{pd['is_streamer']}','{pd['title']}'"
        # saving to rds table players
        insert_into_table(table_players, columns_players, values_players)
        player_games = retrieve_games_per_month_series(
            player, months_list, years_list)
        # saving to disk
        save_player_games('output_data', player_games, player,
                        s3Client, disk_or_cloud="disk")
        # saving to cloud s3
        save_player_games('output_data', player_games, player,
                        s3Client, disk_or_cloud="cloud", bucket='chess-games-js')
        # iterate over years and months chosen
        for year in years_list:
            for month in months_list:
                # iterate over retrieved games in that month
                for game in player_games[year][month]:
                    # extract game portable game notation data
                    pgn = game["pgn"]
                    #new game_id
                    game_num = get_max_games() + 1
                    # translate game into usable structure for SQL insert
                    moves_data, game_date, game_time, game_opening = extract_game_pgn(
                        pgn)
                    start_time = game_time
                    player_black_username = game["black"]["username"]
                    player_white_username = game["white"]["username"]
                    result_black = game["black"]["result"]
                    result_white = game["white"]["result"]
                    game_mode = game["rules"]
                    timeclass = game["time_control"].split("+")
                    # accounting for cases where there is no increment
                    time_control = ["NULL", "NULL"]
                    for i in range(len(timeclass)):
                        time_control[i] = timeclass[i]
                    date = game_date
                    opening = game_opening
                    white_elo = game["white"]["rating"]
                    black_elo = game["black"]["rating"]

                    game_id = str(game_num)
                    values_games = "{},'{}','{}','{}','{}','{}',{},{},'{}','{}',{},{},'{}'".format(game_id,
                                player_black_username, player_white_username, result_black, result_white,
                                game_mode, time_control[0], time_control[1], date, opening, white_elo, black_elo, start_time)
                    # add game Id identifier to every move 
                    MD = [[game_id]+moves_data[i] for i in range(len(moves_data))]
                    # map into a list of tuples for single multiple insert
                    MD = list(map(tuple, MD))

                    #moves to string of tuples per move
                    values_moves = ','.join("({}, {}, '{}', {}, '{}', {})".format(
                        i, j, k, x, y, z) for i, j, k, x, y, z in MD)
                    #insert into games table
                    insert_into_table("games", columns_games, values_games)
                    #insert into moves table
                    insert_multi_into_table("moves", columns_moves, values_moves)
def main():
    # get player usernames that are currently holding this title
    names = get_names_by_title(["GM"])
    # print(names)
    # initialize all player games dict and games per month dict
    player_games = {}
    games_per_month = {}

    years_list = [2020]  # chosen years
    # months_list = [i for i in range(1, 13)]  # chosen months
    months_list = [1]
    player_games = {}
    folder = "output_data"
    # tables used
    table_players = "players"
    table_games = "games"
    table_moves = "moves"
    # columns to insert into
    columns_moves = "game_id, move_num, white_move, white_clck, black_move, black_clck"
    columns_games = "game_id, player_black_username, player_white_username, result_black, result_white, game_mode, time_control, inc, date, opening, white_elo, black_elo, start_time"
    columns_players = "player_id, username, name, join_date, country_code, streamer_status, title"
    # retrieve players in chosen titles
    #%%
    scrape_par = partial(scrape,table_players = table_players, columns_players = columns_players,
     months_list = months_list, years_list = years_list,columns_games = columns_games,columns_moves = columns_moves)
    pool = mp.Pool(os.cpu_count())
    pool.map(scrape_par, [player for player in names["GM"]])

if __name__ == "__main__":
    main()
# %%
