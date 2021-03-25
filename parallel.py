from ChessComData import (get_player_data, get_names_by_title,
                          retrieve_games_per_month_series, save_player_games, extract_game_pgn,
                          retrieve_game_values)
from boto3 import resource
from rds_db_interact import (insert_into_table,insert_multi_into_table,
                             clear_table, insert_to_moves_games, get_table_as_pd)

from Secrets import (username, access_key_ID, secret_access_ID)

import multiprocessing as mp
import os
from functools import partial
import csv
import tqdm
class bigchessscrape:
    def __init__(self, folder=None):
        self = self
        self.folder = folder
        self.columns_moves = "game_id, move_num, white_move, white_clck, black_move, black_clck"
        self.columns_games = "player_black_username, player_white_username, result_black, result_white, game_mode, time_control, inc, date, opening, white_elo, black_elo, start_time"
        self.columns_players = "player_id, username, name, join_date, country_code, streamer_status, title"

    def retrieve_save_player_data_RDS(self, player):
        # print(player)
        pd = get_player_data(player)

        # values to be inputted in rds players sql table
        values_players = f"{pd['player_id']},'{player}','{pd['name']}',{pd['join_date']}," + \
            f"'{pd['country_code']}','{pd['is_streamer']}','{pd['title']}'"
        # saving to rds table players
        insert_into_table("players", self.columns_players, values_players)

    def retrieve_save_gamefiles(self, player, months_list, years_list):
        player_games = retrieve_games_per_month_series(
            player, months_list, years_list)
        s3Client = resource("s3", aws_access_key_id=access_key_ID,
                            aws_secret_access_key=secret_access_ID)
        # saving to disk
        if self.folder != None:
            save_player_games('output_data', player_games, player,
                              s3Client, disk_or_cloud="disk")
        # # saving to cloud s3
        save_player_games('output_data', player_games, player,
                          s3Client, disk_or_cloud="cloud", bucket='chess-games-js')
        return player_games

    def save_parallel(self, game):
        values_games, moves_data = retrieve_game_values(game)
        # add game Id identifier to every move
        insert_to_moves_games(
            "moves", "games", self.columns_moves, self.columns_games, values_games, moves_data)
        #             #insert into games table

    def scrape(self, player, months_list, years_list):
        # initialize all player games dict and games per month dict
        player_games = {}

        # return dictionary of player data
        self.retrieve_save_player_data_RDS(player)

        player_games = self.retrieve_save_gamefiles(
            player, months_list, years_list)
        # iterate over years and months chosen
        
        for year in years_list:
            for month in months_list:
                for game in player_games[year][month]:
                    self.save_parallel(game)

    def scrape_by_title_parallel(self, title_list, months_list, years_list):
        names = get_names_by_title(title_list)
        scrape_par = partial(self.scrape, months_list=months_list,
                             years_list=years_list)
        
        for title in title_list:
            print(title)
            

            pool2 = mp.Pool(4)
            for _ in tqdm.tqdm(pool2.map(scrape_par, [player for player in names[title]]), total=len(names[title])):
                pass
            pool2.close()      

def main():
    # print(min([len(player) for player in names]))
    years_list = [2020]  # chosen years
    # months_list = [i for i in range(1, 13)]  # chosen months
    months_list = [i for i in range(1, 13)]
    folder = "output_data"
    # initialize scraper
    scraper = bigchessscrape(folder)
    scraper.scrape_by_title_parallel(
        title_list=["GM"], months_list=months_list, years_list=years_list)
    # %%

if __name__ == "__main__":
    main()