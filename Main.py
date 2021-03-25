# %%
from ChessComData import (get_names_by_title,get_player_data,retrieve_game_values,
save_player_games,extract_game_pgn,retrieve_games_per_month_series)
from boto3 import resource
from rds_db_interact import (insert_into_table,insert_to_moves_games)
import Secrets 
s3Client = resource("s3", aws_access_key_id=Secrets.access_key_ID,
                    aws_secret_access_key=Secrets.secret_access_ID)

class ChessWebScraper():
    def __init__(self,years,months,titles=[],usernames=[]):
        self = self
        self.usernames = usernames
        self.years_list = years
        self.months_list = months
        self.output_folder =""
        self.titles = titles
    # def
# %%
# get player usernames that are currently holding this title
names = get_names_by_title(["GM"])
print(len(names))
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
columns_games = "player_black_username, player_white_username, result_black, result_white, game_mode, time_control, inc, date, opening, white_elo, black_elo, start_time"
columns_players = "player_id, username, name, join_date, country_code, streamer_status, title"
# retrieve players in chosen titles
#%%
for player in names["GM"]:
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
                # add game Id identifier to every move 
                values_games, moves_data = retrieve_game_values(game)
        # add game Id identifier to every move
                insert_to_moves_games(
                "moves", "games", columns_moves, columns_games, values_games, moves_data)
        #             #insert into games table