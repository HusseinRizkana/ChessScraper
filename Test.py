import ChessComData
from pprint import pprint
from time import sleep
names = ChessComData.get_names_by_title(["GM","IM"])
player_games = {}
games_per_month = {}
# %%
years_list = [2020]  # chosen years
months_list = [i for i in range(1, 13)]  # chosen months
player_games = {}
folder = "output_data"
for player in names["GM"]:
    player_games = ChessComData.retrieve_games_per_month(player, months_list, years_list)
    ChessComData.save_player_games(folder, player_games, player)
    sleep(2)
#select output destination folder


