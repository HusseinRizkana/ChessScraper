# %%
from asyncio import gather  # for running multiple processes
from chessdotcom.aio import Client, get_titled_players, get_player_games_by_month
from pprint import pprint
from time import sleep
import json
import os

def get_names_by_title(titles):
    ''' 
    takes in titles of players and returns a dictionary by title of all players holding that title
    '''
    names_by_title = {}
    # coroutines for the case of multiple title request
    cors = [get_titled_players(title) for title in titles]
    # loop through requests until complete. # use asyincio for parallel.
    response = Client.loop.run_until_complete(gather(*cors))
    for i, name in enumerate(titles):
        # dictionary of key=title : values=[players]
        names_by_title[name] = response[i].json["players"]
    return names_by_title


names = get_names_by_title(["GM"])

# pprint(grand_master_name)
player_games = {}
games_per_month = {}
# %%


# %%
years_list = [2020]  # chosen years
months_list = [i for i in range(1, 13)]  # chosen months
player = "123lt"  # chosen player chessdotcom usernames
def retrieve_games_per_month(player, months_list, years_list):
    '''
    retrieves all games played in the list of years in the list of months by player stated
    inputs: 
            player username as string
            months as list 1-12
            years as integer list

    '''
    nothings = 0
    successes = 0
    games_p_year = {}
    games = {}
    # print(player)
    for year in years_list:
        print(year)
        # try:
            # coroutines for the case of multiple title request
        cors = [get_player_games_by_month(
            player, year, month) for month in months_list]
        
        # loop through requests until complete. # use asyincio for parallel.
        #response returns a list of chessdotcom response objects
        response = Client.loop.run_until_complete(gather(*cors))
        for i in months_list:
            # dictionary of key=month : values=gamedata
            games_p_year[i] = response[i-1].json["games"]
        successes += 1
        # except:
        #     print(f"error {player},{year}")
        #     # print('player = ' + player + ' month = '+str(month) + ' year =' + str(year))
        #     nothings += 1
        # # key=year:values=all games played in specified months
        games[year] = games_p_year
    return games

    print("player years returned: " + str(successes))
    print("months not returned: " + str(nothings))

    # + = create if not available

    # ensure_ascii=False don't convert to ascii
    
player_games = {}
player_games = retrieve_games_per_month(player, months_list, years_list)
folder = "output_data"

def save_player_games(folder, player_games,player_name,additiondesc=""):
    # use multiple files
        # os.path.join works on all OS's
    files_saved = []
    for year in player_games:
        with open(os.path.join(folder, f"{player_name}_{year}_games.json"), 'w+') as fp:
            #allow file to accept none-ascii values when writing
            json.dump(player_games[year], fp, ensure_ascii=False)
            files_saved.append(os.path.join(folder, f"{player_name}_{year}_games.json"))
    
    print(files_saved)
            # ,indent=4,sort_keys=True

save_player_games(folder,player_games,player)





# %%

# %%
