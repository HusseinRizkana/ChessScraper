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
        try:
            # coroutines for the case of multiple title request
            cors = [get_player_games_by_month(
                player, year, month) for month in months_list]
            # loop through requests until complete. # use asyincio for parallel.
            #response returns a list of chessdotcom response objects
            response = Client.loop.run_until_complete(gather(*cors))
            for i, month in enumerate(months_list):
                # dictionary of key=month : values=gamedata
                games_p_year[month] = response[i].json["players"]
            successes += 1
        except:
            print(f"error {player},{year}")
            # print('player = ' + player + ' month = '+str(month) + ' year =' + str(year))
            nothings += 1
        # key=year:values=all games played in specified months
        games[year] = games_p_year
    return games

    print("player years returned: " + str(successes))
    print("months not returned: " + str(nothings))

    # + = create if not available

    # ensure_ascii=False don't convert to ascii
    
player_games = {}
player_games = retrieve_games_per_month(player, months_list, years_list)
folder = "output_data"



# %%

# %%
