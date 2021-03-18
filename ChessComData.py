from asyncio import gather  # for running multiple processes
from chessdotcom.aio import Client, get_titled_players, get_player_games_by_month
from pprint import pprint
from time import sleep
import json
import os
import boto3


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
                player, years, month) for month in months_list]

            # loop through requests until complete. # use asyincio for parallel.
            # response returns a list of chessdotcom response objects
            response = Client.loop.run_until_complete(gather(*cors))
            for i in months_list:
                # dictionary of key=month : values=gamedata
                games_p_year[i] = response[i-1].json["games"]
            successes += 1
        except:
            try:
                #allow api reset time in case of api overload error
                sleep(30) 
                cors = [get_player_games_by_month(
                    player, year, month) for month in months_list]
                # loop through requests until complete. # use asyincio for parallel.
                # response returns a list of chessdotcom response objects
                response = Client.loop.run_until_complete(gather(*cors))
                for i in months_list:
                    # dictionary of key=month : values=gamedata
                    games_p_year[i] = response[i-1].json["games"]
                successes += 1

            except Exception as e:
                print(e)
                print(f"error {player},{year}")
                # print('player = ' + player + ' month = '+str(month) + ' year =' + str(year))
                nothings += 1
        # key=year:values=all games played in specified months
        games[year] = games_p_year
    print("successes: " + str(successes))
    print("fails: " + str(nothings))
    return games

# # initialize games dictionary
# class game: 
#     def __init__(self):
#         self.



def save_player_games(folder, player_games, player_name, client ,disk_or_cloud=0, bucket=None):
    # use multiple files
    files_saved = []
    for year in player_games:
        # + = create if not available
        # os.path.join works on all OS's
        if disk_or_cloud == 0:
            with open(os.path.join(folder, f"{player_name}_{year}_games.json"), 'w+') as fp:
                # allow file to accept none-ascii values when writing
                json.dump(player_games[year], fp, ensure_ascii=False)
                files_saved.append(os.path.join(
                    folder, f"{player_name}_{year}_games.json"))
        else:
            file_key = folder + f"/{player_name}_{year}_games.json"
            content=json.dumps(player_games[year],ensure_ascii=False)
            client.Object(bucket, file_key).put(Body=content)
            files_saved.append(os.path.join(
                    folder, f"{player_name}_{year}_games.txt"))
    print(files_saved)

