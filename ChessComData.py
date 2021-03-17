#%%
from asyncio import gather # for running multiple processes
from chessdotcom.aio import Client,get_titled_players,get_player_games_by_month
from pprint import pprint
from time import sleep
import json
import os

# titles = ["GM","IM"] # Rank to retrieve
# cors = [get_titled_players(title) for title in titles] #coroutines for the case of multiple title request
# response = Client.loop.run_until_complete(gather(*cors))
# grand_master_names =response[0].json["players"]
# internation_master_names = response[1].json["players"]
def get_names_by_title(titles):
    ''' 
    takes in titles of players and returns a dictionary by title of all players holding that title
    '''
    names_by_title = {}
    cors = [get_titled_players(title) for title in titles] #coroutines for the case of multiple title request
    response = Client.loop.run_until_complete(gather(*cors))
    for i,name in enumerate(titles):
        names_by_title[name] = response[i].json["players"]
    return names_by_title


names = get_names_by_title(["GM","IM"])
nothings = 0
successes= 0 
# pprint(grand_master_name)
player_games = {}
games_per_month={}
#%%


#%%
for player in grand_master_names:
    print(player)
    for i in range(1,13):
        print(i)
        try:
            games_per_month[i] = chessdotcom.get_player_games_by_month(player,2020,i).json["games"]
            successes +=1  
        except:
            print('player = ' + player + ' month = '+str(i))
            nothings +=1
    player_games[player] = games_per_month
    #+ = create if not available 
    folder = "output_data"
    # os.path.join works on all OS's
    with open(os.path.join(folder, f"{player}_games.json"),'w+') as fp:
        json.dump(player_games[player],fp,ensure_ascii=False)
        #ensure_ascii=False don't convert to ascii
        # ,indent=4,sort_keys=True
    
    # use multiple files 

#%%
print("successes: " + str(successes))
print("fails: " + str(nothings))
