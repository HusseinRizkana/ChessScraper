#%%
from asyncio import gather  # for running multiple processes
from chessdotcom.aio import Client, get_titled_players, get_player_games_by_month
from pprint import pprint
from time import sleep
import json
import os
import boto3
import requests
#%%


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
                sleep(5) 
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

# initialize games dictionary
# class game: 
#     def __init__(self):
#         self.url = 
#         self.pgn = 
#         self.time_control = 
#         self.end_time=
#         self.rated = 
#         self.fe

# "url": "https://www.chess.com/game/live/4394781256",
#             "pgn": "[Event \"Live Chess\"]\n[Site \"Chess.com\"]\n[Date \"2020.01.13\"]\n[Round \"-\"]\n[White \"XuYinglun\"]\n[Black \"123lt\"]\n[Result \"0-1\"]\n[CurrentPosition \"6k1/5ppp/4p3/pp1p4/3P2P1/bPP4P/5PK1/1QBq4 w - -\"]\n[Timezone \"UTC\"]\n[ECO \"A45\"]\n[ECOUrl \"https://www.chess.com/openings/Indian-Game\"]\n[UTCDate \"2020.01.13\"]\n[UTCTime \"14:16:06\"]\n[WhiteElo \"2607\"]\n[BlackElo \"2669\"]\n[TimeControl \"180+2\"]\n[Termination \"123lt won by resignation\"]\n[StartTime \"14:16:06\"]\n[EndDate \"2020.01.13\"]\n[EndTime \"14:24:20\"]\n[Link \"https://www.chess.com/game/live/4394781256\"]\n\n1. d4 {[%clk 0:03:01.3]} 1... Nf6 {[%clk 0:02:56.5]} 2. Bf4 {[%clk 0:03:00.6]} 2... d5 {[%clk 0:02:55.8]} 3. e3 {[%clk 0:03:00.2]} 3... c5 {[%clk 0:02:53.6]} 4. Nf3 {[%clk 0:02:59.1]} 4... Nc6 {[%clk 0:02:54]} 5. Nbd2 {[%clk 0:02:53.7]} 5... cxd4 {[%clk 0:02:51.6]} 6. exd4 {[%clk 0:02:53.5]} 6... Qb6 {[%clk 0:02:49.3]} 7. Nb3 {[%clk 0:02:17.6]} 7... Bf5 {[%clk 0:02:49.1]} 8. Bd3 {[%clk 0:02:06.2]} 8... Bxd3 {[%clk 0:02:49.7]} 9. Qxd3 {[%clk 0:02:06]} 9... e6 {[%clk 0:02:50]} 10. O-O {[%clk 0:02:06.1]} 10... Be7 {[%clk 0:02:45.2]} 11. a4 {[%clk 0:01:39.3]} 11... a6 {[%clk 0:02:39.9]} 12. a5 {[%clk 0:01:38.4]} 12... Qd8 {[%clk 0:02:39.8]} 13. Ne5 {[%clk 0:01:14]} 13... Rc8 {[%clk 0:02:33.2]} 14. c3 {[%clk 0:01:10.2]} 14... O-O {[%clk 0:02:32.7]} 15. Nxc6 {[%clk 0:01:07.6]} 15... Rxc6 {[%clk 0:02:32.7]} 16. h3 {[%clk 0:01:07.9]} 16... Ne4 {[%clk 0:02:27.2]} 17. Rfe1 {[%clk 0:01:03.7]} 17... Qc8 {[%clk 0:02:09.2]} 18. Nd2 {[%clk 0:01:02.1]} 18... Nxd2 {[%clk 0:02:07.1]} 19. Qxd2 {[%clk 0:01:01.4]} 19... Bd6 {[%clk 0:02:06.7]} 20. Bg5 {[%clk 0:00:58.8]} 20... Qd7 {[%clk 0:02:04.4]} 21. Re2 {[%clk 0:00:56.1]} 21... Rfc8 {[%clk 0:02:01.9]} 22. Qd3 {[%clk 0:00:44.5]} 22... Rc4 {[%clk 0:01:59]} 23. Qf3 {[%clk 0:00:45]} 23... Ra4 {[%clk 0:01:52.1]} 24. Ree1 {[%clk 0:00:30.2]} 24... Rxa1 {[%clk 0:01:45.3]} 25. Rxa1 {[%clk 0:00:31.1]} 25... Qb5 {[%clk 0:01:46.9]} 26. Bc1 {[%clk 0:00:32.2]} 26... Qb3 {[%clk 0:01:44.2]} 27. Qe2 {[%clk 0:00:30.7]} 27... Rc4 {[%clk 0:01:42.6]} 28. Be3 {[%clk 0:00:17]} 28... Ra4 {[%clk 0:01:14.5]} 29. Rxa4 {[%clk 0:00:14.2]} 29... Qxa4 {[%clk 0:01:14.4]} 30. g4 {[%clk 0:00:12.4]} 30... Qxa5 {[%clk 0:00:41.5]} 31. Kg2 {[%clk 0:00:13.3]} 31... Qc7 {[%clk 0:00:40]} 32. Qc2 {[%clk 0:00:13.9]} 32... b5 {[%clk 0:00:40.2]} 33. Qd2 {[%clk 0:00:14.9]} 33... a5 {[%clk 0:00:40.6]} 34. Qd3 {[%clk 0:00:15.5]} 34... Qc4 {[%clk 0:00:40.5]} 35. Qb1 {[%clk 0:00:16.4]} 35... Qb3 {[%clk 0:00:40.5]} 36. Bc1 {[%clk 0:00:16.4]} 36... Qd1 {[%clk 0:00:38.2]} 37. b3 {[%clk 0:00:14.2]} 37... Ba3 {[%clk 0:00:26]} 0-1",
#             "time_control": "180+2",
#             "end_time": 1578925460,
#             "rated": true,
#             "fen": "6k1/5ppp/4p3/pp1p4/3P2P1/bPP4P/5PK1/1QBq4 w - -",
#             "time_class": "blitz",
#             "rules": "chess",
#             "white": {
#                 "rating": 2607,
#                 "result": "resigned",
#                 "@id": "https://api.chess.com/pub/player/xuyinglun",
#                 "username": "XuYinglun"
#             },
#             "black": {
#                 "rating": 2669,
#                 "result": "win",
#                 "@id": "https://api.chess.com/pub/player/123lt",
#                 "username": "123lt"
#             }



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
            file_key = folder + f"/{player_name}_{year}_games.txt"
            content=json.dumps(player_games[year],ensure_ascii=False)
            client.Object(bucket, file_key).put(Body=content)
            files_saved.append(os.path.join(
                    folder, f"{player_name}_{year}_games.txt"))
    print(files_saved)



def get_player_data(player):
    '''
    function to return player_id, streamer status, join date
    country,title and name on chess.com 
    input: user
    '''
    playerdict = {}
    url = "https://api.chess.com/pub/player/"
    end_point = player
    r = requests.get(url+end_point)
    playerdict["player_id"] = r.json()["player_id"]
    playerdict["title"] = r.json()["title"]
    playerdict["is_streamer"] = r.json()["is_streamer"]
    playerdict["join_date"] = r.json()["joined"]
    playerdict["country"] = r.json()["country"].replace('https://api.chess.com/pub/country/','')
    playerdict["name"] = r.json()["name"]

    return playerdict