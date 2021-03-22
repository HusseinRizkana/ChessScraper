from asyncio import gather  # for running multiple processes in parallel
from chessdotcom.aio import Client, get_titled_players, get_player_games_by_month
from pprint import pprint
from time import sleep
import json
import os
import boto3
import requests
from chess import pgn
import chess
import io




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

def retrieve_games_per_month_conc(player, months_list, years_list):
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
                # allow api reset time in case of api overload error
                sleep(25)
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
def retrieve_games_per_month_series(player, months_list, years_list):
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
            # response returns a list of chessdotcom response objects
            for i in months_list:
                # dictionary of key=month : values=gamedata
                
                print(i)
                response = Client.loop.run_until_complete(get_player_games_by_month(player,year=year,month=i))
                games_p_year[i] = response.json["games"]
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

def save_player_games(folder, player_games, player_name, client, disk_or_cloud="disk", bucket=None):
    # use multiple files
    files_saved = []
    for year in player_games:
        # + = create if not available
        # os.path.join works on all OS's
        if disk_or_cloud == "disk":
            with open(os.path.join(folder, f"{player_name}_{year}_games.json"), 'w+') as fp:
                # allow file to accept none-ascii values when writing
                json.dump(player_games[year], fp, ensure_ascii=False)
                files_saved.append(os.path.join(
                    folder, f"{player_name}_{year}_games.json"))
        elif disk_or_cloud == "cloud":
            file_key = folder + f"/{player_name}_{year}_games.txt"
            content = json.dumps(player_games[year], ensure_ascii=False)
            client.Object(bucket, file_key).put(Body=content)
            files_saved.append(os.path.join(
                folder, f"{player_name}_{year}_games.txt"))
    
    # if disk_or_cloud != "disk" or disk_or_cloud !="cloud":
    #     print("invalid save location, no files saved")
    # else:
    print(files_saved)
    print(f"to {disk_or_cloud}")

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
    playerdict["country_code"] = r.json()["country"].replace(
        'https://api.chess.com/pub/country/', '')
    playerdict["name"] = r.json()["name"]

    return playerdict

# class getClockVisitor(chess.pgn.BaseVisitor):
#     def visit_move(self, board, move):
#         print(board.san(move))

#     def result(self):
#         return None
    
def get_sec(time_str):
    """Get Seconds from time."""
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(float(s))


def extract_game_pgn(game):
    move_data = []
    moves_data = []

    game_data = chess.pgn.read_game(io.StringIO(game))
    game_date = game_data.headers["UTCDate"]
    game_opening = game_data.headers["ECOUrl"].replace("https://www.chess.com/openings/","")
    game_time = game_data.headers["UTCTime"]
    moves = str(game_data.mainline_moves())
    moves = moves.replace(" ","")
    moves = moves.replace("{[%","")
    moves=  moves.replace("]","")
    moves_split = moves.split("}")

    for i in moves_split[:-1]:
        i = i.split("clk")
        move_number = i[0].split(".",1)[0]
        if i[0].split(".",1)[1][0]!=".":
            white_move = i[0].rsplit(".",1)
            whiteMove = white_move[1]
            white_clk = i[1]
            move_data.extend([whiteMove,get_sec(white_clk)]) 
            moves_data.append([move_number,move_data])
        else:
            black_move = i[0].rsplit(".",1)
            blackMove = black_move[1]
            black_clk = i[1]
            move_data.extend([blackMove,get_sec(black_clk)])
            moves_data[int(move_number)-1][1:]= move_data
            move_data=[] 
    return moves_data,game_date,game_time,game_opening
