#%%
import pandas as pd
from rds_db_interact import Table_sqls
from Secrets import hostname,DB_password,DB_username,DB_name

table_interact = Table_sqls(hostname,DB_username,DB_username,DB_password)

# %%
class Data_cleaner:
    def __init__(self,table_interact):
        self = self
        self.table_interact = table_interact

    def drop_duplicates(self,df,on_columns,identifier):
        subset=  on_columns
        dups = df.duplicated(subset=subset)
        print(dups)
        # print(dups.describe(include = ["category"]))
    def describe_categories(self,table,column,descriptions):
        df = table.groupby(column)[descriptions].nunique()
        print(df)


clean_data = Data_cleaner(table_interact)
#%%
games = table_interact.get_table_as_pd("games")
moves = table_interact.get_table_as_pd("moves")
countries = table_interact.get_table_as_pd("countries")
players = table_interact.get_table_as_pd("players")


# %%
# clean_data.drop_duplicates(games)
clean_data.describe_categories(games,"game_mode",["game_id"])
# %%
on_columns =["player_white_username","player_black_username","start_time","date"]
clean_data.drop_duplicates(games,on_columns,"game_id")
# %%
