# AiCore Project: ChessScraper

This is my repository for scraping and analysing chess data from chess.com.
It can currently:  
1.Retrieve public game, moves and player data using the chessdotcom api  
2.Save the raw data to local folder or to given AWS s3 credentials  
3.Create relevant rds postgresql tables(given AWS rds credentials) and populate them with the relevant structured data  
4.Perform basic data cleaning operations  
5.(Optional): run on multiple processors to accelerate the process.

