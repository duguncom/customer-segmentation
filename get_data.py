import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import src.utils as utils
import src.config as config

print(config)


#Reading all the sql files and listing them
directory = config.SQL_DIR
sql_list_name = []
sql_list = [f for f in os.listdir(directory) if f.endswith("sql")]
for i in range(len(sql_list)):
    sql_list_name.append(str(sql_list[i]).rsplit( ".", 1 )[ 0 ])

#Saving SQL files output as Pandas in bulk mode
utils.get_data(sql_list_name,directory=config.SQL_DIR)