import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import os
import warnings
warnings.filterwarnings("ignore")
import csv
from sqlalchemy import create_engine, types
import matplotlib.pyplot as plt
from sqlalchemy import inspect
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String, Float, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text as sa_text
import mysql.connector
import datetime
import config
import auth


metadata = MetaData()

# Please make comments in English

def db_conn(conn_type,DB_name,host=auth.STAGING_DB_ADDRESS, user_name=auth.STAGING_DB_USER,password=auth.STAGING_DB_PASS,local_user=auth.LOCAL_USER,local_pass=auth.LOCAL_PASS):
  """
    Connects to a database and returns a database engine.

    Args:
        DB_name (str): The name of the database to connect to. If "local",
            connects to a local MySQL database. Otherwise, connects to a
            remote MySQL database on AWS.

    Returns:
        A SQLAlchemy database engine connected to the specified database.

    Raises:
        None.

    Example:
        To connect to a local MySQL database:

        >>> engine = db_conn("local")

        To connect to a remote MySQL database on AWS:

        >>> engine = db_conn("")
    """
  if conn_type=="local":
    conn = create_engine(f'mysql://{auth.local_user}:{auth.local_pass}@localhost/{DB_name}')
  if conn_type=="prod":
    conn = create_engine(f'mysql://{auth.PROD_DB_USER}:{auth.PROD_DB_PASS}@{auth.PROD_DB_ADDRESS}/{DB_name}')
  if conn_type=="staging":
    conn = create_engine(f'mysql://{auth.STAGING_DB_USER}:{auth.STAGING_DB_PASS}@{auth.STAGING_DB_ADDRESS}/{DB_name}')
  else:
    conn = create_engine(f'mysql://{auth.ELSE_DB_USER}:{auth.ELSE_DB_PASS}@{auth.ELSE_DB_ADDRES}/{DB_name}')
  return conn


# Checks if table exists indb and return True/False accordingly
def table_exists( conn_type,DB_name,table_name):
  """
    Check if the specified table exists in the given database.

    Arguments:
    DB_name -- the name of the database to check for the table
    table_name -- the name of the table to be checked

    Returns:
    True if the table exists in the database, False otherwise.
    """
  conn = db_conn( conn_type,DB_name)
  
  return inspect(conn).has_table(table_name)

def create_table(conn_type,DB_name):
  """
  Create a new table in the specified database.

    Args:
        DB_name (str): The name of the database to create the table in.
    Returns:
        None.

    Raises:
        ValueError: If the database name is invalid.
        RuntimeError: If there is an error creating the table.
    """
  try:
    conn = db_conn( conn_type,DB_name)
    with conn.connect() as conn:
      f=os.path.join('..','sql','ddl','create_table_1.sql')
      fd = open(f, 'r')
      sqlFile = fd.read()
      conn.execute(sa_text(sqlFile).execution_options(autocommit=True))
  except Exception as e:
    raise RuntimeError("Error creating table: {}".format(str(e)))


def write_to_db(file_path,conn_type, DB_name,table_name):
  """
    Write the results from a file to a database table.

    Args:
        file_path (str): The file path to read the data from.
        DB_name (str): The name of the database to write the data to.
        table_name (str): The name of the table to insert the data into.

    Returns:
        str: A message indicating the action has completed.

    Raises:
        None

    Examples:
        >>> write_to_db('data/results.csv', 'my_database', 'my_table')
        'Action is completed'
    """
  engine = db_conn(conn_type,DB_name) 
  df = pd.read_csv(file_path,sep=',',quotechar='\'',encoding='utf8') 
  df.to_sql(table_name,con=engine,index=False,if_exists='append') 
  return print("action is completed")



def truncate_table(conn_type,DB_name,table_name):
  """Truncate all records in a specified database table.

    Only use this function when necessary, as it permanently deletes all records in the specified table.
    
    Args:
        DB_name (str): The name of the database to connect to.
        table_name (str): The name of the table to truncate.

    Returns:
        None

    Raises:
        Any exceptions raised by the underlying database connection or SQL execution will be passed through.

    """
  conn = db_conn(conn_type,DB_name)
  with conn.connect() as conn:
    conn.execute(sa_text(f'''TRUNCATE TABLE {table_name}''').execution_options(autocommit=True))


# Reading data from mysql to Pandas, note : we need to reconnect because this execution works with mysql-connector
def get_data(sql_list_name,directory):
  
  """
    Execute SQL commands and pull the necessary datasets from a database, writing the results to CSV files.

    Args:
        sql_list_name (list): A list of the names of the files to read SQL commands from.
        directory (str): The path to the directory containing the SQL files.

    Returns:
        None.

    Raises:
        None.

    This function connects to a MySQL database using the provided credentials, reads SQL commands from
    the specified files. in the given directory, and executes those commands to retrieve data
    from the database. The resulting data is then written to CSV files in the `data` directory, with filenames based on
    the names provided in `sql_list_name`. If any errors occur during the execution of the function,
    they will be raised as exceptions.
  """
  i=0
  myconn = mysql.connector.connect(host = auth.PROD_DB_ADDRESS , user = auth.PROD_DB_USER,passwd = auth.PROD_DB_PASS,database='DUGUN')
  for filename in [f for f in os.listdir(directory) if os.path.isfile(directory+os.sep+f)]:
    
     f=  os.path.join(directory, filename)
    
     fd = open(f, 'r')
     sqlFile = fd.read()
     database_selected = pd.read_sql(sqlFile,myconn)
     data_folder = os.path.join(os.getcwd(),'data')
     data_path = os.path.join(data_folder,f'{sql_list_name[i]}.csv')
     database_selected.to_csv(data_path)
     f"the file {filename} has read and saved"
     i+=1
  return print("done")
