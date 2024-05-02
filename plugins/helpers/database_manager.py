import os
import sys
import pandas as pd 
import numpy as np 
import pymysql
import mysql.connector
from dotenv import load_dotenv
load_dotenv()


class DatabaseConnection:

    def __init__(self, host, user, password, database):
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.connection.cursor()

        except Exception as e:
            print(e)
    def execute_query(self, query):
        """
        Executes a SQL query and returns the results
        
        Args:
            query (str): SQL query to execute.
            
        Returns: query results.
        """
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            results = self.cursor.fetchall()  # Fetch all results
            return results
        except Exception as e:
            return f"Error executing query: {str(e)}"
        
        
    def execute_query_to_dataframe(self,query):
        """
        Executes a SQL query and returns the results as a pandas DataFrame.

        Args:
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()  # Fetch all results
            columns = self.cursor.column_names
            df = pd.DataFrame(results, columns=columns)
            return df

        except Exception as e:
                return f"Error executing query: {str(e)}"

        

    def insert_data_batch(self, query, data, BATCH_SIZE):
        """
        

        Args:
            query (str): This method will be take query arguments as string
            data (take data as tuple): pass data as tuple
            BATCH_SIZE (int): Give batch size acording to your needs.
        """
        try:
            
            for i in range(0, len(data), BATCH_SIZE):
                batch_data = data[i:i + BATCH_SIZE]
                self.cursor.executemany(query, batch_data)
                self.connection.commit()
                
        except Exception as e:
            print(e)
            
    def save_to_csv(self, data, file_name, path):
        
        '''
        arguments: data, file_name, path
        This method will save the data to csv file
        '''
        try:
            # create the folder if it does not exist
            if not os.path.exists(path):
                os.makedirs(path)

            # specify the full file path
            full_file_path = os.path.join(path, file_name)

            # ssave the data to a CSV file....
            data.to_csv(full_file_path, index=False)
            print(f"File '{file_name}' saved to disk in the '{path}' folder.")
        
        except Exception as e:
            print(e)
        
        
    def close(self):
        self.connection.close()
        print("close connection")