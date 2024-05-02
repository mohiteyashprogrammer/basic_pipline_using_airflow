import os
import sys
import pandas as pd 
import numpy as np 


class DataTransformation:
    
    def __init__(self):
        pass
    def data_transformation(self,data):
        
        '''
        This method will take data as input and return transformed data
        
        arguments: data
        
        return: transformed data
        '''
        
        try:
            #drop index column from data
            data.drop(data.columns[0], axis=1,inplace=True)
            
            # replacr unknown feature with most frequent value Software Engineer
            data["career_aspiration"] = data["career_aspiration"].str.replace("Unknown","Software Engineer")
            
            # geting total score of each student
            data["total_score"] = data["math_score"] + data["history_score"] + data["physics_score"] + data["chemistry_score"] + data["biology_score"] +data["english_score"] + data["geography_score"]
            
            return data
        
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