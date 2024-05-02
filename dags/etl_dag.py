import os
import sys
import pandas as pd 
import numpy as np 
import boto3
import pymysql
import mysql.connector
from dotenv import load_dotenv
load_dotenv()
from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import datetime
from helpers.database_manager import DatabaseConnection
from helpers.data_transformation import DataTransformation
from helpers.aws_s3_manager import AWS_S3Manager


def database_manager():
    try:
        sql_executer = DatabaseConnection(
        host = os.getenv("HOST"),
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        database = os.getenv("DATABASE"))
        
        data = sql_executer.execute_query_to_dataframe(
        query=os.getenv("QUERY"))
        
        return data
        
    except Exception as e:
        print(e)
        
        
def data_transformation_manager():
    try:
        
        data = database_manager()
        dt = DataTransformation()
        transform_data = dt.data_transformation(data)
        
        dt.save_to_csv(
        data = transform_data,
        file_name = "transform_student_data.csv",
        path = "data/")
        
        return transform_data
        
    except Exception as e:
        print(e)
        
        
def aws_s3_manager():
    try:
        aws = AWS_S3Manager(
        bucket_name = os.getenv("BUCKET_NAME"),
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("REGION_NAME"))
        
        aws.upload_file(
        local_filename = "data/transform_student_data.csv",
        s3_key = "transform_student_data.csv")
        
        data_s3 = aws.read_csv_from_s3(s3_key="transform_student_data.csv")
        
        return data_s3
        
    except Exception as e:
        print(e)
        
        
def transform_student_data_dump_to_sql():
    try:
        sql_executer = DatabaseConnection(
        host = os.getenv("HOST"),
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        database = os.getenv("DATABASE"))
        
        data_s3 = aws_s3_manager()
        
        data_s3 = list(data_s3.itertuples(index=False,name=None))
        
        # create 2 table ym_transform_students_data in mysql data base 
        sql_executer.execute_query("""
            CREATE TABLE IF NOT EXISTS ym_transform_students_data (
            id BIGINT NOT NULL,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            gender VARCHAR(255) NOT NULL,
            part_time_job BIGINT NOT NULL,
            absence_days BIGINT NOT NULL,
            extracurricular_activities BIGINT NOT NULL,
            weekly_self_study_hours BIGINT NOT NULL,
            career_aspiration BIGINT NOT NULL,
            math_score BIGINT NOT NULL,
            history_score BIGINT NOT NULL,
            physics_score BIGINT NOT NULL,
            chemistry_score BIGINT NOT NULL,
            biology_score BIGINT NOT NULL,
            english_score BIGINT NOT NULL,
            geography_score BIGINT NOT NULL,
            total_score  BIGINT NOT NULL);
        """)
        

        insert_qury = """
            INSERT INTO ym_transform_students_data (
            id,
            first_name,
            last_name,
            email,
            gender,
            part_time_job,
            absence_days,
            extracurricular_activities,
            weekly_self_study_hours,
            career_aspiration,
            math_score,
            history_score,
            physics_score,
            chemistry_score,
            biology_score,
            english_score,
            geography_score,
            total_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
    
        sql_executer.insert_data_batch(
            insert_qury,
            data_s3,
            BATCH_SIZE= 200)
        
    except Exception as e:
        print(e)  
        
        
default_args ={
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ETL_Dag',
    default_args=default_args,
    description = "etl",
    start_date = datetime.datetime(2024,5,2),
    schedule_interval = '@daily',   
) as dag:
    
    
    data_extract_from_sql = PythonOperator(
        task_id = 'data_extract_from_sql',
        python_callable = database_manager,
    )
    
    
    data_transformation_dag  = PythonOperator(
        task_id = 'data_transformation_dag',
        python_callable = data_transformation_manager,
    )
    
    uplode_to_s3bucket  = PythonOperator(
        task_id = 'uplode_to_s3bucket',
        python_callable = aws_s3_manager,
    )
    
    dump_transform_data_to_sql  = PythonOperator(
        task_id = 'dump_transform_data_to_sql',
        python_callable = transform_student_data_dump_to_sql,
    )
    
    
data_extract_from_sql >> data_transformation_dag >> uplode_to_s3bucket >> dump_transform_data_to_sql
        
        
    
