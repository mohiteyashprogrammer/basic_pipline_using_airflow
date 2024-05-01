FROM apache/airflow:2.9.0
COPY . .
# RUN pip install --user --upgrade pip 
RUN pip install -r requirements.txt