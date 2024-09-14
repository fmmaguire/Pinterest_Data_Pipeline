import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = ""
        self.USER = ''
        self.PASSWORD = ''
        self.DATABASE = ''
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()



def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        pin_invoke_url = ""
        geo_invoke_url = ""
        user_invoke_url = ""


        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

                pin_payload = json.dumps({
                    "records": [{"value": {
                    "index": pin_result["index"],
                    "unique_id": pin_result["unique_id"],
                    "title": pin_result["title"],
                    "description": pin_result["description"],
                    "poster_name": pin_result["poster_name"],
                    "follower_count": pin_result["follower_count"],
                    "tag_list": pin_result["tag_list"],
                    "is_image_or_video": pin_result["is_image_or_video"],
                    "image_src": pin_result["image_src"],
                    "downloaded": pin_result["downloaded"],
                    "save_location": pin_result["save_location"],
                    "category": pin_result["category"]
            }}]})

                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", pin_invoke_url, headers=headers, data=pin_payload)
                print(response.status_code)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

                geo_payload = json.dumps({
                    "records": [{"value": {
                    "ind": geo_result["ind"],
                    "timestamp": geo_result["timestamp"],
                    "latitude": geo_result["latitude"],
                    "longitude": geo_result["longitude"],
                    "country": geo_result["country"]

            }}]},default=str)

                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", geo_invoke_url, headers=headers, data=geo_payload)
                print(response.status_code)


            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

                user_payload = json.dumps({
                    "records": [{"value": {
                    "ind": user_result["ind"],
                    "first_name": user_result["first_name"],
                    "last_name": user_result["last_name"],
                    "age": user_result["age"],
                    "date_joined": user_result["date_joined"]
            }}]},default=str)

                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", user_invoke_url, headers=headers, data=user_payload)
                print(response.status_code)



            # print(pin_result)
            # print(geo_result)
            # print(user_result)


def run_infinite_post_data_loop_v2():
    with open("retrieved_data.txt", "a") as file:  # Open the file in append mode
        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)

                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)

                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                
                for row in user_selected_row:
                    user_result = dict(row._mapping)
                
                # Write results to the file
                file.write(f"Pinterest Data: {pin_result}\n")
                file.write(f"Geolocation Data: {geo_result}\n")
                file.write(f"User Data: {user_result}\n")
                file.write("-" * 40 + "\n")  # Separate entries with a line


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


