from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'json/key.json'

with open("json/table_id.json", "r") as f:
    input_data = json.load(f)
    table_id = input_data["table_id"]

def create_table_big_query(table_id):
    client = bigquery.Client()
    schema = [
                bigquery.SchemaField("Date", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("Open", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("High", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("Low", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("Close", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("Volume", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("Market_Cap", "STRING", mode="REQUIRED")
            ]
    try :
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print("Success to create a Bigquery table.")
    except :
        print("Failed to create a Bigquery table.")

    return client


def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "bitcoin.price.avro.consumer.2",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["bitcoin.price"])
    client = create_table_big_query(table_id)

    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                try:
                    client.insert_rows_json(table_id, [message.value()])
                    print("insert data success")
                except:
                    print("insert data failed")
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()

