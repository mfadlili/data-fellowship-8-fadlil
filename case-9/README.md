# Case 9 Data Fellowship 8 IYKRA

## Problems

Make an avro producer and avro consumer like the example in the code above has been shared but uses the bitcoin_price_Training dataset. 
If possible load to Big Query, will be a plus for this task.

## How to run the program?

1. Clone this repository

2. Open table_id.json in json folder, change the table id with "your_project_ID.your_dataset_name.your_table_ID".

3. Still in the json folder, change key.json with your own key, don't forget to change the file name to key.json

4. Open Command Prompt in this directory, and type this command below to dockerize the Confluent Kafka:

```sh
docker compose -f docker-compose.yml up
```

5. Create a python virtual environment, activate it, and install the dependencies inside requirements.txt, you can use this command:

```sh
pip install -r requirements.txt
```

6. To run the producer, type this command in the CMD:

```sh
python3 producer.py
```

7. To run the consumer, type this command in the CMD:

```sh
python3 consumer.py
```

8. If you failed to install the dependencies in step 5, you can use Windows Subsystem for Linux (WSL) as terminal in steps 5 through 7 instead of using Command Prompt.

## Result

![](img/1.png)<br>

![](img/2.png)<br>
