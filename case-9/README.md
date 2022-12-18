# Case 7 Data Fellowship 8 IYKRA

## Problems

Create streaming processing using Kafka basic components which consume/subscribe data from twitter API. 
Twitter data thatused is up to students, feel free to explore. 
Scripts can be pushed to github or upload to Gdrive.

## How to run the program?

1. Clone this repository

2. Open bearer.json with notepad, change the bearer token inside it with yours.

3. Open input.json with notepad, change the "search" value with a topic that you want to search in twitter.

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
python3 kafka_producer.py
```

7. To run the consumer, type this command in the CMD:

```sh
python3 kafka_consumer.py
```

8. If you failed to install the dependencies in step 5, you can use Windows Subsystem for Linux (WSL) as terminal in steps 5 through 7 instead of using Command Prompt.

9. Open your browser and enter http://localhost:9021/clusters to see the Control Center of Confluent Kafka.
