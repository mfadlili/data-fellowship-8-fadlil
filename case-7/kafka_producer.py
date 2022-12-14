from confluent_kafka import Producer
import json
import logging
import requests
import time

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

####################
p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')
#####################
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
#####################

bearer_token = "AAAAAAAAAAAAAAAAAAAAACLxYAEAAAAAn%2FvfZ%2BgGDX52Vpp8xvaaEny7D3g%3DJSbX7jp7hmoGTDcTnU9CXwcLxqCM01Bq6MTbB1wxR0vRL73EeI"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def main():

    response = requests.get(
        "https://api.twitter.com/2/tweets/sample/stream", auth=bearer_oauth, stream=True
    )

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            data = {"id": json_response["data"]["id"], "tweet":json_response["data"]["text"]}
            m = json.dumps(data)
            p.poll(1)
            p.produce('twitter-stream-sample', m.encode('utf-8'), callback=receipt)
            p.flush()
            time.sleep(3)
        
if __name__ == '__main__':
    main()