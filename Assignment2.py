import random
import time
from azure.eventhub import EventHubProducerClient, EventData

# Replace the below connection string and event hub name with your actual values
CONNECTION_STR = 'Endpoint=sb://<NAMESPACE_NAME>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>'
EVENT_HUB_NAME = '<EVENT_HUB_NAME>'

def generate_random_number():
    return random.randint(50, 100)

def send_event_to_hub(producer, event_data):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(event_data))
    producer.send_batch(event_data_batch)

def stream_random_numbers():
    producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    try:
        while True:
            random_number = generate_random_number()
            print(f"Generated number: {random_number}")
            send_event_to_hub(producer, str(random_number))
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopped streaming.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_random_numbers()
