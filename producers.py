import json
import threading
from kafka import KafkaProducer

sensors = ["sensor_0", "sensor_1", "sensor_2", "sensor_3", "sensor_4", "sensor_5", "sensor_6", "sensor_7", "sensor_8",
           "sensor_9", "sensor_10", "sensor_11", "sensor_12", "sensor_13", "sensor_14", "sensor_15", "sensor_16",
           "sensor_17", "sensor_18", "sensor_19", "sensor_20", "sensor_21"]
file_name = './datasets/test_FD003.txt'
topic = "topic_sensors"  # Kafka topic
id = 1  # message ID


# Function to create KafkaProducer instances for each sensor
def create_producers():
    producers = []

    for sensor in sensors:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], key_serializer=str.encode,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producers.append(producer)

    return producers


# Function to send a message to Kafka using the given producer
def send_message(producer, message):
    key = ++id
    producer.send(topic, key=str(key), value=message)
    print(message)


# Function to create a new thread to send a message to Kafka
def message_thread(producer, message):
    thread = threading.Thread(target=send_message, args=(producer, message,))
    thread.start()
    thread.join()


# Function to read a line of sensor data from a file and send each value as a separate message to Kafka
def read_line(producers, line_content):
    # Parse the sensor values from the line of data
    sensors_values = line_content.split(" ")
    sensors_values = sensors_values[5:]
    sensors_values = sensors_values[:-2]

    for index, sensor_value in enumerate(sensors_values):
        producer = producers[index]  # Get the KafkaProducer instance for the current sensor
        sensor_name = sensors[index]  # Get the name of the current sensor

        # Create a message dictionary with the sensor name and value
        message = {
            "sensor_name": sensor_name,
            "value": sensor_value
        }

        message_thread(producer, message)


# Function to read sensor data from a file and send it to Kafka
def start():
    producers = create_producers()  # Create KafkaProducer instances for each sensor

    with open(file_name, mode='r') as file:
        while True:
            line_content = file.readline()  # Read a line of sensor data from the file

            if not line_content:  # If the end of the file is reached, stop reading
                break

            read_line(producers, line_content)  # Send the sensor data to Kafka


start()
exit(0)
