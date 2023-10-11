import json
import time
import threading
from kafka import KafkaProducer

topic = "topic_sensors"  # Kafka topic

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to send a message to Kafka using the given producer
def send_message(message):
    producer.send(topic, key=message['created_at'], value=message)
    print(message)


# Function to create a new thread to send a message to Kafka
def message_thread(message):
    thread = threading.Thread(target=send_message, args=(message,))
    thread.start()
    thread.join()


# Function to read a line of sensor data from a file and send each value as a separate message to Kafka
def read_line(file_type, line_content):
    # Parse the sensor values from the line of data
    sensors_values = line_content.split(" ")

    # Create a message dictionary with the sensor name and value
    message = {
        "created_at": str(int(time.time())),
        "file_type": file_type,
        "unit_number": sensors_values[0],
        "cycles": sensors_values[1],
        "setting1": sensors_values[2],
        "setting2": sensors_values[3],
        "setting3": sensors_values[4],
        "sensor_1": sensors_values[5],
        "sensor_2": sensors_values[6],
        "sensor_3": sensors_values[7],
        "sensor_4": sensors_values[8],
        "sensor_5": sensors_values[9],
        "sensor_6": sensors_values[10],
        "sensor_7": sensors_values[11],
        "sensor_8": sensors_values[12],
        "sensor_9": sensors_values[13],
        "sensor_10": sensors_values[14],
        "sensor_11": sensors_values[15],
        "sensor_12": sensors_values[16],
        "sensor_13": sensors_values[17],
        "sensor_14": sensors_values[18],
        "sensor_15": sensors_values[19],
        "sensor_16": sensors_values[20],
        "sensor_17": sensors_values[21],
        "sensor_18": sensors_values[22],
        "sensor_19": sensors_values[23],
        "sensor_20": sensors_values[24],
        "sensor_21": sensors_values[25],
    }

    message_thread(message)



# Function to read sensor data from a file and send it to Kafka
def start(file_type, file_name):
    with open(file_name, mode='r') as file:
        while True:
            line_content = file.readline()  # Read a line of sensor data from the file

            if not line_content:  # If the end of the file is reached, stop reading
                break

            read_line(file_type, line_content)  # Send the sensor data to Kafka


start("test", './datasets/test_FD001.txt')
start("train", './datasets/train_FD001.txt')
exit(0)
