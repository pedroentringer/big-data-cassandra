from time import sleep
import json
from kafka import KafkaProducer
import pandas as pd

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'breastcancer'

# Load the breast cancer dataset
data = pd.read_csv('./datasets/breast-cancer-wisconsin.data', header=0)

# Remove missing values (represented as '?')
#data = data.replace('?', pd.np.nan).dropna()

# Create Kafka producer
producer = KafkaProducer (bootstrap_servers =['localhost:9092'],
                              key_serializer = str.encode, value_serializer =
                              lambda v : json.dumps(v).encode('utf-8'))

# Define the producer function
def send_messages():
    for _, row in data.iterrows():
        # Convert each row of the DataFrame to a comma-separated string
        message = ','.join(str(x) for x in row.values)
        message = message.split(",")
        messageFinal = {
            #'codenumber': message[0],
            'clumpthickness': message[1],
            'uniformitycellsize': message[2],
            'uniformitycellshape': message[3],
            'marginaladhesion': message[4],
            'singleepithelialcellsize': message[5],
            'barenuclei': message[6],
            'blandchromatin': message[7],
            'normalnucleoli': message[8],
            'mitoses': message[9],
            'classes': message[10]
        }
        print(messageFinal)
        sleep(2)
        producer.send(topic, key = message[0], value = messageFinal)
        producer.flush()

# Send messages to Kafka
send_messages()

# Close the producer
producer.close()