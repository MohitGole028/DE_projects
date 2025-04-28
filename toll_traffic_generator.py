## A streaming data pipeline that collects data about different vehicles passing through different toll plazas and streams the data to kafka
## which further store the collected data into a database.

# DETAILS:
# Topic = "toll"
# Database = "tolldata"
# Table = "livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint)"
# Zookeeper - "To manage event brokers"
# Kafka Server - "To stream the events"
# Traffic Simulator - "Producer that randomly generates vehicle and toll plaza data to stream to kafka."
# Streaming Data Reader - "Consumer that receives the data from kakfa stream and stores the data into the tolldata database."
# Data_Generator_Output - "Results of the producer sending the data into kafka stream"
# Data_Consumer_Output - "Results of the consumer receiving the data from the kafka stream"
# Database Results - "A sample view of the stored data in a livetolldata table of tolldata database."
# Data_Generator_Output - "Results of the producer sending the data into kafka stream"
# Data_Consumer_Output - "Results of the consumer receiving the data from the kafka stream"
# Database Results - "A sample view of the stored data in a livetolldata table of tolldata database."


"""
Top Traffic Simulator
"""
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'toll'

VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")
for _ in range(100000):
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    now = ctime(time())
    plaza_id = randint(4000, 4010)
    message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
    message = bytearray(message.encode("utf-8"))
    print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
    producer.send(TOPIC, message)
    sleep(random() * 2)
