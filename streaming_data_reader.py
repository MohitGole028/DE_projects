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


"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'MjM1NjItc2Fpa3Jp'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()
