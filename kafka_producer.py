from kafka import KafkaProducer
import json
from time import sleep
prod = KafkaProducer(bootstrap_servers=['b-2.msktutorialcluster.dt472o.c23.kafka.us-east-1.amazonaws.com:9092'])

f = open("/home/ec2-user/Data.csv","r")

for msg in f:
    data = msg
    prod.send('MSK-Provisioned-Topic',json.dumps(data).encode('utf-8'))
    sleep(2)
