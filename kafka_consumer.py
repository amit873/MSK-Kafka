from ensurepip import bootstrap
from kafka import KafkaConsumer

consumer = KafkaConsumer('MSK-Provisioned-Topic',bootstrap_servers=['b-2.msktutorialcluster.dt472o.c23.kafka.us-east-1.amazonaws.com:9092'])

for msg in consumer:
    rec_data = msg.value.decode('utf-8')
    r = rec_data.replace('"','')
    record = r.strip('\\n')
    print(record)
