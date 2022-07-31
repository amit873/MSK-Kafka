import psycopg2
from kafka import KafkaConsumer

num=100
consumer = KafkaConsumer('MSK-Provisioned-Topic',bootstrap_servers=['b-2.msktutorialcluster.dt472o.c23.kafka.us-east-1.amazonaws.com:9092'])

#Establishing the connection
conn = psycopg2.connect(
    database = "msk_postgresql",
    user = "postgresql",
    password = "1qaz2wsx",
    host = "database-postgre-msk.cttr1wnurh5r.us-east-1.rds.amazonaws.com",
    port = "5432"
)

#Setting auto commit false
conn.autocommit = True
print("Connected to Postgre DB Sucessfully")

#Setting auto commit false
cur = conn.cursor()

# Preparing SQL queries to INSERT a record into the database
for msg in consumer:
    num = num+1
    rec_data = msg.value.decode('utf-8')
    r = rec_data.replace('"','')
    record = r.strip('\\n')
    
    f_rec = record.split(",")
    name = f_rec[0]
    city = f_rec[1]
    country = f_rec[2]
    
    print(name)
    print(num)
    print(city)
    print(country)
    print("-----------")
    cur.execute('''INSERT INTO msk-schema.msk_postgresql_table("username","sub_no","city","country") VALUES ('{}',{},'{}','{}')'''.format(name,num,city,country))
    #cur.execute('''INSERT INTO msk-schema.msk_postgresql_table(username,sub_no,city,country) VALUES ('Amit',602,'Kumar','Gupta')''')
    
    # Commit your changes in the database
    conn.commit
    print("Records inserted........")
    
# Closing the connections
cur.close
conn.close
