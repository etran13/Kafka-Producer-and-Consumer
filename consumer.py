import sys
import json

from kafka import KafkaConsumer
import ssl

#Get the topic to access
topic = sys.argv[1:]

##Connection information
conf = {
        'bootstrap_servers': ["etran13.novalocal:9092","etran13-2.novalocal:9092","etran13-3.novalocal:9092"],
    'topic_name': 'Topic_One',
    'sasl_plain_username': 'usercc',
    'sasl_plain_password': 'MyUserPasswd2024',
     'consumer_id': 'consumer_id'
} 

context = ssl.create_default_context()
context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
context.verify_mode = ssl.CERT_REQUIRED
##Certificate file
context.load_verify_locations("cert.crt")

print('start consumer')
consumer = KafkaConsumer(conf['topic_name'],
                        bootstrap_servers=conf['bootstrap_servers'],
                        group_id=conf['consumer_id'],
                        sasl_mechanism="PLAIN",
                        ssl_context=context,
                        security_protocol='SASL_SSL',
                        auto_offset_reset='earliest',
                        consumer_timeout_ms = 15000,
                        sasl_plain_username=conf['sasl_plain_username'],
                        sasl_plain_password=conf['sasl_plain_password'])

sum = 0
message_count = 0

#Get all the messages
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    sum += message["random_int"]
    message_count += 1

    #Only read the first million messages
    if message_count == 1000000:
        break

#Print message count and sum
print(f"Consumer read {message_count} messages, the sum for the topic is {sum}")