import sys
import uuid
import random
from datetime import datetime
import json

from kafka import KafkaProducer
import ssl

#Get the topic to send to
topic = sys.argv[1]
print(str(topic))

##Connection information
conf = {
        'bootstrap_servers': ["etran13.novalocal:9092",
                              "etran13-2.novalocal:9092",
                              "etran13-3.novalocal:9092"],
    'topic_name': str(topic),
    'sasl_plain_username': 'usercc',
    'sasl_plain_password': 'MyUserPasswd2024'
}

context = ssl.create_default_context()
context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
context.verify_mode = ssl.CERT_REQUIRED
##Certificate file
context.load_verify_locations("cert.crt")

print('start producer')
producer = KafkaProducer(bootstrap_servers=conf['bootstrap_servers'],
                        sasl_mechanism="PLAIN",
                        ssl_context=context,
                        security_protocol='SASL_SSL',
                        sasl_plain_username=conf['sasl_plain_username'],
                        sasl_plain_password=conf['sasl_plain_password'])

#Main sending loop
for i in range(1000000):
  print(f"Sending {i}")

  #Create JSON payload
  id = str(uuid.uuid4())
  payload_dict = {"id": id,
                  "random_int": str(random.randint(1, 1000)),
                  "timestamp": str(datetime.now()),}
  data = json.dumps(payload_dict).encode("utf-8")

  #Send to topic
  producer.send(conf["topic_name"], data)
  producer.flush()
  
  #Echo last id if done
  if i == 999999:
    print(id)

#Finish
producer.close()
print('end producer')

