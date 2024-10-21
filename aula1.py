from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Configurações
bootstrap_servers = ['192.168.1.3:29092']
topic_name = 'logs_topic'
num_partitions = 3
replication_factor = 1

# Criar tópico com múltiplas partições
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
admin_client.create_topics(new_topics=[topic], validate_only=False)

# Produtor de mensagens
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
for i in range(30):
    message = f"Log message {i}".encode('utf-8')
    producer.send(topic_name, message)
    time.sleep(1)

producer.flush()

# Consumidor de mensagens
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=1000)
consumer.assign([TopicPartition(topic_name, p) for p in range(num_partitions)])

print("Consumed messages:")
for message in consumer:
    print(f"Partition: {message.partition} | Message: {message.value.decode('utf-8')}")

# Limpeza
#admin_client.delete_topics([topic_name])
