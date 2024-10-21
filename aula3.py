from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Configurações
bootstrap_servers = ['localhost:9092']
topic_name = 'optimized_logs_topic'

# Configuração e criação do tópico
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
admin_client.create_topics(new_topics=[topic], validate_only=False)

# Produtor otimizado
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    batch_size=32*1024,  # 32 KB
    linger_ms=100,       # 100 ms
    acks='all'
)

# Produzir mensagens
for i in range(100):
    message = f"Data event {i}".encode('utf-8')
    producer.send(topic_name, message)
    time.sleep(0.01)  # Simula a geração de dados em intervalos regulares

producer.flush()

# Consumidor otimizado
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    fetch_min_bytes=1024,  # 1 KB
    fetch_max_wait_ms=200, # 200 ms
    consumer_timeout_ms=1000
)

# Consumir mensagens
print("Consumed messages:")
for message in consumer:
    print(f"Message: {message.value.decode('utf-8')}")

# Limpeza
admin_client.delete_topics([topic_name])
