from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time

def test_kafka_connection():
    """Teste la connexion à Kafka"""
    try:
        # Utilisez le port 29095 pour la connexion externe
        bootstrap_servers = 'localhost:29095'
        
        # Test admin client
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='test_client'
        )
        
        print("✅ Connexion Kafka réussie!")
        return True, bootstrap_servers
        
    except Exception as e:
        print(f"❌ Erreur de connexion Kafka: {e}")
        return False, None

def create_test_topic(bootstrap_servers):
    """Crée un topic de test"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        # Vérifie si le topic existe déjà
        existing_topics = admin_client.list_topics()
        if 'test_topic' not in existing_topics:
            topic_list = [NewTopic(name="test_topic", num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("✅ Topic 'test_topic' créé!")
        else:
            print("✅ Topic 'test_topic' existe déjà")
            
    except Exception as e:
        print(f"Erreur création topic: {e}")

def test_producer_consumer(bootstrap_servers):
    """Teste l'envoi et la réception de messages"""
    try:
        # Producteur
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Message test
        test_message = {
            "message": "Hello Kafka!", 
            "timestamp": time.time(),
            "test_id": "12345"
        }
        
        # Envoi
        future = producer.send('test_topic', test_message)
        result = future.get(timeout=10)  # Timeout de 10 secondes
        producer.flush()
        print("✅ Message test envoyé!")
        
        # Consommateur
        consumer = KafkaConsumer(
            'test_topic',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # Timeout de 10 secondes
        )
        
        # Lecture des messages
        print("🔍 Recherche de messages...")
        for message in consumer:
            print(f"✅ Message reçu: {message.value}")
            break
        else:
            print("❌ Aucun message reçu")
            
        consumer.close()
        
    except Exception as e:
        print(f"❌ Erreur producteur/consommateur: {e}")

if __name__ == "__main__":
    print("🧪 Test de l'infrastructure Kafka...")
    
    success, bootstrap_servers = test_kafka_connection()
    
    if success:
        create_test_topic(bootstrap_servers)
        test_producer_consumer(bootstrap_servers)
    
    print("🎉 Test Kafka terminé!")