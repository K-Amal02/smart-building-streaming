from kafka import KafkaConsumer
import json
import pymongo
from datetime import datetime
import time
import sys

class SimpleKafkaConsumer:
    def __init__(self):
        # VOTRE CONFIGURATION : 29095 pour l'externe, 9092 pour l'interne
        self.kafka_servers = [
            "localhost:29095",  # Depuis l'hôte
            "kafka:9092"        # Depuis les conteneurs Docker
        ]
        self.mongodb_uri = "mongodb://localhost:27017/"
        self.setup_mongodb()
        
    def setup_mongodb(self):
        """Connexion à MongoDB avec authentification"""
        try:
            self.client = pymongo.MongoClient(
                self.mongodb_uri,
                serverSelectionTimeoutMS=5000,
                authSource="admin"
            )
            self.client.admin.command('ismaster')
            self.db = self.client.smart_buildings
            self.raw_data = self.db.raw_sensor_data
            self.aggregated_data = self.db.aggregated_metrics
            print("✅ Connecté à MongoDB avec authentification")
        except Exception as e:
            print(f"❌ Erreur connexion MongoDB: {e}")
            sys.exit(1)
        
    def test_kafka_connection(self):
        """Test de connexion à Kafka avec timeouts corrigés"""
        for server in self.kafka_servers:
            try:
                print(f"🔄 Test connexion Kafka: {server}")
                
                # Configuration avec timeouts corrigés
                consumer = KafkaConsumer(
                    bootstrap_servers=[server],
                    group_id='test-connection',
                    auto_offset_reset='earliest',
                    request_timeout_ms=30000,  # Plus long que session_timeout_ms
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000
                )
                
                topics = consumer.topics()
                consumer.close()
                print(f"✅ Kafka accessible sur: {server}")
                print(f"📊 Topics disponibles: {topics}")
                return server
                
            except Exception as e:
                print(f"❌ Échec connexion à {server}: {e}")
                continue
        return None
    
    def create_kafka_consumer(self, server):
        """Crée un consumer Kafka avec configuration robuste"""
        return KafkaConsumer(
            'building-sensors',
            bootstrap_servers=[server],
            group_id='smart-building-consumer',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            # Configuration des timeouts
            request_timeout_ms=30000,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000,
            fetch_max_wait_ms=500
        )
    
    def process_message(self, message):
        """Transformations des messages"""
        try:
            data = message.value
            
            print(f"📨 {data.get('sensor_type', 'unknown')}: {data.get('value', 'N/A')} "
                  f"| Bâtiment: {data.get('building_id', 'N/A')}")
            
            # FILTRES ET ALERTES
            sensor_type = data.get('sensor_type')
            value = data.get('value', 0)
            
            if sensor_type == 'temperature':
                if value > 28:
                    print(f"🔥 ALERTE TEMPÉRATURE ÉLEVÉE: {value}°C")
                elif value < 16:
                    print(f"❄️ ALERTE TEMPÉRATURE BASSE: {value}°C")
                    
            elif sensor_type == 'electricity':
                if value > 5000:
                    print(f"⚡ ALERTE CONSO ÉLECTRIQUE: {value}W")
                    
            elif sensor_type == 'water':
                if value > 100:
                    print(f"💧 ALERTE CONSO EAU: {value}L/min")
            
            # AGRÉGATION
            self.update_aggregations(data)
            
            return data
            
        except Exception as e:
            print(f"❌ Erreur traitement message: {e}")
            return None
    
    def update_aggregations(self, data):
        """Calcul de métriques en temps réel"""
        try:
            building_id = data.get('building_id', 'unknown')
            sensor_type = data.get('sensor_type', 'unknown')
            value = data.get('value', 0)
            location = data.get('location', 'unknown')
            
            # Mise à jour des données brutes
            doc_id = self.raw_data.insert_one({
                **data,
                'processed_at': datetime.now(),
                'kafka_timestamp': datetime.now()
            })
            
            # Mise à jour des agrégations
            self.aggregated_data.update_one(
                {
                    'building_id': building_id, 
                    'sensor_type': sensor_type,
                    'location': location
                },
                {
                    '$push': {
                        'recent_values': {
                            '$each': [{
                                'value': value,
                                'timestamp': datetime.now()
                            }],
                            '$slice': -20  # Garder les 20 dernières valeurs
                        }
                    },
                    '$set': {
                        'last_update': datetime.now(),
                        'location': location
                    },
                    '$inc': {'message_count': 1},
                    '$setOnInsert': {
                        'building_id': building_id,
                        'sensor_type': sensor_type
                    }
                },
                upsert=True
            )
            
            # Calcul de la moyenne
            doc = self.aggregated_data.find_one({
                'building_id': building_id, 
                'sensor_type': sensor_type
            })
            
            if doc and 'recent_values' in doc:
                values = [item['value'] for item in doc['recent_values'] if 'value' in item]
                if values:
                    avg_value = sum(values) / len(values)
                    self.aggregated_data.update_one(
                        {'_id': doc['_id']},
                        {'$set': {'average_value': avg_value}}
                    )
                    
                    print(f"📊 {sensor_type} - Moyenne: {avg_value:.2f}")
            
        except Exception as e:
            print(f"❌ Erreur lors de l'agrégation: {e}")
    
    def start_consuming(self):
        """Démarrage du consumer Kafka"""
        print("🚀 Démarrage Consumer Kafka...")
        print("🔧 Configuration détectée: Port 29095 (externe), 9092 (interne)")
        
        working_server = self.test_kafka_connection()
        if not working_server:
            print("💥 Aucun serveur Kafka accessible")
            print("\n🔧 Diagnostic:")
            print("1. Vérifiez que Kafka est démarré: docker ps")
            print("2. Testez manuellement: docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092")
            print("3. Vérifiez les logs: docker logs kafka")
            return
        
        print(f"🎯 Utilisation du serveur: {working_server}")
        
        try:
            consumer = self.create_kafka_consumer(working_server)
            print("✅ Consumer Kafka créé avec succès!")
            print("📡 En attente de messages sur le topic 'building-sensors'...")
            print("⏹️  Ctrl+C pour arrêter")
            print("-" * 60)
            
            message_count = 0
            start_time = time.time()
            
            for message in consumer:
                message_count += 1
                processed_data = self.process_message(message)
                
                if processed_data:
                    elapsed_time = time.time() - start_time
                    print(f"📈 Statistiques: {message_count} messages traités | " 
                          f"Temps écoulé: {elapsed_time:.1f}s")
                    print("-" * 60)
                
        except KeyboardInterrupt:
            print("\n🛑 Arrêt demandé par l'utilisateur")
        except Exception as e:
            print(f"💥 Erreur critique dans le consumer: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                consumer.close()
                print("🔌 Consumer Kafka fermé")
            except:
                pass
            self.client.close()
            print("🔌 Connexion MongoDB fermée")

def main():
    print("=" * 60)
    print("🏢 SMART BUILDING - KAFKA CONSUMER")
    print("🔧 Configuré pour Docker (29095)")
    print("=" * 60)
    
    consumer = SimpleKafkaConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()