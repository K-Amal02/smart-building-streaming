import pandas as pd
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import os

class BuildingDataProducer:
    def __init__(self, bootstrap_servers='localhost:29095'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.building_data = None
        
    def connect_kafka(self):
        """Connexion à Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=16384,
                linger_ms=10
            )
            print(f"✅ Connecté à Kafka sur {self.bootstrap_servers}")
            return True
        except Exception as e:
            print(f"❌ Erreur connexion Kafka: {e}")
            return False
    
    def load_sample_data(self):
        """Charge les données d'exemple depuis le dataset"""
        try:
            # Chemin vers vos données
            base_path = "../../data/raw/smart-building-system"
            buildings = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
            
            if not buildings:
                print("❌ Aucun bâtiment trouvé dans le dataset")
                return False
            
            # Charger les données du premier bâtiment comme exemple
            sample_building = buildings[0]
            building_path = os.path.join(base_path, sample_building)
            
            # Charger les données de température
            temp_file = os.path.join(building_path, "temperature.csv")
            if os.path.exists(temp_file):
                self.building_data = pd.read_csv(temp_file)
                print(f"✅ Données chargées - {len(self.building_data)} enregistrements")
                return True
            else:
                print("❌ Fichier temperature.csv non trouvé")
                return False
                
        except Exception as e:
            print(f"❌ Erreur chargement données: {e}")
            return False
    
    def generate_sensor_data(self, building_id):
        """Génère des données de capteur simulées"""
        base_temp = random.uniform(18.0, 26.0)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'building_id': building_id,
            'sensor_type': 'temperature',
            'value': round(base_temp + random.uniform(-2.0, 2.0), 2),
            'unit': 'celsius',
            'room': f"Room_{random.randint(1, 10)}",
            'floor': random.randint(1, 5)
        }
    
    def stream_historical_data(self, topic_name='building-sensors', speed_factor=10):
        """Stream les données historiques en temps accéléré"""
        if self.building_data is None:
            print("❌ Aucune donnée chargée")
            return
        
        print(f"🚀 Démarrage du streaming vers le topic '{topic_name}'...")
        print(f"📊 Vitesse: {speed_factor}x temps réel")
        
        record_count = 0
        start_time = time.time()
        
        for index, row in self.building_data.iterrows():
            # Créer un message à partir des données historiques
            message = {
                'timestamp': datetime.now().isoformat(),
                'building_id': 'building_001',
                'sensor_type': 'temperature',
                'value': float(row.iloc[1]) if len(row) > 1 else random.uniform(18.0, 26.0),
                'unit': 'celsius',
                'room': f"Room_{random.randint(1, 10)}",
                'floor': random.randint(1, 5),
                'source': 'historical'
            }
            
            # Envoyer le message à Kafka
            self.producer.send(topic_name, message)
            record_count += 1
            
            # Afficher progression
            if record_count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"📨 {record_count} messages envoyés - {elapsed:.1f}s")
            
            # Simulation du temps réel accéléré
            time.sleep(1.0 / speed_factor)
        
        # Finaliser
        self.producer.flush()
        total_time = time.time() - start_time
        print(f"🎉 Streaming terminé - {record_count} messages en {total_time:.1f}s")
    
    def stream_live_data(self, topic_name='building-sensors', duration=60):
        """Génère des données en temps réel simulé"""
        buildings = ['building_001', 'building_002', 'building_003']
        sensor_types = ['temperature', 'humidity', 'co2', 'occupancy']
        
        print(f"🚀 Démarrage génération données temps réel...")
        print(f"⏱️ Durée: {duration} secondes")
        print(f"🏢 Bâtiments: {buildings}")
        print(f"📟 Capteurs: {sensor_types}")
        
        start_time = time.time()
        record_count = 0
        
        while (time.time() - start_time) < duration:
            for building in buildings:
                for sensor in sensor_types:
                    # Générer données réalistes selon le type de capteur
                    if sensor == 'temperature':
                        value = round(random.uniform(18.0, 26.0), 2)
                        unit = 'celsius'
                    elif sensor == 'humidity':
                        value = round(random.uniform(30.0, 70.0), 2)
                        unit = 'percent'
                    elif sensor == 'co2':
                        value = random.randint(400, 1200)
                        unit = 'ppm'
                    else:  # occupancy
                        value = random.randint(0, 1)
                        unit = 'binary'
                    
                    message = {
                        'timestamp': datetime.now().isoformat(),
                        'building_id': building,
                        'sensor_type': sensor,
                        'value': value,
                        'unit': unit,
                        'room': f"Room_{random.randint(1, 10)}",
                        'floor': random.randint(1, 5),
                        'source': 'live_simulation'
                    }
                    
                    # Envoyer à Kafka
                    self.producer.send(topic_name, message)
                    record_count += 1
            
            # Afficher progression
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                print(f"📨 {record_count} messages envoyés - {elapsed:.1f}s")
            
            # Attendre 2 secondes entre chaque batch
            time.sleep(2)
        
        self.producer.flush()
        print(f"🎉 Génération terminée - {record_count} messages en {duration}s")

def main():
    # Initialisation du producteur
    producer = BuildingDataProducer(bootstrap_servers='localhost:29095')
    
    # Connexion à Kafka
    if not producer.connect_kafka():
        return
    
    # Choix du mode de streaming
    print("\n🎛️  CHOIX DU MODE DE STREAMING:")
    print("1. Données historiques accélérées")
    print("2. Données temps réel simulées")
    
    choice = input("Entrez votre choix (1 ou 2): ").strip()
    
    if choice == "1":
        # Charger les données historiques
        if producer.load_sample_data():
            # Stream les données historiques 10x plus vite
            producer.stream_historical_data(speed_factor=10)
        else:
            print("❌ Impossible de charger les données historiques")
    
    elif choice == "2":
        # Générer des données temps réel
        duration = int(input("Durée en secondes (défaut: 60): ") or "60")
        producer.stream_live_data(duration=duration)
    
    else:
        print("❌ Choix invalide")

if __name__ == "__main__":
    main()